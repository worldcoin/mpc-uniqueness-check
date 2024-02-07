use std::sync::Arc;

use distance::Template;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, Mutex};
use tracing::instrument;

use crate::config::ParticipantConfig;
use crate::db::participant::ParticipantDb;
use crate::distance::{self, DistanceEngine, EncodedBits};

pub struct Participant {
    listener: tokio::net::TcpListener,
    batch_size: usize,
    database: Arc<ParticipantDb>,
    shares: Arc<Mutex<Vec<EncodedBits>>>,
}

impl Participant {
    pub async fn new(config: ParticipantConfig) -> eyre::Result<Self> {
        tracing::info!("Initializing participant");

        let database = ParticipantDb::new(&config.db).await?;

        tracing::info!("Fetching shares from database");
        let shares = database.fetch_shares(0).await?;
        let shares = Arc::new(Mutex::new(shares));

        let database = Arc::new(database);

        Ok(Self {
            listener: tokio::net::TcpListener::bind(config.socket_addr).await?,
            batch_size: config.batch_size,
            database,
            shares,
        })
    }

    pub async fn spawn(&self) -> eyre::Result<()> {
        tracing::info!("Spawning participant");

        let batch_size = self.batch_size;

        loop {
            let mut stream =
                tokio::io::BufWriter::new(self.listener.accept().await?.0);

            // We could do this and reading from the stream simultaneously
            self.sync_shares().await?;

            tracing::info!("Incoming connection accepted");
            let mut template = Template::default();
            stream
                .read_exact(bytemuck::bytes_of_mut(&mut template))
                .await?;

            //TODO: add id comm
            tracing::info!("Received query");
            let shares_ref = self.shares.clone();
            // Process in worker thread
            let (sender, mut receiver) = mpsc::channel(4);

            let worker = tokio::task::spawn_blocking(move || {
                calculate_share_distances(
                    shares_ref, template, batch_size, sender,
                )
            });

            while let Some(buffer) = receiver.recv().await {
                tracing::info!(batch_size = ?buffer.len(), "Sending batch result to coordinator");
                stream.write_all(&buffer).await?;
            }
            worker.await??;
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn sync_shares(&self) -> eyre::Result<()> {
        let mut shares = self.shares.lock().await;

        let next_share_number = shares.len();
        let new_shares = self.database.fetch_shares(next_share_number).await?;

        shares.extend(new_shares);

        Ok(())
    }
}

#[instrument(skip(shares, template, sender))]
fn calculate_share_distances(
    shares: Arc<Mutex<Vec<EncodedBits>>>,
    template: Template,
    batch_size: usize,
    sender: mpsc::Sender<Vec<u8>>,
) -> eyre::Result<()> {
    let shares = shares.blocking_lock();
    let patterns: &[EncodedBits] = bytemuck::cast_slice(&shares);
    let engine = DistanceEngine::new(&distance::encode(&template));

    for chunk in patterns.chunks(batch_size) {
        let mut result = vec![
            0_u8;
            chunk.len()
                * std::mem::size_of::<[u16; 31]>() //TODO: make this a const
        ];

        engine.batch_process(bytemuck::cast_slice_mut(&mut result), chunk);

        sender.blocking_send(result)?;
    }

    Ok(())
}
