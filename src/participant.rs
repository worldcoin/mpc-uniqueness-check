use std::sync::Arc;

use distance::Template;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::ParticipantConfig;
use crate::distance::{self, DistanceEngine, EncodedBits};

pub struct Participant {
    listener: tokio::net::TcpListener,
    batch_size: usize,
}

impl Participant {
    pub async fn new(config: &ParticipantConfig) -> eyre::Result<Self> {
        Ok(Self {
            listener: tokio::net::TcpListener::bind(config.socket_addr).await?,
            batch_size: config.batch_size,
        })
    }

    pub async fn spawn(&self) -> eyre::Result<()> {
        tracing::info!("Starting participant");

        let shares = Arc::new(self.initialize_shares().await?);
        let batch_size = self.batch_size;

        loop {
            // TODO: Sync from database

            //TODO:NOTE: this drops at the end of the loop, closing the stream
            let mut stream =
                tokio::io::BufWriter::new(self.listener.accept().await?.0);

            tracing::info!("Incoming connection accepted");

            let mut template = Template::default();
            stream
                .read_exact(bytemuck::bytes_of_mut(&mut template))
                .await?;

            tracing::info!(?template, "Received template");

            let shares_ref = shares.clone();
            // Process in worker thread
            let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
            let worker = tokio::task::spawn_blocking(move || {
                let patterns: &[EncodedBits] =
                    bytemuck::cast_slice(&shares_ref);
                let engine = DistanceEngine::new(&distance::encode(&template));

                for chunk in patterns.chunks(batch_size) {
                    let mut result = vec![
                        0_u8;
                        chunk.len()
                            * std::mem::size_of::<[u16; 31]>() //TODO: make this a const
                    ];

                    engine.batch_process(
                        bytemuck::cast_slice_mut(&mut result),
                        chunk,
                    );

                    sender.blocking_send(result)?;
                }
                Ok::<_, eyre::Report>(())
            });

            while let Some(buffer) = receiver.recv().await {
                stream.write_all(&buffer).await?;
            }
            worker.await??;
        }
    }

    //TODO: init shares from the db
    pub async fn initialize_shares(&self) -> eyre::Result<Vec<EncodedBits>> {
        Ok(vec![])
    }
}
