use std::net::SocketAddr;
use std::sync::Arc;

use distance::Template;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::distance::{self, DistanceEngine, EncodedBits};

pub struct Participant {
    listener: tokio::net::TcpListener,
}

impl Participant {
    pub async fn new(socket_address: SocketAddr) -> eyre::Result<Self> {
        Ok(Self {
            listener: tokio::net::TcpListener::bind(socket_address).await?,
        })
    }

    pub async fn spawn(&self) -> eyre::Result<()> {
        let (stream, _) = self.listener.accept().await?;
        let mut stream = tokio::io::BufWriter::new(stream);

        let shares = Arc::new(self.initialize_shares().await?);

        loop {
            // TODO: Sync from database

            let mut template = Template::default();
            stream
                .read_exact(bytemuck::bytes_of_mut(&mut template))
                .await?;

            let shares_ref = shares.clone();
            // Process in worker thread
            let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
            let worker = tokio::task::spawn_blocking(move || {
                let patterns: &[EncodedBits] =
                    bytemuck::cast_slice(&shares_ref);
                let engine = DistanceEngine::new(&distance::encode(&template));

                //TODO: make batch size configurable
                for chunk in patterns.chunks(20_000) {
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
