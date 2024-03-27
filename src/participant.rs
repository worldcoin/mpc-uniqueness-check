use std::io;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_sqs::types::Message;
use distance::Template;
use eyre::ContextCompat;
use futures::stream::FuturesUnordered;
use opentelemetry::trace::{
    SpanContext, SpanId, TraceFlags, TraceId, TraceState,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tracing::instrument;

use crate::config::ParticipantConfig;
use crate::db::Db;
use crate::distance::{self, encode, DistanceEngine, EncodedBits};
use crate::item_kind::Shares;
use crate::utils;
use crate::utils::aws::{
    sqs_client_from_config, sqs_delete_message, sqs_dequeue,
};
use crate::utils::tasks::finalize_futures_unordered;

const IDLE_SLEEP_TIME: Duration = Duration::from_secs(1);

pub struct Participant {
    listener: tokio::net::TcpListener,
    batch_size: usize,
    database: Arc<Db>,
    shares: Arc<Mutex<Vec<EncodedBits>>>,
    sqs_client: aws_sdk_sqs::Client,
    config: ParticipantConfig,
}

impl Participant {
    pub async fn new(config: ParticipantConfig) -> eyre::Result<Self> {
        tracing::info!("Initializing participant");

        let database = Db::new(&config.db).await?;

        let mut shares = vec![];

        if let Some(snapshot_dir) = config.snapshot_dir.as_deref() {
            tracing::info!("Reading shares from snapshot");

            let snapshot_files =
                crate::snapshot::open_dir_files(snapshot_dir).await?;

            for snapshot in snapshot_files {
                let shares_snapshot =
                    crate::snapshot::read_parquet::<Shares, _>(snapshot)
                        .await?;

                shares.extend(
                    shares_snapshot.into_iter().map(|(_id, share)| share),
                );
            }
        }

        tracing::info!("Fetching shares from database");
        shares.extend(database.fetch_shares(shares.len()).await?);
        let shares = Arc::new(Mutex::new(shares));

        let database = Arc::new(database);

        let sqs_client = sqs_client_from_config(&config.aws).await?;

        Ok(Self {
            listener: tokio::net::TcpListener::bind(config.socket_addr).await?,
            batch_size: config.batch_size,
            database,
            shares,
            sqs_client,
            config,
        })
    }

    pub async fn spawn(self: Arc<Self>) -> eyre::Result<()> {
        tracing::info!("Spawning participant");

        let tasks = FuturesUnordered::new();

        tasks.push(tokio::spawn(self.clone().handle_uniqueness_checks()));
        tasks.push(tokio::spawn(self.clone().handle_db_sync()));

        finalize_futures_unordered(tasks).await?;

        Ok(())
    }

    async fn handle_uniqueness_checks(self: Arc<Self>) -> eyre::Result<()> {
        loop {
            let stream = match self.listener.accept().await {
                Ok((stream, addr)) => {
                    tracing::debug!(?addr, "Accepting connection");

                    stream
                }
                Err(error) => {
                    tracing::error!(
                        ?error,
                        "Failed to accept incoming connection"
                    );
                    continue;
                }
            };

            if let Err(error) = self.handle_uniqueness_check(stream).await {
                tracing::error!(?error, "Uniqueness check failed");
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn handle_uniqueness_check(
        &self,
        stream: TcpStream,
    ) -> eyre::Result<()> {
        let mut stream = tokio::io::BufWriter::new(stream);

        // Process the trace and span ids to correlate traces between services
        self.handle_traces_payload(&mut stream).await?;

        // Process the query
        self.uniqueness_check(stream).await?;

        Ok(())
    }

    async fn handle_traces_payload(
        &self,
        stream: &mut BufWriter<TcpStream>,
    ) -> io::Result<()> {
        // Read the span ID from the stream and add to the current span
        let mut trace_id_bytes = [0_u8; 16];
        let mut span_id_bytes = [0_u8; 8];

        stream
            .read_exact(bytemuck::bytes_of_mut(&mut trace_id_bytes))
            .await?;

        stream
            .read_exact(bytemuck::bytes_of_mut(&mut span_id_bytes))
            .await?;

        // Create span parent context
        let parent_ctx = SpanContext::new(
            TraceId::from_bytes(trace_id_bytes),
            SpanId::from_bytes(span_id_bytes),
            TraceFlags::default(),
            true,
            TraceState::default(),
        );

        // Set the parent context for the current span to correlate traces between services
        telemetry_batteries::tracing::trace_from_ctx(parent_ctx);

        Ok(())
    }

    #[tracing::instrument(skip(self, stream))]
    async fn uniqueness_check(
        &self,
        mut stream: BufWriter<TcpStream>,
    ) -> eyre::Result<()> {
        // We could do this and reading from the stream simultaneously
        self.sync_shares().await?;

        let mut template = Template::default();
        stream
            .read_exact(bytemuck::bytes_of_mut(&mut template))
            .await?;

        tracing::info!("Received query");
        let shares_ref = self.shares.clone();

        let batch_size = self.batch_size;

        // Process in worker thread
        let (sender, mut receiver) = mpsc::channel(4);
        let worker = tokio::task::spawn_blocking(move || {
            calculate_share_distances(shares_ref, template, batch_size, sender)
        });

        tracing::info!("Sending batch result to coordinator");
        while let Some(buffer) = receiver.recv().await {
            let buffer_len = buffer.len() as u64;
            stream.write_u64(buffer_len).await?;

            stream.write_all(&buffer).await?;
            stream.flush().await?;
        }
        worker.await??;

        Ok(())
    }

    async fn handle_db_sync(self: Arc<Self>) -> eyre::Result<()> {
        loop {
            // Dequeue the max number of messages possible from the queue
            let messages = match sqs_dequeue(
                &self.sqs_client,
                &self.config.queues.db_sync_queue_url,
                Some(10),
            )
            .await
            {
                Ok(messages) => messages,
                Err(error) => {
                    tracing::error!(
                        ?error,
                        "Failed to dequeue db-sync messages"
                    );
                    continue;
                }
            };

            if messages.is_empty() {
                tokio::time::sleep(IDLE_SLEEP_TIME).await;
            }

            for message in messages {
                if let Err(error) = self.db_sync(message).await {
                    tracing::error!(?error, "Failed to handle db-sync message");
                }
            }
        }
    }

    #[tracing::instrument(skip(self, message))]
    async fn db_sync(&self, message: Message) -> eyre::Result<()> {
        let receipt_handle = message
            .receipt_handle
            .context("Missing receipt handle in message")?;

        if let Some(message_attributes) = &message.message_attributes {
            utils::aws::trace_from_message_attributes(
                message_attributes,
                &receipt_handle,
            )?;
        } else {
            tracing::warn!(
                ?receipt_handle,
                "SQS message missing message attributes"
            );
        }

        let body = message.body.context("Missing message body")?;

        let items = if let Ok(items) =
            serde_json::from_str::<Vec<DbSyncPayload>>(&body)
        {
            items
        } else {
            tracing::error!(?receipt_handle, "Failed to parse message body");

            sqs_delete_message(
                &self.sqs_client,
                &self.config.queues.db_sync_queue_url,
                receipt_handle,
            )
            .await?;

            return Ok(());
        };

        let shares: Vec<_> = items
            .into_iter()
            .map(|item| (item.id, item.share))
            .collect();

        tracing::info!(
            num_new_shares = shares.len(),
            "Inserting shares into database"
        );
        self.database.insert_shares(&shares).await?;

        sqs_delete_message(
            &self.sqs_client,
            &self.config.queues.db_sync_queue_url,
            receipt_handle,
        )
        .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn sync_shares(&self) -> eyre::Result<()> {
        let mut shares = self.shares.lock().await;

        let next_share_number = shares.len();
        let new_shares = self.database.fetch_shares(next_share_number).await?;

        shares.extend(new_shares);

        tracing::info!(num_shares = shares.len(), "Shares synchronized");
        metrics::gauge!("participant.latest_serial_id", shares.len() as f64);

        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DbSyncPayload {
    pub id: u64,
    pub share: EncodedBits,
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

    let template_rotations = template.rotations().map(|r| encode(&r));
    let engine = DistanceEngine::new(template_rotations);

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
