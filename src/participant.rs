use std::sync::Arc;
use std::time::Duration;

use distance::Template;
use eyre::ContextCompat;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use telemetry_batteries::opentelemetry::trace::{
    SpanContext, SpanId, TraceFlags, TraceId, TraceState,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tracing::instrument;

use crate::config::ParticipantConfig;
use crate::db::Db;
use crate::distance::{self, DistanceEngine, EncodedBits};
use crate::utils::aws::{
    sqs_client_from_config, sqs_delete_message, sqs_dequeue,
};

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

        tracing::info!("Fetching shares from database");
        let shares = database.fetch_shares(0).await?;
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

        let mut tasks = FuturesUnordered::new();

        tasks.push(tokio::spawn(self.clone().handle_uniqueness_check()));
        tasks.push(tokio::spawn(self.clone().handle_db_sync()));

        while let Some(result) = tasks.next().await {
            result??;
        }

        Ok(())
    }

    async fn handle_uniqueness_check(self: Arc<Self>) -> eyre::Result<()> {
        loop {
            let mut stream =
                tokio::io::BufWriter::new(self.listener.accept().await?.0);

            // Process the trace and span ids to correlate traces between services
            self.handle_traces_payload(&mut stream).await?;

            tracing::info!("Incoming connection accepted");

            // Process the query
            self.uniqueness_check(stream).await?;
        }
    }

    async fn handle_traces_payload(
        &self,
        stream: &mut BufWriter<TcpStream>,
    ) -> eyre::Result<()> {
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

        while let Some(buffer) = receiver.recv().await {
            tracing::info!(num_bytes = ?buffer.len(), "Sending batch result to coordinator");
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
            self.db_sync().await?;
        }
    }

    #[tracing::instrument(skip(self))]
    async fn db_sync(&self) -> eyre::Result<()> {
        let messages = sqs_dequeue(
            &self.sqs_client,
            &self.config.queues.db_sync_queue_url,
        )
        .await?;

        if messages.is_empty() {
            tokio::time::sleep(IDLE_SLEEP_TIME).await;
            return Ok(());
        }

        for message in messages {
            let body = message.body.context("Missing message body")?;
            let receipt_handle = message
                .receipt_handle
                .context("Missing receipt handle in message")?;

            let items = if let Ok(items) =
                serde_json::from_str::<Vec<DbSyncPayload>>(&body)
            {
                items
            } else {
                tracing::error!(
                    ?receipt_handle,
                    "Failed to parse message body"
                );
                continue;
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
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn sync_shares(&self) -> eyre::Result<()> {
        let mut shares = self.shares.lock().await;

        let next_share_number = shares.len();
        let new_shares = self.database.fetch_shares(next_share_number).await?;

        shares.extend(new_shares);

        tracing::info!(num_shares = shares.len(), "Shares synchronized");

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
