use std::sync::Arc;
use std::time::Duration;

use aws_sdk_sqs::types::Message;
use eyre::{Context, ContextCompat};
use futures::stream::FuturesUnordered;
use futures::{future, StreamExt};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

use crate::bits::Bits;
use crate::config::CoordinatorConfig;
use crate::db::Db;
use crate::distance::{self, Distance, DistanceResults, MasksEngine};
use crate::template::Template;
use crate::utils;
use crate::utils::aws::{
    sqs_client_from_config, sqs_delete_message, sqs_dequeue, sqs_enqueue,
};
use crate::utils::templating::resolve_template;

const BATCH_SIZE: usize = 20_000;
const BATCH_ELEMENT_SIZE: usize = std::mem::size_of::<[u16; 31]>();
const IDLE_SLEEP_TIME: Duration = Duration::from_secs(1);
const RESPONSE_MESSAGE_GROUP_ID: &str = "mpc-uniqueness-check-response";

pub struct Coordinator {
    participants: Vec<String>,
    hamming_distance_threshold: f64,
    database: Arc<Db>,
    masks: Arc<Mutex<Vec<Bits>>>,
    sqs_client: Arc<aws_sdk_sqs::Client>,
    config: CoordinatorConfig,
}

impl Coordinator {
    pub async fn new(config: CoordinatorConfig) -> eyre::Result<Self> {
        tracing::info!("Initializing coordinator");
        let database = Arc::new(Db::new(&config.db).await?);

        tracing::info!("Fetching masks from database");
        let masks = database.fetch_masks(0).await?;
        let masks = Arc::new(Mutex::new(masks));

        tracing::info!("Initializing SQS client");
        let sqs_client = Arc::new(sqs_client_from_config(&config.aws).await?);

        let participants = config
            .participants
            .0
            .iter()
            .map(String::as_str)
            .map(resolve_template)
            .collect::<Result<_, _>>()?;

        Ok(Self {
            hamming_distance_threshold: config.hamming_distance_threshold,
            participants,
            database,
            masks,
            sqs_client,
            config,
        })
    }

    pub async fn spawn(self: Arc<Self>) -> eyre::Result<()> {
        tracing::info!("Spawning coordinator");
        let mut tasks = FuturesUnordered::new();

        // TODO: Error handling
        tracing::info!("Spawning uniqueness check");
        tasks.push(tokio::spawn(self.clone().handle_uniqueness_checks()));

        tracing::info!("Spawning db sync");
        tasks.push(tokio::spawn(self.clone().handle_db_sync()));

        while let Some(result) = tasks.next().await {
            result??;
        }

        Ok(())
    }

    async fn handle_uniqueness_checks(
        self: Arc<Self>,
    ) -> Result<(), eyre::Error> {
        loop {
            let messages = sqs_dequeue(
                &self.sqs_client,
                &self.config.queues.queries_queue_url,
            )
            .await?;

            for message in messages {
                self.handle_uniqueness_check(message).await?;
            }
        }
    }

    #[tracing::instrument(skip(self, message))]
    pub async fn handle_uniqueness_check(
        &self,
        message: Message,
    ) -> eyre::Result<()> {
        tracing::debug!(?message, "Handling message");

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

        if let Ok(UniquenessCheckRequest {
            plain_code,
            signup_id,
        }) = serde_json::from_str::<UniquenessCheckRequest>(&body)
        {
            self.uniqueness_check(receipt_handle, plain_code, signup_id)
                .await?;
        } else {
            tracing::error!(
                ?receipt_handle,
                "Failed to parse template from message"
            );

            sqs_delete_message(
                &self.sqs_client,
                &self.config.queues.queries_queue_url,
                receipt_handle,
            )
            .await?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, template))]
    pub async fn uniqueness_check(
        &self,
        receipt_handle: String,
        template: Template,
        signup_id: String,
    ) -> Result<(), eyre::Error> {
        tracing::info!("Processing message");
        self.sync_masks().await?;

        tracing::info!("Sending query to participants");
        let streams = self.send_query_to_participants(&template).await?;
        let mut tasks = FuturesUnordered::new();

        tracing::info!("Computing denominators");
        let (denominator_rx, denominator_handle) =
            self.compute_denominators(template.mask);
        tasks.push(denominator_handle);

        tracing::info!("Processing participant shares");
        let (batch_process_shares_rx, batch_process_shares_handle) =
            self.batch_process_participant_shares(denominator_rx, streams);
        tasks.push(batch_process_shares_handle);

        tracing::info!("Processing results");
        let distance_results =
            self.process_results(batch_process_shares_rx).await?;

        let result = UniquenessCheckResult {
            serial_id: distance_results.serial_id,
            matches: distance_results.matches,
            signup_id,
        };
        tracing::info!(?result, "MPC results processed");

        sqs_enqueue(
            &self.sqs_client,
            &self.config.queues.distances_queue_url,
            RESPONSE_MESSAGE_GROUP_ID,
            &result,
        )
        .await?;

        sqs_delete_message(
            &self.sqs_client,
            &self.config.queues.queries_queue_url,
            receipt_handle,
        )
        .await?;

        while let Some(result) = tasks.next().await {
            result??;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, query))]
    pub async fn send_query_to_participants(
        &self,
        query: &Template,
    ) -> eyre::Result<Vec<BufReader<TcpStream>>> {
        let (trace_id, span_id) =
            telemetry_batteries::tracing::extract_span_ids();

        // Write each share to the corresponding participant
        let streams =
            future::try_join_all(self.participants.iter().enumerate().map(
                |(i, participant_host)| async move {
                    tracing::info!(
                        participant = i,
                        ?participant_host,
                        "Connecting to participant"
                    );
                    let mut stream = TcpStream::connect(participant_host)
                        .await
                        .context("Connecting to participant")?;

                    // Send the trace and span IDs
                    stream.write_all(&trace_id.to_bytes()).await?;
                    stream.write_all(&span_id.to_bytes()).await?;

                    // Send the query
                    stream.write_all(bytemuck::bytes_of(query)).await?;

                    tracing::info!(
                        participant = i,
                        ?participant_host,
                        "Query sent to participant"
                    );

                    Ok::<_, eyre::Report>(BufReader::new(stream))
                },
            ))
            .await?;

        Ok(streams)
    }

    #[tracing::instrument(skip(self))]
    pub fn compute_denominators(
        &self,
        mask: Bits,
    ) -> (Receiver<Vec<[u16; 31]>>, JoinHandle<eyre::Result<()>>) {
        let (sender, denom_receiver) = tokio::sync::mpsc::channel(4);
        let masks = self.masks.clone();

        let denominator_handle = tokio::task::spawn_blocking(move || {
            let masks = masks.blocking_lock();
            let masks: &[Bits] = bytemuck::cast_slice(&masks);
            let engine = MasksEngine::new(&mask);
            let total_masks: usize = masks.len();

            tracing::info!("Processing denominators");

            for (i, chunk) in masks.chunks(BATCH_SIZE).enumerate() {
                let mut result = vec![[0_u16; 31]; chunk.len()];
                engine.batch_process(&mut result, chunk);

                tracing::debug!(
                    masks_processed = (i + 1) * BATCH_SIZE,
                    ?total_masks,
                    "Denominator batch processed"
                );
                sender.blocking_send(result)?;
            }

            tracing::info!("Denominators processed");

            Ok(())
        });

        (denom_receiver, denominator_handle)
    }

    pub fn batch_process_participant_shares(
        &self,
        mut denominator_rx: Receiver<Vec<[u16; 31]>>,
        mut streams: Vec<BufReader<TcpStream>>,
    ) -> (
        Receiver<(Vec<[u16; 31]>, Vec<Vec<[u16; 31]>>)>,
        JoinHandle<eyre::Result<()>>,
    ) {
        // Collect batches of shares
        let (processed_shares_tx, processed_shares_rx) = mpsc::channel(4);

        tracing::info!("Spawning batch worker");
        let batch_worker = tokio::task::spawn(async move {
            loop {
                // Collect futures of denominator and share batches
                let streams_future =
                    future::try_join_all(streams.iter_mut().enumerate().map(
                        |(i, stream)| async move {


                            let buffer_size = match stream.read_u64().await{
                                Ok(buffer_size) => buffer_size as usize,
                                Err(e) if e.kind() == tokio::io::ErrorKind::UnexpectedEof => {
                                    tracing::info!("Connection closed by participant");
                                    return Ok(vec![]);
                                }
                                Err(e) => Err(e)?
                            };
                            if buffer_size % BATCH_ELEMENT_SIZE != 0 {
                                return Err(eyre::eyre!(
                                    "Buffer size is not a multiple of the batch part size"
                                ));
                            }

                            // Calculate the batch size
                            let batch_size =
                                buffer_size / BATCH_ELEMENT_SIZE;
                            let mut batch = vec![[0u16; 31]; batch_size];
                            let buffer =
                                bytemuck::cast_slice_mut(&mut batch);

                            // Read in the batch results
                            stream.read_exact(buffer).await?;

                            tracing::info!(
                                participant = i,
                                batch_size = batch.len(),
                                "Shares batch received"
                            );

                            Ok::<_, eyre::Report>(batch)
                        },
                    ));

                // Wait on all parts concurrently
                let (denom, shares) =
                    tokio::join!(denominator_rx.recv(), streams_future);

                //TODO: do we want this to be unwrap_or_default()?
                let mut denom = denom.unwrap_or_default();
                let mut shares = shares?;

                //NOTE: we need to make sure that the batch sizes are the same, otherwise, it will truncate to the smaller size every time while still in the middle of batches
                // Find the shortest prefix
                let batch_size = shares
                    .iter()
                    .map(Vec::len)
                    .fold(denom.len(), core::cmp::min);

                denom.truncate(batch_size);
                shares
                    .iter_mut()
                    .for_each(|batch| batch.truncate(batch_size));

                tracing::info!(?batch_size, "Batch processed");

                // Send batches
                processed_shares_tx.send((denom, shares)).await?;
                if batch_size == 0 {
                    break;
                }
            }
            Ok(())
        });

        (processed_shares_rx, batch_worker)
    }

    // Returns the latest id shared across the coordinator and participants, the closest n distances and ids of all matches
    pub async fn process_results(
        &self,
        mut processed_shares_rx: Receiver<(
            Vec<[u16; 31]>,
            Vec<Vec<[u16; 31]>>,
        )>,
    ) -> eyre::Result<DistanceResults> {
        // Collect any ids where the distance is less than the threshold
        let mut matches = vec![];

        // Keep track of entry ids
        let mut i: usize = 0;

        while let Some((denom_batch, shares)) = processed_shares_rx.recv().await
        {
            let batch_size = denom_batch.len();
            if batch_size == 0 {
                tracing::warn!("Batch size is empty");
                break;
            }

            tracing::info!("Computing distances");
            // Compute batch of distances in Rayon
            let worker = tokio::task::spawn_blocking(move || {
                (0..batch_size)
                    .into_par_iter()
                    .map(|i| {
                        let denominator = denom_batch[i];
                        let mut numerator = [0_u16; 31];
                        for share in shares.iter() {
                            let share = share[i];
                            for (n, &s) in
                                numerator.iter_mut().zip(share.iter())
                            {
                                *n = n.wrapping_add(s);
                            }
                        }

                        distance::decode_distance(&numerator, &denominator)
                    })
                    .collect::<Vec<_>>()
            });

            let distances = worker.await?;

            for (j, distance) in distances.into_iter().enumerate() {
                let id = j + i;

                if distance < self.hamming_distance_threshold {
                    matches.push(Distance::new(id as u64, distance));
                }
            }

            // Update counter
            i += batch_size;
        }

        if !matches.is_empty() {
            tracing::info!(?matches, "Matches found");
        }

        let distance_results = DistanceResults::new(i as u64, matches);

        Ok(distance_results)
    }

    #[tracing::instrument(skip(self))]
    async fn sync_masks(&self) -> eyre::Result<()> {
        let mut masks = self.masks.lock().await;
        let next_mask_number = masks.len();

        tracing::info!(?next_mask_number, "Synchronizing masks");

        let new_masks = self.database.fetch_masks(next_mask_number).await?;

        masks.extend(new_masks);

        tracing::info!(num_masks = masks.len(), "New masks synchronized");

        Ok(())
    }

    async fn handle_db_sync(self: Arc<Self>) -> eyre::Result<()> {
        loop {
            let messages = sqs_dequeue(
                &self.sqs_client,
                &self.config.queues.db_sync_queue_url,
            )
            .await?;

            if messages.is_empty() {
                tokio::time::sleep(IDLE_SLEEP_TIME).await;
            }

            for message in messages {
                self.db_sync(message).await?;
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

        let masks: Vec<_> =
            items.into_iter().map(|item| (item.id, item.mask)).collect();

        tracing::info!(
            num_new_masks = masks.len(),
            "Inserting masks into database"
        );

        self.database.insert_masks(&masks).await?;

        sqs_delete_message(
            &self.sqs_client,
            &self.config.queues.db_sync_queue_url,
            receipt_handle,
        )
        .await?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbSyncPayload {
    pub id: u64,
    pub mask: Bits,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UniquenessCheckRequest {
    pub plain_code: Template,
    pub signup_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UniquenessCheckResult {
    pub serial_id: u64,
    pub matches: Vec<Distance>,
    pub signup_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_serialization() {
        let input = UniquenessCheckRequest {
            plain_code: Template::default(),
            signup_id: "signup_id".to_string(),
        };

        const EXPECTED: &str = indoc::indoc! {r#"
            {
              "plain_code": {
                "code": "/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////w==",
                "mask": "/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////w=="
              },
              "signup_id": "signup_id"
            }
        "#};

        let s = serde_json::to_string_pretty(&input).unwrap();

        similar_asserts::assert_eq!(s.trim(), EXPECTED.trim());
    }

    #[test]
    fn result_serialization() {
        let output = UniquenessCheckResult {
            serial_id: 1,
            matches: vec![Distance::new(0, 0.5), Distance::new(1, 0.2)],
            signup_id: "signup_id".to_string(),
        };

        const EXPECTED: &str = indoc::indoc! {r#"
            {
              "serial_id": 1,
              "matches": [
                {
                  "distance": 0.5,
                  "serial_id": 0
                },
                {
                  "distance": 0.2,
                  "serial_id": 1
                }
              ],
              "signup_id": "signup_id"
            }
        "#};

        let serialized = serde_json::to_string_pretty(&output).unwrap();

        similar_asserts::assert_eq!(serialized.trim(), EXPECTED.trim());

        let deserialized: UniquenessCheckResult =
            serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized, output);
    }

    #[test]
    fn result_serialization_zero_serial_id() {
        let output = UniquenessCheckResult {
            serial_id: 0,
            matches: vec![Distance::new(0, 0.5), Distance::new(1, 0.2)],
            signup_id: "signup_id".to_string(),
        };

        const EXPECTED: &str = indoc::indoc! {r#"
            {
              "serial_id": 0,
              "matches": [
                {
                  "distance": 0.5,
                  "serial_id": 0
                },
                {
                  "distance": 0.2,
                  "serial_id": 1
                }
              ],
              "signup_id": "signup_id"
            }
        "#};

        let serialized = serde_json::to_string_pretty(&output).unwrap();

        similar_asserts::assert_eq!(serialized.trim(), EXPECTED.trim());

        let deserialized: UniquenessCheckResult =
            serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized, output);
    }
}
