use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::Duration;

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
use crate::db::coordinator::CoordinatorDb;
use crate::distance::{self, Distance, DistanceResults, MasksEngine};
use crate::template::Template;
use crate::utils::aws::{
    sqs_client_from_config, sqs_delete_message, sqs_dequeue, sqs_enqueue,
};

const BATCH_SIZE: usize = 20_000;
const IDLE_SLEEP_TIME: Duration = Duration::from_secs(1);

pub struct Coordinator {
    //TODO: Consider maintaining an open stream and using read_exact preceeded by a bytes length payload
    participants: Vec<String>,
    hamming_distance_threshold: f64,
    n_closest_distances: usize,
    database: Arc<CoordinatorDb>,
    masks: Arc<Mutex<Vec<Bits>>>,
    sqs_client: Arc<aws_sdk_sqs::Client>,
    config: CoordinatorConfig,
}

impl Coordinator {
    pub async fn new(config: CoordinatorConfig) -> eyre::Result<Self> {
        let database = Arc::new(CoordinatorDb::new(&config.db).await?);

        let masks = database.fetch_masks(0).await?;
        let masks = Arc::new(Mutex::new(masks));

        let sqs_client = Arc::new(sqs_client_from_config(&config.aws).await?);

        Ok(Self {
            hamming_distance_threshold: config.hamming_distance_threshold,
            n_closest_distances: config.n_closest_distances,
            participants: config.participants.0.clone(),
            database,
            masks,
            sqs_client,
            config,
        })
    }

    pub async fn spawn(self: Arc<Self>) -> eyre::Result<()> {
        tracing::info!("Starting coordinator");

        let mut tasks = FuturesUnordered::new();

        // TODO: Error handling
        tasks.push(tokio::spawn(self.clone().handle_uniqueness_check()));
        tasks.push(tokio::spawn(self.clone().handle_db_sync()));

        while let Some(result) = tasks.next().await {
            result??;
        }

        Ok(())
    }

    async fn handle_uniqueness_check(
        self: Arc<Self>,
    ) -> Result<(), eyre::Error> {
        loop {
            let messages = sqs_dequeue(
                &self.sqs_client,
                &self.config.queues.shares_queue_url,
            )
            .await?;

            for message in messages {
                let receipt_handle = message
                    .receipt_handle
                    .context("Missing receipt handle in message")?;
                let body = message.body.context("Missing message body")?;

                let template: Template = serde_json::from_str(&body)
                    .context("Failed to parse message")?;

                tracing::info!(?template, "Query received");

                self.sync_masks().await?;

                let streams =
                    self.send_query_to_participants(&template).await?;

                let mut handles = vec![];

                let (denominator_rx, denominator_handle) =
                    self.compute_denominators(template.mask);

                handles.push(denominator_handle);

                let (batch_process_shares_rx, batch_process_shares_handle) =
                    self.batch_process_participant_shares(
                        denominator_rx,
                        streams,
                    );

                handles.push(batch_process_shares_handle);

                let distance_results =
                    self.process_results(batch_process_shares_rx).await?;

                sqs_enqueue(
                    &self.sqs_client,
                    &self.config.queues.distances_queue_url,
                    &distance_results,
                )
                .await?;

                sqs_delete_message(
                    &self.sqs_client,
                    &self.config.queues.shares_queue_url,
                    receipt_handle,
                )
                .await?;

                // TODO: Make sure that all workers receive signal to stop.
                for handle in handles {
                    handle.await??;
                }
            }
        }
    }

    pub async fn send_query_to_participants(
        &self,
        query: &Template,
    ) -> eyre::Result<Vec<BufReader<TcpStream>>> {
        // Write each share to the corresponding participant

        let streams = future::try_join_all(self.participants.iter().map(
            |participant_host| async move {
                let mut stream = TcpStream::connect(participant_host).await?;
                tracing::info!(?participant_host, "Connected to participant");

                stream.write_all(bytemuck::bytes_of(query)).await?;
                tracing::info!(?query, "Query sent to participant");

                Ok::<_, eyre::Report>(BufReader::new(stream))
            },
        ))
        .await?;

        Ok(streams)
    }

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

            tracing::info!(?mask, "Computing denominators");

            for chunk in masks.chunks(BATCH_SIZE) {
                let mut result = vec![[0_u16; 31]; chunk.len()];
                engine.batch_process(&mut result, chunk);
                sender.blocking_send(result)?;
            }
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

        let batch_worker = tokio::task::spawn(async move {
            loop {
                // Collect futures of denominator and share batches
                let streams_future = future::try_join_all(
                    streams.iter_mut().map(|stream| async move {
                        let mut batch = vec![[0_u16; 31]; BATCH_SIZE];
                        let mut buffer: &mut [u8] =
                            bytemuck::cast_slice_mut(batch.as_mut_slice());

                        // We can not use read_exact here as we might get EOF before the
                        // buffer is full But we should
                        // still try to fill the entire buffer.
                        // If nothing else, this guarantees that we read batches at a
                        // [u16;31] boundary.
                        while !buffer.is_empty() {
                            tracing::info!("Reading buffer");

                            let bytes_read =
                                stream.read_buf(&mut buffer).await?;

                            tracing::info!("Buffer read");

                            if bytes_read == 0 {
                                let n_incomplete = (buffer.len()
                                        + std::mem::size_of::<[u16; 31]>() //TODO: make this a const
                                        - 1)
                                    / std::mem::size_of::<[u16; 31]>(); //TODO: make this a const
                                batch.truncate(batch.len() - n_incomplete);
                                break;
                            }
                        }

                        Ok::<_, eyre::Report>(batch)
                    }),
                );

                // Wait on all parts concurrently
                let (denom, shares) =
                    tokio::join!(denominator_rx.recv(), streams_future);

                //TODO: do we want this to be unwrap_or_default()?
                let mut denom = denom.unwrap_or_default();
                let mut shares = shares?;

                // Find the shortest prefix
                let batch_size = shares
                    .iter()
                    .map(Vec::len)
                    .fold(denom.len(), core::cmp::min);

                denom.truncate(batch_size);
                shares
                    .iter_mut()
                    .for_each(|batch| batch.truncate(batch_size));

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
        // Keep track of min distances
        let mut closest_distances =
            BinaryHeap::with_capacity(self.n_closest_distances);

        let mut max_closest_distance = 0.0;

        // Collect any ids where the distance is less than the threshold
        let mut matches = vec![];

        // Keep track of entry ids
        let mut i: usize = 0;

        loop {
            // Fetch batches of denominators and shares
            let (denom_batch, shares) =
                processed_shares_rx.recv().await.expect("channel closed");
            let batch_size = denom_batch.len();
            if batch_size == 0 {
                break;
            }

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

                if distance > self.hamming_distance_threshold {
                    if closest_distances.len() < self.n_closest_distances {
                        closest_distances
                            .push((ordered_float::OrderedFloat(distance), id));

                        max_closest_distance = closest_distances
                            .peek()
                            .expect("There should be at least one element")
                            .0
                            .into_inner();
                    } else if distance < max_closest_distance {
                        closest_distances.pop();
                        closest_distances
                            .push((ordered_float::OrderedFloat(distance), id));

                        max_closest_distance = closest_distances
                            .peek()
                            .expect("There should be at least one element")
                            .0
                            .into_inner();
                    }
                } else {
                    matches.push(id);
                }
            }

            // Update counter
            i += batch_size;
        }

        let closest_n_distances = closest_distances
            .into_iter()
            .map(|d| Distance::new(d.0.into_inner(), d.1))
            .collect::<Vec<Distance>>();

        let distance_results =
            DistanceResults::new(i, closest_n_distances, matches);

        Ok(distance_results)
    }

    async fn sync_masks(&self) -> eyre::Result<()> {
        let mut masks = self.masks.lock().await;
        let next_mask_number = masks.len();

        let new_masks = self.database.fetch_masks(next_mask_number).await?;

        masks.extend(new_masks);

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
                continue;
            }

            for message in messages {
                let body = message.body.context("Missing message body")?;
                let receipt_handle = message
                    .receipt_handle
                    .context("Missing receipt handle in message")?;

                let items: Vec<DbSyncPayload> = serde_json::from_str(&body)?;
                let masks: Vec<_> = items
                    .into_iter()
                    .map(|item| (item.id, item.mask))
                    .collect();

                self.database.insert_masks(&masks).await?;

                sqs_delete_message(
                    &self.sqs_client,
                    &self.config.queues.db_sync_queue_url,
                    receipt_handle,
                )
                .await?;
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbSyncPayload {
    pub id: u64,
    pub mask: Bits,
}
