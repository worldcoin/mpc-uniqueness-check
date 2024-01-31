use std::collections::BinaryHeap;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::future;

use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

use crate::bits::Bits;
use crate::distance::{self, Distance, DistanceResults, MasksEngine};
use crate::template::Template;

const BATCH_SIZE: usize = 20_000; //TODO: make this configurable

pub struct Coordinator {
    aws_client: aws_sdk_sqs::Client,
    shares_queue_url: String,
    distances_queue_url: String,
    participants: Arc<Mutex<Vec<BufReader<TcpStream>>>>,
    hamming_distance_threshold: f64,
    n_closest_distances: usize,
}

impl Coordinator {
    pub async fn new(
        participants: Vec<SocketAddr>,
        shares_queue_url: &str,
        distances_queue_url: &str,
        hamming_distance_threshold: f64,
        n_closest_distances: usize,
        //TODO: Update error handling
    ) -> eyre::Result<Self> {
        let aws_config =
            aws_config::load_defaults(aws_config::BehaviorVersion::latest())
                .await;

        let aws_client = aws_sdk_sqs::Client::new(&aws_config);

        let mut streams = vec![];
        for participant in participants {
            let stream = BufReader::new(TcpStream::connect(participant).await?);

            streams.push(stream);
        }

        Ok(Self {
            aws_client,
            shares_queue_url: shares_queue_url.to_string(),
            distances_queue_url: distances_queue_url.to_string(),
            participants: Arc::new(Mutex::new(streams)),
            n_closest_distances,
            hamming_distance_threshold,
        })
    }

    //TODO: update error handling
    pub async fn spawn(mut self) -> eyre::Result<()> {
        let masks: Arc<Vec<Bits>> = Arc::new(self.initialize_masks());

        loop {
            if let Some(messages) = self.dequeue_queries().await? {
                for message in messages {
                    let template = serde_json::from_str::<Template>(
                        //TODO: handle this error
                        &message.body.expect("No body in message"),
                    )?;

                    self.send_query_to_participants(&template).await?;

                    let mut handles = vec![];

                    let (denominator_rx, denominator_handle) =
                        self.compute_denominators(masks.clone(), template.mask);

                    handles.push(denominator_handle);

                    //TODO: process_participant shares / collect batches of shares and denoms
                    let (batch_process_shares_rx, batch_process_shares_handle) =
                        self.batch_process_participant_shares(denominator_rx);

                    handles.push(batch_process_shares_handle);

                    let distance_results =
                        self.process_results(batch_process_shares_rx).await?;

                    self.enqueue_distance_results(distance_results).await?;

                    // TODO: Make sure that all workers receive signal to stop.
                    for handle in handles {
                        handle.await??;
                    }
                }
            }

            //TODO: decide how long to sleep
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn send_query_to_participants(
        &mut self,
        query: &Template,
    ) -> eyre::Result<()> {
        // Write each share to the corresponding participant
        future::try_join_all(self.participants.lock().await.iter_mut().map(
            |stream| async move {
                // Send query
                stream.write_all(bytemuck::bytes_of(query)).await
            },
        ))
        .await?;
        Ok(())
    }

    pub fn compute_denominators(
        &self,
        masks: Arc<Vec<Bits>>,
        mask: Bits,
    ) -> (Receiver<Vec<[u16; 31]>>, JoinHandle<eyre::Result<()>>) {
        let (sender, denom_receiver) = tokio::sync::mpsc::channel(4);
        let denominator_handle = tokio::task::spawn_blocking(move || {
            let masks: &[Bits] = bytemuck::cast_slice(&masks);
            let engine = MasksEngine::new(&mask);
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
        &mut self,
        mut denominator_rx: Receiver<Vec<[u16; 31]>>,
    ) -> (
        Receiver<(Vec<[u16; 31]>, Vec<Vec<[u16; 31]>>)>,
        JoinHandle<eyre::Result<()>>,
    ) {
        // Collect batches of shares
        let (processed_shares_tx, processed_shares_rx) = mpsc::channel(4);

        let participants = self.participants.clone();

        let batch_worker = tokio::task::spawn(async move {
            loop {
                let mut participants = participants.lock().await;
                // Collect futures of denominator and share batches
                let streams_future = future::try_join_all(
                    participants.iter_mut().enumerate().map(
                        |(_i, stream)| async move {
                            let mut batch = vec![[0_u16; 31]; BATCH_SIZE];
                            let mut buffer: &mut [u8] =
                                bytemuck::cast_slice_mut(batch.as_mut_slice());

                            // We can not use read_exact here as we might get EOF before the
                            // buffer is full But we should
                            // still try to fill the entire buffer.
                            // If nothing else, this guarantees that we read batches at a
                            // [u16;31] boundary.
                            while !buffer.is_empty() {
                                let bytes_read =
                                    stream.read_buf(&mut buffer).await?;
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
                        },
                    ),
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
                processed_shares_rx.recv().await.unwrap();
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

    pub fn initialize_masks(&self) -> Vec<Bits> {
        //TODO:
        vec![]
    }

    pub async fn dequeue_queries(
        &self,
    ) -> eyre::Result<Option<Vec<aws_sdk_sqs::types::Message>>> {
        let messages = self
            .aws_client
            .receive_message()
            .queue_url(self.shares_queue_url.clone())
            .send()
            .await?
            .messages;

        Ok(messages)
    }

    //TODO: update error handling
    pub async fn enqueue_distance_results(
        &self,
        distance_results: DistanceResults,
    ) -> eyre::Result<()> {
        //TODO: Implement DLQ logic

        self.aws_client
            .send_message()
            .queue_url(self.distances_queue_url.clone())
            .message_body(serde_json::to_string(&distance_results)?)
            .send()
            .await?;

        Ok(())
    }
}
