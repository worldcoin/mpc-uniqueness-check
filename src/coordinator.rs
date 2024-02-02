use std::collections::BinaryHeap;
use std::net::SocketAddr;
use std::sync::Arc;

use futures::future;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

use crate::bits::Bits;
use crate::config::CoordinatorConfig;
use crate::db::coordinator::CoordinatorDb;
use crate::distance::{self, Distance, DistanceResults, MasksEngine};
use crate::gateway::{self, Gateway};
use crate::template::Template;

const BATCH_SIZE: usize = 20_000;

pub struct Coordinator {
    //TODO: Consider maintaining an open stream and using read_exact preceeded by a bytes length payload
    participants: Vec<SocketAddr>,
    hamming_distance_threshold: f64,
    n_closest_distances: usize,
    gateway: Arc<dyn Gateway>,
    database: Arc<CoordinatorDb>,
    masks: Arc<Mutex<Vec<Bits>>>,
}

impl Coordinator {
    pub async fn new(config: CoordinatorConfig) -> eyre::Result<Self> {
        let gateway = gateway::from_config(&config.gateway).await?;
        let database = Arc::new(CoordinatorDb::new(&config.db).await?);

        let masks = database.fetch_masks(0).await?;
        let masks = Arc::new(Mutex::new(masks));

        Ok(Self {
            gateway,
            hamming_distance_threshold: config.hamming_distance_threshold,
            n_closest_distances: config.n_closest_distances,
            participants: config.participants,
            database,
            masks,
        })
    }

    //TODO: update error handling
    pub async fn spawn(mut self) -> eyre::Result<()> {
        tracing::info!("Starting coordinator");

        loop {
            for template in self.gateway.receive_queries().await? {
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

                self.gateway.send_results(&distance_results).await?;

                // TODO: Make sure that all workers receive signal to stop.
                for handle in handles {
                    handle.await??;
                }
            }

            //TODO: Do we want to sleep?
            // tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn send_query_to_participants(
        &mut self,
        query: &Template,
    ) -> eyre::Result<Vec<BufReader<TcpStream>>> {
        // Write each share to the corresponding participant

        let streams = future::try_join_all(self.participants.iter().map(
            |socket_address| async move {
                // Send query

                let mut stream = TcpStream::connect(socket_address).await?;
                tracing::info!(?socket_address, "Connected to participant");

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
        &mut self,
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
}
