use std::io::ErrorKind;
use std::net::SocketAddr;
use std::panic::panic_any;
use std::sync::Arc;

use aws_sdk_sqs::operation::receive_message::builders::ReceiveMessageFluentBuilder;
use aws_sdk_sqs::operation::send_message::builders::SendMessageFluentBuilder;
use bytemuck::bytes_of;
use eyre::{anyhow, Error};
use futures::future;
use memmap::{Mmap, MmapOptions};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{self, mpsc};
use tokio::task::JoinHandle;

use crate::bits::Bits;
use crate::distance::{self, MasksEngine};
use crate::encoded_bits::EncodedBits;
use crate::template::{self, Template};

const BATCH_SIZE: usize = 20_000;

pub struct Coordinator {
    aws_client: aws_sdk_sqs::Client,
    shares_queue_url: String,
    distances_queue_url: String,
    participants: Vec<BufReader<TcpStream>>,
}

impl Coordinator {
    pub async fn new(
        participants: Vec<SocketAddr>,
        shares_queue_url: &str,
        distances_queue_url: &str,
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
            participants: streams,
        })
    }

    //TODO: update error handling
    pub async fn spawn(mut self) -> eyre::Result<()> {
        let mmap_db: Arc<Mmap> = Arc::new(self.initialize_mmap_db());

        loop {
            if let Some(messages) = self.dequeue_queries().await? {
                for message in messages {
                    let template = serde_json::from_str::<Template>(
                        //TODO: handle this error
                        &message.body.expect("No body in message"),
                    )?;

                    self.send_query_to_participants(&template).await?;

                    let mut handles = vec![];

                    let (denominator_rx, denominator_handle) = self
                        .compute_denominators(mmap_db.clone(), template.mask);

                    handles.push(denominator_handle);

                    //TODO: process_participant shares / collect batches of shares and denoms
                    let (processed_shares_rx, process_shares_handle) =
                        self.process_participant_shares(denominator_rx);

                    handles.push(process_shares_handle);

                    //TODO: process results Handle each that comes through and calc the min distance and min index

                    for handle in handles {
                        handle.await??;
                    }
                }
            }

            //TODO: sleep for some amount of time
        }
    }

    pub async fn send_query_to_participants(
        &mut self,
        query: &Template,
    ) -> eyre::Result<()> {
        // Write each share to the corresponding participant
        future::try_join_all(self.participants.iter_mut().map(
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
        mmap_db: Arc<Mmap>,
        mask: Bits,
    ) -> (Receiver<Vec<[u16; 31]>>, JoinHandle<eyre::Result<()>>) {
        let (sender, denom_receiver) = tokio::sync::mpsc::channel(4);
        let denominator_handle = tokio::task::spawn_blocking(move || {
            let masks: &[Bits] = bytemuck::cast_slice(&mmap_db);
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

    pub fn process_participant_shares(
        &mut self,
        denominator_rx: Receiver<Vec<[u16; 31]>>,
    ) -> (
        Receiver<(Vec<[u16; 31]>, Vec<Vec<[u16; 31]>>)>,
        JoinHandle<eyre::Result<()>>,
    ) {
        // Collect batches of shares
        let (processed_shares_tx, mut processed_shares_rx) = mpsc::channel(4);

        let streams_future =
            future::try_join_all(self.participants.iter_mut().enumerate().map(
                |(i, stream)| async move {
                    let mut batch = vec![[0_u16; 31]; BATCH_SIZE];
                    let mut buffer: &mut [u8] =
                        bytemuck::cast_slice_mut(batch.as_mut_slice());

                    // We can not use read_exact here as we might get EOF before the
                    // buffer is full But we should
                    // still try to fill the entire buffer.
                    // If nothing else, this guarantees that we read batches at a
                    // [u16;31] boundary.
                    while !buffer.is_empty() {
                        let bytes_read = stream.read_buf(&mut buffer).await?;
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
            ));

        let batch_worker = tokio::task::spawn(async move {
            loop {
                // Collect futures of denominator and share batches
                let streams_future = future::try_join_all(
                    self.participants.iter_mut().enumerate().map(
                        |(i, stream)| async move {
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

    pub async fn process_results(
        &self,
        processed_shares_rx: Receiver<(Vec<[u16; 31]>, Vec<Vec<[u16; 31]>>)>,
    ) -> eyre::Result<()> {
        todo!()
    }

    pub fn initialize_mmap_db(&self) -> Mmap {
        todo!()
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
    pub async fn enqueue_distance_shares(&self) -> eyre::Result<()> {
        todo!();

        // let distances_queue = self
        //     .aws_client
        //     .send_message()
        //     .queue_url(self.distances_queue_url.clone())
        //     // .message_body(input) //TODO: update/uncomment this
        //     .send()
        //     .await?;

        Ok(())
    }
}
