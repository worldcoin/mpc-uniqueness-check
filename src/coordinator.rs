use std::io::ErrorKind;
use std::net::SocketAddr;
use std::panic::panic_any;
use std::sync::Arc;

use aws_sdk_sqs::operation::receive_message::builders::ReceiveMessageFluentBuilder;
use aws_sdk_sqs::operation::send_message::builders::SendMessageFluentBuilder;
use bytemuck::bytes_of;
use eyre::anyhow;
use futures::future;
use memmap::{Mmap, MmapOptions};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
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

                    let (denominator_handle, denominator_rx) = self
                        .compute_denominators(mmap_db.clone(), template.mask);

                    handles.push(denominator_handle);

                    //TODO: handle participant shares

                    //TODO:
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
    ) -> (
        tokio::sync::mpsc::Receiver<Vec<[u16; 31]>>,
        JoinHandle<eyre::Result<()>>,
    ) {
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

    pub fn collect_participant_shares(&self) -> JoinHandle<eyre::Result<()>> {
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
