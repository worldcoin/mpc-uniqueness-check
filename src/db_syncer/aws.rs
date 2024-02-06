use eyre::{Context, ContextCompat};

use super::DbSyncer;
use crate::utils::aws::sqs_dequeue_raw;

pub struct AwsSyncer {
    sqs_client: aws_sdk_sqs::Client,
    queue_url: String,
}

impl AwsSyncer {
    pub async fn new(queue_url: impl ToString) -> eyre::Result<Self> {
        let aws_config =
            aws_config::load_defaults(aws_config::BehaviorVersion::latest())
                .await;

        let sqs_client = aws_sdk_sqs::Client::new(&aws_config);

        Ok(Self {
            sqs_client,
            queue_url: queue_url.to_string(),
        })
    }
}

#[async_trait::async_trait]
impl DbSyncer for AwsSyncer {
    async fn receive_items(&self) -> eyre::Result<Vec<String>> {
        sqs_dequeue_raw(&self.sqs_client, &self.queue_url)
            .await
            .context("Fetching messages for db sync")?
            .into_iter()
            .map(|msg| msg.body.context("Missing body"))
            .collect()
    }
}
