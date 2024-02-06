use eyre::Context;

use super::Gateway;
use crate::config::SqsGatewayConfig;
use crate::distance::DistanceResults;
use crate::template::Template;
use crate::utils::aws::sqs_dequeue;

pub struct SqsGateway {
    aws_client: aws_sdk_sqs::Client,
    config: SqsGatewayConfig,
}

impl SqsGateway {
    pub async fn new(config: &SqsGatewayConfig) -> eyre::Result<Self> {
        let aws_config =
            aws_config::load_defaults(aws_config::BehaviorVersion::latest())
                .await;

        let aws_client = aws_sdk_sqs::Client::new(&aws_config);

        Ok(Self {
            aws_client,
            config: config.clone(),
        })
    }
}

#[async_trait::async_trait]
impl Gateway for SqsGateway {
    async fn receive_queries(&self) -> eyre::Result<Vec<Template>> {
        sqs_dequeue(&self.aws_client, &self.config.shares_queue_url)
            .await
            .context("Fetching coordinator messages")
    }

    async fn send_results(
        &self,
        results: &DistanceResults,
    ) -> eyre::Result<()> {
        self.aws_client
            .send_message()
            .queue_url(self.config.distances_queue_url.clone())
            .message_body(serde_json::to_string(&results)?)
            .send()
            .await?;

        Ok(())
    }
}
