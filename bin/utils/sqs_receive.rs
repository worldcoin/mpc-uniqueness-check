use clap::Args;
use eyre::ContextCompat;
use mpc::config::AwsConfig;
use mpc::coordinator::UniquenessCheckResult;
use mpc::utils::aws::sqs_client_from_config;

#[derive(Debug, Clone, Args)]
pub struct SQSReceive {
    /// The endpoint URL for the AWS service
    ///
    /// Useful when using LocalStack
    #[clap(short, long)]
    pub endpoint_url: Option<String>,

    /// The AWS region
    #[clap(short, long)]
    pub region: Option<String>,

    /// The URL of the SQS queue
    #[clap(short, long)]
    pub queue_url: String,
}

pub async fn sqs_receive(args: &SQSReceive) -> eyre::Result<()> {
    let sqs_client = sqs_client_from_config(&AwsConfig {
        endpoint: args.endpoint_url.clone(),
        region: args.region.clone(),
    })
    .await?;

    loop {
        let sqs_msg = sqs_client
            .receive_message()
            .queue_url(args.queue_url.clone())
            .send()
            .await?;

        let messages = sqs_msg.messages.unwrap_or_default();

        if messages.is_empty() {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            continue;
        }

        for message in messages {
            let body = message.body.context("Missing message body")?;
            let receipt_handle = message
                .receipt_handle
                .context("Missing receipt handle in message")?;

            let request: UniquenessCheckResult = serde_json::from_str(&body)?;

            tracing::info!(?request, "Received message");

            sqs_client
                .delete_message()
                .queue_url(args.queue_url.clone())
                .receipt_handle(receipt_handle)
                .send()
                .await?;
        }
    }
}
