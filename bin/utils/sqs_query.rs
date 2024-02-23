use clap::Args;
use mpc::config::AwsConfig;
use mpc::coordinator::UniquenessCheckRequest;
use mpc::template::Template;
use mpc::utils::aws::sqs_client_from_config;
use rand::{thread_rng, Rng};

use crate::generate_random_string;

#[derive(Debug, Clone, Args)]
pub struct SQSQuery {
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

pub async fn sqs_query(args: &SQSQuery) -> eyre::Result<()> {
    let sqs_client = sqs_client_from_config(&AwsConfig {
        endpoint: args.endpoint_url.clone(),
        region: args.region.clone(),
    })
    .await?;

    let mut rng = thread_rng();
    let plain_code: Template = rng.gen();

    let signup_id = generate_random_string(10);
    let group_id = generate_random_string(10);

    tracing::info!(?signup_id, ?group_id, "Sending message");

    let request = UniquenessCheckRequest {
        plain_code,
        signup_id,
    };

    sqs_client
        .send_message()
        .queue_url(args.queue_url.clone())
        .message_group_id(group_id)
        .message_body(serde_json::to_string(&request)?)
        .send()
        .await?;

    Ok(())
}
