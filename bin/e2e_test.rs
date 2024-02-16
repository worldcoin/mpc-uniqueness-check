use std::fs;
use std::time::Duration;

use aws_sdk_sqs::types::QueueAttributeName;
use clap::Parser;
use eyre::ContextCompat;
use mpc::config::AwsConfig;
use mpc::coordinator::{UniquenessCheckRequest, UniquenessCheckResult};
use mpc::encoded_bits::EncodedBits;
use mpc::template::Template;
use mpc::utils::aws::sqs_client_from_config;
use mpc::{coordinator, participant};
use rand::distributions::Alphanumeric;
use rand::Rng;

// const EQUAL_MATCH_THRESHOLD: f64 = 0.01;

#[derive(Parser)]
#[clap(version)]
struct Args {
    #[clap(env)]
    aws_endpoint: String,

    #[clap(env)]
    aws_region: String,

    #[clap(env)]
    coordinator_db_sync_queue: String,

    #[clap(env)]
    participant_db_sync_queue: String,

    #[clap(env)]
    coordinator_query_queue: String,

    #[clap(env)]
    coordinator_results_queue: String,

    //Path to templates file
    #[clap(env, long, short)]
    templates: Option<String>,

    #[clap(env, long, short)]
    db_sync: bool,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    //generate random template
    let sqs_client = sqs_client_from_config(&AwsConfig {
        endpoint: Some(args.aws_endpoint.clone()),
        region: Some(args.aws_region.clone()),
    })
    .await?;

    let mock_templates: Vec<Template> = if let Some(templates) = &args.templates
    {
        serde_json::from_str(&fs::read_to_string(templates)?)?
    } else {
        let mut rng = rand::thread_rng();
        (0..10).map(|_| rng.gen()).collect()
    };

    tracing::info!("Waiting for queues to be ready");
    tokio::time::sleep(Duration::from_secs(2)).await;
    wait_for_queues(&args, &sqs_client).await?;

    for (id, template) in mock_templates.into_iter().enumerate() {
        send_query(template, &sqs_client, &args.coordinator_query_queue)
            .await?;

        //TODO: inspect elements from results queue
        // handle_results(&sqs_client, &args.coordinator_results_queue).await?;

        if args.db_sync {
            tracing::info!("Encoding shares");
            let shares: Box<[EncodedBits]> =
                mpc::distance::encode(&template).share(1);

            seed_db_sync(&args, &sqs_client, template, shares, id as u64)
                .await?;
        }
    }

    Ok(())
}

pub async fn handle_results(
    sqs_client: &aws_sdk_sqs::Client,
    results_queue: &str,
) -> eyre::Result<()> {
    let messages = loop {
        // Get a message with results back
        let messages = sqs_client
            .receive_message()
            .queue_url(results_queue)
            // .wait_time_seconds(10)
            .send()
            .await?;

        let Some(messages) = messages.messages else {
            tracing::warn!("No messages in response, will retry");
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            continue;
        };

        break messages;
    };

    for message in messages {
        let body = message.body.context("Missing message body")?;
        let result: UniquenessCheckResult = serde_json::from_str(&body)?;

        tracing::info!(
            result_serial_id = result.serial_id,
            num_matches = result.matches.len(),
            matches = ?result.matches,
            "Result received"
        );

        //Delete message from the results queue
        let receipt_handle = message
            .receipt_handle
            .context("Could not get receipt handle")?;

        tracing::info!("Deleting message from results queue");

        match sqs_client
            .delete_message()
            .queue_url(results_queue)
            .receipt_handle(receipt_handle)
            .send()
            .await
        {
            Ok(deleted_msg_output) => {
                tracing::info!(?deleted_msg_output, "Message deleted")
            }
            Err(e) => tracing::error!("Error deleting message: {:?}", e),
        };
    }

    Ok(())
}

async fn seed_db_sync(
    args: &Args,
    sqs_client: &aws_sdk_sqs::Client,
    template: Template,
    shares: Box<[EncodedBits]>,
    serial_id: u64,
) -> eyre::Result<()> {
    let coordinator_payload =
        serde_json::to_string(&vec![coordinator::DbSyncPayload {
            id: serial_id,
            mask: template.mask,
        }])?;

    tracing::info!(
        "Sending {} bytes to coordinator db sync queue",
        coordinator_payload.len()
    );

    sqs_client
        .send_message()
        .queue_url(&args.coordinator_db_sync_queue)
        .message_body(coordinator_payload)
        .send()
        .await?;

    let participant_payload =
        serde_json::to_string(&vec![participant::DbSyncPayload {
            id: serial_id,
            share: shares[0],
        }])?;

    tracing::info!(
        "Sending {} bytes to participant",
        participant_payload.len()
    );

    sqs_client
        .send_message()
        .queue_url(&args.participant_db_sync_queue)
        .message_body(participant_payload)
        .send()
        .await?;

    tracing::info!("Waiting for db sync to complete");
    tokio::time::sleep(Duration::from_secs(1)).await;

    Ok(())
}

async fn wait_for_queues(
    args: &Args,
    sqs_client: &aws_sdk_sqs::Client,
) -> eyre::Result<()> {
    let queues = vec![
        &args.coordinator_db_sync_queue,
        &args.participant_db_sync_queue,
        &args.coordinator_query_queue,
        &args.coordinator_results_queue,
    ];

    for queue in queues {
        tracing::info!(?queue, "Waiting for queue");
        loop {
            let Ok(response) = sqs_client
                .get_queue_attributes()
                .queue_url(queue)
                .attribute_names(
                    QueueAttributeName::ApproximateNumberOfMessages,
                )
                .send()
                .await
            else {
                continue;
            };

            let Some(attributes) = response.attributes else {
                continue;
            };

            let Some(_num_messages) = attributes
                .get(&QueueAttributeName::ApproximateNumberOfMessages)
            else {
                continue;
            };

            break;
        }
    }

    Ok(())
}

async fn send_query(
    template: Template,
    sqs_client: &aws_sdk_sqs::Client,
    query_queue: &str,
) -> eyre::Result<()> {
    let signup_id = generate_random_string(4);
    let group_id = generate_random_string(4);

    tracing::info!(?signup_id, ?group_id, "Sending request");

    let request = UniquenessCheckRequest {
        plain_code: template,
        signup_id,
    };

    sqs_client
        .send_message()
        .queue_url(query_queue)
        .message_group_id(group_id)
        .message_body(serde_json::to_string(&request)?)
        .send()
        .await?;

    Ok(())
}

fn generate_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
