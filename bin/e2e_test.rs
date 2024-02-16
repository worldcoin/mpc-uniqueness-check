use clap::Parser;
use eyre::ContextCompat;
use mpc::config::AwsConfig;
use mpc::coordinator::{UniquenessCheckRequest, UniquenessCheckResult};
use mpc::encoded_bits::EncodedBits;
use mpc::template::Template;
use mpc::utils::aws::sqs_client_from_config;
use mpc::{coordinator, participant};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

const EQUAL_MATCH_THRESHOLD: f64 = 0.01;

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

    /// The total number of templates to use
    #[clap(env, default_value = "100")]
    num_total_templates: usize,

    /// How many of the total templates to send via db-sync
    /// before starting the test
    #[clap(env, default_value = "0.05")]
    share_of_templates_seeded: f64,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let sqs_client = sqs_client_from_config(&AwsConfig {
        endpoint: Some(args.aws_endpoint.clone()),
        region: Some(args.aws_region.clone()),
    })
    .await?;

    let mut rng = thread_rng();

    let templates: Vec<Template> =
        (0..args.num_total_templates).map(|_| rng.gen()).collect();

    let shares: Vec<Box<[EncodedBits]>> = templates
        .iter()
        .map(|t| mpc::distance::encode(t).share(1))
        .collect();

    let num_templates_to_seed = (args.num_total_templates as f64
        * args.share_of_templates_seeded)
        as usize;

    let (non_unique_templates, _unique_templates) =
        templates.split_at(num_templates_to_seed);

    let (non_unique_shares, _unique_shares) =
        shares.split_at(num_templates_to_seed);

    seed_db_sync(&args, &sqs_client, non_unique_templates, non_unique_shares)
        .await?;

    test_non_unique_templates(&args, &sqs_client, non_unique_templates).await?;

    // TODO: Test unique templates

    Ok(())
}

async fn seed_db_sync(
    args: &Args,
    sqs_client: &aws_sdk_sqs::Client,
    templates: &[Template],
    shares: &[Box<[EncodedBits]>],
) -> eyre::Result<()> {
    let mut coordinator_payload = vec![];
    let mut participant_payload = vec![];

    eyre::ensure!(
        templates.len() == shares.len(),
        "templates and shares must have the same length"
    );

    tracing::info!("Sending {} templates via db-sync", templates.len());

    for (id, template) in templates.iter().enumerate() {
        coordinator_payload.push(coordinator::DbSyncPayload {
            id: id as u64,
            mask: template.mask,
        });
    }

    for (id, share) in shares.iter().enumerate() {
        participant_payload.push(participant::DbSyncPayload {
            id: id as u64,
            share: share[0],
        });
    }

    let msg = serde_json::to_string(&coordinator_payload)?;
    tracing::info!("Sending {} bytes to coordinator", msg.len());
    sqs_client
        .send_message()
        .queue_url(&args.coordinator_db_sync_queue)
        .message_body(msg)
        .send()
        .await?;

    let msg = serde_json::to_string(&participant_payload)?;
    tracing::info!("Sending {} bytes to participant", msg.len());
    sqs_client
        .send_message()
        .queue_url(&args.participant_db_sync_queue)
        .message_body(msg)
        .send()
        .await?;

    Ok(())
}

async fn test_non_unique_templates(
    args: &Args,
    sqs_client: &aws_sdk_sqs::Client,
    templates: &[Template],
) -> eyre::Result<()> {
    for (serial_id, template) in templates.iter().enumerate() {
        test_non_unique_template(serial_id, args, sqs_client, *template)
            .await?;
    }

    Ok(())
}

#[tracing::instrument(skip(args, sqs_client, template))]
async fn test_non_unique_template(
    serial_id: usize,
    args: &Args,
    sqs_client: &aws_sdk_sqs::Client,
    template: Template,
) -> eyre::Result<()> {
    loop {
        let signup_id = generate_random_string(4);
        let group_id = generate_random_string(4);

        let request = UniquenessCheckRequest {
            plain_code: template,
            signup_id,
        };

        tracing::info!("Sending a request");
        sqs_client
            .send_message()
            .queue_url(args.coordinator_query_queue.clone())
            .message_group_id(group_id)
            .message_body(serde_json::to_string(&request)?)
            .send()
            .await?;

        tracing::info!("Getting results");
        // Get a message with results back
        let messages = sqs_client
            .receive_message()
            .queue_url(args.coordinator_results_queue.clone())
            // .wait_time_seconds(10)
            .send()
            .await?;

        let Some(mut messages) = messages.messages else {
            tracing::warn!("No messages in response, will retry");
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            continue;
        };

        let message = messages.pop().context("No messages in response")?;

        let body = message.body.context("Missing message body")?;
        let result: UniquenessCheckResult = serde_json::from_str(&body)?;

        tracing::info!(
            result_serial_id = result.serial_id,
            num_matches = result.matches.len(),
            "Got result"
        );

        let nodes_are_synced =
            result.serial_id > 0 && result.serial_id as usize >= serial_id;
        if nodes_are_synced {
            eyre::ensure!(
                result.matches.len() == 1,
                "Expected one exact match"
            );

            eyre::ensure!(
                result.matches[0].serial_id == serial_id as u64,
                "Expected the same serial_id in the result"
            );

            eyre::ensure!(
                result.matches[0].distance < EQUAL_MATCH_THRESHOLD,
                "Should be an exact match",
            );
        } else {
            tracing::warn!("Nodes not synced yet, will retry");
        };

        if let Some(receipt_handle) = message.receipt_handle {
            tracing::info!("Deleting received message");
            sqs_client
                .delete_message()
                .queue_url(args.coordinator_results_queue.clone())
                .receipt_handle(receipt_handle)
                .send()
                .await?;
        }

        if nodes_are_synced {
            tracing::info!("Nodes are synced and we got the expected results");
            break;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    Ok(())
}

fn generate_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
