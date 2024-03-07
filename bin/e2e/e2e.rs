use std::fs;
use std::path::PathBuf;
mod common;

use clap::Parser;
use eyre::ContextCompat;
use mpc::config::{load_config, AwsConfig, DbConfig};
use mpc::coordinator::UniquenessCheckResult;
use mpc::db::Db;
use mpc::template::{Bits, Template};
use mpc::utils::aws::{self, sqs_client_from_config};
use serde::Deserialize;
use telemetry_batteries::tracing::stdout::StdoutBattery;

#[derive(Debug, Deserialize)]
pub struct SimpleSignupSequenceConfig {
    aws: AwsConfig,
    coordinator_queue: CoordinatorQueueConfig,
    db_sync: DbSyncConfig,
}

#[derive(Debug, Deserialize)]
pub struct CoordinatorQueueConfig {
    query_queue: String,
    results_queue: String,
}

#[derive(Debug, Deserialize)]
struct DbSyncConfig {
    coordinator_db_url: String,
    coordinator_db_sync_queue: String,
    participant_0_db_sync_queue: String,
    participant_1_db_sync_queue: String,
}

#[derive(Parser, Debug, Deserialize)]
#[clap(version)]
#[clap(rename_all = "kebab-case")]
struct Args {
    /// The path to the config file
    #[clap(short, long)]
    config: Option<PathBuf>,

    /// The path to the signup sequence file to use
    #[clap(short, long, default_value = "bin/e2e/signup_sequence.json")]
    signup_sequence: String,
}

#[derive(Debug, Deserialize)]
pub struct SignupSequenceElement {
    signup_id: String,
    iris_code: Bits,
    mask_code: Bits,
    matched_with: Vec<Match>,
}

#[derive(Debug, Deserialize)]
pub struct Match {
    serial_id: u64,
    distance: f64,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let args = Args::parse();

    if std::env::var("AWS_ACCESS_KEY_ID").is_err() {
        tracing::warn!("AWS_ACCESS_KEY_ID not set");
    }
    if std::env::var("AWS_SECRET_ACCESS_KEY").is_err() {
        tracing::warn!("AWS_SECRET_ACCESS_KEY not set");
    }
    if std::env::var("AWS_DEFAULT_REGION").is_err() {
        tracing::warn!("AWS_DEFAULT_REGION not set");
    }

    let _shutdown_tracing_provider = StdoutBattery::init();

    tracing::info!("Loading config");
    let config: SimpleSignupSequenceConfig =
        load_config("E2E", args.config.as_deref())?;

    let signup_sequence: Vec<SignupSequenceElement> = serde_json::from_str(
        &fs::read_to_string(args.signup_sequence.parse::<PathBuf>()?)?,
    )?;

    //generate random template
    let sqs_client = sqs_client_from_config(&config.aws).await?;

    tracing::info!("Waiting for queues");
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    common::wait_for_queues(
        &sqs_client,
        vec![
            &config.coordinator_queue.query_queue,
            &config.coordinator_queue.results_queue,
            &config.db_sync.coordinator_db_sync_queue,
            &config.db_sync.participant_0_db_sync_queue,
            &config.db_sync.participant_1_db_sync_queue,
        ],
    )
    .await?;

    // Get the latest serial id from the coordinator, this assumes that the participants are synced
    let mut next_serial_id = {
        let db = Db::new(&DbConfig {
            url: config.db_sync.coordinator_db_url.to_string(),
            migrate: false,
            create: false,
        })
        .await?;

        let masks = db.fetch_masks(0).await?;

        (masks.len() as u64).checked_sub(1)
    };

    let participant_db_sync_queues = vec![
        config.db_sync.participant_0_db_sync_queue.as_str(),
        config.db_sync.participant_1_db_sync_queue.as_str(),
    ];

    for element in signup_sequence {
        let template = Template {
            code: element.iris_code,
            mask: element.mask_code,
        };

        // Send the query to the coordinator
        common::send_query(
            template,
            &sqs_client,
            &config.coordinator_queue.query_queue,
            &element.signup_id,
            &common::generate_random_string(4),
        )
        .await?;

        let result = common::receive_result(
            &sqs_client,
            &config.coordinator_queue.results_queue,
        )
        .await?;

        let message_body = result.body.context("Could not get message body")?;

        let uniqueness_check_result =
            serde_json::from_str::<UniquenessCheckResult>(&message_body)?;

        // Check that signup id and serial id match expected values
        assert_eq!(uniqueness_check_result.signup_id, element.signup_id);
        assert_eq!(uniqueness_check_result.serial_id, next_serial_id);

        // If there are matches, check that the distances match the expected values
        if !uniqueness_check_result.matches.is_empty() {
            for (i, distance) in
                uniqueness_check_result.matches.iter().enumerate()
            {
                assert_eq!(distance.distance, element.matched_with[i].distance);
                assert_eq!(
                    distance.serial_id,
                    element.matched_with[i].serial_id
                );
            }
        } else {
            if let Some(id) = next_serial_id.as_mut() {
                *id += 1;
            } else {
                next_serial_id = Some(0);
            }

            common::seed_db_sync(
                &sqs_client,
                &config.db_sync.coordinator_db_sync_queue,
                &participant_db_sync_queues,
                template,
                next_serial_id.context("Could not get next serial id")?,
            )
            .await?;

            // Sleep a little to give the nodes time to sync dbs
            tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        }

        // Delete message from queue
        aws::sqs_delete_message(
            &sqs_client,
            &config.coordinator_queue.results_queue,
            result
                .receipt_handle
                .context("Could not get receipt handle")?,
        )
        .await?;
    }

    Ok(())
}
