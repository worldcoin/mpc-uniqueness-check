use std::fs;
mod common;

use mpc::config::{AwsConfig, DbConfig};
use mpc::db::Db;
use mpc::template::{Bits, Template};
use mpc::utils::aws::sqs_client_from_config;
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

pub fn load_config() -> eyre::Result<SimpleSignupSequenceConfig> {
    let signup_sequence_path = std::path::Path::new("e2e.toml");

    let settings = config::Config::builder()
        .add_source(config::File::from(signup_sequence_path).required(true))
        .build()?;

    let config = settings.try_deserialize::<SimpleSignupSequenceConfig>()?;

    Ok(config)
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
    let _shutdown_tracing_provider = StdoutBattery::init();

    let config = load_config()?;

    //TODO: read in signup sequence json
    let signup_sequence: Vec<SignupSequenceElement> = serde_json::from_str(
        &fs::read_to_string("tests/signup_sequence/signup_sequence.json")?,
    )?;

    //generate random template
    let sqs_client = sqs_client_from_config(&config.aws).await?;

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

    // If db sync is enabled, connect to the coordinator db to get the latest serial_id
    let mut next_serial_id = {
        let db = Db::new(&DbConfig {
            url: config.db_sync.coordinator_db_url.to_string(),
            migrate: false,
            create: false,
        })
        .await?;

        tracing::info!("Getting next serial id");

        let masks = db.fetch_masks(0).await?;

        masks.len() as u64
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

        //TODO: fix result queue handling

        // let results = common::receive_results(
        //     &sqs_client,
        //     &config.coordinator_queue.results_queue,
        // )
        // .await?;

        // let message_body = results
        //     .first()
        //     .context("Could not get message")?
        //     .clone()
        //     .body
        //     .context("Could not get message body")?;

        // let uniqueness_check_result =
        //     serde_json::from_str::<UniquenessCheckResult>(&message_body)?;

        // if !uniqueness_check_result.matches.is_empty() {
        //     for (i, distance) in
        //         uniqueness_check_result.matches.iter().enumerate()
        //     {
        //         assert_eq!(distance.distance, element.matched_with[i].distance);
        //     }
        // } else {

        common::seed_db_sync(
            &sqs_client,
            &config.db_sync.coordinator_db_sync_queue,
            &participant_db_sync_queues,
            template,
            next_serial_id,
        )
        .await?;

        next_serial_id += 1;
        // }
    }

    Ok(())
}
