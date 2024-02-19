use std::fs;

use mpc::config::AwsConfig;
use mpc::template::{Bits, Template};
use mpc::utils::aws::{self, sqs_client_from_config};
use serde::Deserialize;
use telemetry_batteries::tracing::stdout::StdoutBattery;

use crate::common;

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
    let signup_sequence_path = std::path::Path::new(
        "tests/simple_signup_sequence/simple_signup_sequence.toml",
    );

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

#[tokio::test]
async fn test_simple_signup_sequence() -> eyre::Result<()> {
    let _ = StdoutBattery::init();

    let config = load_config()?;

    //TODO: read in signup sequence json
    let signup_sequence: Vec<SignupSequenceElement> =
        serde_json::from_str(&fs::read_to_string(
            "tests/simple_signup_sequence/simple_signup_sequence.json",
        )?)?;

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

    const REQUEST_MESSAGE_GROUP_ID: &str = "mpc-uniqueness-check-response";

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
            REQUEST_MESSAGE_GROUP_ID,
        )
        .await?;

        // Receive the result

        //TODO: check that the serial id is correct

        //TODO: check the distance result against the signup sequence json

        //TODO: if template passes check, initiate db sync

        //TODO: wait for latest serial id to match expected  id
    }

    Ok(())
}
