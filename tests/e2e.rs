use std::collections::HashMap;
use std::sync::{Arc, Once};

use aws_sdk_sqs::types::{Message, QueueAttributeName};
use config::Config;
use eyre::ContextCompat;
use futures::future::select_all;
use mpc::bits::Bits;
use mpc::config::{AwsConfig, CoordinatorConfig, ParticipantConfig};
use mpc::coordinator::{
    self, Coordinator, MpcMessage, UniquenessCheckRequest,
    UniquenessCheckResult,
};
use mpc::distance::EncodedBits;
use mpc::participant::{self, Participant};
use mpc::template::Template;
use mpc::utils::aws::{self, sqs_client_from_config};
use rand::distributions::Alphanumeric;
use rand::Rng;
use serde::Deserialize;
use serial_test::serial;
use telemetry_batteries::tracing::stdout::StdoutBattery;
use testcontainers::{clients, Container};
use testcontainers_modules::localstack::LocalStack;
use testcontainers_modules::postgres::Postgres;

pub const E2E_CONFIG: &str = include_str!("./e2e_config.toml");
pub const REGULAR_SIGNUP_SEQUENCE: &str =
    include_str!("regular_e2e_sequence.json");
pub const MULTI_MATCH_SIGNUP_SEQUENCE: &str =
    include_str!("multi_match_e2e_sequence.json");
pub const MULTI_MATCH_TRUNCATED_SIGNUP_SEQUENCE: &str =
    include_str!("multi_match_truncated_e2e_sequence.json");

static INIT: Once = Once::new();

#[derive(Debug, Deserialize)]
pub struct E2EConfig {
    coordinator: CoordinatorConfig,
    participant: Vec<ParticipantConfig>,
}

#[derive(Debug, Deserialize)]
pub struct SignupSequenceElement {
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
#[serial]
async fn test_regular_e2e() -> color_eyre::Result<()> {
    color_eyre::install().ok();
    let settings = Config::builder()
        .add_source(config::File::from_str(
            E2E_CONFIG,
            config::FileFormat::Toml,
        ))
        .build()?;

    let mut e2e_config = settings.try_deserialize::<E2EConfig>()?;
    let signup_scenario = serde_json::from_str(REGULAR_SIGNUP_SEQUENCE)?;

    return run_e2e_scenario(&mut e2e_config, signup_scenario).await;
}

#[tokio::test]
#[serial]
async fn test_multi_match_e2e() -> color_eyre::Result<()> {
    color_eyre::install().ok();
    let settings = Config::builder()
        .add_source(config::File::from_str(
            E2E_CONFIG,
            config::FileFormat::Toml,
        ))
        .build()?;

    let mut e2e_config = settings.try_deserialize::<E2EConfig>()?;
    let signup_scenario = serde_json::from_str(MULTI_MATCH_SIGNUP_SEQUENCE)?;

    return run_e2e_scenario(&mut e2e_config, signup_scenario).await;
}

#[tokio::test]
#[serial]
async fn test_multi_match_truncated_e2e() -> color_eyre::Result<()> {
    color_eyre::install().ok();
    let settings = Config::builder()
        .add_source(config::File::from_str(
            E2E_CONFIG,
            config::FileFormat::Toml,
        ))
        .build()?;

    let mut e2e_config = settings.try_deserialize::<E2EConfig>()?;

    // set n_closest_distances to 1 and expect truncation
    e2e_config.coordinator.n_closest_distances = 1;
    let signup_sequence =
        serde_json::from_str(MULTI_MATCH_TRUNCATED_SIGNUP_SEQUENCE)?;

    return run_e2e_scenario(&mut e2e_config, signup_sequence).await;
}

async fn run_e2e_scenario(
    e2e_config: &mut E2EConfig,
    signup_sequence: Vec<SignupSequenceElement>,
) -> eyre::Result<()> {
    // initialize tracing once for all tests
    INIT.call_once(|| {
        let _tracing_handle = StdoutBattery::init();
    });

    tracing::info!("Initializing resources");

    let docker = clients::Cli::default();
    let (_containers, sqs_client) =
        initialize_resources(&docker, e2e_config).await?;

    let participant_0 =
        Arc::new(Participant::new(e2e_config.participant[0].clone()).await?);
    let participant_1 =
        Arc::new(Participant::new(e2e_config.participant[1].clone()).await?);
    let coordinator =
        Arc::new(Coordinator::new(e2e_config.coordinator.clone()).await?);

    let tasks = vec![
        tokio::spawn(coordinator.spawn()),
        tokio::spawn(participant_0.spawn()),
        tokio::spawn(participant_1.spawn()),
    ];

    tracing::info!("Waiting for queues");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    let signup_sequence =
        test_signup_sequence(signup_sequence, sqs_client, e2e_config);

    tokio::select! {
        signup_result = signup_sequence => {
            match signup_result {
                Ok(_) => {
                    tracing::info!("Signup sequence test complete");
                },
                Err(e) => {
                    tracing::error!("Signup sequence failed: {:?}", e);
                    return Err(e);
                }
            }
        },

        tasks_result = select_all(tasks) => {
            tracing::error!("Task exited early");
            let _ = tasks_result.0?;
        }
    }

    Ok(())
}

async fn initialize_resources<'a>(
    docker: &'a testcontainers::clients::Cli,
    e2e_config: &mut E2EConfig,
) -> eyre::Result<(
    (
        Container<'a, LocalStack>,
        Container<'a, Postgres>,
        Vec<Container<'a, Postgres>>,
    ),
    aws_sdk_sqs::Client,
)> {
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");

    tracing::info!("Initializing localstack");

    let localstack_container = docker.run(LocalStack);
    let localstack_host_port = localstack_container.get_host_port_ipv4(4566);

    let aws_config = AwsConfig {
        endpoint: Some(format!("http://localhost:{}", localstack_host_port)),
        region: None,
    };

    e2e_config.coordinator.aws = aws_config.clone();
    for participant in e2e_config.participant.iter_mut() {
        participant.aws = aws_config.clone();
    }

    let sqs_client = sqs_client_from_config(&aws_config).await?;

    // Set up databases
    tracing::info!("Initializing coordinator database");
    let coordinator_db_container =
        docker.run(Postgres::default().with_host_auth());
    let coordinator_db_port = coordinator_db_container.get_host_port_ipv4(5432);
    e2e_config.coordinator.db.url = format!(
        "postgres://postgres:postgres@localhost:{}/postgres",
        coordinator_db_port
    );

    tracing::info!("Initializing participant databases");
    let mut participant_db_containers = vec![];
    for participant in e2e_config.participant.iter_mut() {
        let participant_db = docker.run(Postgres::default().with_host_auth());
        let participant_db_port = participant_db.get_host_port_ipv4(5432);
        participant.db.url = format!(
            "postgres://postgres:postgres@localhost:{}/postgres",
            participant_db_port
        );

        participant_db_containers.push(participant_db);
    }

    let fifo_attributes = {
        let mut map = HashMap::new();
        map.insert(QueueAttributeName::FifoQueue, "true".to_string());
        map.insert(
            QueueAttributeName::ContentBasedDeduplication,
            "true".to_string(),
        );
        map
    };

    // Create SQS queues
    tracing::info!("Creating SQS queues");
    create_queue(
        &sqs_client,
        "coordinator-uniqueness-check.fifo",
        Some(fifo_attributes.clone()),
    )
    .await?;
    e2e_config.coordinator.queues.queries_queue_url = format!(
        "http://localhost:{}/000000000000/coordinator-uniqueness-check.fifo",
        localstack_host_port
    );

    create_queue(
        &sqs_client,
        "coordinator-results-queue.fifo",
        Some(fifo_attributes),
    )
    .await?;
    create_queue(&sqs_client, "coordinator-db-sync-queue", None).await?;
    create_queue(&sqs_client, "participant-0-db-sync-queue", None).await?;
    create_queue(&sqs_client, "participant-1-db-sync-queue", None).await?;

    //Update queues to map to new localstack port
    e2e_config.coordinator.queues.db_sync_queue_url = format!(
        "http://localhost:{}/000000000000/coordinator-db-sync-queue",
        localstack_host_port
    );
    e2e_config.coordinator.queues.distances_queue_url = format!(
        "http://localhost:{}/000000000000/coordinator-results-queue.fifo",
        localstack_host_port
    );
    e2e_config.coordinator.queues.queries_queue_url = format!(
        "http://localhost:{}/000000000000/coordinator-uniqueness-check.fifo",
        localstack_host_port
    );

    for (i, participant) in e2e_config.participant.iter_mut().enumerate() {
        participant.queues.db_sync_queue_url = format!(
            "http://localhost:{}/000000000000/participant-{}-db-sync-queue",
            localstack_host_port, i
        );
    }

    Ok((
        (
            localstack_container,
            coordinator_db_container,
            participant_db_containers,
        ),
        sqs_client,
    ))
}

async fn create_queue(
    sqs_client: &aws_sdk_sqs::Client,
    queue_name: &str,
    attributes: Option<HashMap<QueueAttributeName, String>>,
) -> eyre::Result<()> {
    let req = sqs_client
        .create_queue()
        .queue_name(queue_name.to_string())
        .set_attributes(attributes);
    req.send().await?;

    tracing::info!("Queue created: {}", queue_name);

    Ok(())
}

async fn test_signup_sequence(
    signup_sequence: Vec<SignupSequenceElement>,
    sqs_client: aws_sdk_sqs::Client,
    e2e_config: &E2EConfig,
) -> eyre::Result<()> {
    run_signup_sequence(&signup_sequence, &sqs_client, &e2e_config, 0).await?;

    Ok(())
}

// Runs the signup sequence and asserts that the results match the expected values
// Returns the latest serial id after the signup sequence has successfully run
async fn run_signup_sequence(
    signup_sequence: &[SignupSequenceElement],
    sqs_client: &aws_sdk_sqs::Client,
    e2e_config: &E2EConfig,
    mut latest_serial_id: u64,
) -> eyre::Result<u64> {
    // When running the signup sequence after deletions, the last serial id will be the last serial id committed in the db
    // We cache this so that we can assert the signup_sequence_serial_id + serial_id_offset matches the latest_serial_id returned in the uniqueness check result
    let serial_id_offset = latest_serial_id;

    for element in signup_sequence {
        let template = Template {
            code: element.iris_code,
            mask: element.mask_code,
        };
        let signup_id = generate_random_string(4);

        // Send the query to the coordinator
        send_query(
            template,
            &sqs_client,
            &e2e_config.coordinator.queues.queries_queue_url,
            &signup_id,
            &generate_random_string(4),
        )
        .await?;

        // Wait for the uniqueness check result
        let (uniqueness_check_result, receipt_handle) =
            handle_uniqueness_check_result(&sqs_client, &e2e_config).await?;

        // Check that signup id and serial id match expected values
        assert_eq!(uniqueness_check_result.signup_id, signup_id);
        assert_eq!(uniqueness_check_result.serial_id, latest_serial_id);
        assert_eq!(
            uniqueness_check_result.matches.len(),
            element.matched_with.len(),
        );
        assert!(
            uniqueness_check_result.n_untruncated_matches as usize
                >= element.matched_with.len()
        );

        // Assert matches against expected values
        if !uniqueness_check_result.matches.is_empty() {
            for (i, distance) in
                uniqueness_check_result.matches.iter().enumerate()
            {
                assert_eq!(distance.distance, element.matched_with[i].distance);
                assert_eq!(
                    distance.serial_id,
                    element.matched_with[i].serial_id + serial_id_offset
                );
            }
        } else {
            // If there are no matches, send db sync messages to add masks/shares to the db
            let next_serial_id = latest_serial_id + 1;

            seed_db_sync(
                &sqs_client,
                &e2e_config.coordinator.queues.db_sync_queue_url,
                &[
                    &e2e_config.participant[0].queues.db_sync_queue_url,
                    &e2e_config.participant[1].queues.db_sync_queue_url,
                ],
                template,
                next_serial_id,
            )
            .await?;

            latest_serial_id = next_serial_id;

            // Sleep a little to give the nodes time to sync dbs
            tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        }
        // Delete message from queue
        aws::sqs_delete_message(
            &sqs_client,
            &e2e_config.coordinator.queues.distances_queue_url,
            receipt_handle,
        )
        .await?;
    }

    Ok(latest_serial_id)
}

pub async fn handle_uniqueness_check_result(
    sqs_client: &aws_sdk_sqs::Client,
    e2e_config: &E2EConfig,
) -> eyre::Result<(UniquenessCheckResult, String)> {
    let (uniqueness_check_result, receipt_handle) = loop {
        let result = receive_result(
            &sqs_client,
            &e2e_config.coordinator.queues.distances_queue_url,
        )
        .await?;

        let message_body = result.body.context("Could not get message body")?;

        let receipt_handle = result
            .receipt_handle
            .context("Could not get receipt handle")?;

        match serde_json::from_str::<MpcMessage>(&message_body)? {
            MpcMessage::UniquenessCheckResult(uniqueness_check_result) => {
                break (uniqueness_check_result, receipt_handle);
            }
            _ => {
                tracing::info!("Received unexpected message type, retrying");

                aws::sqs_delete_message(
                    &sqs_client,
                    &e2e_config.coordinator.queues.distances_queue_url,
                    receipt_handle,
                )
                .await?;

                continue;
            }
        }
    };

    Ok((uniqueness_check_result, receipt_handle))
}

pub async fn delete_shares(
    serial_ids: &[u64],
    participant_db_sync_queues: &[&str],
    sqs_client: &aws_sdk_sqs::Client,
) -> eyre::Result<()> {
    //Randomly generate shares
    let mut rng = rand::thread_rng();
    let shares = serial_ids
        .iter()
        .map(|_| {
            if rng.gen() {
                vec![EncodedBits::MAX, EncodedBits::ZERO]
            } else {
                vec![EncodedBits::ZERO, EncodedBits::MAX]
            }
        })
        .collect::<Vec<_>>();

    // Send the deletions through the queues
    for (id, shares) in serial_ids.iter().zip(shares) {
        for (i, participant_queue) in
            participant_db_sync_queues.iter().enumerate()
        {
            let participant_payload =
                serde_json::to_string(&vec![participant::DbSyncPayload {
                    id: *id,
                    share: shares[i],
                }])?;

            sqs_client
                .send_message()
                .queue_url(*participant_queue)
                .message_body(participant_payload)
                .send()
                .await?;
        }
    }

    for queue in participant_db_sync_queues {
        wait_for_empty_queue(sqs_client, queue).await?;
    }

    Ok(())
}

pub async fn delete_masks(
    serial_ids: &[u64],
    coordinator_db_sync_queue: &str,
    sqs_client: &aws_sdk_sqs::Client,
) -> eyre::Result<()> {
    for id in serial_ids {
        let coordinator_payload =
            serde_json::to_string(&vec![coordinator::DbSyncPayload {
                id: *id,
                mask: Bits::MAX,
            }])?;

        sqs_client
            .send_message()
            .queue_url(coordinator_db_sync_queue)
            .message_body(coordinator_payload)
            .send()
            .await?;
    }

    wait_for_empty_queue(sqs_client, coordinator_db_sync_queue).await?;

    Ok(())
}

pub async fn wait_for_queues(
    sqs_client: &aws_sdk_sqs::Client,
    queues: Vec<&str>,
) -> eyre::Result<()> {
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

pub async fn send_query(
    template: Template,
    sqs_client: &aws_sdk_sqs::Client,
    query_queue: &str,
    signup_id: &str,
    group_id: &str,
) -> eyre::Result<()> {
    tracing::info!(?signup_id, ?group_id, "Sending request");

    let request = UniquenessCheckRequest {
        plain_code: template,
        signup_id: signup_id.to_string(),
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

pub async fn receive_result(
    sqs_client: &aws_sdk_sqs::Client,
    results_queue: &str,
) -> eyre::Result<Message> {
    wait_for_messages(sqs_client, results_queue).await?;

    let messages = sqs_client
        .receive_message()
        .queue_url(results_queue)
        .max_number_of_messages(1)
        .send()
        .await?;

    let message = messages.messages.context("No messages found")?.remove(0);

    tracing::info!(?message, "Message received from queue");

    Ok(message)
}

pub async fn wait_for_messages(
    sqs_client: &aws_sdk_sqs::Client,
    results_queue: &str,
) -> eyre::Result<()> {
    loop {
        let queue_attributes = sqs_client
            .get_queue_attributes()
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .queue_url(results_queue)
            .send()
            .await?;

        let attributes = queue_attributes
            .attributes
            .expect("Could not get queue attributes ");

        let approx_num_messages = attributes
            .get(&QueueAttributeName::ApproximateNumberOfMessages)
            .expect("Could not get approximate number of messages in queue");

        if approx_num_messages == "0" {
            tracing::debug!("No messages in queue, retrying");
            tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
            continue;
        } else {
            return Ok(());
        }
    }
}

pub async fn seed_db_sync(
    sqs_client: &aws_sdk_sqs::Client,
    coordinator_db_sync_queue: &str,
    participant_db_sync_queues: &[&str],
    template: Template,
    serial_id: u64,
) -> eyre::Result<()> {
    let mut rng = rand::thread_rng();

    tracing::info!("Encoding shares");
    let shares: Box<[EncodedBits]> = mpc::distance::encode(&template)
        .share(participant_db_sync_queues.len(), &mut rng);

    let coordinator_payload =
        serde_json::to_string(&vec![coordinator::DbSyncPayload {
            id: serial_id,
            mask: template.mask,
        }])?;

    tracing::info!(
        serial_id,
        "Sending {} bytes to coordinator db sync queue",
        coordinator_payload.len()
    );

    sqs_client
        .send_message()
        .queue_url(coordinator_db_sync_queue)
        .message_body(coordinator_payload)
        .send()
        .await?;

    for (i, participant_queue) in participant_db_sync_queues.iter().enumerate()
    {
        let participant_payload =
            serde_json::to_string(&vec![participant::DbSyncPayload {
                id: serial_id,
                share: shares[i],
            }])?;

        tracing::info!(
            serial_id,
            "Sending {} bytes to participant db sync queue",
            participant_payload.len()
        );

        sqs_client
            .send_message()
            .queue_url(*participant_queue)
            .message_body(participant_payload)
            .send()
            .await?;
    }

    tracing::info!("Waiting until db sync messages are received");
    wait_for_empty_queue(sqs_client, coordinator_db_sync_queue).await?;
    for queue in participant_db_sync_queues {
        wait_for_empty_queue(sqs_client, queue).await?;
    }

    Ok(())
}

async fn wait_for_empty_queue(
    sqs_client: &aws_sdk_sqs::Client,
    queue_url: &str,
) -> eyre::Result<()> {
    loop {
        let queue_attributes = sqs_client
            .get_queue_attributes()
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .queue_url(queue_url)
            .send()
            .await?;

        let attributes = queue_attributes
            .attributes
            .context("Missing queue attributes")?;

        let approx_num_messages = attributes
            .get(&QueueAttributeName::ApproximateNumberOfMessages)
            .context("Could not get approximate number of messages in queue")?;

        if approx_num_messages == "0" {
            return Ok(());
        } else {
            tracing::debug!(queue_url, "Messages still in queue");
            tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
            continue;
        }
    }
}

pub fn generate_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
