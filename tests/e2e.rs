use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_sqs::operation::get_queue_attributes::GetQueueAttributes;
use aws_sdk_sqs::types::QueueAttributeName;
use config::{Config, FileFormat};
use mpc::config::{AwsConfig, CoordinatorConfig, ParticipantConfig};
use mpc::coordinator::Coordinator;
use mpc::db::Db;
use mpc::participant::Participant;
use mpc::utils::aws::sqs_client_from_config;
use serde::Deserialize;
use testcontainers::core::WaitFor;
use testcontainers::{clients, Container, GenericImage, Image, RunnableImage};
use testcontainers_modules::localstack::LocalStack;
use testcontainers_modules::postgres::{self, Postgres};
use url::Url;

pub const E2E_CONFIG: &str = r#"
    [coordinator]
    service_name = "mpc-coordinator"
    db.url = "postgres://postgres:postgres@localhost:8432/coordinator_db"
    db.migrate = true
    db.create = true
    queues.queries_queue_url = "http://localhost:4566/000000000000/coordinator-queries.fifo"
    queues.distances_queue_url = "http://localhost:4566/000000000000/coordinator-distances.fifo"
    queues.db_sync_queue_url = "http://localhost:4566/000000000000/coordinator-db-sync"
    participants = '["0.0.0.0:8080", "0.0.0.0:8081"]'
    hamming_distance_threshold = 0.375

    [[participant]]
    socket_addr = "0.0.0.0:8080"
    batch_size = 20000
    db.url = "postgres://postgres:postgres@localhost:8433/participant_0_db"
    db.migrate = true
    db.create = true
    queues.db_sync_queue_url = "http://localhost:4566/000000000000/participant-0-db-sync"

    [[participant]]
    socket_addr = "0.0.0.0:8081"
    batch_size = 20000
    db.url = "postgres://postgres:postgres@localhost:8434/participant_1_db"
    db.migrate = true
    db.create = true
    queues.db_sync_queue_url = "http://localhost:4566/000000000000/participant-1-db-sync"
"#;

#[derive(Debug, Deserialize)]
struct E2EConfig {
    coordinator: CoordinatorConfig,
    participant: Vec<ParticipantConfig>,
}

#[tokio::test]
async fn test_e2e() -> eyre::Result<()> {
    let settings = Config::builder()
        .add_source(config::File::from_str(
            E2E_CONFIG,
            config::FileFormat::Toml,
        ))
        .build()?;

    let mut e2e_config = settings.try_deserialize::<E2EConfig>()?;

    println!("Initializing resources");
    initialize_resources(&mut e2e_config).await?;

    let database = Arc::new(Db::new(&e2e_config.coordinator.db).await?);

    let coordinator = Coordinator::new(e2e_config.coordinator).await?;
    // let participant_0 =
    //     Participant::new(e2e_config.participant[0].clone()).await?;
    // let participant_1 =
    //     Participant::new(e2e_config.participant[1].clone()).await?;

    //TODO: spin up all of the nodes

    //TODO: run the e2e test

    Ok(())
}

async fn initialize_resources(e2e_config: &mut E2EConfig) -> eyre::Result<()> {
    let docker = clients::Cli::default();

    // Setup LocalStack
    let localstack_image = GenericImage::new("localstack/localstack", "latest")
        .with_env_var("SERVICES", "sqs")
        .with_exposed_port(4566);
    let localstack_container = docker.run(localstack_image);
    let localstack_host_port = localstack_container.get_host_port_ipv4(4566);

    let sqs_client = sqs_client_from_config(&AwsConfig {
        endpoint: Some(format!("http://localhost:{:?}", localstack_host_port)),
        region: None,
    })
    .await?;

    let coordinator_port = Url::parse(&e2e_config.coordinator.db.url)?
        .port()
        .expect("Could not extract port");

    // Setup databases for Coordinator and Participants
    let coordinator_db_container =
        setup_database_container(&docker, coordinator_port).await?;

    let port = coordinator_db_container.get_host_port_ipv4(coordinator_port);

    e2e_config.coordinator.db.url = format!(
        "postgres://postgres:postgres@localhost:{}/coordinator_db",
        port
    );

    for (i, participant) in e2e_config.participant.iter_mut().enumerate() {
        let participant_port = Url::parse(&participant.db.url)?
            .port()
            .expect("Could not extract port");

        let participant_db_container =
            setup_database_container(&docker, participant_port).await?;

        let port =
            participant_db_container.get_host_port_ipv4(participant_port);

        participant.db.url = format!(
            "postgres://postgres:postgres@localhost:{}/participant_{}_db",
            port, i
        );
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

    create_queue(
        &sqs_client,
        "coordinator-uniqueness-check.fifo",
        Some(fifo_attributes.clone()),
    )
    .await?;
    create_queue(
        &sqs_client,
        "coordinator-results-queue.fifo",
        Some(fifo_attributes),
    )
    .await?;
    create_queue(&sqs_client, "coordinator-db-sync-queue", None).await?;
    create_queue(&sqs_client, "participant-0-db-sync-queue", None).await?;
    create_queue(&sqs_client, "participant-1-db-sync-queue", None).await?;

    Ok(())
}

async fn setup_database_container(
    docker: &clients::Cli,
    port: u16,
) -> eyre::Result<Container<GenericImage>> {
    dbg!(port);
    let db_container = docker.run(
        GenericImage::new("postgres", "latest")
            .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
            .with_exposed_port(port),
    );
    Ok(db_container)
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
