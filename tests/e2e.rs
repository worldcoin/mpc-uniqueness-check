use std::collections::HashMap;

use aws_sdk_sqs::operation::get_queue_attributes::GetQueueAttributes;
use aws_sdk_sqs::types::QueueAttributeName;
use mpc::config::CoordinatorConfig;
use mpc::coordinator::Coordinator;
use mpc::utils::aws::sqs_client_from_config;
use testcontainers::{clients, RunnableImage};
use testcontainers_modules::localstack::LocalStack;
use testcontainers_modules::postgres::{self, Postgres};

#[tokio::test]
async fn e2e() -> eyre::Result<()> {
    let docker = clients::Cli::default();

    // Coordinator
    let postgres_image = Postgres::default().with_host_auth();
    let pg_node_coordinator = docker.run(postgres_image);
    let pg_port_coordinator = pg_node_coordinator.get_host_port_ipv4(5432);

    // Paricipant 0
    let postgres_image = Postgres::default().with_host_auth();
    let pg_node_participant_0 = docker.run(postgres_image);
    let pg_port_participant_0 = pg_node_participant_0.get_host_port_ipv4(5432);

    // Paricipant 1
    let postgres_image = Postgres::default().with_host_auth();
    let pg_node_participant_1 = docker.run(postgres_image);
    let pg_port_participant_1 = pg_node_participant_1.get_host_port_ipv4(5432);

    let local_stack_node = docker.run(LocalStack);
    let local_stack_host_port = local_stack_node.get_host_port_ipv4(4566);

    let sqs_client = sqs_client_from_config(&mpc::config::AwsConfig { endpoint:None, region:None }).await?;

    let fifo_attributes = {
        let mut map = HashMap::new();
        map.insert(QueueAttributeName::FifoQueue, "true".to_string());
        map.insert(QueueAttributeName::ContentBasedDeduplication, "true".to_string());
        map
    };

    create_queue(&sqs_client, "coordinator-uniqueness-check.fifo", Some(fifo_attributes.clone())).await?;
    create_queue(&sqs_client, "coordinator-results-queue.fifo", Some(fifo_attributes)).await?;
    create_queue(&sqs_client, "coordinator-db-sync-queue", None).await?;
    create_queue(&sqs_client, "participant-0-db-sync-queue", None).await?;
    create_queue(&sqs_client, "participant-1-db-sync-queue", None).await?;


    // Create coordinator

    Coordinator::new(CoordinatorConfig{

        // should just be a mpc::config::json_wrapper::JsonStrWrapper<Vec<_>>` of the addresses
      participants: vec!["localhost:5432".to_string(), "localhost:5432".to_string()],
   
        db: mpc::config::DbConfig {
            url: format!("postgres://localhost:{}", pg_port_coordinator),
            migrate: true,
            create: true,
        },
        
        hamming_distance_threshold: 0.375,
        queues: mpc::config::CoordinatorQueuesConfig {
            queries_queue_url: "https://sqs.us-east-1.amazonaws.com/1234567890/mpc-query-queue".to_string(),
            distances_queue_url: "https://sqs.us-east-1.amazonaws.com/1234567890/mpc-distance-results-queue".to_string(),
            db_sync_queue_url: "https://sqs.us-east-1.amazonaws.com/1234567890/mpc-query-queue".to_string(),
        },

        aws: mpc::config::AwsConfig {
            endpoint: Some("http://localhost:4566".to_string()),
            region: None,
        },

    }).await?;

    // Create participant 0

    // Create participant 1

    // Run signup sequence

    Ok(())
}

async fn create_queue(
    sqs_client: &aws_sdk_sqs::Client,
    queue_name: &str,
    attributes: Option<HashMap<QueueAttributeName, String>>,
) -> eyre::Result<()> {
    let  req = sqs_client.create_queue().queue_name(queue_name.to_string())set_attributes(attributes);
    req.send().await?;
    tracing::info!("Queue created: {}", queue_name);

    Ok(())
}
