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

    Ok(())
}
