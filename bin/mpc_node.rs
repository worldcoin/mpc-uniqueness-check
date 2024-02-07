use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use mpc::config::Config;
use mpc::coordinator::Coordinator;
use mpc::health_check::HealthCheck;
use mpc::participant::Participant;
use telemetry_batteries::metrics::batteries::StatsdBattery;
use telemetry_batteries::tracing::batteries::DatadogBattery;
use tokio::task::JoinHandle;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Parser)]
#[clap(version)]
pub struct Args {
    #[clap(short, long, env)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    dotenv::dotenv().ok();

    let args = Args::parse();

    let mut settings = config::Config::builder();

    if let Some(path) = args.config {
        settings = settings.add_source(config::File::from(path).required(true));
    }

    let settings = settings
        .add_source(
            config::Environment::with_prefix("MPC")
                .separator("__")
                .try_parsing(true),
        )
        .build()?;

    let config = settings.try_deserialize::<Config>()?;

    if let Some(service) = &config.service {
        DatadogBattery::init(None, &service.service_name, None, true);

        StatsdBattery::init(
            &service.metrics_host,
            service.metrics_port,
            service.metrics_queue_size,
            service.metrics_buffer_size,
            Some(&service.metrics_prefix),
        )?;
    } else {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().pretty().compact())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .init();
    }

    let mut tasks: Vec<JoinHandle<eyre::Result<()>>> = vec![];

    if let Some(coordinator) = config.coordinator {
        tasks.push(tokio::spawn(async move {
            let coordinator = Arc::new(Coordinator::new(coordinator).await?);

            coordinator.spawn().await?;

            Ok(())
        }));
    }

    if let Some(participant) = config.participant {
        tasks.push(tokio::spawn(async move {
            let participant = Participant::new(participant).await?;

            participant.spawn().await?;

            Ok(())
        }));
    }

    if let Some(health_check) = config.health_check {
        let health_check = HealthCheck::spawn(health_check.socket_addr);
        tasks.push(health_check);
    }

    for task in tasks {
        task.await??;
    }

    Ok(())
}
