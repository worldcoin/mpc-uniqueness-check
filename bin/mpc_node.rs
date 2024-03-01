use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use mpc::config::{load_config, Config};
use mpc::coordinator::Coordinator;
use mpc::health_check::HealthCheck;
use mpc::participant::Participant;
use telemetry_batteries::metrics::statsd::StatsdBattery;
use telemetry_batteries::tracing::datadog::DatadogBattery;
use telemetry_batteries::tracing::TracingShutdownHandle;
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

    let config: Config = load_config("MPC", args.config.as_deref())?;

    let _tracing_shutdown_handle = if let Some(service) = &config.service {
        let tracing_shutdown_handle = DatadogBattery::init(
            service.traces_endpoint.as_deref(),
            &service.service_name,
            None,
            true,
        );

        if let Some(metrics_config) = &service.metrics {
            StatsdBattery::init(
                &metrics_config.host,
                metrics_config.port,
                metrics_config.queue_size,
                metrics_config.buffer_size,
                Some(&metrics_config.prefix),
            )?;
        }

        tracing_shutdown_handle
    } else {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().pretty().compact())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .init();

        TracingShutdownHandle
    };

    let mut tasks: Vec<JoinHandle<eyre::Result<()>>> = vec![];

    if let Some(coordinator) = config.coordinator {
        let coordinator = Arc::new(Coordinator::new(coordinator).await?);

        tasks.push(tokio::spawn(async move {
            coordinator.spawn().await?;

            Ok(())
        }));
    }

    if let Some(participant) = config.participant {
        let participant = Arc::new(Participant::new(participant).await?);

        tasks.push(tokio::spawn(async move {
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
