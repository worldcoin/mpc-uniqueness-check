use std::path::PathBuf;

use aws_config::BehaviorVersion;
use clap::Parser;
use mpc::config::CoordinatorConfig;
use mpc::coordinator::Coordinator;
use telemetry_batteries::metrics::batteries::StatsdBattery;
use telemetry_batteries::tracing::batteries::DatadogBattery;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub const SERVICE_NAME: &str = "mpc-coordinator";

pub const METRICS_HOST: &str = "localhost";
pub const METRICS_PORT: u16 = 8125;
pub const METRICS_QUEUE_SIZE: usize = 5000;
pub const METRICS_BUFFER_SIZE: usize = 1024;
pub const METRICS_PREFIX: &str = "mpc-coordinator";

#[derive(Parser)]
#[clap(version)]
pub struct Args {
    #[clap(short, long, env)]
    telemetry: bool,

    #[clap(short, long, env)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    dotenv::dotenv().ok();

    let args = Args::parse();

    if args.telemetry {
        DatadogBattery::init(None, SERVICE_NAME, None, true);

        StatsdBattery::init(
            METRICS_HOST,
            METRICS_PORT,
            METRICS_QUEUE_SIZE,
            METRICS_BUFFER_SIZE,
            Some(METRICS_PREFIX),
        )?;
    } else {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().pretty().compact())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .init();
    }

    let mut settings = config::Config::builder();

    if let Some(path) = args.config {
        settings = settings.add_source(config::File::from(path).required(true));
    }

    let settings = settings
        .add_source(config::Environment::with_prefix("MPC").separator("__"))
        .build()?;

    let config = settings.try_deserialize::<CoordinatorConfig>()?;

    let coordinator = Coordinator::new(
        vec![],
        "template_queue_url",
        "distance_queue_url",
        0.375,
        1000,
    )
    .await?;

    coordinator.spawn().await?;

    Ok(())
}
