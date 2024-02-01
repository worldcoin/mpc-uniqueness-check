use std::net::{SocketAddr, SocketAddrV4};
use std::path::PathBuf;

use clap::Parser;
use mpc::participant::Participant;
use telemetry_batteries::metrics::batteries::StatsdBattery;
use telemetry_batteries::tracing::batteries::DatadogBattery;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub const SERVICE_NAME: &str = "mpc-node";

pub const METRICS_HOST: &str = "localhost";
pub const METRICS_PORT: u16 = 8125;
pub const METRICS_QUEUE_SIZE: usize = 5000;
pub const METRICS_BUFFER_SIZE: usize = 1024;
pub const METRICS_PREFIX: &str = "mpc-node";

#[derive(Parser)]
#[clap(version)]
pub struct Args {
    #[clap(short, long, env)]
    local: bool,

    #[clap(short, long, env)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    dotenv::dotenv().ok();

    let args = Args::parse();

    // Initialize tracing exporter
    if args.local {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().pretty().compact())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .init();
    } else {
        DatadogBattery::init(None, SERVICE_NAME, None, true);
    }

    // Initalize metrics exporter
    StatsdBattery::init(
        METRICS_HOST,
        METRICS_PORT,
        METRICS_QUEUE_SIZE,
        METRICS_BUFFER_SIZE,
        Some(METRICS_PREFIX),
    )?;

    let mut settings = config::Config::builder();

    if let Some(path) = args.config {
        settings = settings.add_source(config::File::from(path).required(true));
    }

    let _settings = settings
        .add_source(config::Environment::with_prefix("MPC").separator("__"))
        .build()?;

    let participant =
        Participant::new("127.0.0.1:8080".parse::<SocketAddr>()?, 20_000)
            .await?;

    participant.spawn().await?;

    Ok(())
}
