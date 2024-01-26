use clap::Parser;
use telemetry_batteries::{metrics::batteries::StatsdBattery, tracing::batteries::DatadogBattery};

pub const SERVICE_NAME: &str = "service";

pub const METRICS_HOST: &str = "localhost";
pub const METRICS_PORT: u16 = 8125;
pub const METRICS_QUEUE_SIZE: usize = 5000;
pub const METRICS_BUFFER_SIZE: usize = 1024;
pub const METRICS_PREFIX: &str = "service";

#[derive(Parser)]
pub struct Args {}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let _args = Args::parse();

    // Initialize tracing exporter
    DatadogBattery::init(None, SERVICE_NAME, None, true);

    // Initalize metrics exporter
    StatsdBattery::init(
        METRICS_HOST,
        METRICS_PORT,
        METRICS_QUEUE_SIZE,
        METRICS_BUFFER_SIZE,
        Some(METRICS_PREFIX),
    )?;

    Ok(())
}
