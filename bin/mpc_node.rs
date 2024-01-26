use clap::Parser;
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

    let mut n = 0;

    loop {
        foo(n).await;

        n += 1;

        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

#[tracing::instrument]
async fn foo(n: usize) {
    tracing::info!(n, "Foo");

    metrics::gauge!("foo", n as f64);
}
