use clap::Args;
use mpc::config::DbConfig;
use mpc::db::kinds::Masks;
use mpc::db::Db;
use telemetry_batteries::tracing::stdout::StdoutBattery;

#[derive(Debug, Clone, Args)]
pub struct MeasureFetch {
    #[clap(short, long)]
    pub coordinator_db_url: String,
}

pub async fn measure_fetch(args: &MeasureFetch) -> eyre::Result<()> {
    let db = Db::<Masks>::new(&DbConfig {
        url: args.coordinator_db_url.clone(),
        migrate: true,
        create: true,
    })
    .await?;

    let start = std::time::Instant::now();
    let shares = db.fetch_items(0).await?;

    tracing::info!("Masks {} fetched in {:?}", shares.len(), start.elapsed());

    Ok(())
}
