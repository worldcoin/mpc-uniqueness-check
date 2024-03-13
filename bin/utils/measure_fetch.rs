use clap::Args;
use mpc::config::DbConfig;
use mpc::db::Db;

#[derive(Debug, Clone, Args)]
pub struct MeasureFetch {
    #[clap(short, long)]
    pub participant_db_url: String,
}

pub async fn measure_fetch(args: &MeasureFetch) -> eyre::Result<()> {
    let db = Db::new(&DbConfig {
        url: args.participant_db_url.clone(),
        migrate: true,
        create: true,
    })
    .await?;

    let start = std::time::Instant::now();
    let shares = db.fetch_shares(0).await?;

    println!("Shares {} fetched in {:?}", shares.len(), start.elapsed());

    Ok(())
}
