use std::time::Duration;

use clap::Parser;
use mpc::template::Template;
use telemetry_batteries::tracing::stdout::StdoutBattery;

use crate::iris_db::IrisDb;
use crate::mpc_db::MPCDb;

mod iris_db;
mod mpc_db;

#[derive(Parser)]
pub struct Args {
    #[clap(long, env)]
    pub iris_code_db: String,
    #[clap(long, env)]
    pub left_coordinator_db: String,
    #[clap(long, env)]
    pub left_participant_db: Vec<String>,
    #[clap(long, env)]
    pub right_coordinator_db: String,
    #[clap(long, env)]
    pub right_participant_db: Vec<String>,
    #[clap(long, env, default_value = "10000")]
    pub batch_size: usize,
    #[clap(long, env, default_value = "28800")] //NOTE: default 8 Hours
    pub wait_time: usize,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let _shutdown_tracing_provider = StdoutBattery::init();

    let args = Args::parse();

    assert_eq!(
        args.left_participant_db.len(),
        args.right_participant_db.len()
    );

    // Connect to the dbs
    let mpc_db = MPCDb::new(
        args.left_coordinator_db,
        args.left_participant_db,
        args.right_coordinator_db,
        args.right_participant_db,
    )
    .await?;

    let iris_db = IrisDb::new(args.iris_code_db).await?;

    loop {
        let iris_code_entries = iris_db.get_iris_code_snapshot().await?;

        let mut next_serial_id = mpc_db.fetch_latest_serial_id().await? + 1;

        for entries in
            iris_code_entries[next_serial_id as usize..].chunks(args.batch_size)
        {
            let (left_templates, right_templates): (
                Vec<Template>,
                Vec<Template>,
            ) = entries
                .iter()
                .map(|entry| {
                    (
                        Template {
                            code: entry.iris_code_left,
                            mask: entry.mask_code_left,
                        },
                        Template {
                            code: entry.iris_code_left,
                            mask: entry.mask_code_left,
                        },
                    )
                })
                .unzip();

            mpc_db
                .insert_shares_and_masks(
                    next_serial_id,
                    &left_templates,
                    &right_templates,
                )
                .await?;

            next_serial_id += 1;
        }

        tokio::time::sleep(Duration::from_secs(args.wait_time as u64)).await;
    }

    Ok(())
}
