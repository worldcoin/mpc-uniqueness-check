use std::path::PathBuf;

use ::config::{Config, File};
use clap::Parser;
use mpc::db;
use mpc::template::Template;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde::Deserialize;
use telemetry_batteries::tracing::stdout::StdoutBattery;

use crate::iris_db::{IrisCodeEntry, IrisDb};
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
}

//TODO: update this to be configurable
pub const INSERTION_BATCH_SIZE: usize = 10;

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

    //TODO: Get the latest serial ids from all mpc db
    let mut latest_serial_id = 0;

    let iris_code_entries = iris_db.get_iris_code_snapshot().await?;

    for entries in iris_code_entries.chunks(INSERTION_BATCH_SIZE) {
        let (left_templates, right_templates): (Vec<Template>, Vec<Template>) =
            entries
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
                latest_serial_id,
                &left_templates,
                &right_templates,
            )
            .await?;

        latest_serial_id += 1;
    }

    Ok(())
}
