use std::path::PathBuf;

use ::config::{Config, File};
use clap::Parser;
use mpc::db;
use mpc::template::Template;
use mpc_db::MPCDbConfig;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde::Deserialize;
use telemetry_batteries::tracing::stdout::StdoutBattery;

use crate::iris_db::{IrisCodeEntry, IrisDb};
use crate::mpc_db::MPCDbs;

mod iris_db;
mod mpc_db;

#[derive(Parser)]
pub struct Args {
    #[clap(short, long, env)]
    config: PathBuf,
}

#[derive(Deserialize)]
pub struct SeedDbConfig {
    pub iris_code_db: String,
    pub mpc_db: MPCDbConfig,
}

//TODO: update this
pub const INSERTION_BATCH_SIZE: usize = 10;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let _shutdown_tracing_provider = StdoutBattery::init();

    let args = Args::parse();
    let settings = Config::builder()
        .add_source(File::from(args.config).required(true))
        .build()?;
    let config = settings.try_deserialize::<SeedDbConfig>()?;

    assert_eq!(
        config.mpc_db.left_participant_dbs.len(),
        config.mpc_db.right_participant_dbs.len()
    );

    // Connect to the dbs
    let mpc_db = MPCDbs::new(config.mpc_db).await?;
    let iris_db = IrisDb::new(&config.iris_code_db).await?;

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

//NOTE: util for testing TODO: delete this
pub async fn populate_iris_db() -> eyre::Result<()> {
    let iris_db = IrisDb::new("").await?;

    let num_templates = 100;
    let mut rng = thread_rng();

    let left_templates = (0..num_templates)
        .map(|_| rng.gen())
        .collect::<Vec<Template>>();

    let right_templates = (0..num_templates)
        .map(|_| rng.gen())
        .collect::<Vec<Template>>();

    let collection = iris_db.db.collection::<IrisCodeEntry>("codes.v2"); // Specify your collection name

    for (serial_id, (left, right)) in left_templates
        .iter()
        .zip(right_templates.iter())
        .enumerate()
    {
        let iris_code_entry = IrisCodeEntry {
            signup_id: generate_random_string(10),
            serial_id: serial_id as u64,
            iris_code_left: left.code,
            mask_code_left: left.mask,
            iris_code_right: right.code,
            mask_code_right: right.mask,
        };

        collection.insert_one(iris_code_entry, None).await?;
    }

    Ok(())
}

//NOTE: util for testing TODO: delete this
fn generate_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
