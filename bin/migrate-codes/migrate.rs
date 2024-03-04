use std::time::Duration;

use clap::Parser;
use iris_db::IrisCodeEntry;
use itertools::Itertools;
use mpc::bits::Bits;
use mpc::distance::EncodedBits;
use mpc::template::Template;
use rand::thread_rng;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
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
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let _shutdown_tracing_provider = StdoutBattery::init();

    let args = Args::parse();

    assert_eq!(
        args.left_participant_db.len(),
        args.right_participant_db.len()
    );

    let num_participants = args.left_participant_db.len() as u64;

    let mpc_db = MPCDb::new(
        args.left_coordinator_db,
        args.left_participant_db,
        args.right_coordinator_db,
        args.right_participant_db,
    )
    .await?;

    let iris_db = IrisDb::new(args.iris_code_db).await?;

    let iris_code_entries = iris_db.get_iris_code_snapshot().await?;
    let (left_templates, right_templates) =
        extract_templates(iris_code_entries);

    let mut next_serial_id = mpc_db.fetch_latest_serial_id().await? + 1;

    let left_data = encode_shares(left_templates, num_participants as usize)?;
    let right_data = encode_shares(right_templates, num_participants as usize)?;

    //TODO: insert in chunks

    Ok(())
}

pub struct MPCIrisData {
    pub masks: Vec<(usize, Bits)>,
    pub shares: Vec<(usize, Box<[EncodedBits]>)>,
}

pub fn encode_shares(
    template_data: Vec<(usize, Template)>,
    num_participants: usize,
) -> eyre::Result<(Vec<(usize, Bits, Box<[EncodedBits]>)>)> {
    let iris_data = template_data
        .into_par_iter()
        .map(|(serial_id, template)| {
            let mut rng = thread_rng();

            let shares = mpc::distance::encode(&template)
                .share(num_participants, &mut rng);

            (serial_id, template.mask, shares)
        })
        .collect();

    Ok(iris_data)
}

pub fn extract_templates(
    iris_code_snapshot: Vec<IrisCodeEntry>,
) -> (Vec<(usize, Template)>, Vec<(usize, Template)>) {
    let (left_templates, right_templates) = iris_code_snapshot
        .into_iter()
        .map(|entry| {
            (
                (
                    entry.mpc_serial_id as usize,
                    Template {
                        code: entry.iris_code_left,
                        mask: entry.mask_code_left,
                    },
                ),
                (
                    entry.mpc_serial_id as usize,
                    Template {
                        code: entry.iris_code_right,
                        mask: entry.mask_code_right,
                    },
                ),
            )
        })
        .unzip();

    (left_templates, right_templates)
}
