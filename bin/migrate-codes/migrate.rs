use clap::Parser;
use iris_db::IrisCodeEntry;
use mpc::bits::Bits;
use mpc::db::Db;
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
    /// Connection string for the iris MongoDB
    #[clap(alias = "ic", long, env)]
    pub iris_code_db: String,
    /// Connection string for the left coordinator Postgres DB
    #[clap(alias = "lc", long, env)]
    pub left_coordinator_db: String,
    /// Connection strings for the left participant Postgres DBs
    #[clap(alias = "lp", long, env)]
    pub left_participant_db: Vec<String>,
    /// Connection strings for the right coordinator Postgres DBs
    #[clap(alias = "rc", long, env)]
    pub right_coordinator_db: String,
    /// Connection string for the right participant Postgres DB
    #[clap(alias = "rp", long, env)]
    pub right_participant_db: Vec<String>,

    #[clap(long, env, default_value = "10000")]
    pub batch_size: usize,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let _shutdown_tracing_provider = StdoutBattery::init();

    let args = Args::parse();

    eyre::ensure!(
        args.left_participant_db.len() == args.right_participant_db.len(),
        "Number of participants on left & right must match (left: {}, right: {})",
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

    insert_masks_and_shares(
        &left_data,
        &mpc_db.left_coordinator_db,
        &mpc_db.left_participant_dbs,
    )
    .await?;

    insert_masks_and_shares(
        &right_data,
        &mpc_db.right_coordinator_db,
        &mpc_db.right_participant_dbs,
    )
    .await?;

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

async fn insert_masks_and_shares(
    data: &[(usize, Bits, Box<[EncodedBits]>)],
    coordinator_db: &Db,
    participant_dbs: &[Db],
) -> eyre::Result<()> {
    // Insert masks
    let left_masks: Vec<_> = data
        .iter()
        .map(|(serial_id, mask, _)| (*serial_id as u64, mask.clone()))
        .collect();

    coordinator_db.insert_masks(&left_masks).await?;

    // Insert shares to each participant
    for (i, participant_db) in participant_dbs.iter().enumerate() {
        let shares: Vec<_> = data
            .iter()
            .map(|(serial_id, _, shares)| (*serial_id as u64, shares[i]))
            .collect();

        participant_db.insert_shares(&shares).await?;
    }

    Ok(())
}
