#![allow(clippy::type_complexity)]

use clap::Parser;
use eyre::ContextCompat;
use futures::{pin_mut, Stream, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use mpc::bits::Bits;
use mpc::db::Db;
use mpc::distance::EncodedBits;
use mpc::iris_db::{
    FinalResult, IrisCodeEntry, IrisDb, SideResult, FINAL_RESULT_STATUS,
};
use mpc::template::Template;
use rand::thread_rng;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use telemetry_batteries::tracing::stdout::StdoutBattery;

use crate::mpc_db::MPCDb;

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

    /// Batch size for encoding shares
    #[clap(short, long, default_value = "100")]
    batch_size: usize,

    /// If set to true, no migration or creation of the database will occur on the Postgres side
    #[clap(long)]
    no_migrate_or_create: bool,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    dotenvy::dotenv().ok();

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
        args.no_migrate_or_create,
    )
    .await?;

    let iris_db = IrisDb::new(args.iris_code_db).await?;

    let latest_serial_id = mpc_db.fetch_latest_serial_id().await?;
    tracing::info!("Latest serial id {latest_serial_id}");

    // Cleanup items with larger ids
    // as they might be assigned new values in the future
    mpc_db.prune_items(latest_serial_id).await?;
    iris_db.prune_final_results(latest_serial_id).await?;

    let first_unsynced_iris_serial_id = if let Some(final_result) = iris_db
        .get_final_result_by_serial_id(latest_serial_id)
        .await?
    {
        iris_db
            .get_entry_by_signup_id(&final_result.signup_id)
            .await?
            .context("Could not find iris code entry")?
            .serial_id
    } else {
        0
    };

    let num_iris_codes = iris_db
        .count_whitelisted_iris_codes(first_unsynced_iris_serial_id)
        .await?;
    tracing::info!("Processing {} iris codes", num_iris_codes);

    let pb =
        ProgressBar::new(num_iris_codes).with_message("Migrating iris codes");
    let pb_style = ProgressStyle::default_bar()
        .template("{spinner:.green} {msg} [{elapsed_precise}] [{wide_bar:.green}] {pos:>7}/{len:7} ({eta})")
        .expect("Could not create progress bar");
    pb.set_style(pb_style);

    let iris_code_entries = iris_db
        .stream_whitelisted_iris_codes(first_unsynced_iris_serial_id)
        .await?
        .chunks(args.batch_size)
        .map(|chunk| chunk.into_iter().collect::<Result<Vec<_>, _>>());

    handle_templates_stream(
        iris_code_entries,
        &iris_db,
        &mpc_db,
        num_participants as usize,
        latest_serial_id,
        &pb,
    )
    .await?;

    pb.finish();

    Ok(())
}

pub struct MPCIrisData {
    pub masks: Vec<(usize, Bits)>,
    pub shares: Vec<(usize, Box<[EncodedBits]>)>,
}

pub fn encode_shares(
    template_data: Vec<(usize, Template)>,
    num_participants: usize,
) -> eyre::Result<Vec<(usize, Bits, Box<[EncodedBits]>)>> {
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

async fn handle_templates_stream(
    iris_code_entries: impl Stream<
        Item = mongodb::error::Result<Vec<IrisCodeEntry>>,
    >,
    iris_db: &IrisDb,
    mpc_db: &MPCDb,
    num_participants: usize,
    mut latest_serial_id: u64,
    pb: &ProgressBar,
) -> eyre::Result<()> {
    pin_mut!(iris_code_entries);

    // Consume the stream
    while let Some(entries) = iris_code_entries.next().await {
        let entries = entries?;

        let count = entries.len() as u64;

        let left_data: Vec<_> = entries
            .iter()
            .enumerate()
            .map(|(idx, entry)| {
                let template = Template {
                    code: entry.iris_code_left,
                    mask: entry.mask_code_left,
                };

                (latest_serial_id as usize + 1 + idx, template)
            })
            .collect();

        let right_data: Vec<_> = entries
            .iter()
            .enumerate()
            .map(|(idx, entry)| {
                let template = Template {
                    code: entry.iris_code_right,
                    mask: entry.mask_code_right,
                };

                (latest_serial_id as usize + 1 + idx, template)
            })
            .collect();

        let left = handle_side_data_chunk(
            left_data,
            num_participants,
            &mpc_db.left_coordinator_db,
            &mpc_db.left_participant_dbs,
        );

        let right = handle_side_data_chunk(
            right_data,
            num_participants,
            &mpc_db.right_coordinator_db,
            &mpc_db.right_participant_dbs,
        );

        let final_results: Vec<_> = entries
            .iter()
            .enumerate()
            .map(|(idx, entry)| FinalResult {
                status: FINAL_RESULT_STATUS.to_string(),
                serial_id: latest_serial_id + 1 + idx as u64,
                signup_id: entry.signup_id.clone(),
                unique: true,
                right_result: SideResult {},
                left_result: SideResult {},
            })
            .collect();

        let results = iris_db.save_final_results(&final_results);

        futures::try_join!(left, right, results)?;

        latest_serial_id += count;
        pb.inc(count);
    }

    Ok(())
}

async fn handle_side_data_chunk(
    templates: Vec<(usize, Template)>,
    num_participants: usize,
    coordinator_db: &Db,
    participant_dbs: &[Db],
) -> eyre::Result<()> {
    let data = encode_shares(templates, num_participants)?;

    insert_masks_and_shares(&data, coordinator_db, participant_dbs).await?;

    Ok(())
}

async fn insert_masks_and_shares(
    data: &[(usize, Bits, Box<[EncodedBits]>)],
    coordinator_db: &Db,
    participant_dbs: &[Db],
) -> eyre::Result<()> {
    let masks: Vec<_> = data
        .iter()
        .map(|(serial_id, mask, _)| (*serial_id as u64, *mask))
        .collect();

    coordinator_db.insert_masks(&masks).await?;

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
