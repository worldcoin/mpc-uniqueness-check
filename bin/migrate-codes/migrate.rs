#![allow(clippy::type_complexity)]

use clap::Parser;
use futures::{pin_mut, Stream, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use mpc::bits::Bits;
use mpc::db::Db;
use mpc::distance::EncodedBits;
use mpc::iris_db::{IrisCodeEntry, IrisDb};
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
    dotenv::dotenv().ok();

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

    // TODO: This is wrong, we need to run this value against the final result mapping
    // in mongo
    let latest_serial_id = mpc_db.fetch_latest_serial_id().await?;
    tracing::info!("Latest serial id {latest_serial_id}");

    let iris_db = IrisDb::new(args.iris_code_db).await?;

    let num_iris_codes = iris_db.count_iris_codes(latest_serial_id).await?;
    tracing::info!("Processing {} iris codes", num_iris_codes);

    let pb =
        ProgressBar::new(num_iris_codes).with_message("Migrating iris codes");
    let pb_style = ProgressStyle::default_bar()
        .template("{spinner:.green} {msg} [{elapsed_precise}] [{wide_bar:.green}] {pos:>7}/{len:7} ({eta})")
        .expect("Could not create progress bar");
    pb.set_style(pb_style);

    let iris_code_entries = iris_db
        .stream_whitelisted_iris_codes(latest_serial_id)
        .await?;
    let iris_code_chunks = iris_code_entries.chunks(args.batch_size);
    let iris_code_template_chunks = iris_code_chunks
        .map(|chunk| chunk.into_iter().collect::<Result<Vec<_>, _>>())
        .map(|chunk| Ok(extract_templates(chunk?)));

    handle_templates_stream(
        iris_code_template_chunks,
        &mpc_db,
        num_participants as usize,
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

pub fn extract_templates(
    iris_code_entries: Vec<IrisCodeEntry>,
) -> (Vec<(usize, Template)>, Vec<(usize, Template)>) {
    let (left_templates, right_templates) = iris_code_entries
        .into_iter()
        .map(|entry| {
            (
                (
                    entry.serial_id as usize,
                    Template {
                        code: entry.iris_code_left,
                        mask: entry.mask_code_left,
                    },
                ),
                (
                    entry.serial_id as usize,
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

async fn handle_templates_stream(
    template_chunks: impl Stream<
        Item = mongodb::error::Result<(
            Vec<(usize, Template)>,
            Vec<(usize, Template)>,
        )>,
    >,
    mpc_db: &MPCDb,
    num_participants: usize,
    pb: &ProgressBar,
) -> eyre::Result<()> {
    pin_mut!(template_chunks);

    // Consume the stream
    while let Some(template_chunk) = template_chunks.next().await {
        let (left_templates, right_templates) = template_chunk?;

        let count = left_templates.len() as u64;

        let left = handle_side_data_chunk(
            left_templates,
            num_participants,
            &mpc_db.left_coordinator_db,
            &mpc_db.left_participant_dbs,
        );

        let right = handle_side_data_chunk(
            right_templates,
            num_participants,
            &mpc_db.right_coordinator_db,
            &mpc_db.right_participant_dbs,
        );

        futures::try_join!(left, right)?;

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
    let left_data = encode_shares(templates, num_participants)?;

    insert_masks_and_shares(&left_data, coordinator_db, participant_dbs)
        .await?;

    Ok(())
}

async fn insert_masks_and_shares(
    data: &[(usize, Bits, Box<[EncodedBits]>)],
    coordinator_db: &Db,
    participant_dbs: &[Db],
) -> eyre::Result<()> {
    // Insert masks
    let left_masks: Vec<_> = data
        .iter()
        .map(|(serial_id, mask, _)| (*serial_id as u64, *mask))
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
