use core::num;
use std::mem;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use clap::Args;
use eyre::Error;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use indicatif::ProgressBar;
use metrics::atomics::AtomicU64;
use mpc::bits::Bits;
use mpc::config::DbConfig;
use mpc::db::Db;
use mpc::distance::EncodedBits;
use mpc::template::Template;
use mpc::{coordinator, template};
use rand::{thread_rng, Rng};
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, ParallelIterator,
};
use sqlx::Encode;

#[derive(Debug, Clone, Args)]
pub struct SeedMPCDb {
    #[clap(short, long)]
    pub coordinator_db_url: String,

    #[clap(short, long)]
    pub participant_db_url: Vec<String>,

    #[clap(short, long, default_value = "3000000")]
    pub num_templates: usize,

    #[clap(short, long, default_value = "3000")]
    pub batch_size: usize,
}

pub async fn seed_mpc_db(args: &SeedMPCDb) -> eyre::Result<()> {
    if args.participant_db_url.is_empty() {
        return Err(eyre::eyre!("No participant DBs provided"));
    }

    let (coordinator_db, participant_dbs) = initialize_dbs(args).await?;

    let templates = generate_templates(args);

    let (batched_masks, batched_shares) =
        generate_shares_and_masks(args, templates);

    insert_masks_and_shares(
        batched_masks,
        batched_shares,
        coordinator_db,
        participant_dbs,
        args.batch_size,
        args.num_templates,
    )
    .await?;

    Ok(())
}

async fn initialize_dbs(
    args: &SeedMPCDb,
) -> eyre::Result<(Arc<Db>, Vec<Arc<Db>>)> {
    let coordinator_db = Arc::new(
        Db::new(&DbConfig {
            url: args.coordinator_db_url.clone(),
            migrate: true,
            create: true,
        })
        .await?,
    );

    let mut participant_dbs = vec![];

    for db_config in args.participant_db_url.iter() {
        participant_dbs.push(Arc::new(
            Db::new(&DbConfig {
                url: db_config.clone(),
                migrate: true,
                create: true,
            })
            .await?,
        ));
    }

    Ok((coordinator_db, participant_dbs))
}

fn generate_templates(args: &SeedMPCDb) -> Vec<Template> {
    // Generate templates
    let template_counter = AtomicU64::new(0);
    let templates = (0..args.num_templates)
        .into_par_iter()
        .map(|_| {
            let mut rng = thread_rng();

            println!(
                "Generating template {}/{}",
                template_counter.load(Ordering::Relaxed),
                args.num_templates
            );
            let template = rng.gen();
            template_counter.fetch_add(1, Ordering::Relaxed);
            template
        })
        .collect::<Vec<Template>>();

    templates
}

pub type BatchedShares = Vec<Vec<Vec<(u64, EncodedBits)>>>;
pub type BatchedMasks = Vec<Vec<(u64, Bits)>>;

fn generate_shares_and_masks(
    args: &SeedMPCDb,
    templates: Vec<Template>,
) -> (BatchedMasks, BatchedShares) {
    // Generate shares and masks
    let mut batched_shares = vec![];
    let mut batched_masks = vec![];

    let num_participants = args.participant_db_url.len();

    for (idx, chunk) in templates.chunks(args.batch_size).enumerate() {
        let mut chunk_masks = Vec::with_capacity(chunk.len());
        let mut chunk_shares: Vec<_> = (0..num_participants)
            .map(|_| Vec::with_capacity(chunk.len()))
            .collect();

        let encoded_shares_counter =
            AtomicU64::new((idx * args.batch_size) as u64);

        let shares_chunk = chunk
            .into_par_iter()
            .enumerate()
            .map(|(i, template)| {
                println!(
                    "Encoding template {}/{}",
                    encoded_shares_counter.load(Ordering::Relaxed),
                    args.num_templates
                );
                let shares =
                    mpc::distance::encode(template).share(num_participants);

                encoded_shares_counter.fetch_add(1, Ordering::Relaxed);

                shares
            })
            .collect::<Vec<Box<[EncodedBits]>>>();

        for (offset, (shares, template)) in
            shares_chunk.iter().zip(chunk).enumerate()
        {
            let id = offset + (idx * args.batch_size);

            chunk_masks.push((id as u64, template.mask));

            for (idx, share) in shares.iter().enumerate() {
                chunk_shares[idx].push((id as u64, *share));
            }
        }

        batched_shares.push(chunk_shares);
        batched_masks.push(chunk_masks);
    }

    (batched_masks, batched_shares)
}

async fn insert_masks_and_shares(
    batched_masks: BatchedMasks,
    batched_shares: BatchedShares,
    coordinator_db: Arc<Db>,
    participant_dbs: Vec<Arc<Db>>,
    batch_size: usize,
    num_templates: usize,
) -> eyre::Result<()> {
    // Commit shares and masks to db

    let mut tasks = FuturesUnordered::new();

    tasks.push(tokio::spawn(insert_masks(
        batched_masks,
        coordinator_db,
        batch_size,
        num_templates,
    )));

    tasks.push(tokio::spawn(insert_shares(
        batched_shares,
        participant_dbs,
        batch_size,
        num_templates,
    )));

    while let Some(result) = tasks.next().await {
        result??;
    }

    Ok(())
}

async fn insert_masks(
    batched_masks: BatchedMasks,
    coordinator_db: Arc<Db>,
    batch_size: usize,
    num_templates: usize,
) -> eyre::Result<()> {
    for (i, masks) in batched_masks.iter().enumerate() {
        coordinator_db.insert_masks(&masks).await?;

        println!(
            "Inserted masks {}/{} into coordinator db",
            (i + 1) * batch_size,
            num_templates
        );
    }
    Ok(())
}

async fn insert_shares(
    batched_shares: BatchedShares,
    participant_dbs: Vec<Arc<Db>>,
    batch_size: usize,
    num_templates: usize,
) -> eyre::Result<()> {
    let mut tasks = FuturesUnordered::new();

    for (i, mut shares) in batched_shares.into_iter().enumerate() {
        for (idx, db) in participant_dbs.iter().enumerate() {
            let participant_shares = mem::take(&mut shares[idx]);
            let db = db.clone();

            tasks.push(tokio::spawn(async move {
                db.insert_shares(&participant_shares).await?;
                Ok::<_, Error>(idx)
            }));
        }

        while let Some(result) = tasks.next().await {
            let idx = result??;
            println!(
                "Inserted shares {}/{} into participant {} db",
                (i + 1) * batch_size,
                num_templates,
                idx,
            );
        }
    }
    Ok(())
}
