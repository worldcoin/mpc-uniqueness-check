use std::mem;
use std::sync::Arc;
use std::thread::available_parallelism;
use std::time::Duration;

use clap::Args;
use eyre::Error;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use mpc::bits::Bits;
use mpc::config::DbConfig;
use mpc::db::Db;
use mpc::distance::EncodedBits;
use mpc::template::Template;
use rand::{thread_rng, Rng};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::{current_num_threads, ThreadPoolBuilder};

#[derive(Debug, Clone, Args)]
pub struct SeedMPCDb {
    #[clap(short, long)]
    pub coordinator_db_url: String,

    #[clap(short, long)]
    pub participant_db_url: Vec<String>,

    #[clap(short, long, default_value = "1000000")]
    pub num_templates: usize,

    #[clap(short, long, default_value = "5000")]
    pub batch_size: usize,
}

pub async fn seed_mpc_db(args: &SeedMPCDb) -> eyre::Result<()> {
    if args.participant_db_url.is_empty() {
        return Err(eyre::eyre!("No participant DBs provided"));
    }

    let (coordinator_db, participant_dbs) = initialize_dbs(args).await?;

    let now = std::time::Instant::now();

    let templates = generate_templates(args);
    println!("Templates generated in {:?}", now.elapsed());

    let (batched_masks, batched_shares) =
        generate_shares_and_masks(args, templates);
    println!("Shares and masks generated in {:?}", now.elapsed());

    insert_masks_and_shares(
        batched_masks,
        batched_shares,
        coordinator_db,
        participant_dbs,
        args.batch_size,
    )
    .await?;

    println!("Time elapsed: {:?}", now.elapsed());

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
    let pb = ProgressBar::new(args.num_templates as u64)
        .with_message("Generating templates...");

    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} {msg} [{elapsed_precise}] [{wide_bar:.green}] {pos:>7}/{len:7} ({eta})")
        .expect("Could not create progress bar"));

    // Generate templates
    let templates = (0..args.num_templates)
        .into_par_iter()
        .map(|_| {
            let mut rng = thread_rng();

            let template = rng.gen();

            pb.inc(1);
            template
        })
        .collect::<Vec<Template>>();

    pb.finish_with_message("Created templates");

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

    let pb = ProgressBar::new(args.num_templates as u64)
        .with_message("Generating shares and masks...");

    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} {msg} [{elapsed_precise}] [{wide_bar:.green}] {pos:>7}/{len:7} ({eta})")
        .expect("Could not create progress bar"));

    for (idx, chunk) in templates.chunks(args.batch_size).enumerate() {
        let mut chunk_masks = Vec::with_capacity(chunk.len());
        let mut chunk_shares: Vec<_> = (0..num_participants)
            .map(|_| Vec::with_capacity(chunk.len()))
            .collect();

        let shares_chunk = chunk
            .into_par_iter()
            .map(|template| {
                let shares =
                    mpc::distance::encode(template).share(num_participants);

                pb.inc(1);
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

    pb.finish_with_message("Created shares and masks");

    (batched_masks, batched_shares)
}

async fn insert_masks_and_shares(
    batched_masks: BatchedMasks,
    batched_shares: BatchedShares,
    coordinator_db: Arc<Db>,
    participant_dbs: Vec<Arc<Db>>,
    batch_size: usize,
) -> eyre::Result<()> {
    println!("Inserting masks and shares into db...");

    // Commit shares and masks to db
    let mut tasks = FuturesUnordered::new();

    tasks.push(tokio::spawn(insert_masks(
        batched_masks,
        coordinator_db,
        batch_size,
    )));

    tasks.push(tokio::spawn(insert_shares(
        batched_shares,
        participant_dbs,
        batch_size,
    )));

    while let Some(result) = tasks.next().await {
        result??;
    }

    Ok(())
}

async fn insert_masks(
    batched_masks: BatchedMasks,
    coordinator_db: Arc<Db>,
    num_templates: usize,
) -> eyre::Result<()> {
    let pb = ProgressBar::new(num_templates as u64)
        .with_message("Inserting masks...");

    pb.set_style(ProgressStyle::default_bar()
    .template("{spinner:.green} {msg} [{elapsed_precise}] [{wide_bar:.green}] {pos:>7}/{len:7} ({eta})")
    .expect("Could not create progress bar"));

    pb.inc(0);

    let mut tasks = FuturesUnordered::new();

    for masks in batched_masks.iter() {
        tasks.push(coordinator_db.insert_masks(masks));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    while let Some(result) = tasks.next().await {
        result?;
    }

    pb.finish_with_message("Inserted masks");

    Ok(())
}

async fn insert_shares(
    batched_shares: BatchedShares,
    participant_dbs: Vec<Arc<Db>>,
    num_templates: usize,
) -> eyre::Result<()> {
    let pb = ProgressBar::new(num_templates as u64)
        .with_message("Inserting shares...");

    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} {msg} [{elapsed_precise}] [{wide_bar:.green}] {pos:>7}/{len:7} ({eta})")
        .expect("Could not create progress bar"));
    pb.inc(0);

    let mut tasks = FuturesUnordered::new();

    for mut shares in batched_shares.into_iter() {
        for (idx, db) in participant_dbs.iter().enumerate() {
            let participant_shares = mem::take(&mut shares[idx]);
            let db = db.clone();

            tasks.push(tokio::spawn(async move {
                db.insert_shares(&participant_shares).await?;
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok::<_, Error>(idx)
            }));
        }
    }

    while let Some(result) = tasks.next().await {
        result??;
    }

    pb.finish_with_message("Inserted shares");

    Ok(())
}
