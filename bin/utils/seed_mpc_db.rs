use std::sync::Arc;

use clap::Args;
use futures::stream::FuturesUnordered;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use mpc::bits::Bits;
use mpc::config::DbConfig;
use mpc::db::Db;
use mpc::distance::EncodedBits;
use mpc::rng_source::RngSource;
use mpc::template::Template;
use mpc::utils::tasks::finalize_futures_unordered;
use rand::{thread_rng, Rng};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

#[derive(Debug, Clone, Args)]
pub struct SeedMPCDb {
    #[clap(short, long)]
    pub coordinator_db_url: String,

    #[clap(short, long)]
    pub participant_db_url: Vec<String>,

    #[clap(short, long, default_value = "100000")]
    pub num_templates: usize,

    #[clap(short, long, default_value = "100")]
    pub batch_size: usize,

    #[clap(short, long, env, default_value = "thread")]
    pub rng: RngSource,
}

pub async fn seed_mpc_db(args: &SeedMPCDb) -> eyre::Result<()> {
    if args.participant_db_url.is_empty() {
        return Err(eyre::eyre!("No participant DBs provided"));
    }

    let (coordinator_db, participant_dbs) = initialize_dbs(args).await?;

    let now = std::time::Instant::now();

    let latest_serial_id =
        get_latest_serial_id(coordinator_db.clone(), participant_dbs.clone())
            .await?;

    let templates = generate_templates(args);
    println!("Templates generated in {:?}", now.elapsed());

    let (batched_masks, batched_shares) = generate_shares_and_masks(
        args,
        templates,
        (latest_serial_id + 1) as usize,
    );
    println!("Shares and masks generated in {:?}", now.elapsed());

    insert_masks_and_shares(
        batched_masks,
        batched_shares,
        coordinator_db,
        participant_dbs,
        args.num_templates,
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
    next_serial_id: usize,
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
        let mut batch_masks = Vec::with_capacity(chunk.len());
        let mut batch_shares: Vec<_> = (0..num_participants)
            .map(|_| Vec::with_capacity(chunk.len()))
            .collect();

        let shares_chunk = chunk
            .into_par_iter()
            .map(|template| {
                let mut rng = thread_rng();

                let shares = mpc::distance::encode(template)
                    .share(num_participants, &mut rng);

                pb.inc(1);
                shares
            })
            .collect::<Vec<Box<[EncodedBits]>>>();

        for (offset, (shares, template)) in
            shares_chunk.iter().zip(chunk).enumerate()
        {
            let id = offset + (idx * args.batch_size) + next_serial_id;

            batch_masks.push((id as u64, template.mask));

            for (idx, share) in shares.iter().enumerate() {
                batch_shares[idx].push((id as u64, *share));
            }
        }

        batched_shares.push(batch_shares);
        batched_masks.push(batch_masks);
    }

    pb.finish_with_message("Created shares and masks");

    (batched_masks, batched_shares)
}

async fn insert_masks_and_shares(
    batched_masks: BatchedMasks,
    batched_shares: BatchedShares,
    coordinator_db: Arc<Db>,
    participant_dbs: Vec<Arc<Db>>,
    num_templates: usize,
    batch_size: usize,
) -> eyre::Result<()> {
    println!("Inserting masks and shares into db...");

    // Commit shares and masks to db
    let mpb = MultiProgress::new();
    let style = ProgressStyle::default_bar().template("{spinner:.green} {msg} [{elapsed_precise}] [{wide_bar:.green}] {pos:>7}/{len:7} ({eta})")?;

    let participant_progress_bars = participant_dbs
        .iter()
        .enumerate()
        .map(|(i, _)| {
            mpb.add(ProgressBar::new(num_templates as u64).with_message(
                format!("Inserting shares for participant {}", i),
            ))
        })
        .collect::<Vec<_>>();

    for pb in participant_progress_bars.iter() {
        pb.set_style(style.clone());
    }

    let coordinator_progress_bar = mpb.add(
        ProgressBar::new(num_templates as u64)
            .with_message("Inserting masks for coordinator"),
    );
    coordinator_progress_bar.set_style(style.clone());

    for (shares, masks) in
        batched_shares.into_iter().zip(batched_masks.into_iter())
    {
        let tasks = FuturesUnordered::new();

        for (idx, db) in participant_dbs.iter().enumerate() {
            let shares = shares[idx].clone();
            let db = db.clone();
            let pb = participant_progress_bars[idx].clone();

            tasks.push(tokio::spawn(async move {
                db.insert_shares(&shares).await?;
                pb.inc(batch_size as u64);
                eyre::Result::<()>::Ok(())
            }));
        }

        let coordinator_db = coordinator_db.clone();
        let coordinator_progress_bar = coordinator_progress_bar.clone();
        tasks.push(tokio::spawn(async move {
            coordinator_db.insert_masks(&masks).await?;
            coordinator_progress_bar.inc(batch_size as u64);
            eyre::Result::<()>::Ok(())
        }));

        finalize_futures_unordered(tasks).await?;
    }

    Ok(())
}

async fn get_latest_serial_id(
    coordinator_db: Arc<Db>,
    participant_dbs: Vec<Arc<Db>>,
) -> eyre::Result<u64> {
    let coordinator_serial_id = coordinator_db.fetch_latest_mask_id().await?;
    println!("Coordinator serial id: {}", coordinator_serial_id);

    for (i, db) in participant_dbs.iter().enumerate() {
        let participant_id = db.fetch_latest_share_id().await?;

        println!("Participant {} ids: {}", i, participant_id);
        assert_eq!(
            coordinator_serial_id, participant_id,
            "Databases are not in sync"
        );
    }

    Ok(coordinator_serial_id)
}
