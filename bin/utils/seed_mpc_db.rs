use std::mem;
use std::sync::Arc;

use clap::Args;
use eyre::Error;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use indicatif::ProgressBar;
use mpc::config::DbConfig;
use mpc::coordinator;
use mpc::db::Db;
use mpc::template::Template;
use rand::{thread_rng, Rng};

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
    let now = std::time::Instant::now();

    if args.participant_db_url.is_empty() {
        return Err(eyre::eyre!("No participant DBs provided"));
    }

    let mut templates: Vec<Template> = Vec::with_capacity(args.num_templates);

    println!("Generating templates");
    let mut rng = thread_rng();

    for _ in 0..args.num_templates {
        println!(
            "Generating template {}/{}",
            templates.len(),
            args.num_templates
        );
        templates.push(rng.gen());
    }

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

    //TODO: would be nice to make this parallel
    let mut batched_shares = vec![];
    let mut batched_masks = vec![];

    for (idx, chunk) in templates.chunks(args.batch_size).enumerate() {
        let mut chunk_masks = Vec::with_capacity(chunk.len());
        let mut chunk_shares: Vec<_> = (0..participant_dbs.len())
            .map(|_| Vec::with_capacity(chunk.len()))
            .collect();

        for (offset, template) in chunk.iter().enumerate() {
            println!(
                "Encoding shares {}/{}",
                idx * args.batch_size + offset,
                templates.len()
            );
            let shares =
                mpc::distance::encode(template).share(participant_dbs.len());

            let id = offset + (idx * args.batch_size);

            chunk_masks.push((id as u64, template.mask));
            for (idx, share) in shares.iter().enumerate() {
                chunk_shares[idx].push((id as u64, *share));
            }
        }

        batched_shares.push(chunk_shares);
        batched_masks.push(chunk_masks);
    }

    let mut tasks = FuturesUnordered::new();

    let mut i = 0;

    //Insert into dbs
    for (masks, mut shares) in
        batched_masks.into_iter().zip(batched_shares.into_iter())
    {
        for (idx, db) in participant_dbs.iter().enumerate() {
            let participant_shares = mem::take(&mut shares[idx]);
            let db = db.clone();

            println!(
                "Inserting shares {} to {} into participant {} db",
                i,
                i + args.batch_size,
                idx,
            );

            tasks.push(tokio::spawn(async move {
                db.insert_shares(&participant_shares).await?;
                Ok::<_, Error>(Insertion::Shares(idx))
            }));
        }

        println!(
            "Inserting shares {} to {} into coordinator db",
            i,
            i + args.batch_size,
        );

        let coordinator_db = coordinator_db.clone();
        tasks.push(tokio::spawn(async move {
            coordinator_db.insert_masks(&masks).await?;
            Ok::<_, Error>(Insertion::Masks)
        }));

        i += args.batch_size;
    }

    //wait for tasks

    let mut participant_counters = vec![0_usize; participant_dbs.len()];
    let mut coordinator_counter = 0;

    while let Some(result) = tasks.next().await {
        match result?? {
            Insertion::Shares(idx) => {
                let counter = participant_counters[idx] + args.batch_size;
                participant_counters[idx] = counter;

                println!(
                    "Successfully inserted shares {}/{} into participant {} db",
                    counter,
                    templates.len(),
                    idx,
                );
            }
            Insertion::Masks => {
                coordinator_counter += args.batch_size;
                println!(
                    "Successfully inserted masks {}/{} into coordinator db",
                    coordinator_counter,
                    templates.len()
                );
            }
        }
    }

    println!("Time elapsed {:?}", now.elapsed());

    Ok(())
}

//Insertion type to keep track of progress for each db
pub enum Insertion {
    Shares(usize),
    Masks,
}
