use clap::Args;
use eyre::Error;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use indicatif::ProgressBar;
use mpc::config::DbConfig;
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

    #[clap(short, long, default_value = "10000")]
    pub batch_size: usize,
}

pub async fn seed_mpc_db(args: &SeedMPCDb) -> eyre::Result<()> {
    if args.participant_db_url.is_empty() {
        return Err(eyre::eyre!("No participant DBs provided"));
    }

    let mut templates: Vec<Template> = Vec::with_capacity(args.num_templates);

    tracing::info!("Generating templates");
    let mut rng = thread_rng();

    for _ in 0..args.num_templates {
        tracing::info!(
            "Generating template {}/{}",
            templates.len(),
            args.num_templates
        );
        templates.push(rng.gen());
    }

    let coordinator_db = Db::new(&DbConfig {
        url: args.coordinator_db_url.clone(),
        migrate: true,
        create: true,
    })
    .await?;

    let mut participant_dbs = vec![];

    for db_config in args.participant_db_url.iter() {
        participant_dbs.push(
            Db::new(&DbConfig {
                url: db_config.clone(),
                migrate: true,
                create: true,
            })
            .await?,
        );
    }

    let num_chunks = (templates.len() / args.batch_size) + 1;

    //TODO: update logging to num shares rather than chunks

    for (idx, chunk) in templates.chunks(args.batch_size).enumerate() {
        let mut chunk_masks = Vec::with_capacity(chunk.len());
        let mut chunk_shares: Vec<_> = (0..participant_dbs.len())
            .map(|_| Vec::with_capacity(chunk.len()))
            .collect();

        tracing::info!("Encoding shares {}/{}", idx + 1, num_chunks);
        for (offset, template) in chunk.iter().enumerate() {
            let shares =
                mpc::distance::encode(template).share(participant_dbs.len());

            let id = offset + (idx * args.batch_size);

            chunk_masks.push((id as u64, template.mask));
            for (idx, share) in shares.iter().enumerate() {
                chunk_shares[idx].push((id as u64, *share));
            }
        }

        let mut tasks = FuturesUnordered::new();

        for (i, db) in participant_dbs.iter().enumerate() {
            tracing::info!(
                "Inserting shares chunk {}/{num_chunks} into participant DB {i}",
                idx + 1
            );

            tasks.push(Box::pin(db.insert_shares(&chunk_shares[idx])));
        }

        tracing::info!(
            "Inserting masks chunk {}/{num_chunks} into coordinator DB",
            idx + 1
        );

        let coordinator_task: Result<(), eyre::Report> =
            coordinator_db.insert_masks(&chunk_masks).await;

        tasks.push(coordinator_task);

        while let Some(result) = tasks.next().await {
            println!("Task completed with result: {:?}", result);
        }

        let (coordinator, participants) = tokio::join!(
            ,
            tasks.await.collect::<Result<_, _>>(),
        );

        // coordinator?;
        // participants.into_iter().collect::<Result<_, _>>()?;
    }

    Ok(())
}
