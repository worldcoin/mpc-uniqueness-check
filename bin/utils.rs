use clap::{Args, Parser};
use indicatif::ProgressBar;
use mpc::config::DbConfig;
use mpc::db::coordinator::CoordinatorDb;
use mpc::db::participant::ParticipantDb;
use mpc::template::Template;
use rand::{thread_rng, Rng};

#[derive(Debug, Clone, Parser)]
enum Opt {
    HttpQuery(RandomQuery),
    SQSQuery(RandomQuery),
    SeedDb(SeedDb),
}

#[derive(Debug, Clone, Args)]
struct RandomQuery {
    #[clap(short, long, default_value = "http://localhost:8080")]
    pub url: String,
}

#[derive(Debug, Clone, Args)]
struct SeedDb {
    #[clap(short, long)]
    pub coordinator_db_url: String,

    #[clap(short, long)]
    pub participant_db_url: Vec<String>,

    #[clap(short, long, default_value = "3000000")]
    pub num: usize,

    #[clap(short, long, default_value = "10000")]
    pub batch_size: usize,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    dotenv::dotenv().ok();

    let args = Opt::parse();

    match args {
        Opt::HttpQuery(args) => {
            let mut rng = thread_rng();

            let template: Template = rng.gen();

            reqwest::Client::new()
                .post(&format!("{}/", args.url))
                .json(&template)
                .send()
                .await?
                .error_for_status()?;
        }
        Opt::SeedDb(args) => {
            seed_db(&args).await?;
        }
        Opt::SQSQuery(args) => {
            let aws_config = aws_config::load_defaults(
                aws_config::BehaviorVersion::latest(),
            )
            .await;

            let aws_client = aws_sdk_sqs::Client::new(&aws_config);

            let mut rng = thread_rng();

            let template: Template = rng.gen();

            aws_client
                .send_message()
                .queue_url(args.url)
                .message_body(serde_json::to_string(&template)?)
                .send()
                .await?;
        }
    }

    Ok(())
}

async fn seed_db(args: &SeedDb) -> eyre::Result<()> {
    let mut templates: Vec<Template> = Vec::with_capacity(args.num);

    let pb =
        ProgressBar::new(args.num as u64).with_message("Generating templates");

    for _ in 0..args.num {
        templates.push(thread_rng().gen());

        pb.inc(1);
    }

    pb.finish_with_message("done");

    let coordinator_db = CoordinatorDb::new(&DbConfig {
        url: args.coordinator_db_url.clone(),
        migrate: true,
        create: true,
    })
    .await?;

    let participant_db = ParticipantDb::new(&DbConfig {
        url: args.participant_db_url[0].clone(),
        migrate: true,
        create: true,
    })
    .await?;

    let pb = ProgressBar::new(args.num as u64).with_message("Seeding DBs");

    for (idx, chunk) in templates.chunks(args.batch_size).enumerate() {
        let mut chunk_masks = Vec::with_capacity(chunk.len());
        let mut chunk_shares = Vec::with_capacity(chunk.len());

        for (offset, template) in chunk.into_iter().enumerate() {
            let shares = mpc::encode::encode(&template).share(1);

            let id = offset + (idx * args.batch_size);

            chunk_masks.push((id as u64, template.mask));
            chunk_shares.push((id as u64, shares[0]));
        }

        let (coordinator, participant) = tokio::join!(
            coordinator_db.insert_masks(&chunk_masks),
            participant_db.insert_shares(&chunk_shares),
        );

        coordinator?;
        participant?;

        pb.inc(args.batch_size as u64);
    }

    pb.finish_with_message("done");

    Ok(())
}
