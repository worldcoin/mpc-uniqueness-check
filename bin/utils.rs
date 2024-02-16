use clap::{Args, Parser};
use eyre::ContextCompat;
use indicatif::ProgressBar;
use mpc::config::{AwsConfig, DbConfig};
use mpc::coordinator::{self, UniquenessCheckRequest, UniquenessCheckResult};
use mpc::db::Db;
use mpc::template::Template;
use mpc::utils::aws::sqs_client_from_config;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Parser)]
enum Opt {
    SQSQuery(SQSQuery),
    SeedDb(SeedDb),
    SQSReceive(SQSReceive),
    GenerateMockTemplates(GenerateMockTemplates),
}

#[derive(Debug, Clone, Args)]
struct SQSQuery {
    /// The endpoint URL for the AWS service
    ///
    /// Useful when using LocalStack
    #[clap(short, long)]
    pub endpoint_url: Option<String>,

    /// The AWS region
    #[clap(short, long)]
    pub region: Option<String>,

    /// The URL of the SQS queue
    #[clap(short, long)]
    pub queue_url: String,
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

#[derive(Debug, Clone, Args)]
struct SQSReceive {
    /// The endpoint URL for the AWS service
    ///
    /// Useful when using LocalStack
    #[clap(short, long)]
    pub endpoint_url: Option<String>,

    /// The AWS region
    #[clap(short, long)]
    pub region: Option<String>,

    /// The URL of the SQS queue
    #[clap(short, long)]
    pub queue_url: String,
}

#[derive(Debug, Clone, Args)]
struct GenerateMockTemplates {
    #[clap(short, long)]
    pub output: String,

    #[clap(short, long)]
    pub num_templates: usize,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    dotenv::dotenv().ok();

    let args = Opt::parse();

    match args {
        Opt::SeedDb(args) => {
            seed_db(&args).await?;
        }
        Opt::SQSQuery(args) => {
            sqs_query(&args).await?;
        }
        Opt::SQSReceive(args) => {
            sqs_receive(&args).await?;
        }
        Opt::GenerateMockTemplates(args) => {
            generate_mock_templates(&args).await?;
        }
    }

    Ok(())
}

async fn seed_db(args: &SeedDb) -> eyre::Result<()> {
    let mut templates: Vec<Template> = Vec::with_capacity(args.num);

    let pb =
        ProgressBar::new(args.num as u64).with_message("Generating templates");

    let mut rng = thread_rng();

    for _ in 0..args.num {
        templates.push(rng.gen());

        pb.inc(1);
    }

    pb.finish_with_message("done");

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

    let pb = ProgressBar::new(args.num as u64).with_message("Seeding DBs");

    for (idx, chunk) in templates.chunks(args.batch_size).enumerate() {
        let mut chunk_masks = Vec::with_capacity(chunk.len());
        let mut chunk_shares: Vec<_> = (0..participant_dbs.len())
            .map(|_| Vec::with_capacity(chunk.len()))
            .collect();

        for (offset, template) in chunk.iter().enumerate() {
            let shares =
                mpc::distance::encode(template).share(participant_dbs.len());

            let id = offset + (idx * args.batch_size);

            chunk_masks.push((id as u64, template.mask));
            for (idx, share) in shares.iter().enumerate() {
                chunk_shares[idx].push((id as u64, *share));
            }
        }

        let mut tasks = vec![];

        for (idx, db) in participant_dbs.iter().enumerate() {
            tasks.push(db.insert_shares(&chunk_shares[idx]));
        }

        let (coordinator, participants) = tokio::join!(
            coordinator_db.insert_masks(&chunk_masks),
            futures::future::join_all(tasks),
        );

        coordinator?;
        participants.into_iter().collect::<Result<_, _>>()?;

        pb.inc(args.batch_size as u64);
    }

    pb.finish_with_message("done");

    Ok(())
}

async fn generate_mock_templates(
    args: &GenerateMockTemplates,
) -> eyre::Result<()> {
    let mut rng = thread_rng();

    let templates: Vec<Template> =
        (0..args.num_templates).map(|_| rng.gen()).collect();

    let json = serde_json::to_string(&templates)?;

    //write to file
    std::fs::write(&args.output, json)?;

    Ok(())
}

async fn sqs_query(args: &SQSQuery) -> eyre::Result<()> {
    let sqs_client = sqs_client_from_config(&AwsConfig {
        endpoint: args.endpoint_url.clone(),
        region: args.region.clone(),
    })
    .await?;

    let mut rng = thread_rng();

    let plain_code: Template = rng.gen();

    let signup_id = generate_random_string(4);
    let group_id = generate_random_string(4);

    let request = UniquenessCheckRequest {
        plain_code,
        signup_id,
    };

    sqs_client
        .send_message()
        .queue_url(args.queue_url.clone())
        .message_group_id(group_id)
        .message_body(serde_json::to_string(&request)?)
        .send()
        .await?;

    Ok(())
}

async fn sqs_receive(args: &SQSReceive) -> eyre::Result<()> {
    let sqs_client = sqs_client_from_config(&AwsConfig {
        endpoint: args.endpoint_url.clone(),
        region: args.region.clone(),
    })
    .await?;

    loop {
        let sqs_msg = sqs_client
            .receive_message()
            .queue_url(args.queue_url.clone())
            .send()
            .await?;

        let messages = sqs_msg.messages.unwrap_or_default();

        if messages.is_empty() {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            continue;
        }

        for message in messages {
            let body = message.body.context("Missing message body")?;
            let receipt_handle = message
                .receipt_handle
                .context("Missing receipt handle in message")?;

            let request: UniquenessCheckResult = serde_json::from_str(&body)?;

            tracing::info!(?request, "Received message");

            sqs_client
                .delete_message()
                .queue_url(args.queue_url.clone())
                .receipt_handle(receipt_handle)
                .send()
                .await?;
        }
    }
}

fn generate_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
