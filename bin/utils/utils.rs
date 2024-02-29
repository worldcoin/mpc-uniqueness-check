use clap::Parser;
use generate_mock_templates::{generate_mock_templates, GenerateMockTemplates};
use rand::distributions::Alphanumeric;
use rand::Rng;
use seed_iris_db::{seed_iris_db, SeedIrisDb};
use seed_mpc_db::{seed_mpc_db, SeedMPCDb};
use sqs_query::{sqs_query, SQSQuery};
use sqs_receive::{sqs_receive, SQSReceive};


mod generate_mock_templates;
mod seed_iris_db;
mod seed_mpc_db;
mod sqs_query;
mod sqs_receive;

#[derive(Debug, Clone, Parser)]
enum Opt {
    SQSQuery(SQSQuery),
    SeedMPCDb(SeedMPCDb),
    SeedIrisDb(SeedIrisDb),
    SQSReceive(SQSReceive),
    GenerateMockTemplates(GenerateMockTemplates),
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    dotenv::dotenv().ok();

    let args = Opt::parse();

    match args {
        Opt::SeedMPCDb(args) => {
            seed_mpc_db(&args).await?;
        }
        Opt::SeedIrisDb(args) => {
            seed_iris_db(&args).await?;
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

pub fn generate_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
