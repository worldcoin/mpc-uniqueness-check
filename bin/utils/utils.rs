use clap::Parser;
use generate_mock_templates::{generate_mock_templates, GenerateMockTemplates};
use seed_iris_db::{seed_iris_db, SeedIrisDb};
use seed_mpc_db::{seed_mpc_db, SeedMPCDb};
use sqs_query::{sqs_query, SQSQuery};
use sqs_receive::{sqs_receive, SQSReceive};
use telemetry_batteries::tracing::stdout::StdoutBattery;

mod common;
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

    let _shutdown_tracing_provider = StdoutBattery::init();

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
