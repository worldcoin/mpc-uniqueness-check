#![feature(array_chunks)]

use clap::Parser;
use generate_mock_templates::{generate_mock_templates, GenerateMockTemplates};
use seed_iris_db::{seed_iris_db, SeedIrisDb};
use seed_mpc_db::{seed_mpc_db, SeedMPCDb};
use sqs_query::{sqs_query, SQSQuery};
use sqs_receive::{sqs_receive, SQSReceive};
use sum_shares::{sum_shares, SumShares};
use hex_to_base64::{hex_to_base64, HexToBase64};
use base64_to_hex::{base64_to_hex, Base64ToHex};

mod common;
mod generate_mock_templates;
mod seed_iris_db;
mod seed_mpc_db;
mod sqs_query;
mod sqs_receive;
mod sum_shares;
mod hex_to_base64;
mod base64_to_hex;

#[derive(Debug, Clone, Parser)]
enum Opt {
    SQSQuery(SQSQuery),
    SeedMPCDb(SeedMPCDb),
    SeedIrisDb(SeedIrisDb),
    SQSReceive(SQSReceive),
    GenerateMockTemplates(GenerateMockTemplates),
    SumShares(SumShares),
    HexToBase64(HexToBase64),
    Base64ToHex(Base64ToHex),
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
        Opt::SumShares(args) => {
            sum_shares(&args).await?;
        }
        Opt::HexToBase64(args) => {
            hex_to_base64(&args).await?;
        }
        Opt::Base64ToHex(args) => {
            base64_to_hex(&args).await?;
        }
    }

    Ok(())
}
