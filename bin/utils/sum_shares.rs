use std::fs::read_to_string;

use clap::Args;
use mpc::distance::{decode, EncodedBits};

#[derive(Debug, Clone, Args)]
pub struct SumShares {
    path: String,
}

pub async fn sum_shares(args: &SumShares) -> eyre::Result<()> {
    let shares_json = read_to_string(&args.path)?;
    let data = serde_json::from_str::<Vec<EncodedBits>>(&shares_json)?;

    let encoded: EncodedBits = data.iter().sum();

    let decoded = decode(&encoded)?;

    println!("Code: {:?}", decoded.code);
    println!("Mask: {:?}", decoded.mask);

    Ok(())
}
