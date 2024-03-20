use std::fs::read_to_string;

use clap::Args;
use mpc::bits::Bits;
use mpc::distance::EncodedBits;

#[derive(Debug, Clone, Args)]
pub struct SumShares {
    path: String,
}

pub async fn sum_shares(args: &SumShares) -> eyre::Result<()> {
    let shares =
        serde_json::from_str::<Vec<EncodedBits>>(&read_to_string(&args.path)?)?;

    let sum = shares.iter().sum::<EncodedBits>();
    let plain_text_code = Bits::from(&sum);
    println!("Iris Code: {plain_text_code:?}");

    Ok(())
}
