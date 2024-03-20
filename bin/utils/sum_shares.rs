use clap::Args;
use mpc::bits::Bits;
use mpc::distance::EncodedBits;

#[derive(Debug, Clone, Args)]
pub struct SumShares {
    shares: Vec<Bits>,
}

pub async fn sum_shares(args: &SumShares) -> eyre::Result<()> {
    let encoded_bits = args
        .shares
        .iter()
        .map(EncodedBits::from)
        .collect::<Vec<_>>();

    let sum = encoded_bits.iter().sum::<EncodedBits>();

    println!("Sum: {sum:?}");

    Ok(())
}
