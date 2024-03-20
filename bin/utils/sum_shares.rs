use std::fs::read_to_string;

use clap::Args;
use mpc::bits::{Bits, BITS};
use mpc::distance::EncodedBits;
use mpc::template::Template;
use rand::Rng;

#[derive(Debug, Clone, Args)]
pub struct SumShares {
    path: String,
}

pub async fn sum_shares(args: &SumShares) -> eyre::Result<()> {
    let shares_json = read_to_string(&args.path)?;
    let shares = serde_json::from_str::<Vec<String>>(&shares_json)?;

    let mut encoded_shares = vec![];
    for share in shares {
        let bytes = hex::decode(share)?;
        encoded_shares.push(decode_share(&bytes)?);
    }

    let sum = encoded_shares.iter().sum::<EncodedBits>();
    let plain_text_code = Bits::from(&sum);

    let serialized_code = serde_json::to_string(&plain_text_code)?;
    println!("Iris Code: {:?}", serialized_code);

    Ok(())
}

const BYTES_PER_ENCODED_BITS: usize = BITS * 2;

fn decode_share(bytes: &[u8]) -> eyre::Result<EncodedBits> {
    if bytes.len() != BYTES_PER_ENCODED_BITS {
        return Err(eyre::eyre!("Incorrect length"));
    }

    let bytes: [u8; BYTES_PER_ENCODED_BITS] =
        bytes.try_into().expect("Slice with incorrect length");

    let bits: Vec<u16> = bytes
        .array_chunks::<2>()
        .map(|x| u16::from_be_bytes(*x))
        .collect();

    let bits = bits.try_into().expect("Conversion to target type failed");

    Ok(EncodedBits(bits))
}
