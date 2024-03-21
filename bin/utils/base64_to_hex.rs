use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use clap::Args;

#[derive(Debug, Clone, Args)]
pub struct Base64ToHex {
    pub base64: String,
}

pub async fn base64_to_hex(args: &Base64ToHex) -> eyre::Result<()> {
    let hex_code =
        format!("\"{}\"", hex::encode(STANDARD.decode(&args.base64)?));

    println!("{}", hex_code);

    Ok(())
}
