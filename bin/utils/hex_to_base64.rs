use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use clap::Args;

#[derive(Debug, Clone, Args)]
pub struct HexToBase64 {
    pub hex: String,
}

pub async fn hex_to_base64(args: &HexToBase64) -> eyre::Result<()> {
    let base64_code =
        format!("\"{}\"", STANDARD.encode(hex::decode(&args.hex)?));

    println!("{}", base64_code);

    Ok(())
}
