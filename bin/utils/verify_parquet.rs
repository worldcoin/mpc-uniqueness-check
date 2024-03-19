use clap::Args;

#[derive(Debug, Clone, Args)]
pub struct VerifyParquet {}

pub async fn verify_parquet(args: &VerifyParquet) -> eyre::Result<()> {
    Ok(())
}
