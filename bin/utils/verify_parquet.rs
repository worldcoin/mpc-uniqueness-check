use std::path::PathBuf;

use clap::Args;
use mpc::item_kind::{ItemKind, Masks, Shares};

#[derive(Debug, Clone, Args)]
pub struct VerifyParquet {
    #[clap(short, long)]
    pub dir: PathBuf,

    #[clap(short, long)]
    pub item_kind: ItemKind,
}

pub async fn verify_parquet(args: &VerifyParquet) -> eyre::Result<()> {
    let files = mpc::snapshot::open_dir_files(&args.dir).await?;

    match args.item_kind {
        ItemKind::Shares => {
            for file in files {
                mpc::snapshot::read_parquet::<Shares, _>(file).await?;
            }
        }
        ItemKind::Masks => {
            for file in files {
                mpc::snapshot::read_parquet::<Masks, _>(file).await?;
            }
        }
    }

    Ok(())
}
