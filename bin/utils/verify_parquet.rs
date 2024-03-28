use std::path::PathBuf;
use std::time::Instant;

use clap::Args;
use mpc::config::DbConfig;
use mpc::db::Db;
use mpc::item_kind::{ItemKind, Masks, Shares};

#[derive(Debug, Clone, Args)]
pub struct VerifyParquet {
    #[clap(short = 'D', long)]
    pub dir: PathBuf,

    #[clap(short, long)]
    pub db: String,

    #[clap(short, long)]
    pub item_kind: ItemKind,
}

pub async fn verify_parquet(args: &VerifyParquet) -> eyre::Result<()> {
    tracing::info!(dir = ?args.dir, kind = %args.item_kind, "Verifying");

    let files = mpc::snapshot::open_dir_files(&args.dir).await?;

    match args.item_kind {
        ItemKind::Shares => {
            let participant_db = Db::new(&DbConfig {
                url: args.db.clone(),
                migrate: false,
                create: false,
            })
            .await?;

            // Start reading in the background
            let db_fetched_items = tokio::spawn(async move {
                let now = Instant::now();

                let items = participant_db.fetch_shares(0).await?;

                let elapsed = now.elapsed();
                let num = items.len();

                tracing::info!(
                    ?elapsed,
                    num,
                    "Finished fetching shares from the database"
                );

                eyre::Ok(items)
            });

            let parquet_fetch_files = tokio::spawn(async move {
                let now = Instant::now();

                let mut items = vec![];
                for file in files {
                    items.extend(
                        mpc::snapshot::read_parquet::<Shares, _>(file).await?,
                    );
                }

                let elapsed = now.elapsed();

                let now = Instant::now();
                items.sort_by_key(|(i, _)| *i);

                let shares = items
                    .into_iter()
                    .map(|(_, share)| share)
                    .collect::<Vec<_>>();

                let sorting_time = now.elapsed();

                tracing::info!(
                    ?elapsed,
                    ?sorting_time,
                    num = shares.len(),
                    "Finished reading and sorting shares from parquet files"
                );

                eyre::Ok(shares)
            });

            let (db_fetched_items, parquet_fetched_items) =
                tokio::try_join!(db_fetched_items, parquet_fetch_files)?;

            let db_fetched_items = db_fetched_items?;
            let parquet_fetched_items = parquet_fetched_items?;

            assert_eq!(db_fetched_items, parquet_fetched_items);
        }
        ItemKind::Masks => {
            let coordinator_db = Db::new(&DbConfig {
                url: args.db.clone(),
                migrate: false,
                create: false,
            })
            .await?;

            // Start reading in the background
            let db_fetched_items = tokio::spawn(async move {
                let now = Instant::now();

                let items = coordinator_db.fetch_masks(0).await?;

                let elapsed = now.elapsed();
                let num = items.len();

                tracing::info!(
                    ?elapsed,
                    num,
                    "Finished fetching masks from the database"
                );

                eyre::Ok(items)
            });

            let parquet_fetch_files = tokio::spawn(async move {
                let now = Instant::now();

                let mut items = vec![];
                for file in files {
                    items.extend(
                        mpc::snapshot::read_parquet::<Masks, _>(file).await?,
                    );
                }

                let elapsed = now.elapsed();

                let now = Instant::now();
                items.sort_by_key(|(i, _)| *i);

                let masks =
                    items.into_iter().map(|(_, mask)| mask).collect::<Vec<_>>();

                let sorting_time = now.elapsed();

                tracing::info!(
                    ?elapsed,
                    ?sorting_time,
                    num = masks.len(),
                    "Finished reading and sorting masks from parquet files"
                );

                eyre::Ok(masks)
            });

            let (db_fetched_items, parquet_fetched_items) =
                tokio::try_join!(db_fetched_items, parquet_fetch_files)?;

            let db_fetched_items = db_fetched_items?;
            let parquet_fetched_items = parquet_fetched_items?;

            assert_eq!(db_fetched_items, parquet_fetched_items);
        }
    }

    Ok(())
}
