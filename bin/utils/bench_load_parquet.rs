use std::path::PathBuf;
use std::time::Instant;

use clap::Args;
use futures::future::try_join_all;
use mpc::item_kind::Shares;

#[derive(Debug, Clone, Args)]
pub struct BenchParquet {
    #[clap(short = 'D', long)]
    pub dir: PathBuf,

    #[clap(short = 'p', long)]
    pub parallel: bool,
}

pub async fn bench_load_parquet(args: &BenchParquet) -> eyre::Result<()> {
    tracing::info!(dir = ?args.dir, parallel = %args.parallel, "Verifying");

    println!("DIR: {:?}", args.dir);
    let files = mpc::snapshot::open_dir_files(&args.dir).await?;

    if args.parallel {
        let parquet_fetch_files = tokio::spawn(async move {
            let now = Instant::now();

            let read_tasks = files
                .into_iter()
                .map(|file| mpc::snapshot::read_parquet::<Shares, _>(file))
                .collect::<Vec<_>>();

            // Use `try_join_all` to run all read tasks in parallel and await their completion
            let results = try_join_all(read_tasks).await?;

            // Flatten the vector of vectors into a single vector of items
            let mut items = results.into_iter().flatten().collect::<Vec<_>>();

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

        let parquet_fetched_items = parquet_fetch_files.await.unwrap();
        let parquet_fetched_items = parquet_fetched_items?;
        tracing::info!(num = parquet_fetched_items.len(), "Finished");
    } else {
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

        let parquet_fetched_items = parquet_fetch_files.await.unwrap();
        let parquet_fetched_items = parquet_fetched_items?;
        tracing::info!(num = parquet_fetched_items.len(), "Finished");
    }
    Ok(())
}
