use std::future::Future;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::task::JoinError;

pub async fn finalize_futures_unordered<F>(
    mut tasks: FuturesUnordered<F>,
) -> eyre::Result<()>
where
    F: Future<Output = Result<eyre::Result<()>, JoinError>> + Unpin,
{
    while let Some(result) = tasks.next().await {
        result??;
    }

    Ok(())
}
