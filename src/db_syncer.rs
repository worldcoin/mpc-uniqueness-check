use std::sync::Arc;

use crate::config::SyncerConfig;

mod aws;

#[async_trait::async_trait]
pub trait DbSyncer: Send + Sync {
    // TODO: Find some way to return a typed result here
    async fn receive_items(&self) -> eyre::Result<Vec<String>>;
}

pub async fn from_config(
    config: &SyncerConfig,
) -> eyre::Result<Arc<dyn DbSyncer>> {
    match config {
        SyncerConfig::Sqs(config) => {
            Ok(Arc::new(aws::AwsSyncer::new(&config.queue_url).await?))
        }
    }
}
