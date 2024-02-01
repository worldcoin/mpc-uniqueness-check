use std::sync::Arc;

use crate::config::GatewayConfig;
use crate::distance::DistanceResults;
use crate::template::Template;

pub mod aws;
pub mod http;

#[async_trait::async_trait]
pub trait Gateway: Send + Sync {
    async fn receive_queries(&self) -> eyre::Result<Vec<Template>>;

    async fn send_results(&self, results: &DistanceResults)
        -> eyre::Result<()>;
}

pub async fn from_config(
    config: &GatewayConfig,
) -> eyre::Result<Arc<dyn Gateway>> {
    match config {
        GatewayConfig::Http(config) => {
            Ok(Arc::new(http::HttpGateway::new(config).await?))
        }
        GatewayConfig::Sqs(config) => {
            Ok(Arc::new(aws::SqsGateway::new(config).await?))
        }
    }
}
