use std::collections::VecDeque;
use std::sync::Arc;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use super::Gateway;
use crate::config::HttpGatewayConfig;
use crate::distance::DistanceResults;
use crate::template::Template;

pub struct HttpGateway {
    incoming_templates: Arc<Mutex<VecDeque<Template>>>,
}

async fn handle(
    state: State<Arc<Mutex<VecDeque<Template>>>>,
    req: Json<Template>,
) {
    state.lock().await.push_back(req.0);
}

impl HttpGateway {
    pub async fn new(config: &HttpGatewayConfig) -> eyre::Result<Self> {
        let addr = config.socket_addr;

        let incoming_templates = Arc::new(Mutex::new(VecDeque::new()));

        let app = Router::new()
            .route("/", post(handle))
            .with_state(incoming_templates.clone());

        let listener = tokio::net::TcpListener::bind(addr).await?;

        let local_addr = listener.local_addr()?;
        tracing::info!("Gateway listening on {local_addr}");

        //TODO: Handle shutdown gracefully
        let _server_handle: JoinHandle<eyre::Result<()>> =
            tokio::spawn(async {
                axum::serve(listener, app).await?;

                Ok(())
            });

        Ok(Self { incoming_templates })
    }
}

#[async_trait::async_trait]
impl Gateway for HttpGateway {
    async fn receive_queries(&self) -> eyre::Result<Vec<Template>> {
        Ok(self.incoming_templates.lock().await.drain(..).collect())
    }

    async fn send_results(
        &self,
        results: &DistanceResults,
    ) -> eyre::Result<()> {
        // TODO: Send the results back
        println!("Sending results: {:?}", results);
        Ok(())
    }
}
