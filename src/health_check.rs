use std::net::SocketAddr;

use axum::http::StatusCode;
use axum::{routing, Router};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck;

impl HealthCheck {
    pub fn spawn(
        addr: impl Into<SocketAddr> + Send + 'static,
    ) -> JoinHandle<eyre::Result<()>> {
        let router = Self::router();

        tokio::spawn(async move {
            axum::serve(
                TcpListener::bind(addr.into()).await?,
                router.into_make_service(),
            )
            .await
            .map_err(|e| eyre::Report::from(e).into())
        })
    }

    pub fn router() -> Router {
        Router::new().route("/health", routing::get(Self::health_check))
    }

    async fn health_check() -> impl axum::response::IntoResponse {
        StatusCode::OK
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use reqwest::{self, StatusCode};

    use super::*;
    use crate::db;

    #[tokio::test]
    async fn test_health_check() -> eyre::Result<()> {
        let addr: SocketAddr = "127.0.0.1:3000".parse()?;

        // Spawn the HealthCheck server
        let health_check = HealthCheck::spawn(addr);

        // Wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Make a request to the health check endpoint
        let client = reqwest::Client::new();
        let response = client
            .get(format!("http://{}/health", addr))
            .send()
            .await
            .expect("Failed to send request");

        // Assert the response is OK
        assert_eq!(response.status(), StatusCode::OK);

        Ok(())
    }
}
