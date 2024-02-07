use axum::http::StatusCode;
use axum::{routing, Router};

pub struct HealthCheck;

impl HealthCheck {
    pub async fn spawn() {}

    pub fn router() -> Router {
        Router::new().route("/health", routing::get(Self::health_check))
    }

    async fn health_check() -> impl axum::response::IntoResponse {
        StatusCode::OK
    }
}
