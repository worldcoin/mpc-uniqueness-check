use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use self::json_wrapper::JsonStrWrapper;

mod json_wrapper;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub service: Option<ServiceConfig>,
    #[serde(default)]
    pub coordinator: Option<CoordinatorConfig>,
    #[serde(default)]
    pub participant: Option<ParticipantConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorConfig {
    pub participants: JsonStrWrapper<Vec<String>>,
    pub hamming_distance_threshold: f64,
    pub n_closest_distances: usize,
    pub gateway: GatewayConfig,
    pub db: DbConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParticipantConfig {
    pub socket_addr: SocketAddr,
    pub batch_size: usize,
    pub db: DbConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbConfig {
    pub url: String,

    #[serde(default)]
    pub migrate: bool,

    #[serde(default)]
    pub create: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    // Service name - used for logging, metrics and tracing
    pub service_name: String,

    // Metrics
    pub metrics_host: String,
    pub metrics_port: u16,
    pub metrics_queue_size: usize,
    pub metrics_buffer_size: usize,
    pub metrics_prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum GatewayConfig {
    Sqs(SqsGatewayConfig),
    Http(HttpGatewayConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsGatewayConfig {
    pub shares_queue_url: String,
    pub distances_queue_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpGatewayConfig {
    pub socket_addr: SocketAddr,
    pub distance_results_url: String,
    pub fire_and_forget: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_toml() {
        let config = Config {
            service: Some(ServiceConfig {
                service_name: "mpc-coordinator".to_string(),
                metrics_host: "localhost".to_string(),
                metrics_port: 8125,
                metrics_queue_size: 5000,
                metrics_buffer_size: 1024,
                metrics_prefix: "mpc-coordinator".to_string(),
            }),
            coordinator: Some(CoordinatorConfig {
                participants: JsonStrWrapper(vec![
                    "127.0.0.1:8000".to_string(),
                    "127.0.0.1:8001".to_string(),
                    "127.0.0.1:8002".to_string(),
                ]),
                hamming_distance_threshold: 0.375,
                n_closest_distances: 20,
                gateway: GatewayConfig::Sqs(SqsGatewayConfig {
                    shares_queue_url: "https://sqs.us-east-1.amazonaws.com/1234567890/mpc-query-queue"
                        .to_string(),
                    distances_queue_url:
                        "https://sqs.us-east-1.amazonaws.com/1234567890/mpc-distance-results-queue"
                            .to_string(),
                }),
                db: DbConfig {
                    url: "postgres://localhost:5432/mpc".to_string(),
                    migrate: true,
                    create: true,
                },
            }),
            participant: None,
        };

        let toml = toml::to_string(&config).unwrap();

        println!("{}", toml);
    }

    #[test]
    fn from_toml_coordinator() {
        const TOML: &str = indoc::indoc! {
            r#"
            [service]
            service_name = "mpc-coordinator"
            metrics_host = "localhost"
            metrics_port = 8125
            metrics_queue_size = 5000
            metrics_buffer_size = 1024
            metrics_prefix = "mpc-coordinator"

            [coordinator]
            participants = '["127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"]'
            hamming_distance_threshold = 0.375
            n_closest_distances = 20

            [coordinator.db]
            url = "postgres://localhost:5432/mpc"
            migrate = true

            [coordinator.gateway]
            type = "sqs"
            shares_queue_url = "https://sqs.us-east-1.amazonaws.com/1234567890/mpc-query-queue"
            distances_queue_url = "https://sqs.us-east-1.amazonaws.com/1234567890/mpc-distance-results-queue"
            "#
        };

        let config: Config = toml::from_str(TOML).unwrap();

        println!("{:#?}", config);
    }
}
