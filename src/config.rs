use std::net::SocketAddr;
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use self::json_wrapper::JsonStrWrapper;

mod json_wrapper;

pub fn load_config<T>(
    prefix: &str,
    config_path: Option<&Path>,
) -> eyre::Result<T>
where
    T: DeserializeOwned,
{
    let mut settings = config::Config::builder();

    if let Some(path) = config_path {
        settings = settings.add_source(config::File::from(path).required(true));
    }

    let settings = settings
        .add_source(
            config::Environment::with_prefix(prefix)
                .separator("__")
                .try_parsing(true),
        )
        .build()?;

    let config = settings.try_deserialize::<T>()?;

    Ok(config)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub service: Option<ServiceConfig>,
    #[serde(default)]
    pub coordinator: Option<CoordinatorConfig>,
    #[serde(default)]
    pub participant: Option<ParticipantConfig>,
    #[serde(default)]
    pub health_check: Option<HealthCheckConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorConfig {
    pub participants: JsonStrWrapper<Vec<String>>,
    pub hamming_distance_threshold: f64,
    pub n_closest_distances: usize,
    pub db: DbConfig,
    pub queues: CoordinatorQueuesConfig,
    #[serde(default)]
    pub aws: AwsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParticipantConfig {
    pub socket_addr: SocketAddr,
    pub batch_size: usize,
    pub db: DbConfig,
    pub queues: ParticipantQueuesConfig,
    #[serde(default)]
    pub aws: AwsConfig,
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
pub struct CoordinatorQueuesConfig {
    pub queries_queue_url: String,
    pub distances_queue_url: String,
    pub db_sync_queue_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParticipantQueuesConfig {
    pub db_sync_queue_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AwsConfig {
    /// Used to override the default endpoint url for the AWS service
    ///
    /// Useful when using something like LocalStack
    pub endpoint: Option<String>,

    #[serde(default)]
    pub region: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    // Service name - used for logging, metrics and tracing
    pub service_name: String,
    // Traces
    pub traces_endpoint: Option<String>,
    // Metrics
    pub metrics: Option<MetricsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    pub host: String,
    pub port: u16,
    pub queue_size: usize,
    pub buffer_size: usize,
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    pub socket_addr: SocketAddr,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_toml() {
        let config = Config {
            service: Some(ServiceConfig {
                service_name: "mpc-coordinator".to_string(),
                traces_endpoint: None,

                metrics: Some(MetricsConfig {
                    // Metrics
                    host: "localhost".to_string(),
                    port: 8125,
                    queue_size: 5000,
                    buffer_size: 1024,
                    prefix: "mpc-coordinator".to_string(),
                }),
            }),
            coordinator: Some(CoordinatorConfig {
                participants: JsonStrWrapper(vec![
                    "127.0.0.1:8000".to_string(),
                    "127.0.0.1:8001".to_string(),
                    "127.0.0.1:8002".to_string(),
                ]),
                hamming_distance_threshold: 0.375,
                n_closest_distances: 20,
                db: DbConfig {
                    url: "postgres://localhost:5432/mpc".to_string(),
                    migrate: true,
                    create: true,
                },
                queues: CoordinatorQueuesConfig {
                    queries_queue_url: "https://sqs.us-east-1.amazonaws.com/1234567890/mpc-query-queue"
                        .to_string(),
                    distances_queue_url: "https://sqs.us-east-1.amazonaws.com/1234567890/mpc-distance-results-queue"
                        .to_string(),
                    db_sync_queue_url: "https://sqs.us-east-1.amazonaws.com/1234567890/mpc-query-queue"
                        .to_string(),
                },
                aws: AwsConfig {
                    endpoint: Some("http://localhost:4566".to_string()),
                    region: None,
                },
            }),
            participant: None,
            health_check: None,
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

            [coordinator.queues]
            queries_queue_url = "https://sqs.us-east-1.amazonaws.com/1234567890/mpc-query-queue"
            distances_queue_url = "https://sqs.us-east-1.amazonaws.com/1234567890/mpc-distance-results-queue"
            db_sync_queue_url = "https://sqs.us-east-1.amazonaws.com/1234567890/mpc-query-queue"

            [coordinator.aws]
            endpoint = "http://localhost:4566"
            "#
        };

        let config: Config = toml::from_str(TOML).unwrap();

        println!("{:#?}", config);
    }
}
