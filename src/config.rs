use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub test: TestConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestConfig {
    #[serde(default = "default::test")]
    pub test: String,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            test: default::test(),
        }
    }
}

pub mod default {
    pub fn test() -> String {
        "default".to_string()
    }
}
