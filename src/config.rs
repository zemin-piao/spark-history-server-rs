use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub server: ServerConfig,
    pub history: HistoryConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub max_applications: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryConfig {
    /// Path to Spark event logs directory
    pub log_directory: String,

    /// Maximum number of applications to retain in memory
    pub max_applications: usize,

    /// Update interval in seconds for checking new event logs
    pub update_interval_seconds: u64,

    /// Maximum number of applications to return in a single request
    pub max_apps_per_request: usize,

    /// Enable event log compression support
    pub compression_enabled: bool,

    /// Directory for RocksDB cache storage
    pub cache_directory: Option<String>,

    /// Enable persistent caching with RocksDB
    pub enable_cache: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                host: "0.0.0.0".to_string(),
                port: 18080,
                max_applications: 1000,
            },
            history: HistoryConfig {
                log_directory: "./test-data/spark-events".to_string(),
                max_applications: 1000,
                update_interval_seconds: 10,
                max_apps_per_request: 100,
                compression_enabled: true,
                cache_directory: Some("./cache/rocksdb".to_string()),
                enable_cache: true,
            },
        }
    }
}

impl Settings {
    pub fn load(config_path: &str) -> Result<Self> {
        if std::path::Path::new(config_path).exists() {
            let contents = fs::read_to_string(config_path)?;
            let settings: Settings = toml::from_str(&contents)?;
            Ok(settings)
        } else {
            tracing::warn!("Config file not found: {}. Using defaults.", config_path);
            Ok(Settings::default())
        }
    }
}
