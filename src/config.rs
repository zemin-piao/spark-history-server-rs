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

    /// Directory for DuckDB database storage
    pub database_directory: Option<String>,

    /// HDFS configuration (optional)
    pub hdfs: Option<HdfsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HdfsConfig {
    /// HDFS namenode URL (e.g., hdfs://namenode:9000)
    pub namenode_url: String,

    /// Connection timeout in milliseconds
    pub connection_timeout_ms: Option<u64>,

    /// Read timeout in milliseconds  
    pub read_timeout_ms: Option<u64>,

    /// Kerberos authentication configuration
    pub kerberos: Option<KerberosConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KerberosConfig {
    /// Kerberos principal (e.g., user@REALM.COM)
    pub principal: String,

    /// Path to keytab file
    pub keytab_path: Option<String>,

    /// Path to krb5.conf file
    pub krb5_config_path: Option<String>,

    /// Kerberos realm
    pub realm: Option<String>,
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
                database_directory: Some("./data".to_string()),
                hdfs: None,
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
