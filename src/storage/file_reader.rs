use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tracing::{debug, info, warn};

use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use crate::config::{HdfsConfig, KerberosConfig, S3Config};
use crate::s3_reader::S3Reader;

/// Trait for reading files from different storage backends
#[async_trait]
pub trait FileReader: Send + Sync {
    async fn read_file(&self, path: &Path) -> Result<String>;
    #[allow(dead_code)]
    async fn list_directory(&self, path: &Path) -> Result<Vec<String>>;
    #[allow(dead_code)]
    async fn file_exists(&self, path: &Path) -> bool;
}

/// Local filesystem reader
pub struct LocalFileReader;

impl Default for LocalFileReader {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalFileReader {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl FileReader for LocalFileReader {
    async fn read_file(&self, path: &Path) -> Result<String> {
        let content = fs::read_to_string(path).await?;
        Ok(content)
    }

    async fn list_directory(&self, path: &Path) -> Result<Vec<String>> {
        let mut entries = Vec::new();
        let mut dir_entries = fs::read_dir(path).await?;

        while let Some(entry) = dir_entries.next_entry().await? {
            if let Some(name) = entry.file_name().to_str() {
                entries.push(name.to_string());
            }
        }

        Ok(entries)
    }

    async fn file_exists(&self, path: &Path) -> bool {
        path.exists()
    }
}

pub struct HdfsFileReader {
    client: hdfs_native::Client,
    config: HdfsConfig,
    circuit_breaker: Arc<CircuitBreaker>,
}

impl HdfsFileReader {
    pub fn new(config: HdfsConfig) -> Result<Self> {
        info!(
            "Initializing HDFS client for namenode: {}",
            config.namenode_url
        );

        let client = if let Some(kerberos_config) = &config.kerberos {
            Self::create_kerberos_client(&config.namenode_url, kerberos_config)?
        } else {
            info!("Creating HDFS client without Kerberos authentication");
            hdfs_native::Client::new(&config.namenode_url)?
        };

        // Create circuit breaker for HDFS operations
        let circuit_breaker_config = CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            timeout_duration: std::time::Duration::from_secs(30),
            window_duration: std::time::Duration::from_secs(300),
        };
        let circuit_breaker = Arc::new(CircuitBreaker::new(
            format!("hdfs-{}", config.namenode_url),
            circuit_breaker_config,
        ));

        Ok(Self {
            client,
            config,
            circuit_breaker,
        })
    }

    #[allow(dead_code)]
    pub fn new_simple(namenode_url: &str) -> Result<Self> {
        info!(
            "Initializing simple HDFS client for namenode: {}",
            namenode_url
        );

        let client = hdfs_native::Client::new(namenode_url)?;
        let config = HdfsConfig {
            namenode_url: namenode_url.to_string(),
            connection_timeout_ms: Some(30000),
            read_timeout_ms: Some(60000),
            kerberos: None,
        };

        // Create circuit breaker for HDFS operations
        let circuit_breaker_config = CircuitBreakerConfig::default();
        let circuit_breaker = Arc::new(CircuitBreaker::new(
            format!("hdfs-{}", namenode_url),
            circuit_breaker_config,
        ));

        Ok(Self {
            client,
            config,
            circuit_breaker,
        })
    }

    fn create_kerberos_client(
        namenode_url: &str,
        kerberos_config: &KerberosConfig,
    ) -> Result<hdfs_native::Client> {
        info!(
            "Setting up Kerberos authentication for principal: {}",
            kerberos_config.principal
        );

        // Set Kerberos environment variables if provided
        if let Some(krb5_config_path) = &kerberos_config.krb5_config_path {
            std::env::set_var("KRB5_CONFIG", krb5_config_path);
            debug!("Set KRB5_CONFIG to: {}", krb5_config_path);
        }

        if let Some(realm) = &kerberos_config.realm {
            std::env::set_var("KRB5_REALM", realm);
            debug!("Set KRB5_REALM to: {}", realm);
        }

        // Set environment variables for keytab authentication
        if let Some(keytab_path) = &kerberos_config.keytab_path {
            info!("Using keytab authentication with file: {}", keytab_path);
            std::env::set_var("KRB5_PRINCIPAL", &kerberos_config.principal);
            std::env::set_var("KRB5_KEYTAB", keytab_path);
        } else {
            info!("Using ticket cache for Kerberos authentication");
        }

        // Create HDFS client - the hdfs-native library should automatically detect
        // Kerberos configuration from environment variables
        let client = hdfs_native::Client::new(namenode_url)
            .map_err(|e| anyhow!("Failed to create HDFS client with Kerberos: {}", e))?;

        info!("Successfully initialized HDFS client with Kerberos authentication");
        Ok(client)
    }

    pub async fn health_check(&self) -> Result<bool> {
        debug!("Performing HDFS health check");

        let result = self
            .circuit_breaker
            .call(async {
                self.client
                    .list_status("/", false)
                    .await
                    .map_err(|e| anyhow!("HDFS health check failed: {}", e))
            })
            .await;

        match result {
            Ok(_) => {
                debug!("HDFS health check passed");
                Ok(true)
            }
            Err(e) => {
                if e.is_circuit_open() {
                    warn!("HDFS health check failed: circuit breaker is open");
                } else {
                    warn!("HDFS health check failed: {:?}", e);
                }
                Err(anyhow!("HDFS health check failed: {:?}", e))
            }
        }
    }
}

#[async_trait]
impl FileReader for HdfsFileReader {
    async fn read_file(&self, path: &Path) -> Result<String> {
        use tokio::time::Duration;

        let path_str = path.to_string_lossy().to_string();
        debug!("Reading HDFS file: {}", path_str);

        let read_timeout = Duration::from_millis(self.config.read_timeout_ms.unwrap_or(60000));

        let result =
            self.circuit_breaker
                .call(async {
                    let file_result = tokio::time::timeout(read_timeout, async {
                        let mut file =
                            self.client.read(&path_str).await.map_err(|e| {
                                anyhow!("Failed to open HDFS file {}: {}", path_str, e)
                            })?;

                        let file_length = file.file_length();
                        debug!("HDFS file {} size: {} bytes", path_str, file_length);

                        let bytes = file
                            .read(file_length)
                            .await
                            .map_err(|e| anyhow!("Failed to read HDFS file {}: {}", path_str, e))?;

                        String::from_utf8(bytes.to_vec())
                            .map_err(|e| anyhow!("Invalid UTF-8 in HDFS file {}: {}", path_str, e))
                    })
                    .await;

                    match file_result {
                        Ok(result) => result,
                        Err(_) => Err(anyhow!(
                            "Timeout reading HDFS file {}: {} ms",
                            path_str,
                            read_timeout.as_millis()
                        )),
                    }
                })
                .await;

        match result {
            Ok(content) => Ok(content),
            Err(e) => {
                if e.is_circuit_open() {
                    Err(anyhow!("HDFS read failed: circuit breaker is open"))
                } else {
                    Err(e
                        .into_inner()
                        .unwrap_or_else(|| anyhow!("Unknown HDFS error")))
                }
            }
        }
    }

    async fn list_directory(&self, path: &Path) -> Result<Vec<String>> {
        let path_str = path.to_string_lossy().to_string();
        debug!("Listing HDFS directory: {}", path_str);

        let result = self
            .circuit_breaker
            .call(async {
                self.client
                    .list_status(&path_str, false)
                    .await
                    .map_err(|e| anyhow!("Failed to list HDFS directory {}: {}", path_str, e))
            })
            .await;

        let entries = match result {
            Ok(entries) => entries,
            Err(e) => {
                if e.is_circuit_open() {
                    return Err(anyhow!("HDFS list failed: circuit breaker is open"));
                } else {
                    return Err(e
                        .into_inner()
                        .unwrap_or_else(|| anyhow!("Unknown HDFS error")));
                }
            }
        };

        let file_names: Vec<String> = entries
            .into_iter()
            .map(|entry| {
                // Extract just the file name from the full path
                entry
                    .path
                    .rsplit('/')
                    .next()
                    .unwrap_or(&entry.path)
                    .to_string()
            })
            .collect();

        debug!(
            "Found {} entries in HDFS directory {}",
            file_names.len(),
            path_str
        );
        Ok(file_names)
    }

    async fn file_exists(&self, path: &Path) -> bool {
        let path_str = path.to_string_lossy().to_string();
        debug!("Checking HDFS file existence: {}", path_str);

        let result = self
            .circuit_breaker
            .call(async {
                self.client
                    .get_file_info(&path_str)
                    .await
                    .map_err(|e| anyhow!("Failed to get file info for {}: {}", path_str, e))
            })
            .await;

        match result {
            Ok(_) => {
                debug!("HDFS file exists: {}", path_str);
                true
            }
            Err(e) => {
                if e.is_circuit_open() {
                    warn!("HDFS file existence check failed: circuit breaker is open");
                } else {
                    debug!("HDFS file does not exist {}: {:?}", path_str, e);
                }
                false
            }
        }
    }
}

pub struct S3FileReader {
    s3_reader: S3Reader,
    prefix: String,
    circuit_breaker: Arc<CircuitBreaker>,
}

impl S3FileReader {
    pub async fn new(config: S3Config, prefix: String) -> Result<Self> {
        info!(
            "Initializing S3 file reader for bucket: {} with prefix: {}",
            config.bucket_name, prefix
        );

        let s3_reader = S3Reader::new(config.clone()).await?;

        // Create circuit breaker for S3 file operations
        let circuit_breaker_config = CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            timeout_duration: std::time::Duration::from_secs(30),
            window_duration: std::time::Duration::from_secs(300),
        };
        let circuit_breaker = Arc::new(CircuitBreaker::new(
            format!("s3-file-reader-{}", config.bucket_name),
            circuit_breaker_config,
        ));

        Ok(Self {
            s3_reader,
            prefix,
            circuit_breaker,
        })
    }

    #[allow(dead_code)]
    pub async fn new_simple(bucket_name: &str, region: Option<&str>, prefix: &str) -> Result<Self> {
        let config = S3Config {
            bucket_name: bucket_name.to_string(),
            region: region.map(|r| r.to_string()),
            endpoint_url: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            connection_timeout_ms: Some(30000),
            read_timeout_ms: Some(60000),
        };

        Self::new(config, prefix.to_string()).await
    }

    pub async fn health_check(&self) -> Result<bool> {
        debug!("Performing S3 file reader health check");

        let result = self
            .circuit_breaker
            .call(async {
                self.s3_reader
                    .health_check()
                    .await
                    .map_err(|e| anyhow!("S3 file reader health check failed: {}", e))
            })
            .await;

        match result {
            Ok(healthy) => {
                debug!("S3 file reader health check passed");
                Ok(healthy)
            }
            Err(e) => {
                if e.is_circuit_open() {
                    warn!("S3 file reader health check failed: circuit breaker is open");
                } else {
                    warn!("S3 file reader health check failed: {:?}", e);
                }
                Err(anyhow!("S3 file reader health check failed: {:?}", e))
            }
        }
    }

    pub fn s3_key_from_path(&self, path: &Path) -> String {
        let path_str = path.to_string_lossy();
        if self.prefix.is_empty() {
            path_str.to_string()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), path_str)
        }
    }
}

#[async_trait]
impl FileReader for S3FileReader {
    async fn read_file(&self, path: &Path) -> Result<String> {
        let s3_key = self.s3_key_from_path(path);
        debug!("Reading S3 file: {}", s3_key);

        let result = self
            .circuit_breaker
            .call(async {
                self.s3_reader
                    .read_object(&s3_key)
                    .await
                    .map_err(|e| anyhow!("Failed to read S3 object {}: {}", s3_key, e))
            })
            .await;

        match result {
            Ok(content) => Ok(content),
            Err(e) => {
                if e.is_circuit_open() {
                    Err(anyhow!("S3 read failed: circuit breaker is open"))
                } else {
                    Err(e
                        .into_inner()
                        .unwrap_or_else(|| anyhow!("Unknown S3 error")))
                }
            }
        }
    }

    async fn list_directory(&self, path: &Path) -> Result<Vec<String>> {
        let s3_prefix = if path.to_string_lossy().is_empty() {
            self.prefix.clone()
        } else {
            self.s3_key_from_path(path)
        };

        debug!("Listing S3 directory with prefix: {}", s3_prefix);

        let result = self
            .circuit_breaker
            .call(async {
                self.s3_reader
                    .list_applications(&s3_prefix)
                    .await
                    .map_err(|e| anyhow!("Failed to list S3 directory {}: {}", s3_prefix, e))
            })
            .await;

        match result {
            Ok(entries) => Ok(entries),
            Err(e) => {
                if e.is_circuit_open() {
                    Err(anyhow!("S3 list failed: circuit breaker is open"))
                } else {
                    Err(e
                        .into_inner()
                        .unwrap_or_else(|| anyhow!("Unknown S3 error")))
                }
            }
        }
    }

    async fn file_exists(&self, path: &Path) -> bool {
        let s3_key = self.s3_key_from_path(path);
        debug!("Checking S3 file existence: {}", s3_key);

        let result = self
            .circuit_breaker
            .call::<_, bool, anyhow::Error>(async {
                Ok(self.s3_reader.object_exists(&s3_key).await)
            })
            .await;

        match result {
            Ok(exists) => {
                debug!(
                    "S3 file exists check result: {} for key: {}",
                    exists, s3_key
                );
                exists
            }
            Err(e) => {
                if e.is_circuit_open() {
                    warn!("S3 file existence check failed: circuit breaker is open");
                } else {
                    debug!("S3 file existence check failed for {}: {:?}", s3_key, e);
                }
                false
            }
        }
    }
}

/// Create a file reader based on configuration
pub async fn create_file_reader(
    log_directory: &str,
    hdfs_config: Option<&HdfsConfig>,
    s3_config: Option<&S3Config>,
) -> Result<Box<dyn FileReader>> {
    if let Some(s3_config) = s3_config {
        info!(
            "Creating S3 file reader for bucket: {} with prefix: {}",
            s3_config.bucket_name, log_directory
        );
        let reader = S3FileReader::new(s3_config.clone(), log_directory.to_string()).await?;

        // Perform health check
        if let Err(e) = reader.health_check().await {
            warn!("S3 health check failed, but continuing: {}", e);
        }

        Ok(Box::new(reader))
    } else if let Some(hdfs_config) = hdfs_config {
        info!("Creating HDFS file reader for directory: {}", log_directory);
        let reader = HdfsFileReader::new(hdfs_config.clone())?;

        // Perform health check
        if let Err(e) = reader.health_check().await {
            warn!("HDFS health check failed, but continuing: {}", e);
        }

        Ok(Box::new(reader))
    } else {
        info!(
            "Creating local file reader for directory: {}",
            log_directory
        );
        Ok(Box::new(LocalFileReader::new()))
    }
}
