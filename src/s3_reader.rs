#![allow(dead_code)]

use anyhow::{anyhow, Result};
use aws_config::meta::region::RegionProviderChain;
use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::Client;
use aws_types::region::Region;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use crate::config::S3Config;

/// S3 client wrapper for reading Spark event logs from S3-compatible storage
pub struct S3Reader {
    client: Client,
    config: S3Config,
    circuit_breaker: Arc<CircuitBreaker>,
}

impl S3Reader {
    /// Create a new S3 reader with configuration
    pub async fn new(config: S3Config) -> Result<Self> {
        info!(
            "Creating S3 reader for bucket: {}, region: {}",
            config.bucket_name,
            config.region.as_deref().unwrap_or("us-east-1")
        );

        let aws_config = Self::build_aws_config(&config).await?;
        let client = Client::new(&aws_config);

        // Create circuit breaker for S3 operations
        let circuit_breaker_config = CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            timeout_duration: Duration::from_secs(30),
            window_duration: Duration::from_secs(300),
        };
        let circuit_breaker = Arc::new(CircuitBreaker::new(
            format!("s3-{}", config.bucket_name),
            circuit_breaker_config,
        ));

        Ok(Self {
            client,
            config,
            circuit_breaker,
        })
    }

    /// Create a simple S3 reader with minimal configuration
    #[allow(dead_code)]
    pub async fn new_simple(bucket_name: &str, region: Option<&str>) -> Result<Self> {
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

        Self::new(config).await
    }

    /// Build AWS configuration from S3Config
    async fn build_aws_config(config: &S3Config) -> Result<aws_types::sdk_config::SdkConfig> {
        let region_str = config.region.as_deref().unwrap_or("us-east-1");
        let region = Region::new(region_str.to_string());
        let region_provider = RegionProviderChain::default_provider().or_else(region);

        let mut config_builder =
            aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region_provider);

        // Set custom endpoint if provided (for S3-compatible services like MinIO)
        if let Some(endpoint_url) = &config.endpoint_url {
            config_builder = config_builder.endpoint_url(endpoint_url);
        }

        // Configure timeouts
        let timeout_config = TimeoutConfig::builder()
            .connect_timeout(Duration::from_millis(
                config.connection_timeout_ms.unwrap_or(30000),
            ))
            .read_timeout(Duration::from_millis(
                config.read_timeout_ms.unwrap_or(60000),
            ))
            .build();
        config_builder = config_builder.timeout_config(timeout_config);

        // Configure retries
        let retry_config = RetryConfig::standard().with_max_attempts(3);
        config_builder = config_builder.retry_config(retry_config);

        // Set credentials if provided
        if let (Some(access_key), Some(secret_key)) =
            (&config.access_key_id, &config.secret_access_key)
        {
            let credentials = Credentials::new(
                access_key,
                secret_key,
                config.session_token.clone(),
                None,
                "spark-history-server-s3",
            );
            config_builder = config_builder.credentials_provider(credentials);
        }

        Ok(config_builder.load().await)
    }

    /// List all application directories (prefixes) in S3 bucket
    pub async fn list_applications(&self, prefix: &str) -> Result<Vec<String>> {
        debug!(
            "Listing applications in S3 bucket: {} with prefix: {}",
            self.config.bucket_name, prefix
        );

        let result = self
            .circuit_breaker
            .call(async {
                let mut request = self
                    .client
                    .list_objects_v2()
                    .bucket(&self.config.bucket_name)
                    .delimiter("/");

                if !prefix.is_empty() {
                    request = request.prefix(prefix);
                }

                request
                    .send()
                    .await
                    .map_err(|e| anyhow!("Failed to list S3 objects: {}", e))
            })
            .await;

        let response = match result {
            Ok(response) => response,
            Err(e) => {
                if e.is_circuit_open() {
                    return Err(anyhow!("S3 list failed: circuit breaker is open"));
                } else {
                    return Err(e
                        .into_inner()
                        .unwrap_or_else(|| anyhow!("Unknown S3 error")));
                }
            }
        };

        let mut app_ids = Vec::new();

        // Process common prefixes (directories)
        let common_prefixes = response.common_prefixes();
        if !common_prefixes.is_empty() {
            for prefix_info in common_prefixes {
                if let Some(prefix_str) = prefix_info.prefix() {
                    // Extract application ID from prefix
                    let app_id = prefix_str
                        .trim_end_matches('/')
                        .rsplit('/')
                        .next()
                        .unwrap_or(prefix_str)
                        .to_string();

                    // Support multiple Spark event log directory patterns
                    if app_id.starts_with("application_")
                        || app_id.starts_with("app-")
                        || app_id.starts_with("eventlog_v2_")
                    {
                        app_ids.push(app_id);
                    }
                }
            }
        }

        debug!("Found {} applications in S3 bucket", app_ids.len());
        Ok(app_ids)
    }

    /// List event log files for a specific application in S3
    #[allow(dead_code)]
    pub async fn list_event_files(&self, prefix: &str, app_id: &str) -> Result<Vec<String>> {
        let app_prefix = if prefix.is_empty() {
            format!("{}/", app_id)
        } else {
            format!("{}/{}/", prefix.trim_end_matches('/'), app_id)
        };

        debug!(
            "Listing event files for app: {} with prefix: {}",
            app_id, app_prefix
        );

        let result = self
            .circuit_breaker
            .call(async {
                self.client
                    .list_objects_v2()
                    .bucket(&self.config.bucket_name)
                    .prefix(&app_prefix)
                    .send()
                    .await
                    .map_err(|e| anyhow!("Failed to list S3 objects for app {}: {}", app_id, e))
            })
            .await;

        let response = match result {
            Ok(response) => response,
            Err(e) => {
                if e.is_circuit_open() {
                    return Err(anyhow!("S3 list failed: circuit breaker is open"));
                } else {
                    return Err(e
                        .into_inner()
                        .unwrap_or_else(|| anyhow!("Unknown S3 error")));
                }
            }
        };

        let mut event_files = Vec::new();

        let objects = response.contents();
        if !objects.is_empty() {
            for object in objects {
                if let Some(key) = object.key() {
                    let filename = key.rsplit('/').next().unwrap_or(key);

                    // Filter for event log files
                    if filename.starts_with("events")
                        || filename.contains("eventLog")
                        || filename.ends_with(".inprogress")
                    {
                        event_files.push(key.to_string());
                    }
                }
            }
        }

        debug!(
            "Found {} event files for app {} in S3",
            event_files.len(),
            app_id
        );
        Ok(event_files)
    }

    /// Read content from an S3 object
    pub async fn read_object(&self, key: &str) -> Result<String> {
        debug!(
            "Reading S3 object: {} from bucket: {}",
            key, self.config.bucket_name
        );

        let result = self
            .circuit_breaker
            .call(async {
                let response = self
                    .client
                    .get_object()
                    .bucket(&self.config.bucket_name)
                    .key(key)
                    .send()
                    .await
                    .map_err(|e| anyhow!("Failed to get S3 object {}: {}", key, e))?;

                let body = response.body;
                let bytes = body
                    .collect()
                    .await
                    .map_err(|e| anyhow!("Failed to read S3 object body {}: {}", key, e))?;

                String::from_utf8(bytes.to_vec())
                    .map_err(|e| anyhow!("Invalid UTF-8 in S3 object {}: {}", key, e))
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

    /// Check if an S3 object exists
    #[allow(dead_code)]
    pub async fn object_exists(&self, key: &str) -> bool {
        debug!(
            "Checking S3 object existence: {} in bucket: {}",
            key, self.config.bucket_name
        );

        let result = self
            .circuit_breaker
            .call(async {
                self.client
                    .head_object()
                    .bucket(&self.config.bucket_name)
                    .key(key)
                    .send()
                    .await
                    .map_err(|e| anyhow!("Failed to head S3 object {}: {}", key, e))
            })
            .await;

        match result {
            Ok(_) => {
                debug!("S3 object exists: {}", key);
                true
            }
            Err(e) => {
                if e.is_circuit_open() {
                    warn!("S3 object existence check failed: circuit breaker is open");
                } else {
                    debug!("S3 object does not exist {}: {:?}", key, e);
                }
                false
            }
        }
    }

    /// Check if S3 connection is healthy
    pub async fn health_check(&self) -> Result<bool> {
        debug!(
            "Performing S3 health check on bucket: {}",
            self.config.bucket_name
        );

        let result = self
            .circuit_breaker
            .call(async {
                // Try to list objects with a small limit to test connectivity
                self.client
                    .list_objects_v2()
                    .bucket(&self.config.bucket_name)
                    .max_keys(1)
                    .send()
                    .await
                    .map_err(|e| anyhow!("S3 health check failed: {}", e))
            })
            .await;

        match result {
            Ok(_) => {
                debug!("S3 health check passed");
                Ok(true)
            }
            Err(e) => {
                if e.is_circuit_open() {
                    warn!("S3 health check failed: circuit breaker is open");
                } else {
                    warn!("S3 health check failed: {:?}", e);
                }
                Err(anyhow!("S3 health check failed: {:?}", e))
            }
        }
    }

    /// Get object metadata
    #[allow(dead_code)]
    pub async fn get_object_info(&self, key: &str) -> Result<S3ObjectInfo> {
        debug!("Getting S3 object info for: {}", key);

        let result = self
            .circuit_breaker
            .call(async {
                self.client
                    .head_object()
                    .bucket(&self.config.bucket_name)
                    .key(key)
                    .send()
                    .await
                    .map_err(|e| anyhow!("Failed to get S3 object info for {}: {}", key, e))
            })
            .await;

        let response = match result {
            Ok(response) => response,
            Err(e) => {
                if e.is_circuit_open() {
                    return Err(anyhow!(
                        "S3 get object info failed: circuit breaker is open"
                    ));
                } else {
                    return Err(e
                        .into_inner()
                        .unwrap_or_else(|| anyhow!("Unknown S3 error")));
                }
            }
        };

        Ok(S3ObjectInfo {
            key: key.to_string(),
            size: response.content_length().unwrap_or(0),
            last_modified: response.last_modified().map(|dt| dt.secs()).unwrap_or(0),
            etag: response.e_tag().unwrap_or("").to_string(),
            content_type: response.content_type().unwrap_or("").to_string(),
        })
    }
}

/// S3 object information
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct S3ObjectInfo {
    pub key: String,
    pub size: i64,
    pub last_modified: i64,
    pub etag: String,
    pub content_type: String,
}
