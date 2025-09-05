use anyhow::{anyhow, Result};
use serde_json::Value;
use tokio::time::Duration;
use tracing::{debug, info, warn};

use crate::config::HdfsConfig;
use crate::spark_events::SparkEvent;

/// HDFS client wrapper for reading Spark event logs
pub struct HdfsReader {
    client: hdfs_native::Client,
    config: HdfsConfig,
}

impl HdfsReader {
    /// Create a new HDFS reader with real HDFS integration
    pub async fn new(namenode_uri: &str, base_path: &str) -> Result<Self> {
        info!(
            "Creating HDFS reader for namenode: {}, path: {}",
            namenode_uri, base_path
        );

        let config = crate::config::HdfsConfig {
            namenode_url: namenode_uri.to_string(),
            connection_timeout_ms: Some(30000),
            read_timeout_ms: Some(60000),
            kerberos: None,
        };

        let client = hdfs_native::Client::new(namenode_uri)
            .map_err(|e| anyhow!("Failed to create HDFS client: {}", e))?;

        info!("Successfully created HDFS client for: {}", namenode_uri);

        Ok(Self { client, config })
    }

    /// Create a new HDFS reader with full configuration
    pub async fn new_with_config(config: crate::config::HdfsConfig) -> Result<Self> {
        info!(
            "Creating HDFS reader with Kerberos config for: {}",
            config.namenode_url
        );

        let client = if let Some(kerberos_config) = &config.kerberos {
            Self::create_kerberos_client(&config.namenode_url, kerberos_config)?
        } else {
            hdfs_native::Client::new(&config.namenode_url)
                .map_err(|e| anyhow!("Failed to create HDFS client: {}", e))?
        };

        Ok(Self { client, config })
    }

    fn create_kerberos_client(
        namenode_url: &str,
        kerberos_config: &crate::config::KerberosConfig,
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

        let client = hdfs_native::Client::new(namenode_url)
            .map_err(|e| anyhow!("Failed to create HDFS client with Kerberos: {}", e))?;

        info!("Successfully initialized HDFS client with Kerberos authentication");
        Ok(client)
    }

    /// List all application directories in HDFS base path
    pub async fn list_applications(&self, base_path: &str) -> Result<Vec<String>> {
        debug!("Listing applications in HDFS: {}", base_path);

        // Apply timeout to HDFS operations
        let timeout = Duration::from_millis(self.config.connection_timeout_ms.unwrap_or(30000));

        let entries_result = tokio::time::timeout(timeout, async {
            self.client
                .list_status(base_path, false)
                .await
                .map_err(|e| anyhow!("Failed to list HDFS directory {}: {}", base_path, e))
        })
        .await;

        let entries = match entries_result {
            Ok(result) => result?,
            Err(_) => {
                return Err(anyhow!(
                    "Timeout listing HDFS directory {}: {} ms",
                    base_path,
                    timeout.as_millis()
                ));
            }
        };

        let mut app_ids = Vec::new();
        for entry in entries {
            if entry.isdir {
                let name = entry
                    .path
                    .rsplit('/')
                    .next()
                    .unwrap_or(&entry.path)
                    .to_string();

                // Support multiple Spark event log directory patterns:
                // - application_* (standard format)
                // - app-* (alternative format)
                // - eventlog_v2_* (structured event logs v2)
                if name.starts_with("application_")
                    || name.starts_with("app-")
                    || name.starts_with("eventlog_v2_")
                {
                    app_ids.push(name);
                }
            }
        }

        debug!("Found {} applications in HDFS", app_ids.len());
        Ok(app_ids)
    }

    /// List event log files for a specific application in HDFS
    pub async fn list_event_files(&self, base_path: &str, app_id: &str) -> Result<Vec<String>> {
        let app_path = format!("{}/{}", base_path, app_id);
        debug!(
            "Listing event files for app: {} at HDFS path: {}",
            app_id, app_path
        );

        let timeout = Duration::from_millis(self.config.connection_timeout_ms.unwrap_or(30000));

        let entries_result = tokio::time::timeout(timeout, async {
            self.client
                .list_status(&app_path, false)
                .await
                .map_err(|e| anyhow!("Failed to list HDFS app directory {}: {}", app_path, e))
        })
        .await;

        let entries = match entries_result {
            Ok(result) => result?,
            Err(_) => {
                return Err(anyhow!(
                    "Timeout listing HDFS app directory {}: {} ms",
                    app_path,
                    timeout.as_millis()
                ));
            }
        };

        let mut event_files = Vec::new();
        for entry in entries {
            if !entry.isdir {
                let filename = entry
                    .path
                    .rsplit('/')
                    .next()
                    .unwrap_or(&entry.path)
                    .to_string();

                if filename.starts_with("events")
                    || filename.contains("eventLog")
                    || filename.ends_with(".inprogress")
                {
                    event_files.push(entry.path);
                }
            }
        }

        debug!(
            "Found {} event files for app {} in HDFS",
            event_files.len(),
            app_id
        );
        Ok(event_files)
    }

    /// Read and parse events from a specific HDFS file
    pub async fn read_events(&self, file_path: &str, app_id: &str) -> Result<Vec<SparkEvent>> {
        debug!("Reading events from HDFS file: {}", file_path);

        let read_timeout = Duration::from_millis(self.config.read_timeout_ms.unwrap_or(60000));

        let content_result = tokio::time::timeout(read_timeout, async {
            let mut file = self
                .client
                .read(file_path)
                .await
                .map_err(|e| anyhow!("Failed to open HDFS file {}: {}", file_path, e))?;

            let file_length = file.file_length();
            debug!("HDFS file {} size: {} bytes", file_path, file_length);

            let bytes = file
                .read(file_length)
                .await
                .map_err(|e| anyhow!("Failed to read HDFS file {}: {}", file_path, e))?;

            String::from_utf8(bytes.to_vec())
                .map_err(|e| anyhow!("Invalid UTF-8 in HDFS file {}: {}", file_path, e))
        })
        .await;

        let content = match content_result {
            Ok(result) => result?,
            Err(_) => {
                return Err(anyhow!(
                    "Timeout reading HDFS file {}: {} ms",
                    file_path,
                    read_timeout.as_millis()
                ));
            }
        };

        let mut events = Vec::new();
        for (line_num, line) in content.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<Value>(line) {
                Ok(json_event) => match SparkEvent::from_json(&json_event, app_id) {
                    Ok(event) => events.push(event),
                    Err(e) => warn!("Failed to parse event at line {}: {}", line_num + 1, e),
                },
                Err(e) => warn!(
                    "Invalid JSON at line {} in {}: {}",
                    line_num + 1,
                    file_path,
                    e
                ),
            }
        }

        info!(
            "Parsed {} events from HDFS file {}",
            events.len(),
            file_path
        );
        Ok(events)
    }

    /// Read all events for a specific application from HDFS
    pub async fn read_application_events(
        &self,
        base_path: &str,
        app_id: &str,
    ) -> Result<Vec<SparkEvent>> {
        let event_files = self.list_event_files(base_path, app_id).await?;

        if event_files.is_empty() {
            return Err(anyhow!("No event files found for application: {}", app_id));
        }

        let mut all_events = Vec::new();

        for file_path in event_files {
            match self.read_events(&file_path, app_id).await {
                Ok(mut events) => all_events.append(&mut events),
                Err(e) => warn!("Failed to read events from HDFS file {}: {}", file_path, e),
            }
        }

        // Sort events by timestamp
        all_events.sort_by_key(|e| e.timestamp);

        info!(
            "Total {} events loaded for application {} from HDFS",
            all_events.len(),
            app_id
        );
        Ok(all_events)
    }

    /// Scan all applications and return events from HDFS
    pub async fn scan_all_events(&self, base_path: &str) -> Result<Vec<SparkEvent>> {
        let app_ids = self.list_applications(base_path).await?;
        let mut all_events = Vec::new();

        info!("Scanning {} applications for events in HDFS", app_ids.len());

        for app_id in app_ids {
            match self.read_application_events(base_path, &app_id).await {
                Ok(mut events) => {
                    debug!("Loaded {} events for {} from HDFS", events.len(), app_id);
                    all_events.append(&mut events);
                }
                Err(e) => warn!("Failed to load events for {} from HDFS: {}", app_id, e),
            }
        }

        // Sort all events by timestamp
        all_events.sort_by_key(|e| e.timestamp);

        info!("Total {} events scanned from HDFS", all_events.len());
        Ok(all_events)
    }

    /// Check if HDFS connection is healthy (real implementation)
    pub async fn health_check(&self) -> Result<bool> {
        debug!("Performing HDFS health check");

        let timeout = Duration::from_millis(self.config.connection_timeout_ms.unwrap_or(30000));

        let health_result = tokio::time::timeout(timeout, async {
            self.client
                .list_status("/", false)
                .await
                .map_err(|e| anyhow!("HDFS health check failed: {}", e))
        })
        .await;

        match health_result {
            Ok(Ok(_)) => {
                debug!("HDFS health check passed");
                Ok(true)
            }
            Ok(Err(e)) => {
                warn!("HDFS health check failed: {}", e);
                Err(e)
            }
            Err(_) => {
                warn!("HDFS health check timeout: {} ms", timeout.as_millis());
                Err(anyhow!(
                    "HDFS health check timeout: {} ms",
                    timeout.as_millis()
                ))
            }
        }
    }

    /// Get file info for monitoring from HDFS
    pub async fn get_file_info(&self, file_path: &str) -> Result<HdfsFileInfo> {
        debug!("Getting HDFS file info for: {}", file_path);

        let timeout = Duration::from_millis(self.config.connection_timeout_ms.unwrap_or(30000));

        let info_result = tokio::time::timeout(timeout, async {
            self.client
                .get_file_info(file_path)
                .await
                .map_err(|e| anyhow!("Failed to get HDFS file info for {}: {}", file_path, e))
        })
        .await;

        let file_status = match info_result {
            Ok(result) => result?,
            Err(_) => {
                return Err(anyhow!(
                    "Timeout getting HDFS file info for {}: {} ms",
                    file_path,
                    timeout.as_millis()
                ));
            }
        };

        Ok(HdfsFileInfo {
            path: file_path.to_string(),
            size: file_status.length as i64,
            modification_time: file_status.modification_time as i64,
            is_directory: file_status.isdir,
        })
    }
}

/// File information from HDFS
#[derive(Debug, Clone)]
pub struct HdfsFileInfo {
    pub path: String,
    pub size: i64,
    pub modification_time: i64,
    pub is_directory: bool,
}

/// File metadata for tracking changes
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileMetadata {
    pub path: String,
    pub last_processed: i64,
    pub file_size: i64,
    pub last_index: Option<i64>,
    pub is_complete: bool,
}
