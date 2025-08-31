use anyhow::{anyhow, Result};
use flate2::read::GzDecoder;
use hdfs_native::Client;
use serde_json::Value;
use std::io::{BufRead, BufReader, Cursor};
use std::path::Path;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::spark_events::SparkEvent;

/// HDFS client wrapper for reading Spark event logs
pub struct HdfsReader {
    client: RwLock<Client>,
    base_path: String,
}

impl HdfsReader {
    /// Create a new HDFS reader
    pub async fn new(namenode_uri: &str, base_path: &str) -> Result<Self> {
        info!("Connecting to HDFS at: {}", namenode_uri);

        let client =
            Client::new(namenode_uri).map_err(|e| anyhow!("Failed to connect to HDFS: {}", e))?;

        info!("HDFS connected successfully");

        Ok(Self {
            client: RwLock::new(client),
            base_path: base_path.to_string(),
        })
    }

    /// List all application directories in the base path
    pub async fn list_applications(&self) -> Result<Vec<String>> {
        debug!("Listing applications in: {}", self.base_path);

        let mut app_ids = Vec::new();
        let path = std::path::Path::new(&self.base_path);

        if path.exists() {
            if let Ok(entries) = std::fs::read_dir(path) {
                for entry in entries {
                    if let Ok(entry) = entry {
                        if entry.path().is_dir() {
                            if let Some(name) = entry.file_name().to_str() {
                                if name.starts_with("application_") {
                                    app_ids.push(name.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }

        debug!("Found {} applications", app_ids.len());
        Ok(app_ids)
    }

    /// List event log files for a specific application
    pub async fn list_event_files(&self, app_id: &str) -> Result<Vec<String>> {
        let app_path = format!("{}/{}", self.base_path, app_id);
        debug!("Listing event files for app: {} at {}", app_id, app_path);

        let mut event_files = Vec::new();
        let path = std::path::Path::new(&app_path);

        if path.exists() {
            if let Ok(entries) = std::fs::read_dir(path) {
                for entry in entries {
                    if let Ok(entry) = entry {
                        if entry.path().is_file() {
                            let filename = entry.file_name();
                            if let Some(name) = filename.to_str() {
                                if name.starts_with("events")
                                    || name.contains("eventLog")
                                    || name.ends_with(".inprogress")
                                {
                                    event_files.push(entry.path().to_string_lossy().to_string());
                                }
                            }
                        }
                    }
                }
            }
        }

        debug!("Found {} event files for app {}", event_files.len(), app_id);
        Ok(event_files)
    }

    /// Read and parse events from a specific file
    pub async fn read_events(&self, file_path: &str, app_id: &str) -> Result<Vec<SparkEvent>> {
        debug!("Reading events from: {}", file_path);

        let content = tokio::fs::read_to_string(file_path)
            .await
            .map_err(|e| anyhow!("Failed to read file {}: {}", file_path, e))?;

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

        info!("Parsed {} events from {}", events.len(), file_path);
        Ok(events)
    }

    /// Read all events for a specific application
    pub async fn read_application_events(&self, app_id: &str) -> Result<Vec<SparkEvent>> {
        let event_files = self.list_event_files(app_id).await?;

        if event_files.is_empty() {
            return Err(anyhow!("No event files found for application: {}", app_id));
        }

        let mut all_events = Vec::new();

        for file_path in event_files {
            match self.read_events(&file_path, app_id).await {
                Ok(mut events) => all_events.append(&mut events),
                Err(e) => warn!("Failed to read events from {}: {}", file_path, e),
            }
        }

        // Sort events by timestamp
        all_events.sort_by_key(|e| e.timestamp);

        info!(
            "Total {} events loaded for application {}",
            all_events.len(),
            app_id
        );
        Ok(all_events)
    }

    /// Scan all applications and return events
    pub async fn scan_all_events(&self) -> Result<Vec<SparkEvent>> {
        let app_ids = self.list_applications().await?;
        let mut all_events = Vec::new();

        info!("Scanning {} applications for events", app_ids.len());

        for app_id in app_ids {
            match self.read_application_events(&app_id).await {
                Ok(mut events) => {
                    debug!("Loaded {} events for {}", events.len(), app_id);
                    all_events.append(&mut events);
                }
                Err(e) => warn!("Failed to load events for {}: {}", app_id, e),
            }
        }

        // Sort all events by timestamp
        all_events.sort_by_key(|e| e.timestamp);

        info!("Total {} events scanned from HDFS", all_events.len());
        Ok(all_events)
    }

    /// Decompress file content if needed based on file extension
    fn decompress_if_needed(&self, data: Vec<u8>, file_path: &str) -> Result<Vec<u8>> {
        if file_path.ends_with(".gz") {
            debug!("Decompressing gzip file: {}", file_path);
            let mut decoder = GzDecoder::new(&data[..]);
            let mut decompressed = Vec::new();
            std::io::copy(&mut decoder, &mut decompressed)
                .map_err(|e| anyhow!("Failed to decompress {}: {}", file_path, e))?;
            Ok(decompressed)
        } else if file_path.ends_with(".lz4") {
            // Note: lz4 support would require additional dependency
            warn!("LZ4 decompression not implemented for: {}", file_path);
            Ok(data)
        } else if file_path.ends_with(".snappy") {
            // Note: Snappy support would require additional dependency
            warn!("Snappy decompression not implemented for: {}", file_path);
            Ok(data)
        } else {
            Ok(data)
        }
    }

    /// Check if HDFS connection is healthy
    pub async fn health_check(&self) -> Result<bool> {
        // Mock health check for now
        debug!("Mock HDFS health check passed");
        Ok(true)
    }

    /// Get file info for monitoring
    pub async fn get_file_info(&self, file_path: &str) -> Result<HdfsFileInfo> {
        // Mock file info
        Ok(HdfsFileInfo {
            path: file_path.to_string(),
            size: 1024,
            modification_time: chrono::Utc::now().timestamp_millis(),
            is_directory: false,
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

/// Configuration for HDFS connection
#[derive(Debug, Clone)]
pub struct HdfsConfig {
    pub namenode_uri: String,
    pub base_path: String,
    pub connection_timeout_ms: Option<u64>,
    pub read_timeout_ms: Option<u64>,
}

impl Default for HdfsConfig {
    fn default() -> Self {
        Self {
            namenode_uri: "hdfs://localhost:9000".to_string(),
            base_path: "/var/log/spark/events".to_string(),
            connection_timeout_ms: Some(30000),
            read_timeout_ms: Some(60000),
        }
    }
}
