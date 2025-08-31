use anyhow::{anyhow, Result};
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::spark_events::SparkEvent;

/// HDFS client wrapper for reading Spark event logs
pub struct HdfsReader {
    base_path: String,
}

impl HdfsReader {
    /// Create a new HDFS reader
    pub async fn new(_namenode_uri: &str, base_path: &str) -> Result<Self> {
        info!("Creating HDFS reader for path: {}", base_path);

        Ok(Self {
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
                for entry in entries.flatten() {
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
                for entry in entries.flatten() {
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

    /// Check if HDFS connection is healthy
    pub async fn health_check(&self) -> Result<bool> {
        // Mock health check for now
        debug!("Mock HDFS health check passed");
        Ok(true)
    }

    /// Get file info for monitoring
    pub async fn get_file_info(&self, file_path: &str) -> Result<HdfsFileInfo> {
        let path = std::path::Path::new(file_path);
        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|e| anyhow!("Failed to get metadata for {}: {}", file_path, e))?;

        let modification_time = metadata
            .modified()
            .map_err(|e| anyhow!("Failed to get modification time for {}: {}", file_path, e))?
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| anyhow!("Invalid modification time for {}: {}", file_path, e))?
            .as_millis() as i64;

        Ok(HdfsFileInfo {
            path: file_path.to_string(),
            size: metadata.len() as i64,
            modification_time,
            is_directory: metadata.is_dir(),
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
