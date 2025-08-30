use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::{
    fs,
    path::Path,
    sync::Arc,
    time::SystemTime,
};
use tokio::{
    time::{interval, Duration},
};
use tracing::{debug, error, info, warn};

use crate::config::HistoryConfig;
use crate::models::{ApplicationInfo, ApplicationStatus};

mod event_log;
pub mod file_reader;
pub mod hybrid_store;

use event_log::EventLogParser;
pub use file_reader::FileReader;
pub use hybrid_store::{ApplicationStore, HybridStore, InMemoryStore, RocksDbStore};

/// History provider that manages Spark application history
pub struct HistoryProvider {
    config: HistoryConfig,
    store: Arc<HybridStore>,
    file_reader: Arc<dyn FileReader>,
    event_parser: EventLogParser,
}

impl HistoryProvider {
    pub async fn new(config: HistoryConfig) -> Result<Self> {
        let file_reader: Arc<dyn FileReader> = Arc::new(file_reader::LocalFileReader::new());
        let event_parser = EventLogParser::new();

        // Initialize hybrid store
        let mut store = HybridStore::new();
        
        // Set up persistent storage if enabled
        if config.enable_cache {
            if let Some(ref cache_dir) = config.cache_directory {
                info!("Initializing persistent cache at: {}", cache_dir);
                std::fs::create_dir_all(cache_dir)?;
                store.initialize_disk_store(cache_dir).await?;
            } else {
                warn!("Cache enabled but no cache directory specified. Using in-memory only.");
            }
        }

        let provider = Self {
            config: config.clone(),
            store: Arc::new(store),
            file_reader,
            event_parser,
        };

        // Initial scan - this will load into memory first
        provider.scan_event_logs_internal().await?;

        // Switch to persistent storage after initial load
        if config.enable_cache && config.cache_directory.is_some() {
            info!("Switching to persistent storage after initial load");
            provider.store.switch_to_persistent().await?;
        }

        // Start background refresh task
        let provider_clone = provider.clone();
        tokio::spawn(async move {
            provider_clone.start_background_refresh().await;
        });

        Ok(provider)
    }

    pub fn set_file_reader(&mut self, file_reader: Arc<dyn FileReader>) {
        self.file_reader = file_reader;
    }

    pub async fn scan_event_logs(&self) -> Result<()> {
        self.scan_event_logs_internal().await
    }

    pub async fn get_applications(
        &self,
        limit: Option<usize>,
        status_filter: Option<Vec<ApplicationStatus>>,
        min_date: Option<DateTime<Utc>>,
        max_date: Option<DateTime<Utc>>,
        min_end_date: Option<DateTime<Utc>>,
        max_end_date: Option<DateTime<Utc>>,
    ) -> Result<Vec<ApplicationInfo>> {
        let mut results = self.store.list().await?;

        // Apply filters
        if let Some(status_filters) = &status_filter {
            results.retain(|app| {
                let is_completed = app.attempts.iter().all(|attempt| attempt.completed);
                let app_status = if is_completed {
                    ApplicationStatus::Completed
                } else {
                    ApplicationStatus::Running
                };
                status_filters.contains(&app_status)
            });
        }

        // Date filters
        if min_date.is_some() || max_date.is_some() || min_end_date.is_some() || max_end_date.is_some() {
            results.retain(|app| {
                app.attempts.iter().any(|attempt| {
                    let start_ok = min_date.map_or(true, |min| attempt.start_time >= min)
                        && max_date.map_or(true, |max| attempt.start_time <= max);
                    
                    let end_ok = if attempt.completed {
                        min_end_date.map_or(true, |min| attempt.end_time >= min)
                            && max_end_date.map_or(true, |max| attempt.end_time <= max)
                    } else {
                        max_end_date.map_or(true, |max| max > Utc::now())
                    };

                    start_ok && end_ok
                })
            });
        }

        // Sort by end time (newest first)
        results.sort_by(|a, b| {
            let a_time = a.attempts.iter().map(|att| att.end_time).max().unwrap_or_default();
            let b_time = b.attempts.iter().map(|att| att.end_time).max().unwrap_or_default();
            b_time.cmp(&a_time)
        });

        // Apply limit
        if let Some(limit) = limit {
            results.truncate(limit.min(self.config.max_apps_per_request));
        }

        Ok(results)
    }

    pub async fn get_application(&self, app_id: &str) -> Result<Option<ApplicationInfo>> {
        self.store.get(app_id).await
    }

    async fn scan_event_logs_internal(&self) -> Result<()> {
        let log_dir = Path::new(&self.config.log_directory);
        if !log_dir.exists() {
            return Err(anyhow!("Log directory does not exist: {}", self.config.log_directory));
        }

        info!("Scanning event logs in: {}", self.config.log_directory);
        let mut app_count = 0;

        for entry in fs::read_dir(log_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                // Single application directory
                if let Ok(app_info) = self.parse_application_directory(&path).await {
                    self.store.put(&app_info.id.clone(), app_info).await?;
                    app_count += 1;
                }
            } else if path.extension().and_then(|s| s.to_str()) == Some("inprogress") 
                || path.to_string_lossy().contains("eventLog") {
                // Single event log file
                if let Ok(app_info) = self.parse_event_log_file(&path).await {
                    self.store.put(&app_info.id.clone(), app_info).await?;
                    app_count += 1;
                }
            }
        }

        info!("Loaded {} applications from event logs", app_count);
        Ok(())
    }

    async fn parse_application_directory(&self, app_dir: &Path) -> Result<ApplicationInfo> {
        let _app_id = app_dir
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow!("Invalid application directory name"))?;

        // Find event log files in the directory
        let mut event_files = Vec::new();
        for entry in fs::read_dir(app_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                let filename = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
                if filename.starts_with("events_") || filename == "eventLog" || filename.contains("eventLog") {
                    event_files.push(path);
                }
            }
        }

        if event_files.is_empty() {
            return Err(anyhow!("No event log files found in directory: {:?}", app_dir));
        }

        // Parse the main event log file (usually the largest or most recent)
        event_files.sort_by_key(|p| {
            p.metadata()
                .and_then(|m| m.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH)
        });
        
        let main_event_file = event_files.last().unwrap();
        self.parse_event_log_file(main_event_file).await
    }

    async fn parse_event_log_file(&self, file_path: &Path) -> Result<ApplicationInfo> {
        debug!("Parsing event log file: {:?}", file_path);
        
        let content = self.file_reader.read_file(file_path).await?;
        
        // Determine if file is compressed
        let decompressed = if self.config.compression_enabled {
            self.decompress_if_needed(&content, file_path)?
        } else {
            content
        };

        let events: Vec<Value> = decompressed
            .lines()
            .filter_map(|line| {
                if line.trim().is_empty() {
                    None
                } else {
                    serde_json::from_str(line).map_err(|e| {
                        warn!("Failed to parse event line: {}", e);
                        e
                    }).ok()
                }
            })
            .collect();

        self.event_parser.parse_application_from_events(events, file_path)
    }

    fn decompress_if_needed(&self, content: &str, file_path: &Path) -> Result<String> {
        if let Some(ext) = file_path.extension().and_then(|s| s.to_str()) {
            match ext {
                "gz" => {
                    use flate2::read::GzDecoder;
                    use std::io::Read;
                    
                    let mut decoder = GzDecoder::new(content.as_bytes());
                    let mut decompressed = String::new();
                    decoder.read_to_string(&mut decompressed)?;
                    Ok(decompressed)
                }
                "lz4" => {
                    // LZ4 decompression would require additional dependency
                    warn!("LZ4 decompression not implemented, treating as plain text");
                    Ok(content.to_string())
                }
                _ => Ok(content.to_string()),
            }
        } else {
            Ok(content.to_string())
        }
    }

    async fn start_background_refresh(&self) {
        let mut interval = interval(Duration::from_secs(self.config.update_interval_seconds));
        
        loop {
            interval.tick().await;
            if let Err(e) = self.scan_event_logs_internal().await {
                error!("Error during background refresh: {}", e);
            }
        }
    }
}

impl Clone for HistoryProvider {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            store: Arc::clone(&self.store),
            file_reader: Arc::clone(&self.file_reader),
            event_parser: self.event_parser.clone(),
        }
    }
}