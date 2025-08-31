use anyhow::Result;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, Duration};
use tracing::{debug, error, info, warn};

use crate::hdfs_reader::{HdfsConfig, HdfsReader, FileMetadata};
use crate::metadata_store::MetadataStore;
use crate::spark_events::SparkEvent;
use crate::storage::duckdb_store::{DuckDbStore, SparkEvent as DbSparkEvent};

/// Event processor that coordinates HDFS reading and DuckDB storage
pub struct EventProcessor {
    hdfs_reader: Arc<HdfsReader>,
    duckdb_store: Arc<DuckDbStore>,
    metadata_store: Arc<MetadataStore>,
    batch_size: usize,
    flush_interval_secs: u64,
    scan_interval_secs: u64,
}

impl EventProcessor {
    /// Create a new event processor
    pub async fn new(
        hdfs_config: HdfsConfig,
        duckdb_path: &Path,
        metadata_path: &Path,
        batch_size: usize,
        flush_interval_secs: u64,
    ) -> Result<Self> {
        Self::new_with_scan_interval(hdfs_config, duckdb_path, metadata_path, batch_size, flush_interval_secs, 30).await
    }

    /// Create a new event processor with custom scan interval (useful for testing)
    pub async fn new_with_scan_interval(
        hdfs_config: HdfsConfig,
        duckdb_path: &Path,
        metadata_path: &Path,
        batch_size: usize,
        flush_interval_secs: u64,
        scan_interval_secs: u64,
    ) -> Result<Self> {
        let hdfs_reader =
            Arc::new(HdfsReader::new(&hdfs_config.namenode_uri, &hdfs_config.base_path).await?);

        let duckdb_store = Arc::new(DuckDbStore::new(duckdb_path).await?);
        let metadata_store = Arc::new(MetadataStore::new(metadata_path).await?);

        Ok(Self {
            hdfs_reader,
            duckdb_store,
            metadata_store,
            batch_size,
            flush_interval_secs,
            scan_interval_secs,
        })
    }

    /// Start the event processing pipeline
    pub async fn start(&mut self) -> Result<()> {
        info!(
            "Starting event processor with batch_size={}, flush_interval={}s",
            self.batch_size, self.flush_interval_secs
        );

        // Create channels for batching
        let (event_tx, event_rx) = mpsc::unbounded_channel::<SparkEvent>();

        // Start batch writer task
        let store_clone = Arc::clone(&self.duckdb_store);
        let batch_size = self.batch_size;
        let flush_interval = self.flush_interval_secs;

        tokio::spawn(async move {
            Self::batch_writer_task(store_clone, event_rx, batch_size, flush_interval).await;
        });

        // Start HDFS scanning task
        let hdfs_clone = Arc::clone(&self.hdfs_reader);

        tokio::spawn(async move {
            Self::hdfs_scanner_task(hdfs_clone).await;
        });

        // Initial incremental scan (will process new files)
        info!("Performing initial incremental scan of HDFS");
        self.incremental_scan(&event_tx).await?;

        // Start periodic incremental scans
        self.start_incremental_scanner(event_tx).await;

        Ok(())
    }

    /// Perform an incremental scan checking only changed files
    async fn incremental_scan(&self, event_tx: &mpsc::UnboundedSender<SparkEvent>) -> Result<()> {
        let start_time = std::time::Instant::now();
        let applications = self.hdfs_reader.list_applications().await?;
        let app_count = applications.len();

        info!("Incremental scan starting for {} applications", app_count);
        let mut total_events = 0;
        let mut files_processed = 0;

        for app_id in applications {
            match self.hdfs_reader.list_event_files(&app_id).await {
                Ok(event_files) => {
                    for file_path in event_files {
                        // Get current file info
                        if let Ok(file_info) = self.hdfs_reader.get_file_info(&file_path).await {
                            // Check if file should be reloaded based on size
                            let should_reload = self.metadata_store.should_reload_file(&file_path, file_info.size).await;
                            debug!("File: {} - Size: {}, Should reload: {}", file_path, file_info.size, should_reload);
                            if should_reload {
                                info!("Processing changed/new file: {}", file_path);
                                
                                match self.hdfs_reader.read_events(&file_path, &app_id).await {
                                    Ok(events) => {
                                        total_events += events.len();
                                        files_processed += 1;

                                        // Send events to batch writer
                                        info!("INCREMENTAL: Sending {} events from {} to batch writer", events.len(), file_path);
                                        for (i, event) in events.iter().enumerate() {
                                            if let Err(e) = event_tx.send(event.clone()) {
                                                error!("Failed to send event {} to batch writer: {}", i, e);
                                            }
                                        }
                                        info!("INCREMENTAL: Finished sending events from {}", file_path);

                                        // Update metadata for this file
                                        let metadata = FileMetadata {
                                            path: file_path.clone(),
                                            last_processed: chrono::Utc::now().timestamp_millis(),
                                            file_size: file_info.size,
                                            last_index: None, // TODO: implement for rolling logs
                                            is_complete: !file_path.ends_with(".inprogress"),
                                        };

                                        if let Err(e) = self.metadata_store.update_metadata(metadata).await {
                                            error!("Failed to update metadata for {}: {}", file_path, e);
                                        }
                                    }
                                    Err(e) => {
                                        warn!("Failed to read events from {}: {}", file_path, e);
                                    }
                                }
                            } else {
                                debug!("Skipping unchanged file: {}", file_path);
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to list event files for {}: {}", app_id, e);
                }
            }
        }

        let duration = start_time.elapsed();
        info!(
            "Incremental scan completed: {} events from {} files ({} applications) in {:?}",
            total_events, files_processed, app_count, duration
        );

        Ok(())
    }

    /// Start incremental scanning for new/updated files
    async fn start_incremental_scanner(&self, event_tx: mpsc::UnboundedSender<SparkEvent>) {
        let metadata_store = Arc::clone(&self.metadata_store);
        let hdfs_reader = Arc::clone(&self.hdfs_reader);
        let scan_interval_secs = self.scan_interval_secs;

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(scan_interval_secs));

            loop {
                interval.tick().await;

                debug!("Starting periodic incremental scan");

                if let Err(e) = Self::perform_incremental_scan(&hdfs_reader, &metadata_store, &event_tx).await {
                    error!("Periodic incremental scan failed: {}", e);
                }
            }
        });
    }

    /// Perform incremental scan with size-based change detection
    async fn perform_incremental_scan(
        hdfs_reader: &Arc<HdfsReader>,
        metadata_store: &Arc<MetadataStore>,
        event_tx: &mpsc::UnboundedSender<SparkEvent>,
    ) -> Result<()> {
        let start_time = std::time::Instant::now();
        let applications = hdfs_reader.list_applications().await?;
        let mut total_events = 0;
        let mut files_checked = 0;
        let mut files_processed = 0;

        for app_id in applications {
            match hdfs_reader.list_event_files(&app_id).await {
                Ok(event_files) => {
                    for file_path in event_files {
                        files_checked += 1;
                        
                        // Get current file info
                        if let Ok(file_info) = hdfs_reader.get_file_info(&file_path).await {
                            // Check if file should be reloaded based on size
                            if metadata_store.should_reload_file(&file_path, file_info.size).await {
                                debug!("Processing changed file: {}", file_path);
                                
                                match hdfs_reader.read_events(&file_path, &app_id).await {
                                    Ok(events) => {
                                        total_events += events.len();
                                        files_processed += 1;

                                        // Send events to batch writer
                                        for event in events {
                                            if let Err(e) = event_tx.send(event) {
                                                error!("Failed to send event to batch writer: {}", e);
                                            }
                                        }

                                        // Update metadata for this file
                                        let metadata = FileMetadata {
                                            path: file_path.clone(),
                                            last_processed: chrono::Utc::now().timestamp_millis(),
                                            file_size: file_info.size,
                                            last_index: None,
                                            is_complete: !file_path.ends_with(".inprogress"),
                                        };

                                        if let Err(e) = metadata_store.update_metadata(metadata).await {
                                            error!("Failed to update metadata for {}: {}", file_path, e);
                                        }
                                    }
                                    Err(e) => {
                                        warn!("Failed to read events from {}: {}", file_path, e);
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to list event files for {}: {}", app_id, e);
                }
            }
        }

        let duration = start_time.elapsed();
        if files_processed > 0 {
            info!(
                "Incremental scan: {} events from {}/{} files in {:?}",
                total_events, files_processed, files_checked, duration
            );
        } else {
            debug!("Incremental scan: no changes detected in {} files", files_checked);
        }

        Ok(())
    }

    /// HDFS scanner background task
    async fn hdfs_scanner_task(hdfs_reader: Arc<HdfsReader>) {
        info!("HDFS scanner task started");

        // Health check loop
        let mut health_check_interval = interval(Duration::from_secs(300)); // 5 minutes

        loop {
            health_check_interval.tick().await;

            if let Err(e) = hdfs_reader.health_check().await {
                error!("HDFS health check failed: {}", e);
                // Could implement reconnection logic here
                sleep(Duration::from_secs(30)).await;
            }
        }
    }

    /// Batch writer task that collects events and writes to DuckDB
    async fn batch_writer_task(
        duckdb_store: Arc<DuckDbStore>,
        mut event_rx: mpsc::UnboundedReceiver<SparkEvent>,
        batch_size: usize,
        flush_interval_secs: u64,
    ) {
        info!("Batch writer task started");

        let mut batch: Vec<DbSparkEvent> = Vec::with_capacity(batch_size);
        let mut flush_timer = interval(Duration::from_secs(flush_interval_secs));
        // Use timestamp-based IDs to ensure uniqueness across restarts
        let mut event_id_counter: i64 = chrono::Utc::now().timestamp_millis();

        loop {
            tokio::select! {
                // Receive new events
                event_opt = event_rx.recv() => {
                    match event_opt {
                        Some(event) => {
                            event_id_counter += 1;
                            info!("BATCH_WRITER: Received event {} from app {}", event_id_counter, event.app_id);

                            // Convert SparkEvent to DbSparkEvent
                            let db_event = DbSparkEvent {
                                id: event_id_counter,
                                app_id: event.app_id.clone(),
                                event_type: event.event_type_str(),
                                timestamp: event.timestamp.to_rfc3339(),
                                raw_data: event.raw_data.clone(),
                                job_id: event.job_id,
                                stage_id: event.stage_id,
                                task_id: event.task_id,
                                duration_ms: event.duration_ms,
                            };

                            batch.push(db_event);
                            debug!("BATCH_WRITER: Added event to batch, batch size now: {}", batch.len());

                            // Flush if batch is full
                            if batch.len() >= batch_size {
                                info!("BATCH_WRITER: Flushing full batch of {} events", batch.len());
                                Self::flush_batch(&duckdb_store, &mut batch).await;
                            }
                        }
                        None => {
                            warn!("Event channel closed, flushing remaining events");
                            if !batch.is_empty() {
                                Self::flush_batch(&duckdb_store, &mut batch).await;
                            }
                            break;
                        }
                    }
                }

                // Periodic flush timer
                _ = flush_timer.tick() => {
                    if !batch.is_empty() {
                        info!("BATCH_WRITER: Timer flush: {} events", batch.len());
                        Self::flush_batch(&duckdb_store, &mut batch).await;
                    } else {
                        debug!("BATCH_WRITER: Timer tick, but batch is empty");
                    }
                }
            }
        }
    }

    /// Flush a batch of events to DuckDB
    async fn flush_batch(duckdb_store: &Arc<DuckDbStore>, batch: &mut Vec<DbSparkEvent>) {
        if batch.is_empty() {
            return;
        }

        let batch_size = batch.len();
        let start_time = std::time::Instant::now();

        match duckdb_store.insert_events_batch(batch.clone()).await {
            Ok(()) => {
                let duration = start_time.elapsed();
                info!("FLUSH_BATCH: Successfully flushed {} events to DuckDB in {:?}", batch_size, duration);
            }
            Err(e) => {
                error!("FLUSH_BATCH: Failed to flush {} events to DuckDB: {}", batch_size, e);
                // In production, might want to implement retry logic or dead letter queue
            }
        }

        batch.clear();
    }

    /// Get processing statistics
    pub async fn get_stats(&self) -> ProcessingStats {
        let metadata_stats = self.metadata_store.get_stats().await;
        
        ProcessingStats {
            total_events_processed: 0, // TODO: track this
            current_batch_size: 0,     // TODO: track this
            last_flush_time: chrono::Utc::now(),
            files_tracked: metadata_stats.total_files,
            files_complete: metadata_stats.complete_files,
            hdfs_healthy: self.hdfs_reader.health_check().await.unwrap_or(false),
        }
    }
}

/// Processing statistics
#[derive(Debug, Clone)]
pub struct ProcessingStats {
    pub total_events_processed: u64,
    pub current_batch_size: usize,
    pub last_flush_time: chrono::DateTime<chrono::Utc>,
    pub files_tracked: usize,
    pub files_complete: usize,
    pub hdfs_healthy: bool,
}

/// Configuration for event processor
#[derive(Debug, Clone)]
pub struct ProcessorConfig {
    pub batch_size: usize,
    pub flush_interval_secs: u64,
    pub scan_interval_secs: u64,
    pub max_concurrent_apps: usize,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            flush_interval_secs: 30,
            scan_interval_secs: 60,
            max_concurrent_apps: 10,
        }
    }
}
