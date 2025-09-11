use anyhow::Result;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, Duration};
use tracing::{debug, error, info, warn};

use crate::config::HdfsConfig;
use crate::hdfs_reader::{FileMetadata, HdfsReader};
use crate::metadata_store::MetadataStore;
use crate::spark_events::SparkEvent;
use crate::storage::duckdb_store::{DuckDbStore, SparkEvent as DbSparkEvent};

/// Event processor that coordinates HDFS reading and DuckDB storage
pub struct EventProcessor {
    hdfs_reader: Arc<HdfsReader>,
    duckdb_store: Arc<DuckDbStore>,
    metadata_store: Arc<MetadataStore>,
    base_path: String,
    batch_size: usize,
    flush_interval_secs: u64,
    scan_interval_secs: u64,
}

impl EventProcessor {
    /// Create a new event processor
    pub async fn new(
        hdfs_config: HdfsConfig,
        base_path: String,
        duckdb_path: &Path,
        metadata_path: &Path,
        batch_size: usize,
        flush_interval_secs: u64,
    ) -> Result<Self> {
        Self::new_with_scan_interval(
            hdfs_config,
            base_path,
            duckdb_path,
            metadata_path,
            batch_size,
            flush_interval_secs,
            30,
        )
        .await
    }

    /// Create a new event processor with custom scan interval (useful for testing)
    pub async fn new_with_scan_interval(
        hdfs_config: HdfsConfig,
        base_path: String,
        duckdb_path: &Path,
        metadata_path: &Path,
        batch_size: usize,
        flush_interval_secs: u64,
        scan_interval_secs: u64,
    ) -> Result<Self> {
        let hdfs_reader = Arc::new(HdfsReader::new(&hdfs_config.namenode_url, &base_path).await?);

        let duckdb_store = Arc::new(DuckDbStore::new(duckdb_path).await?);
        let metadata_store = Arc::new(MetadataStore::new(metadata_path).await?);

        Ok(Self {
            hdfs_reader,
            duckdb_store,
            metadata_store,
            base_path,
            batch_size,
            flush_interval_secs,
            scan_interval_secs,
        })
    }

    /// Start the event processing pipeline with multi-writer architecture
    pub async fn start(&mut self) -> Result<()> {
        let config = ProcessorConfig::default();
        info!(
            "Starting SCALABLE event processor: batch_size={}, flush_interval={}s, writers={}, concurrent_apps={}",
            config.batch_size, config.flush_interval_secs, config.num_batch_writers, config.max_concurrent_apps
        );

        // Create multiple batch writers with load balancing
        let mut writer_channels = Vec::new();
        for writer_id in 0..config.num_batch_writers {
            let (event_tx, event_rx) = mpsc::unbounded_channel::<SparkEvent>();
            writer_channels.push(event_tx);

            // Start each batch writer task
            let store_clone = Arc::clone(&self.duckdb_store);
            let batch_size = config.batch_size;
            let flush_interval = config.flush_interval_secs;

            tokio::spawn(async move {
                info!("MULTI_WRITER: Starting batch writer {} with batch_size={}", writer_id, batch_size);
                Self::batch_writer_task(store_clone, event_rx, batch_size, flush_interval, writer_id).await;
            });
        }

        // Create load balancer for distributing events across writers
        let (main_tx, mut main_rx) = mpsc::unbounded_channel::<SparkEvent>();
        let writer_channels = Arc::new(writer_channels);
        
        tokio::spawn(async move {
            let mut round_robin_counter = 0usize;
            while let Some(event) = main_rx.recv().await {
                let writer_id = round_robin_counter % config.num_batch_writers;
                if let Some(writer_tx) = writer_channels.get(writer_id) {
                    if let Err(e) = writer_tx.send(event) {
                        error!("LOAD_BALANCER: Failed to send event to writer {}: {}", writer_id, e);
                    }
                }
                round_robin_counter = round_robin_counter.wrapping_add(1);
            }
            warn!("LOAD_BALANCER: Main event channel closed");
        });

        // Start HDFS scanning task
        let hdfs_clone = Arc::clone(&self.hdfs_reader);
        tokio::spawn(async move {
            Self::hdfs_scanner_task(hdfs_clone).await;
        });

        // Initial incremental scan (will process new files)
        info!("Performing initial incremental scan of HDFS");
        self.incremental_scan(&main_tx).await?;

        // Start periodic incremental scans
        self.start_incremental_scanner(main_tx).await;

        Ok(())
    }

    /// Perform an incremental scan with parallel application processing
    async fn incremental_scan(&self, event_tx: &mpsc::UnboundedSender<SparkEvent>) -> Result<()> {
        let start_time = std::time::Instant::now();
        let applications = self.hdfs_reader.list_applications(&self.base_path).await?;
        let app_count = applications.len();

        info!("PARALLEL incremental scan starting for {} applications", app_count);

        // Create semaphore to limit concurrent application processing
        let config = ProcessorConfig::default();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(config.max_concurrent_apps));
        
        // Process applications in parallel with controlled concurrency
        let mut handles = Vec::new();
        
        for app_id in applications {
            let permit = semaphore.clone().acquire_owned().await?;
            let hdfs_reader = Arc::clone(&self.hdfs_reader);
            let metadata_store = Arc::clone(&self.metadata_store);
            let event_tx = event_tx.clone();
            let base_path = self.base_path.clone();
            
            let handle = tokio::spawn(async move {
                let _permit = permit; // Hold permit for the duration
                Self::process_application_parallel(hdfs_reader, metadata_store, event_tx, base_path, app_id).await
            });
            
            handles.push(handle);
        }

        // Wait for all applications to complete and collect results
        let mut total_events = 0;
        let mut files_processed = 0;
        
        for handle in handles {
            match handle.await {
                Ok(Ok((app_events, app_files))) => {
                    total_events += app_events;
                    files_processed += app_files;
                }
                Ok(Err(e)) => {
                    warn!("Application processing failed: {}", e);
                }
                Err(e) => {
                    error!("Task join error: {}", e);
                }
            }
        }

        let duration = start_time.elapsed();
        let throughput = total_events as f64 / duration.as_secs_f64();
        info!(
            "PARALLEL incremental scan completed: {} events from {} files ({} applications) in {:?} ({:.0} events/sec)",
            total_events, files_processed, app_count, duration, throughput
        );

        Ok(())
    }

    /// Process a single application's event files in parallel
    async fn process_application_parallel(
        hdfs_reader: Arc<HdfsReader>,
        metadata_store: Arc<MetadataStore>,
        event_tx: mpsc::UnboundedSender<SparkEvent>,
        base_path: String,
        app_id: String,
    ) -> Result<(usize, usize)> {
        let mut app_total_events = 0;
        let mut app_files_processed = 0;

        match hdfs_reader.list_event_files(&base_path, &app_id).await {
            Ok(event_files) => {
                // Process files for this application sequentially (to maintain event ordering)
                for file_path in event_files {
                    if let Ok(file_info) = hdfs_reader.get_file_info(&file_path).await {
                        let should_reload = metadata_store
                            .should_reload_file(&file_path, file_info.size)
                            .await;
                        
                        if should_reload {
                            debug!("PARALLEL: Processing changed/new file: {}", file_path);

                            match hdfs_reader.read_events(&file_path, &app_id).await {
                                Ok(events) => {
                                    app_total_events += events.len();
                                    app_files_processed += 1;

                                    // Send events to load balancer
                                    for event in events {
                                        if let Err(e) = event_tx.send(event) {
                                            error!("Failed to send event to load balancer: {}", e);
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

        Ok((app_total_events, app_files_processed))
    }

    /// Start incremental scanning for new/updated files
    async fn start_incremental_scanner(&self, event_tx: mpsc::UnboundedSender<SparkEvent>) {
        let metadata_store = Arc::clone(&self.metadata_store);
        let hdfs_reader = Arc::clone(&self.hdfs_reader);
        let scan_interval_secs = self.scan_interval_secs;
        let base_path = self.base_path.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(scan_interval_secs));

            loop {
                interval.tick().await;

                debug!("Starting periodic incremental scan");

                if let Err(e) = Self::perform_incremental_scan(
                    &hdfs_reader,
                    &metadata_store,
                    &event_tx,
                    &base_path,
                )
                .await
                {
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
        log_directory: &str,
    ) -> Result<()> {
        let start_time = std::time::Instant::now();
        let applications = hdfs_reader.list_applications(log_directory).await?;
        let mut total_events = 0;
        let mut files_checked = 0;
        let mut files_processed = 0;

        for app_id in applications {
            match hdfs_reader.list_event_files(log_directory, &app_id).await {
                Ok(event_files) => {
                    for file_path in event_files {
                        files_checked += 1;

                        // Get current file info
                        if let Ok(file_info) = hdfs_reader.get_file_info(&file_path).await {
                            // Check if file should be reloaded based on size
                            if metadata_store
                                .should_reload_file(&file_path, file_info.size)
                                .await
                            {
                                debug!("Processing changed file: {}", file_path);

                                match hdfs_reader.read_events(&file_path, &app_id).await {
                                    Ok(events) => {
                                        total_events += events.len();
                                        files_processed += 1;

                                        // Send events to batch writer
                                        for event in events {
                                            if let Err(e) = event_tx.send(event) {
                                                error!(
                                                    "Failed to send event to batch writer: {}",
                                                    e
                                                );
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

                                        if let Err(e) =
                                            metadata_store.update_metadata(metadata).await
                                        {
                                            error!(
                                                "Failed to update metadata for {}: {}",
                                                file_path, e
                                            );
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
            debug!(
                "Incremental scan: no changes detected in {} files",
                files_checked
            );
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
        writer_id: usize,
    ) {
        info!("Batch writer {} started with batch_size={}, flush_interval={}s", writer_id, batch_size, flush_interval_secs);

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
                            debug!("BATCH_WRITER_{}: Received event {} from app {}", writer_id, event_id_counter, event.app_id);

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
                            debug!("BATCH_WRITER_{}: Added event to batch, batch size now: {}", writer_id, batch.len());

                            // Flush if batch is full
                            if batch.len() >= batch_size {
                                info!("BATCH_WRITER_{}: Flushing full batch of {} events", writer_id, batch.len());
                                Self::flush_batch(&duckdb_store, &mut batch, writer_id).await;
                            }
                        }
                        None => {
                            warn!("BATCH_WRITER_{}: Event channel closed, flushing remaining events", writer_id);
                            if !batch.is_empty() {
                                Self::flush_batch(&duckdb_store, &mut batch, writer_id).await;
                            }
                            break;
                        }
                    }
                }

                // Periodic flush timer
                _ = flush_timer.tick() => {
                    if !batch.is_empty() {
                        info!("BATCH_WRITER_{}: Timer flush: {} events", writer_id, batch.len());
                        Self::flush_batch(&duckdb_store, &mut batch, writer_id).await;
                    } else {
                        debug!("BATCH_WRITER_{}: Timer tick, but batch is empty", writer_id);
                    }
                }
            }
        }
    }

    /// Flush a batch of events to DuckDB
    async fn flush_batch(duckdb_store: &Arc<DuckDbStore>, batch: &mut Vec<DbSparkEvent>, writer_id: usize) {
        if batch.is_empty() {
            return;
        }

        let batch_size = batch.len();
        let start_time = std::time::Instant::now();

        match duckdb_store.insert_events_batch(batch.clone()).await {
            Ok(()) => {
                let duration = start_time.elapsed();
                let throughput = batch_size as f64 / duration.as_secs_f64();
                info!(
                    "FLUSH_BATCH_{}: Successfully flushed {} events to DuckDB in {:?} ({:.0} events/sec)",
                    writer_id, batch_size, duration, throughput
                );
            }
            Err(e) => {
                error!(
                    "FLUSH_BATCH_{}: Failed to flush {} events to DuckDB: {}",
                    writer_id, batch_size, e
                );
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
    pub num_batch_writers: usize,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            batch_size: 5000,      // Increased for higher throughput
            flush_interval_secs: 15, // Reduced for lower latency
            scan_interval_secs: 30, // More frequent scanning
            max_concurrent_apps: 50, // Increased parallelism
            num_batch_writers: 8,   // Multiple writers
        }
    }
}
