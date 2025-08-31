use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, sleep};
use tracing::{debug, info, warn, error};

use crate::hdfs_reader::{HdfsReader, HdfsConfig};
use crate::storage::duckdb_store::{DuckDbStore, SparkEvent as DbSparkEvent};
use crate::spark_events::SparkEvent;

/// Event processor that coordinates HDFS reading and DuckDB storage
pub struct EventProcessor {
    hdfs_reader: Arc<HdfsReader>,
    duckdb_store: Arc<DuckDbStore>,
    batch_size: usize,
    flush_interval_secs: u64,
    last_scan_timestamps: HashMap<String, i64>,
}

impl EventProcessor {
    /// Create a new event processor
    pub async fn new(
        hdfs_config: HdfsConfig,
        duckdb_path: &Path,
        batch_size: usize,
        flush_interval_secs: u64,
    ) -> Result<Self> {
        let hdfs_reader = Arc::new(
            HdfsReader::new(&hdfs_config.namenode_uri, &hdfs_config.base_path).await?
        );

        let duckdb_store = Arc::new(DuckDbStore::new(duckdb_path).await?);

        Ok(Self {
            hdfs_reader,
            duckdb_store,
            batch_size,
            flush_interval_secs,
            last_scan_timestamps: HashMap::new(),
        })
    }

    /// Start the event processing pipeline
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting event processor with batch_size={}, flush_interval={}s", 
            self.batch_size, self.flush_interval_secs);

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
        let tx_clone = event_tx.clone();
        
        tokio::spawn(async move {
            Self::hdfs_scanner_task(hdfs_clone, tx_clone).await;
        });

        // Initial full scan
        info!("Performing initial full scan of HDFS");
        self.full_scan(&event_tx).await?;

        // Start periodic incremental scans
        self.start_incremental_scanner(event_tx).await;

        Ok(())
    }

    /// Perform a full scan of all applications
    async fn full_scan(&mut self, event_tx: &mpsc::UnboundedSender<SparkEvent>) -> Result<()> {
        let start_time = std::time::Instant::now();
        let applications = self.hdfs_reader.list_applications().await?;
        let app_count = applications.len();
        
        info!("Full scan starting for {} applications", app_count);
        let mut total_events = 0;

        for app_id in applications {
            match self.hdfs_reader.read_application_events(&app_id).await {
                Ok(events) => {
                    total_events += events.len();
                    
                    // Send events to batch writer
                    for event in events {
                        if let Err(e) = event_tx.send(event) {
                            error!("Failed to send event to batch writer: {}", e);
                        }
                    }

                    // Update last scan timestamp for this app
                    self.last_scan_timestamps.insert(app_id.clone(), chrono::Utc::now().timestamp_millis());
                }
                Err(e) => {
                    warn!("Failed to scan application {}: {}", app_id, e);
                }
            }
        }

        let duration = start_time.elapsed();
        info!("Full scan completed: {} events from {} applications in {:?}", 
            total_events, app_count, duration);

        Ok(())
    }

    /// Start incremental scanning for new/updated files
    async fn start_incremental_scanner(&self, event_tx: mpsc::UnboundedSender<SparkEvent>) {
        let hdfs_reader = Arc::clone(&self.hdfs_reader);
        let scan_interval_secs = 30; // Scan every 30 seconds

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(scan_interval_secs));
            
            loop {
                interval.tick().await;
                
                debug!("Starting incremental scan");
                
                match hdfs_reader.list_applications().await {
                    Ok(app_ids) => {
                        for app_id in app_ids {
                            // For now, do simple periodic full scan of each app
                            // TODO: Implement proper incremental scanning based on file modification times
                            if let Err(e) = Self::scan_application_incremental(
                                &hdfs_reader, 
                                &app_id, 
                                &event_tx
                            ).await {
                                warn!("Incremental scan failed for {}: {}", app_id, e);
                            }
                        }
                    }
                    Err(e) => error!("Failed to list applications during incremental scan: {}", e),
                }
            }
        });
    }

    /// Scan a single application incrementally
    async fn scan_application_incremental(
        hdfs_reader: &Arc<HdfsReader>,
        app_id: &str,
        _event_tx: &mpsc::UnboundedSender<SparkEvent>,
    ) -> Result<()> {
        // Simple implementation: get all events (in real implementation, 
        // we'd track file modification times and only read changed files)
        let events = hdfs_reader.read_application_events(app_id).await?;
        
        debug!("Incremental scan for {}: {} events", app_id, events.len());
        
        for event in events {
            if let Err(e) = _event_tx.send(event) {
                error!("Failed to send incremental event: {}", e);
            }
        }

        Ok(())
    }

    /// HDFS scanner background task
    async fn hdfs_scanner_task(
        hdfs_reader: Arc<HdfsReader>,
        event_tx: mpsc::UnboundedSender<SparkEvent>,
    ) {
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
        let mut event_id_counter: i64 = 0;

        loop {
            tokio::select! {
                // Receive new events
                event_opt = event_rx.recv() => {
                    match event_opt {
                        Some(event) => {
                            event_id_counter += 1;
                            
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
                            
                            // Flush if batch is full
                            if batch.len() >= batch_size {
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
                        debug!("Timer flush: {} events", batch.len());
                        Self::flush_batch(&duckdb_store, &mut batch).await;
                    }
                }
            }
        }
    }

    /// Flush a batch of events to DuckDB
    async fn flush_batch(
        duckdb_store: &Arc<DuckDbStore>,
        batch: &mut Vec<DbSparkEvent>,
    ) {
        if batch.is_empty() {
            return;
        }

        let batch_size = batch.len();
        let start_time = std::time::Instant::now();

        match duckdb_store.insert_events_batch(batch.clone()).await {
            Ok(()) => {
                let duration = start_time.elapsed();
                debug!("Flushed {} events to DuckDB in {:?}", batch_size, duration);
            }
            Err(e) => {
                error!("Failed to flush {} events to DuckDB: {}", batch_size, e);
                // In production, might want to implement retry logic or dead letter queue
            }
        }

        batch.clear();
    }

    /// Get processing statistics
    pub async fn get_stats(&self) -> ProcessingStats {
        // In a real implementation, these would be tracked
        ProcessingStats {
            total_events_processed: 0,
            current_batch_size: 0,
            last_flush_time: chrono::Utc::now(),
            applications_tracked: self.last_scan_timestamps.len(),
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
    pub applications_tracked: usize,
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