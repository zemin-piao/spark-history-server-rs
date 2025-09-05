use anyhow::{anyhow, Result};
use serde_json::json;
use std::path::Path;
use tempfile::TempDir;
use tokio::fs;

use spark_history_server::{
    hdfs_reader::{FileMetadata, HdfsFileInfo},
    metadata_store::MetadataStore,
    spark_events::SparkEvent,
    storage::duckdb_store::DuckDbStore,
};

/// Mock HDFS reader that works with local filesystem for testing
///
/// This allows us to test HDFS-dependent code without requiring a real HDFS cluster.
/// The mock implements the same interface as HdfsReader but uses local filesystem operations,
/// enabling comprehensive testing of incremental scanning, event processing, and metadata tracking
/// in any CI/CD environment.
struct MockHdfsReader {
    base_path: String,
}

impl MockHdfsReader {
    pub fn new(_namenode_uri: &str, base_path: &str) -> Result<Self> {
        // For mocking, we ignore the namenode_uri and just use the local base_path
        Ok(Self {
            base_path: base_path.to_string(),
        })
    }

    pub async fn list_applications(&self, _base_path: &str) -> Result<Vec<String>> {
        let mut applications = Vec::new();
        let mut entries = fs::read_dir(&self.base_path).await?;

        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("app") || name.starts_with("application") {
                        applications.push(name.to_string());
                    }
                }
            }
        }

        Ok(applications)
    }

    pub async fn list_event_files(&self, _base_path: &str, app_id: &str) -> Result<Vec<String>> {
        let app_path = Path::new(&self.base_path).join(app_id);
        let mut event_files = Vec::new();

        if !app_path.exists() {
            return Ok(event_files);
        }

        let mut entries = fs::read_dir(&app_path).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_file() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("events")
                        || name.contains("eventLog")
                        || name.ends_with(".inprogress")
                    {
                        event_files.push(entry.path().to_string_lossy().to_string());
                    }
                }
            }
        }

        Ok(event_files)
    }

    pub async fn read_events(&self, file_path: &str, app_id: &str) -> Result<Vec<SparkEvent>> {
        let content = fs::read_to_string(file_path)
            .await
            .map_err(|e| anyhow!("Failed to read file {}: {}", file_path, e))?;

        parse_events_from_content(&content, app_id)
    }

    pub async fn get_file_info(&self, file_path: &str) -> Result<HdfsFileInfo> {
        let metadata = fs::metadata(file_path)
            .await
            .map_err(|e| anyhow!("Failed to get file info for {}: {}", file_path, e))?;

        Ok(HdfsFileInfo {
            path: file_path.to_string(),
            size: metadata.len() as i64,
            modification_time: metadata
                .modified()
                .unwrap_or(std::time::UNIX_EPOCH)
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            is_directory: metadata.is_dir(),
        })
    }

    #[allow(dead_code)]
    pub async fn health_check(&self) -> Result<bool> {
        // For mock, just check if base path exists
        Ok(Path::new(&self.base_path).exists())
    }
}

/// Mock Event Processor for testing with local files
struct MockEventProcessor {
    hdfs_reader: MockHdfsReader,
    duckdb_store: std::sync::Arc<DuckDbStore>,
    metadata_store: std::sync::Arc<MetadataStore>,
    base_path: String,
    #[allow(dead_code)]
    batch_size: usize,
}

impl MockEventProcessor {
    pub async fn new(
        base_path: String,
        duckdb_path: &Path,
        metadata_path: &Path,
        batch_size: usize,
    ) -> Result<Self> {
        let hdfs_reader = MockHdfsReader::new("mock://localhost:9000", &base_path)?;
        let duckdb_store = std::sync::Arc::new(DuckDbStore::new(duckdb_path).await?);
        let metadata_store = std::sync::Arc::new(MetadataStore::new(metadata_path).await?);

        Ok(Self {
            hdfs_reader,
            duckdb_store,
            metadata_store,
            base_path,
            batch_size,
        })
    }

    /// Simulate initial scan and processing of existing files
    pub async fn initial_scan(&self) -> Result<()> {
        let applications = self.hdfs_reader.list_applications(&self.base_path).await?;

        for app_id in applications {
            let event_files = self
                .hdfs_reader
                .list_event_files(&self.base_path, &app_id)
                .await?;

            for file_path in event_files {
                let file_info = self.hdfs_reader.get_file_info(&file_path).await?;

                // Check if we should process this file
                if self
                    .metadata_store
                    .should_reload_file(&file_path, file_info.size)
                    .await
                {
                    // Process the file
                    let events = self.hdfs_reader.read_events(&file_path, &app_id).await?;

                    // Convert to DuckDB events
                    let mut db_events = Vec::new();
                    let max_id = self.duckdb_store.get_max_event_id().await?.unwrap_or(0);

                    for (i, event) in events.iter().enumerate() {
                        let db_event = spark_history_server::storage::duckdb_store::SparkEvent {
                            id: max_id + (i as i64) + 1,
                            app_id: event.app_id.clone(),
                            event_type: event.event_type_str(),
                            timestamp: event.timestamp.to_rfc3339(),
                            raw_data: event.raw_data.clone(),
                            job_id: event.job_id,
                            stage_id: event.stage_id,
                            task_id: event.task_id,
                            duration_ms: event.duration_ms,
                        };
                        db_events.push(db_event);
                    }

                    if !db_events.is_empty() {
                        self.duckdb_store.insert_events_batch(db_events).await?;
                    }

                    // Update metadata
                    let file_metadata = FileMetadata {
                        path: file_path.clone(),
                        last_processed: chrono::Utc::now().timestamp_millis(),
                        file_size: file_info.size,
                        last_index: None,
                        is_complete: !file_path.ends_with(".inprogress"),
                    };
                    self.metadata_store.update_metadata(file_metadata).await?;
                }
            }
        }

        Ok(())
    }

    /// Simulate incremental scan for new/changed files
    pub async fn incremental_scan(&self) -> Result<()> {
        let applications = self.hdfs_reader.list_applications(&self.base_path).await?;

        for app_id in applications {
            let event_files = self
                .hdfs_reader
                .list_event_files(&self.base_path, &app_id)
                .await?;

            for file_path in event_files {
                let file_info = self.hdfs_reader.get_file_info(&file_path).await?;

                // Check if we should process this file (only if it's new or changed)
                if self
                    .metadata_store
                    .should_reload_file(&file_path, file_info.size)
                    .await
                {
                    // Check if this is a size change (incremental content) or completely new file
                    let existing_metadata = self.metadata_store.get_metadata(&file_path).await;

                    let events = self.hdfs_reader.read_events(&file_path, &app_id).await?;
                    let mut events_to_process = events.clone();

                    // If file existed before and got larger, we only want the new events
                    if let Some(metadata) = existing_metadata {
                        if metadata.file_size > 0 && file_info.size > metadata.file_size {
                            // This is a size change - we need to figure out which events are new
                            // For simplicity in testing, we'll assume the new content is appended
                            // In a real implementation, you'd track the last processed position

                            // For this test, let's read the file content and compare
                            let old_event_count = (metadata.file_size as f64
                                / (file_info.size as f64 / events.len() as f64))
                                as usize;
                            if old_event_count < events.len() {
                                events_to_process =
                                    events.into_iter().skip(old_event_count).collect();
                            } else {
                                events_to_process.clear(); // No new events
                            }
                        }
                    }

                    if !events_to_process.is_empty() {
                        // Convert to DuckDB events
                        let mut db_events = Vec::new();
                        let max_id = self.duckdb_store.get_max_event_id().await?.unwrap_or(0);

                        for (i, event) in events_to_process.iter().enumerate() {
                            let db_event =
                                spark_history_server::storage::duckdb_store::SparkEvent {
                                    id: max_id + (i as i64) + 1,
                                    app_id: event.app_id.clone(),
                                    event_type: event.event_type_str(),
                                    timestamp: event.timestamp.to_rfc3339(),
                                    raw_data: event.raw_data.clone(),
                                    job_id: event.job_id,
                                    stage_id: event.stage_id,
                                    task_id: event.task_id,
                                    duration_ms: event.duration_ms,
                                };
                            db_events.push(db_event);
                        }

                        self.duckdb_store.insert_events_batch(db_events).await?;
                    }

                    // Update metadata
                    let file_metadata = FileMetadata {
                        path: file_path.clone(),
                        last_processed: chrono::Utc::now().timestamp_millis(),
                        file_size: file_info.size,
                        last_index: None,
                        is_complete: !file_path.ends_with(".inprogress"),
                    };
                    self.metadata_store.update_metadata(file_metadata).await?;
                }
            }
        }

        Ok(())
    }
}

/// Test helper to create a sample Spark event JSON
fn create_sample_event_json(app_id: &str, event_type: &str, timestamp: i64) -> String {
    json!({
        "Event": event_type,
        "Timestamp": timestamp,
        "App ID": app_id
    })
    .to_string()
}

/// Test helper to create an event log file with sample events
async fn create_event_log_file(path: &Path, app_id: &str, num_events: usize) -> Result<()> {
    let mut content = String::new();
    for i in 0..num_events {
        let timestamp = 1640000000000 + (i as i64 * 1000); // Start from 2021-12-20
        let event_json =
            create_sample_event_json(app_id, "SparkListenerApplicationStart", timestamp);
        content.push_str(&event_json);
        content.push('\n');
    }

    tokio::fs::create_dir_all(path.parent().unwrap()).await?;
    tokio::fs::write(path, content).await?;
    Ok(())
}

#[tokio::test]
async fn test_new_event_log_file_detection() -> Result<()> {
    // Now using mock HDFS for CI/CD compatibility
    let _ = tracing_subscriber::fmt::try_init();

    let temp_dir = TempDir::new()?;
    let hdfs_path = temp_dir.path().join("spark-events");
    let metadata_path = temp_dir.path().join("metadata");
    let duckdb_path = temp_dir.path().join("test.duckdb");

    // Create mock event processor (no real HDFS required)
    let mock_processor = MockEventProcessor::new(
        hdfs_path.to_string_lossy().to_string(),
        &duckdb_path,
        &metadata_path,
        100,
    )
    .await?;

    // Create application directory
    let app_id = "application_1640000000000_0001";
    let app_dir = hdfs_path.join(app_id);
    tokio::fs::create_dir_all(&app_dir).await?;

    // Create initial event log file
    let event_file_1 = app_dir.join("events_1.log");
    create_event_log_file(&event_file_1, app_id, 5).await?;

    // Run initial scan with mock processor
    mock_processor.initial_scan().await?;

    // Verify initial events were processed
    let initial_count = mock_processor.duckdb_store.count_events().await?;
    println!("Initial event count: {}", initial_count);
    assert_eq!(initial_count, 5, "Initial events should be processed");

    // Create a NEW event log file
    let event_file_2 = app_dir.join("events_2.log");
    println!("Creating second event file: {:?}", event_file_2);
    create_event_log_file(&event_file_2, app_id, 3).await?;

    // Run incremental scan to detect the new file
    mock_processor.incremental_scan().await?;

    // Verify new events were added
    let final_count = mock_processor.duckdb_store.count_events().await?;
    println!("Final event count: {}", final_count);
    assert_eq!(
        final_count, 8,
        "New events from second file should be added. Initial: {}, Final: {}",
        initial_count, final_count
    );

    // Verify applications are properly tracked
    let applications = mock_processor
        .duckdb_store
        .get_applications(None, None, None, None)
        .await?;
    println!("Applications count: {}", applications.len());
    assert!(
        !applications.is_empty(),
        "Should have at least one application"
    );

    for app in &applications {
        println!("App: {} (found by DuckDB query)", app.id);
    }

    println!("✅ New event log file detection test passed");
    Ok(())
}

#[tokio::test]
async fn test_incremental_file_size_change_detection() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let hdfs_path = temp_dir.path().join("spark-events");
    let metadata_path = temp_dir.path().join("metadata");
    let _duckdb_path = temp_dir.path().join("test.duckdb");

    // Setup metadata store
    let metadata_store = MetadataStore::new(&metadata_path).await?;

    // Create application and event file
    let app_id = "application_1640000000000_0002";
    let app_dir = hdfs_path.join(app_id);
    tokio::fs::create_dir_all(&app_dir).await?;

    let event_file = app_dir.join("events.log");

    // Create initial file with 2 events
    create_event_log_file(&event_file, app_id, 2).await?;

    // Get initial file size
    let initial_metadata = tokio::fs::metadata(&event_file).await?;
    let initial_size = initial_metadata.len() as i64;

    // File should be detected as new (should reload)
    let should_reload_new = metadata_store
        .should_reload_file(&event_file.to_string_lossy(), initial_size)
        .await;
    assert!(should_reload_new, "New file should be reloaded");

    // Simulate processing the file by updating metadata
    let file_metadata = spark_history_server::hdfs_reader::FileMetadata {
        path: event_file.to_string_lossy().to_string(),
        last_processed: chrono::Utc::now().timestamp_millis(),
        file_size: initial_size,
        last_index: None,
        is_complete: false,
    };
    metadata_store.update_metadata(file_metadata).await?;

    // File with same size should not be reloaded
    let should_reload_same = metadata_store
        .should_reload_file(&event_file.to_string_lossy(), initial_size)
        .await;
    assert!(
        !should_reload_same,
        "File with same size should not be reloaded"
    );

    // Append more events to the file (simulating new events)
    let additional_content = format!(
        "{}\n{}\n",
        create_sample_event_json(app_id, "SparkListenerJobStart", 1640000003000),
        create_sample_event_json(app_id, "SparkListenerJobEnd", 1640000004000)
    );

    let mut existing_content = tokio::fs::read_to_string(&event_file).await?;
    existing_content.push_str(&additional_content);
    tokio::fs::write(&event_file, existing_content).await?;

    // Get new file size
    let new_metadata = tokio::fs::metadata(&event_file).await?;
    let new_size = new_metadata.len() as i64;

    assert!(new_size > initial_size, "File size should have increased");

    // File with larger size should be reloaded
    let should_reload_larger = metadata_store
        .should_reload_file(&event_file.to_string_lossy(), new_size)
        .await;
    assert!(
        should_reload_larger,
        "File with larger size should be reloaded"
    );

    println!("✅ File size change detection test passed");
    Ok(())
}

#[tokio::test]
async fn test_duckdb_incremental_appends() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let duckdb_path = temp_dir.path().join("test.duckdb");

    // Create DuckDB store
    let duckdb_store = DuckDbStore::new(&duckdb_path).await?;

    // Verify initial count is zero
    let initial_count = duckdb_store.count_events().await?;
    assert_eq!(initial_count, 0, "Initial count should be zero");

    // Insert first batch of events
    let app_id = "application_1640000000000_0003";
    let mut first_batch = Vec::new();

    for i in 0..3 {
        let event = spark_history_server::storage::duckdb_store::SparkEvent {
            id: i + 1,
            app_id: app_id.to_string(),
            event_type: "SparkListenerApplicationStart".to_string(),
            timestamp: format!("2021-12-20T10:{}:00Z", i),
            raw_data: serde_json::from_str(&create_sample_event_json(
                app_id,
                "SparkListenerApplicationStart",
                1640000000000 + i,
            ))?,
            job_id: None,
            stage_id: None,
            task_id: None,
            duration_ms: None,
        };
        first_batch.push(event);
    }

    duckdb_store.insert_events_batch(first_batch).await?;

    let count_after_first = duckdb_store.count_events().await?;
    assert_eq!(
        count_after_first, 3,
        "Should have 3 events after first batch"
    );

    // Insert second batch (simulating incremental append)
    let mut second_batch = Vec::new();

    for i in 3..6 {
        let event = spark_history_server::storage::duckdb_store::SparkEvent {
            id: i + 1,
            app_id: app_id.to_string(),
            event_type: "SparkListenerJobStart".to_string(),
            timestamp: format!("2021-12-20T10:{}:00Z", i),
            raw_data: serde_json::from_str(&create_sample_event_json(
                app_id,
                "SparkListenerJobStart",
                1640000000000 + i,
            ))?,
            job_id: Some(i),
            stage_id: None,
            task_id: None,
            duration_ms: None,
        };
        second_batch.push(event);
    }

    duckdb_store.insert_events_batch(second_batch).await?;

    let final_count = duckdb_store.count_events().await?;
    assert_eq!(final_count, 6, "Should have 6 events after second batch");

    // Verify we can query events from both batches
    let applications = duckdb_store
        .get_applications(None, None, None, None)
        .await?;
    assert_eq!(applications.len(), 1, "Should have one application");
    assert_eq!(applications[0].id, app_id);

    println!("✅ DuckDB incremental appends test passed");
    Ok(())
}

#[tokio::test]
async fn test_full_incremental_scan_workflow() -> Result<()> {
    // Now using mock HDFS for CI/CD compatibility
    let temp_dir = TempDir::new()?;
    let hdfs_path = temp_dir.path().join("spark-events");
    let metadata_path = temp_dir.path().join("metadata");
    let duckdb_path = temp_dir.path().join("test.duckdb");

    // Create mock event processor (no real HDFS required)
    let mock_processor = MockEventProcessor::new(
        hdfs_path.to_string_lossy().to_string(),
        &duckdb_path,
        &metadata_path,
        100,
    )
    .await?;

    // Create application structure
    let app_id = "application_1640000000000_0004";
    let app_dir = hdfs_path.join(app_id);
    tokio::fs::create_dir_all(&app_dir).await?;

    // Create initial event files
    let event_file_1 = app_dir.join("events_1.log");
    let event_file_2 = app_dir.join("events_2.inprogress");

    create_event_log_file(&event_file_1, app_id, 4).await?; // Complete file
    create_event_log_file(&event_file_2, app_id, 2).await?; // In-progress file

    // Run initial processing
    mock_processor.initial_scan().await?;

    // Verify initial data was processed
    let initial_count = mock_processor.duckdb_store.count_events().await?;
    println!("Initial events processed: {}", initial_count);
    assert_eq!(initial_count, 6, "Should process events from both files");

    // Append more events to the in-progress file (simulating real-time streaming)
    let additional_content = format!(
        "{}\n{}\n",
        create_sample_event_json(app_id, "SparkListenerStageSubmitted", 1640000005000),
        create_sample_event_json(app_id, "SparkListenerStageCompleted", 1640000006000)
    );

    let mut existing_content = tokio::fs::read_to_string(&event_file_2).await?;
    existing_content.push_str(&additional_content);
    tokio::fs::write(&event_file_2, existing_content).await?;

    // Run incremental scan to detect the changes
    mock_processor.incremental_scan().await?;

    // Verify new events were processed incrementally
    let final_count = mock_processor.duckdb_store.count_events().await?;
    println!("Final events processed: {}", final_count);
    assert_eq!(
        final_count, 8,
        "Should incrementally process new events. Initial: {}, Final: {}",
        initial_count, final_count
    );

    // Verify metadata store tracking
    let stats = mock_processor.metadata_store.get_stats().await;
    assert!(stats.total_files >= 2, "Should track both event files");
    assert_eq!(
        stats.complete_files, 1,
        "Should mark complete file as complete"
    );
    assert_eq!(
        stats.incomplete_files, 1,
        "Should mark in-progress file as incomplete"
    );

    println!("✅ Full incremental scan workflow test passed");
    Ok(())
}

#[tokio::test]
async fn test_metadata_persistence_across_restarts() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let metadata_path = temp_dir.path().join("metadata");

    // Create first metadata store instance
    {
        let metadata_store = MetadataStore::new(&metadata_path).await?;

        // Add some metadata
        let file_metadata = spark_history_server::hdfs_reader::FileMetadata {
            path: "/test/path/events.log".to_string(),
            last_processed: chrono::Utc::now().timestamp_millis(),
            file_size: 1024,
            last_index: None,
            is_complete: true,
        };

        metadata_store.update_metadata(file_metadata).await?;

        let stats = metadata_store.get_stats().await;
        assert_eq!(stats.total_files, 1);
        assert_eq!(stats.complete_files, 1);
    } // metadata_store goes out of scope

    // Create second metadata store instance (simulating restart)
    {
        let metadata_store_2 = MetadataStore::new(&metadata_path).await?;

        // Verify metadata was persisted
        let stats = metadata_store_2.get_stats().await;
        assert_eq!(
            stats.total_files, 1,
            "Metadata should persist across restarts"
        );
        assert_eq!(stats.complete_files, 1, "Complete files should persist");

        let metadata = metadata_store_2.get_metadata("/test/path/events.log").await;
        assert!(
            metadata.is_some(),
            "Specific file metadata should be retrievable"
        );

        let file_meta = metadata.unwrap();
        assert_eq!(file_meta.file_size, 1024);
        assert!(file_meta.is_complete);
    }

    println!("✅ Metadata persistence test passed");
    Ok(())
}

#[tokio::test]
async fn test_local_file_incremental_detection() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let events_dir = temp_dir.path().join("spark-events");
    let metadata_path = temp_dir.path().join("metadata");
    let duckdb_path = temp_dir.path().join("test.duckdb");

    // Create DuckDB store and metadata store
    let duckdb_store = DuckDbStore::new(&duckdb_path).await?;
    let metadata_store = MetadataStore::new(&metadata_path).await?;

    // Create application structure
    let app_id = "application_local_test";
    let app_dir = events_dir.join(app_id);
    tokio::fs::create_dir_all(&app_dir).await?;

    // Create initial event file
    let event_file_1 = app_dir.join("events_1.log");
    create_event_log_file(&event_file_1, app_id, 3).await?;

    // Simulate processing the first file
    let file1_content = tokio::fs::read_to_string(&event_file_1).await?;
    let events1 = parse_events_from_content(&file1_content, app_id)?;

    // Convert to DuckDB events
    let mut db_events = Vec::new();
    for (i, event) in events1.iter().enumerate() {
        let db_event = spark_history_server::storage::duckdb_store::SparkEvent {
            id: (i as i64) + 1,
            app_id: event.app_id.clone(),
            event_type: event.event_type_str(),
            timestamp: event.timestamp.to_rfc3339(),
            raw_data: event.raw_data.clone(),
            job_id: event.job_id,
            stage_id: event.stage_id,
            task_id: event.task_id,
            duration_ms: event.duration_ms,
        };
        db_events.push(db_event);
    }

    duckdb_store.insert_events_batch(db_events).await?;

    // Update metadata for first file
    let file1_metadata = tokio::fs::metadata(&event_file_1).await?;
    let file_metadata = spark_history_server::hdfs_reader::FileMetadata {
        path: event_file_1.to_string_lossy().to_string(),
        last_processed: chrono::Utc::now().timestamp_millis(),
        file_size: file1_metadata.len() as i64,
        last_index: None,
        is_complete: true,
    };
    metadata_store.update_metadata(file_metadata).await?;

    let initial_count = duckdb_store.count_events().await?;
    assert_eq!(initial_count, 3, "Initial events should be stored");

    // Create a new event file
    let event_file_2 = app_dir.join("events_2.log");
    create_event_log_file(&event_file_2, app_id, 2).await?;

    // Check if the new file should be processed
    let file2_metadata = tokio::fs::metadata(&event_file_2).await?;
    let should_process = metadata_store
        .should_reload_file(&event_file_2.to_string_lossy(), file2_metadata.len() as i64)
        .await;

    assert!(should_process, "New file should be marked for processing");

    // Process the new file
    let file2_content = tokio::fs::read_to_string(&event_file_2).await?;
    let events2 = parse_events_from_content(&file2_content, app_id)?;

    let mut db_events2 = Vec::new();
    for (i, event) in events2.iter().enumerate() {
        let db_event = spark_history_server::storage::duckdb_store::SparkEvent {
            id: (i as i64) + 4, // Continue from previous ID
            app_id: event.app_id.clone(),
            event_type: event.event_type_str(),
            timestamp: event.timestamp.to_rfc3339(),
            raw_data: event.raw_data.clone(),
            job_id: event.job_id,
            stage_id: event.stage_id,
            task_id: event.task_id,
            duration_ms: event.duration_ms,
        };
        db_events2.push(db_event);
    }

    duckdb_store.insert_events_batch(db_events2).await?;

    let final_count = duckdb_store.count_events().await?;
    assert_eq!(
        final_count, 5,
        "All events should be stored after incremental processing"
    );

    println!("✅ Local file incremental detection test passed");
    Ok(())
}

/// Helper function to parse events from file content (for local testing)
fn parse_events_from_content(
    content: &str,
    app_id: &str,
) -> Result<Vec<spark_history_server::spark_events::SparkEvent>> {
    use spark_history_server::spark_events::SparkEvent;

    let mut events = Vec::new();

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }

        match serde_json::from_str::<serde_json::Value>(line) {
            Ok(json) => match SparkEvent::from_json(&json, app_id) {
                Ok(event) => events.push(event),
                Err(e) => {
                    println!("Warning: Failed to parse event: {}", e);
                    continue;
                }
            },
            Err(e) => {
                println!("Warning: Failed to parse JSON: {}", e);
                continue;
            }
        }
    }

    Ok(events)
}

#[tokio::test]
async fn test_mock_hdfs_new_file_detection() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let temp_dir = TempDir::new()?;
    let hdfs_path = temp_dir.path().join("spark-events");
    let metadata_path = temp_dir.path().join("metadata");
    let duckdb_path = temp_dir.path().join("test.duckdb");

    // Create mock event processor
    let mock_processor = MockEventProcessor::new(
        hdfs_path.to_string_lossy().to_string(),
        &duckdb_path,
        &metadata_path,
        100,
    )
    .await?;

    // Create application directory
    let app_id = "application_mock_test_001";
    let app_dir = hdfs_path.join(app_id);
    tokio::fs::create_dir_all(&app_dir).await?;

    // Create initial event log file
    let event_file_1 = app_dir.join("events_1.log");
    create_event_log_file(&event_file_1, app_id, 5).await?;

    // Run initial scan
    mock_processor.initial_scan().await?;

    // Verify initial events were processed
    let initial_count = mock_processor.duckdb_store.count_events().await?;
    println!("Initial event count: {}", initial_count);
    assert_eq!(initial_count, 5, "Initial events should be processed");

    // Create a NEW event log file
    let event_file_2 = app_dir.join("events_2.log");
    println!("Creating second event file: {:?}", event_file_2);
    create_event_log_file(&event_file_2, app_id, 3).await?;

    // Run incremental scan to detect the new file
    mock_processor.incremental_scan().await?;

    // Verify new events were added
    let final_count = mock_processor.duckdb_store.count_events().await?;
    println!("Final event count: {}", final_count);
    assert_eq!(
        final_count, 8,
        "New events from second file should be added. Initial: {}, Final: {}",
        initial_count, final_count
    );

    println!("✅ Mock HDFS new file detection test passed");
    Ok(())
}

#[tokio::test]
async fn test_mock_hdfs_incremental_workflow() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let hdfs_path = temp_dir.path().join("spark-events");
    let metadata_path = temp_dir.path().join("metadata");
    let duckdb_path = temp_dir.path().join("test.duckdb");

    // Create mock event processor
    let mock_processor = MockEventProcessor::new(
        hdfs_path.to_string_lossy().to_string(),
        &duckdb_path,
        &metadata_path,
        100,
    )
    .await?;

    // Create application structure
    let app_id = "application_mock_workflow_001";
    let app_dir = hdfs_path.join(app_id);
    tokio::fs::create_dir_all(&app_dir).await?;

    // Create initial event files
    let event_file_1 = app_dir.join("events_1.log");
    let event_file_2 = app_dir.join("events_2.inprogress");

    create_event_log_file(&event_file_1, app_id, 4).await?; // Complete file
    create_event_log_file(&event_file_2, app_id, 2).await?; // In-progress file

    // Run initial processing
    mock_processor.initial_scan().await?;

    // Verify initial data was processed
    let initial_count = mock_processor.duckdb_store.count_events().await?;
    println!("Initial events processed: {}", initial_count);
    assert_eq!(initial_count, 6, "Should process events from both files");

    // Append more events to the in-progress file (simulating real-time streaming)
    let additional_content = format!(
        "{}\n{}\n",
        create_sample_event_json(app_id, "SparkListenerStageSubmitted", 1640000005000),
        create_sample_event_json(app_id, "SparkListenerStageCompleted", 1640000006000)
    );

    let mut existing_content = tokio::fs::read_to_string(&event_file_2).await?;
    existing_content.push_str(&additional_content);
    tokio::fs::write(&event_file_2, existing_content).await?;

    // Run incremental scan to detect the changes
    mock_processor.incremental_scan().await?;

    // Verify new events were processed incrementally
    let final_count = mock_processor.duckdb_store.count_events().await?;
    println!("Final events processed: {}", final_count);
    assert_eq!(
        final_count, 8,
        "Should incrementally process new events. Initial: {}, Final: {}",
        initial_count, final_count
    );

    // Verify metadata store tracking
    let stats = mock_processor.metadata_store.get_stats().await;
    assert!(stats.total_files >= 2, "Should track both event files");
    assert_eq!(
        stats.complete_files, 1,
        "Should mark complete file as complete"
    );
    assert_eq!(
        stats.incomplete_files, 1,
        "Should mark in-progress file as incomplete"
    );

    println!("✅ Mock HDFS incremental workflow test passed");
    Ok(())
}
