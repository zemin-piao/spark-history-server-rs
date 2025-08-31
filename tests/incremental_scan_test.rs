use anyhow::Result;
use serde_json::json;
use std::path::Path;
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

use spark_history_server::{
    event_processor::EventProcessor,
    hdfs_reader::HdfsConfig,
    metadata_store::MetadataStore,
    storage::duckdb_store::DuckDbStore,
};

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
        let event_json = create_sample_event_json(app_id, "SparkListenerApplicationStart", timestamp);
        content.push_str(&event_json);
        content.push('\n');
    }
    
    tokio::fs::create_dir_all(path.parent().unwrap()).await?;
    tokio::fs::write(path, content).await?;
    Ok(())
}

#[tokio::test]
async fn test_new_event_log_file_detection() -> Result<()> {
    // Initialize tracing for debugging
    tracing_subscriber::fmt::init();
    
    let temp_dir = TempDir::new()?;
    let hdfs_path = temp_dir.path().join("spark-events");
    let metadata_path = temp_dir.path().join("metadata");
    let duckdb_path = temp_dir.path().join("test.duckdb");

    // Setup HDFS config
    let hdfs_config = HdfsConfig {
        namenode_uri: "hdfs://localhost:9000".to_string(),
        base_path: hdfs_path.to_string_lossy().to_string(),
        connection_timeout_ms: Some(5000),
        read_timeout_ms: Some(10000),
    };

    // Create event processor with short scan interval for testing
    let mut event_processor = EventProcessor::new_with_scan_interval(
        hdfs_config,
        &duckdb_path,
        &metadata_path,
        100,
        1, // Short flush interval
        2, // Short scan interval for testing
    ).await?;

    // Create application directory
    let app_id = "application_1640000000000_0001";
    let app_dir = hdfs_path.join(app_id);
    tokio::fs::create_dir_all(&app_dir).await?;

    // Create initial event log file
    let event_file_1 = app_dir.join("events_1.log");
    create_event_log_file(&event_file_1, app_id, 5).await?;

    // Start event processor in background
    tokio::spawn(async move {
        if let Err(e) = event_processor.start().await {
            eprintln!("Event processor error: {}", e);
        }
    });

    // Give some time for initial processing
    sleep(Duration::from_millis(500)).await;

    // Create DuckDB store to verify events were stored  
    println!("Test DuckDB path: {:?}", duckdb_path);
    let duckdb_store = DuckDbStore::new(&duckdb_path).await?;
    let initial_count = duckdb_store.count_events().await?;
    
    // Debug: List files that exist
    let hdfs_reader = spark_history_server::hdfs_reader::HdfsReader::new("", &hdfs_path.to_string_lossy()).await?;
    let applications = hdfs_reader.list_applications().await?;
    println!("Applications found: {:?}", applications);
    for app in &applications {
        let files = hdfs_reader.list_event_files(app).await?;
        println!("Files for {}: {:?}", app, files);
    }
    
    println!("Initial event count: {}", initial_count);
    assert!(initial_count >= 5, "Initial events should be processed");

    // Create a NEW event log file
    let event_file_2 = app_dir.join("events_2.log");
    println!("Creating second event file: {:?}", event_file_2);
    create_event_log_file(&event_file_2, app_id, 3).await?;
    
    println!("Event file 2 created, waiting for incremental scan...");

    // Give time for incremental scan to detect the new file AND for batch writer to flush
    sleep(Duration::from_secs(6)).await;
    
    // Give a small additional delay to ensure database transaction is fully committed
    sleep(Duration::from_millis(100)).await;

    // Debug: Check files again after creating the second file
    let applications_after = hdfs_reader.list_applications().await?;
    println!("Applications found after: {:?}", applications_after);
    for app in &applications_after {
        let files_after = hdfs_reader.list_event_files(app).await?;
        println!("Files for {} after: {:?}", app, files_after);
    }

    // Verify new events were added (create fresh connection to ensure we see latest data)
    let fresh_duckdb_store = DuckDbStore::new(&duckdb_path).await?;
    let final_count = fresh_duckdb_store.count_events().await?;
    println!("Final event count: {}", final_count);
    
    // Also check applications (which uses the same database)
    let applications = duckdb_store.get_applications(None, None, None, None).await?;
    println!("Applications count: {}", applications.len());
    for app in &applications {
        println!("App: {} (found by DuckDB query)", app.id);
    }
    
    // Let me try running the incremental scan manually to see if it detects the new file
    println!("Testing manual incremental scan...");
    
    // Create the necessary components for manual testing
    let hdfs_reader_arc = std::sync::Arc::new(hdfs_reader);
    let metadata_store_arc = std::sync::Arc::new(spark_history_server::metadata_store::MetadataStore::new(&metadata_path).await?);
    let (_, _event_rx): (tokio::sync::mpsc::UnboundedSender<spark_history_server::spark_events::SparkEvent>, _) = tokio::sync::mpsc::unbounded_channel();
    
    // Call the incremental scan method directly
    // (We can't call the private method directly, so let's test the metadata store logic)
    let file2_path = event_file_2.to_string_lossy();
    let file_info = hdfs_reader_arc.get_file_info(&file2_path).await?;
    let should_reload = metadata_store_arc.should_reload_file(&file2_path, file_info.size).await;
    println!("Should reload events_2.log? {}", should_reload);
    
    // Check metadata store statistics
    let metadata_stats = metadata_store_arc.get_stats().await;
    println!("Metadata stats: {:?}", metadata_stats);
    
    // Check if events_2.log has any metadata
    let file2_metadata = metadata_store_arc.get_metadata(&file2_path).await;
    println!("Events_2.log metadata: {:?}", file2_metadata);
    
    // Check events_1.log metadata for comparison
    let file1_path = event_file_1.to_string_lossy();
    let file1_metadata = metadata_store_arc.get_metadata(&file1_path).await;
    println!("Events_1.log metadata: {:?}", file1_metadata);
    
    // Let's try manually running the incremental scan again to see what happens
    println!("Running manual incremental scan method...");
    duckdb_store.count_events().await?;

    // Instead of testing the assert, let's do a simpler test first
    // Just run the test without the assertion to see all debug output
    println!("Manual test completed. Initial: {}, Final: {}, Should reload second file: {}", 
             initial_count, final_count, should_reload);
    
    // Now test with the assertion to see the exact expected vs actual
    assert!(final_count >= initial_count + 3, 
        "New events from second file should be added. Initial: {}, Final: {}. Should reload: {}", 
        initial_count, final_count, should_reload);

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
    let should_reload_new = metadata_store.should_reload_file(
        &event_file.to_string_lossy(), 
        initial_size
    ).await;
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
    let should_reload_same = metadata_store.should_reload_file(
        &event_file.to_string_lossy(),
        initial_size
    ).await;
    assert!(!should_reload_same, "File with same size should not be reloaded");

    // Append more events to the file (simulating new events)
    let additional_content = format!("{}\n{}\n", 
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
    let should_reload_larger = metadata_store.should_reload_file(
        &event_file.to_string_lossy(),
        new_size
    ).await;
    assert!(should_reload_larger, "File with larger size should be reloaded");

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
            raw_data: serde_json::from_str(&create_sample_event_json(app_id, "SparkListenerApplicationStart", 1640000000000 + i))?,
            job_id: None,
            stage_id: None,
            task_id: None,
            duration_ms: None,
        };
        first_batch.push(event);
    }

    duckdb_store.insert_events_batch(first_batch).await?;
    
    let count_after_first = duckdb_store.count_events().await?;
    assert_eq!(count_after_first, 3, "Should have 3 events after first batch");

    // Insert second batch (simulating incremental append)
    let mut second_batch = Vec::new();
    
    for i in 3..6 {
        let event = spark_history_server::storage::duckdb_store::SparkEvent {
            id: i + 1,
            app_id: app_id.to_string(),
            event_type: "SparkListenerJobStart".to_string(),
            timestamp: format!("2021-12-20T10:{}:00Z", i),
            raw_data: serde_json::from_str(&create_sample_event_json(app_id, "SparkListenerJobStart", 1640000000000 + i))?,
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
    let applications = duckdb_store.get_applications(None, None, None, None).await?;
    assert_eq!(applications.len(), 1, "Should have one application");
    assert_eq!(applications[0].id, app_id);

    println!("✅ DuckDB incremental appends test passed");
    Ok(())
}

#[tokio::test]
async fn test_full_incremental_scan_workflow() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let hdfs_path = temp_dir.path().join("spark-events");
    let metadata_path = temp_dir.path().join("metadata");
    let duckdb_path = temp_dir.path().join("test.duckdb");

    // Setup HDFS config
    let hdfs_config = HdfsConfig {
        namenode_uri: "hdfs://localhost:9000".to_string(),
        base_path: hdfs_path.to_string_lossy().to_string(),
        connection_timeout_ms: Some(5000),
        read_timeout_ms: Some(10000),
    };

    // Create application structure
    let app_id = "application_1640000000000_0004";
    let app_dir = hdfs_path.join(app_id);
    tokio::fs::create_dir_all(&app_dir).await?;

    // Create initial event files
    let event_file_1 = app_dir.join("events_1.log");
    let event_file_2 = app_dir.join("events_2.inprogress");
    
    create_event_log_file(&event_file_1, app_id, 4).await?; // Complete file
    create_event_log_file(&event_file_2, app_id, 2).await?; // In-progress file

    // Create event processor with short scan interval for testing
    let mut event_processor = EventProcessor::new_with_scan_interval(
        hdfs_config,
        &duckdb_path,
        &metadata_path,
        100,
        1, // Short flush interval
        2, // Short scan interval for testing
    ).await?;

    // Start processing
    tokio::spawn(async move {
        if let Err(e) = event_processor.start().await {
            eprintln!("Event processor error: {}", e);
        }
    });

    // Give time for initial scan
    sleep(Duration::from_millis(1000)).await;
    
    // Give additional time for database transaction to be committed
    sleep(Duration::from_millis(100)).await;

    // Verify initial data was processed (use fresh connection to see latest data)
    let duckdb_store = DuckDbStore::new(&duckdb_path).await?;
    let initial_count = duckdb_store.count_events().await?;
    println!("Initial events processed: {}", initial_count);
    assert!(initial_count >= 6, "Should process events from both files");

    // Append more events to the in-progress file (simulating real-time streaming)
    let additional_content = format!("{}\n{}\n",
        create_sample_event_json(app_id, "SparkListenerStageSubmitted", 1640000005000),
        create_sample_event_json(app_id, "SparkListenerStageCompleted", 1640000006000)
    );
    
    let mut existing_content = tokio::fs::read_to_string(&event_file_2).await?;
    existing_content.push_str(&additional_content);
    tokio::fs::write(&event_file_2, existing_content).await?;

    // Wait for incremental scan to detect the changes
    sleep(Duration::from_secs(3)).await;

    // Verify new events were processed incrementally (use fresh connection)
    let fresh_duckdb_store_2 = DuckDbStore::new(&duckdb_path).await?;
    let final_count = fresh_duckdb_store_2.count_events().await?;
    println!("Final events processed: {}", final_count);
    assert!(final_count >= initial_count + 2, 
        "Should incrementally process new events. Initial: {}, Final: {}", 
        initial_count, final_count);

    // Verify metadata store tracking
    let metadata_store = MetadataStore::new(&metadata_path).await?;
    let stats = metadata_store.get_stats().await;
    assert!(stats.total_files >= 2, "Should track both event files");
    assert_eq!(stats.complete_files, 1, "Should mark complete file as complete");
    assert_eq!(stats.incomplete_files, 1, "Should mark in-progress file as incomplete");

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
        assert_eq!(stats.total_files, 1, "Metadata should persist across restarts");
        assert_eq!(stats.complete_files, 1, "Complete files should persist");
        
        let metadata = metadata_store_2.get_metadata("/test/path/events.log").await;
        assert!(metadata.is_some(), "Specific file metadata should be retrievable");
        
        let file_meta = metadata.unwrap();
        assert_eq!(file_meta.file_size, 1024);
        assert!(file_meta.is_complete);
    }

    println!("✅ Metadata persistence test passed");
    Ok(())
}