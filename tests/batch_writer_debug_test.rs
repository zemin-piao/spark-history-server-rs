use anyhow::Result;
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::mpsc;

use spark_history_server::{
    hdfs_reader::HdfsReader, spark_events::SparkEvent, storage::duckdb_store::DuckDbStore,
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

#[tokio::test]
async fn test_direct_event_insertion() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let duckdb_path = temp_dir.path().join("test.duckdb");

    // Create DuckDB store
    let duckdb_store = DuckDbStore::new(&duckdb_path).await?;

    // Initial count should be zero
    let initial_count = duckdb_store.count_events().await?;
    println!("Initial count: {}", initial_count);
    assert_eq!(initial_count, 0);

    // Create some test events directly
    let app_id = "application_test_direct";
    let mut events = Vec::new();

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
        events.push(event);
    }

    // Insert events directly
    println!("Inserting {} events directly...", events.len());
    duckdb_store.insert_events_batch(events).await?;

    // Check count immediately
    let after_insert_count = duckdb_store.count_events().await?;
    println!("After direct insert count: {}", after_insert_count);
    assert_eq!(after_insert_count, 3, "Direct insertion should work");

    println!("✅ Direct event insertion test passed");
    Ok(())
}

#[tokio::test]
async fn test_hdfs_to_duckdb_pipeline() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let hdfs_path = temp_dir.path().join("spark-events");
    let duckdb_path = temp_dir.path().join("test.duckdb");

    // Create DuckDB store
    let duckdb_store = DuckDbStore::new(&duckdb_path).await?;

    // Create application and event file
    let app_id = "application_test_pipeline";
    let app_dir = hdfs_path.join(app_id);
    tokio::fs::create_dir_all(&app_dir).await?;

    let event_file = app_dir.join("events.log");
    let mut content = String::new();
    for i in 0..3 {
        let timestamp = 1640000000000 + (i as i64 * 1000);
        let event_json =
            create_sample_event_json(app_id, "SparkListenerApplicationStart", timestamp);
        content.push_str(&event_json);
        content.push('\n');
    }
    tokio::fs::write(&event_file, content).await?;

    // Create HDFS reader and read events
    let hdfs_reader = HdfsReader::new("", &hdfs_path.to_string_lossy()).await?;
    let events = hdfs_reader
        .read_events(&event_file.to_string_lossy(), app_id)
        .await?;

    println!("HDFS reader found {} events", events.len());
    assert_eq!(events.len(), 3, "Should read 3 events from file");

    // Convert to DuckDB events and insert
    let mut db_events = Vec::new();
    for (i, event) in events.iter().enumerate() {
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

    let final_count = duckdb_store.count_events().await?;
    println!("Final count after pipeline: {}", final_count);
    assert_eq!(final_count, 3, "Pipeline should insert all events");

    println!("✅ HDFS to DuckDB pipeline test passed");
    Ok(())
}

#[tokio::test]
async fn test_batch_writer_simulation() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let hdfs_path = temp_dir.path().join("spark-events");
    let duckdb_path = temp_dir.path().join("test.duckdb");

    // Create DuckDB store
    let duckdb_store = std::sync::Arc::new(DuckDbStore::new(&duckdb_path).await?);

    // Create test files
    let app_id = "application_test_batch";
    let app_dir = hdfs_path.join(app_id);
    tokio::fs::create_dir_all(&app_dir).await?;

    let event_file_1 = app_dir.join("events_1.log");
    let event_file_2 = app_dir.join("events_2.log");

    // Create first file
    let mut content1 = String::new();
    for i in 0..2 {
        let event_json =
            create_sample_event_json(app_id, "SparkListenerApplicationStart", 1640000000000 + i);
        content1.push_str(&event_json);
        content1.push('\n');
    }
    tokio::fs::write(&event_file_1, content1).await?;

    // Create second file
    let mut content2 = String::new();
    for i in 2..5 {
        let event_json =
            create_sample_event_json(app_id, "SparkListenerJobStart", 1640000000000 + i);
        content2.push_str(&event_json);
        content2.push('\n');
    }
    tokio::fs::write(&event_file_2, content2).await?;

    // Simulate the batch writer channel
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<SparkEvent>();

    // Start a simulated batch writer
    let duckdb_clone = duckdb_store.clone();
    let batch_writer_handle = tokio::spawn(async move {
        let mut batch = Vec::new();
        let mut event_id_counter = 1i64;

        while let Some(event) = event_rx.recv().await {
            // Convert to DuckDB event
            let db_event = spark_history_server::storage::duckdb_store::SparkEvent {
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
            event_id_counter += 1;

            // Flush every 3 events or after timeout
            if batch.len() >= 3 {
                println!("Flushing batch of {} events", batch.len());
                if let Err(e) = duckdb_clone.insert_events_batch(batch.clone()).await {
                    eprintln!("Batch insert error: {}", e);
                } else {
                    println!("Successfully flushed {} events", batch.len());
                }
                batch.clear();
            }
        }

        // Flush remaining events
        if !batch.is_empty() {
            println!("Final flush of {} events", batch.len());
            if let Err(e) = duckdb_clone.insert_events_batch(batch).await {
                eprintln!("Final batch insert error: {}", e);
            }
        }
        println!("Batch writer finished");
    });

    // Read events from both files and send to batch writer
    let hdfs_reader = HdfsReader::new("", &hdfs_path.to_string_lossy()).await?;

    // Process first file
    let events1 = hdfs_reader
        .read_events(&event_file_1.to_string_lossy(), app_id)
        .await?;
    println!("Sending {} events from file 1", events1.len());
    for event in events1 {
        event_tx
            .send(event)
            .map_err(|e| anyhow::anyhow!("Send error: {}", e))?;
    }

    // Process second file
    let events2 = hdfs_reader
        .read_events(&event_file_2.to_string_lossy(), app_id)
        .await?;
    println!("Sending {} events from file 2", events2.len());
    for event in events2 {
        event_tx
            .send(event)
            .map_err(|e| anyhow::anyhow!("Send error: {}", e))?;
    }

    // Close channel
    drop(event_tx);

    // Wait for batch writer to finish
    batch_writer_handle
        .await
        .map_err(|e| anyhow::anyhow!("Join error: {}", e))?;

    // Check final count
    let final_count = duckdb_store.count_events().await?;
    println!("Final count after batch simulation: {}", final_count);
    assert_eq!(final_count, 5, "Should have processed all 5 events");

    println!("✅ Batch writer simulation test passed");
    Ok(())
}

#[tokio::test]
async fn test_id_conflict_issue() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let duckdb_path = temp_dir.path().join("test.duckdb");

    // Create DuckDB store
    let duckdb_store = DuckDbStore::new(&duckdb_path).await?;

    // Insert events with IDs 1, 2, 3
    let mut first_batch = Vec::new();
    for i in 1..=3 {
        let event = spark_history_server::storage::duckdb_store::SparkEvent {
            id: i,
            app_id: "app1".to_string(),
            event_type: "Type1".to_string(),
            timestamp: format!("2021-12-20T10:{}:00Z", i),
            raw_data: json!({"test": i}),
            job_id: None,
            stage_id: None,
            task_id: None,
            duration_ms: None,
        };
        first_batch.push(event);
    }

    println!("Inserting first batch with IDs 1, 2, 3");
    duckdb_store.insert_events_batch(first_batch).await?;

    let count_after_first = duckdb_store.count_events().await?;
    println!("Count after first batch: {}", count_after_first);
    assert_eq!(count_after_first, 3);

    // Try to insert events with duplicate IDs (should fail or be ignored)
    let mut duplicate_batch = Vec::new();
    for i in 1..=2 {
        // IDs 1, 2 already exist
        let event = spark_history_server::storage::duckdb_store::SparkEvent {
            id: i,
            app_id: "app2".to_string(),
            event_type: "Type2".to_string(),
            timestamp: format!("2021-12-20T11:{}:00Z", i),
            raw_data: json!({"test": format!("duplicate_{}", i)}),
            job_id: None,
            stage_id: None,
            task_id: None,
            duration_ms: None,
        };
        duplicate_batch.push(event);
    }

    println!("Trying to insert duplicate IDs 1, 2");
    let duplicate_result = duckdb_store.insert_events_batch(duplicate_batch).await;
    match duplicate_result {
        Ok(()) => {
            println!("Duplicate insertion succeeded (unexpected)");
        }
        Err(e) => {
            println!("Duplicate insertion failed as expected: {}", e);
        }
    }

    let count_after_duplicate = duckdb_store.count_events().await?;
    println!("Count after duplicate attempt: {}", count_after_duplicate);

    // Insert events with new IDs 4, 5
    let mut new_batch = Vec::new();
    for i in 4..=5 {
        let event = spark_history_server::storage::duckdb_store::SparkEvent {
            id: i,
            app_id: "app3".to_string(),
            event_type: "Type3".to_string(),
            timestamp: format!("2021-12-20T12:{}:00Z", i),
            raw_data: json!({"test": i}),
            job_id: None,
            stage_id: None,
            task_id: None,
            duration_ms: None,
        };
        new_batch.push(event);
    }

    println!("Inserting new batch with IDs 4, 5");
    duckdb_store.insert_events_batch(new_batch).await?;

    let final_count = duckdb_store.count_events().await?;
    println!("Final count: {}", final_count);

    println!("✅ ID conflict test completed");
    Ok(())
}

#[tokio::test]
async fn test_max_id_method() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let duckdb_path = temp_dir.path().join("test.duckdb");

    // Create DuckDB store
    let duckdb_store = DuckDbStore::new(&duckdb_path).await?;

    // Test max ID when database is empty
    let max_id_empty = duckdb_store.get_max_event_id().await?;
    println!("Max ID when empty: {:?}", max_id_empty);

    // Insert some events
    let mut events = Vec::new();
    for i in 10..13 {
        // Start with ID 10 to test non-zero start
        let event = spark_history_server::storage::duckdb_store::SparkEvent {
            id: i,
            app_id: "app_test".to_string(),
            event_type: "Test".to_string(),
            timestamp: format!("2021-12-20T10:{}:00Z", i),
            raw_data: json!({"test": i}),
            job_id: None,
            stage_id: None,
            task_id: None,
            duration_ms: None,
        };
        events.push(event);
    }

    duckdb_store.insert_events_batch(events).await?;

    // Test max ID after insert
    let max_id_after = duckdb_store.get_max_event_id().await?;
    println!("Max ID after insert: {:?}", max_id_after);

    println!("✅ Max ID method test completed");
    Ok(())
}
