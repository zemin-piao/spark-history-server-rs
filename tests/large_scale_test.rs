#![cfg(feature = "performance-tests")]

use std::collections::HashMap;
use std::fs;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

mod load_test_utils;
mod performance_monitor;

use load_test_utils::SyntheticDataGenerator;
use performance_monitor::PerformanceMonitor;
use spark_history_server::storage::duckdb_store::{DuckDbStore, SparkEvent};

#[tokio::test]
async fn test_100k_applications_load() {
    println!("🚀 Starting Large Scale Test: 100K Applications");
    println!("This will create events for 100,000 different Spark applications");
    println!();

    let monitor = PerformanceMonitor::new();
    monitor.start_monitoring(1000).await; // Sample every second for long test

    let num_applications = 100_000usize;
    let events_per_app = 20usize; // Average events per application
    let total_events = num_applications * events_per_app;

    println!("Configuration:");
    println!("  Applications: {}", num_applications);
    println!("  Events per app: {}", events_per_app);
    println!("  Total events: {}", total_events);
    println!();

    // Create temporary database file
    let temp_db_file = NamedTempFile::new().unwrap();
    let db_path = temp_db_file.path().to_path_buf();
    drop(temp_db_file); // Close the file so DuckDB can use it

    let start_time = Instant::now();

    // Initialize DuckDB store
    println!("📊 Initializing DuckDB database...");
    let setup_start = Instant::now();
    let store = DuckDbStore::new(&db_path).await.unwrap();
    let setup_duration = setup_start.elapsed();
    println!(
        "✅ Database setup completed in: {:.2}s",
        setup_duration.as_secs_f64()
    );

    // Initialize data generator
    let mut generator = SyntheticDataGenerator::new();

    let batch_size = 10_000usize;
    let num_batches = total_events.div_ceil(batch_size);

    let mut total_insert_time = Duration::ZERO;
    let mut apps_created = std::collections::HashSet::new();

    println!(
        "📝 Generating events for {} applications in {} batches...",
        num_applications, num_batches
    );

    let generation_start = Instant::now();

    for batch_num in 0..num_batches {
        let batch_start = Instant::now();

        // Generate events for this batch
        let events_in_batch = std::cmp::min(batch_size, total_events - batch_num * batch_size);
        let mut spark_events = Vec::with_capacity(events_in_batch);

        for event_idx in 0..events_in_batch {
            let global_event_id = (batch_num * batch_size + event_idx) as i64;

            // Ensure each application gets its events
            let app_index = global_event_id as usize / events_per_app;
            let app_id = format!("app-{:06}", app_index);
            apps_created.insert(app_id.clone());

            // Generate a single event for this app
            let event_batch = generator.generate_event_batch(1);
            let (_, _, raw_data) = &event_batch[0];

            let spark_event = SparkEvent::from_json(raw_data, &app_id, global_event_id).unwrap();
            spark_events.push(spark_event);
        }

        // Insert batch into database
        let insert_start = Instant::now();
        store.insert_events_batch(spark_events).await.unwrap();
        let insert_duration = insert_start.elapsed();
        total_insert_time += insert_duration;

        let batch_duration = batch_start.elapsed();

        // Progress reporting
        if batch_num % 50 == 0 || batch_num == num_batches - 1 {
            let events_processed = std::cmp::min((batch_num + 1) * batch_size, total_events);
            let progress = (events_processed as f64 / total_events as f64) * 100.0;
            let overall_elapsed = generation_start.elapsed().as_secs_f64();
            let events_per_second = events_processed as f64 / overall_elapsed;

            println!(
                "Batch {}/{}: {} events ({:.1}%), {:.0} events/sec, batch: {:.3}s, insert: {:.3}s, apps: {}",
                batch_num + 1,
                num_batches,
                events_processed,
                progress,
                events_per_second,
                batch_duration.as_secs_f64(),
                insert_duration.as_secs_f64(),
                apps_created.len()
            );
        }
    }

    let generation_duration = generation_start.elapsed();
    let total_duration = start_time.elapsed();

    println!();
    println!("📊 Verifying data integrity...");

    // Verify the data was inserted correctly
    let final_event_count = store.count_events().await.unwrap();
    let applications = store
        .get_applications(None, None, None, None)
        .await
        .unwrap();

    // Count distinct applications in database
    let mut app_counts = HashMap::new();
    let apps_page1 = store
        .get_applications(Some(1000), None, None, None)
        .await
        .unwrap();
    for app in &apps_page1 {
        *app_counts.entry(app.id.clone()).or_insert(0) += 1;
    }

    // Stop performance monitoring
    let performance_snapshot = monitor.stop_monitoring();

    println!();
    println!("=== 100K Applications Load Test Results ===");
    println!("Target Configuration:");
    println!("  Target applications: {}", num_applications);
    println!("  Events per app: {}", events_per_app);
    println!("  Total target events: {}", total_events);
    println!();

    println!("Actual Results:");
    println!("  Applications created: {}", apps_created.len());
    println!("  Events inserted: {}", final_event_count);
    println!("  Applications in DB: {}", applications.len());
    println!();

    println!("Performance Metrics:");
    println!("  Database setup: {:.2}s", setup_duration.as_secs_f64());
    println!(
        "  Data generation: {:.2}s",
        generation_duration.as_secs_f64()
    );
    println!(
        "  Pure insert time: {:.2}s",
        total_insert_time.as_secs_f64()
    );
    println!("  Total duration: {:.2}s", total_duration.as_secs_f64());
    println!();

    println!("Throughput:");
    println!(
        "  Overall: {:.0} events/sec",
        total_events as f64 / total_duration.as_secs_f64()
    );
    println!(
        "  Generation only: {:.0} events/sec",
        total_events as f64 / generation_duration.as_secs_f64()
    );
    println!(
        "  Pure insert: {:.0} events/sec",
        total_events as f64 / total_insert_time.as_secs_f64()
    );
    println!(
        "  Applications/sec: {:.0}",
        apps_created.len() as f64 / total_duration.as_secs_f64()
    );
    println!();

    println!("Database Size:");
    if let Ok(metadata) = fs::metadata(&db_path) {
        println!(
            "  DuckDB file size: {:.2} MB",
            metadata.len() as f64 / 1024.0 / 1024.0
        );
        println!(
            "  Bytes per event: {:.1}",
            metadata.len() as f64 / final_event_count as f64
        );
        println!(
            "  Bytes per app: {:.1}",
            metadata.len() as f64 / apps_created.len() as f64
        );
    }
    println!("==========================================");

    // Print system performance summary
    performance_snapshot.print_summary();

    // Validate results
    assert_eq!(
        final_event_count, total_events as i64,
        "Event count mismatch!"
    );
    assert_eq!(
        apps_created.len(),
        num_applications,
        "Application count mismatch!"
    );

    println!("✅ All validations passed!");

    // Save performance metrics
    let csv_data = performance_snapshot.to_csv_string();
    std::fs::write("100k_applications_performance.csv", csv_data).unwrap();
    println!("📊 Performance metrics saved to: 100k_applications_performance.csv");
}

#[tokio::test]
async fn test_write_performance_scaling() {
    println!("📊 Write Performance Scaling Test");
    println!("Testing how write performance scales with number of applications");
    println!();

    let test_cases: Vec<(usize, usize)> = vec![
        (1_000, 100), // 1K apps, 100 events each = 100K events
        (10_000, 50), // 10K apps, 50 events each = 500K events
        (50_000, 20), // 50K apps, 20 events each = 1M events
    ];

    for (num_apps, events_per_app) in test_cases {
        let total_events = num_apps * events_per_app;
        println!(
            "🔄 Testing: {} applications, {} events each ({} total events)",
            num_apps, events_per_app, total_events
        );

        let monitor = PerformanceMonitor::new();
        monitor.start_monitoring(500).await;

        // Create temporary database
        let temp_db_file = NamedTempFile::new().unwrap();
        let db_path = temp_db_file.path().to_path_buf();
        drop(temp_db_file);

        let store = DuckDbStore::new(&db_path).await.unwrap();
        let mut generator = SyntheticDataGenerator::new();

        let start_time = Instant::now();
        let batch_size = 5_000usize;
        let num_batches = total_events.div_ceil(batch_size);

        for batch_num in 0..num_batches {
            let events_in_batch = std::cmp::min(batch_size, total_events - batch_num * batch_size);
            let mut spark_events = Vec::with_capacity(events_in_batch);

            for event_idx in 0..events_in_batch {
                let global_event_id = (batch_num * batch_size + event_idx) as i64;
                let app_index = global_event_id as usize / events_per_app;
                let app_id = format!("app-{:06}", app_index);

                let event_batch = generator.generate_event_batch(1);
                let (_, _, raw_data) = &event_batch[0];

                let spark_event =
                    SparkEvent::from_json(raw_data, &app_id, global_event_id).unwrap();
                spark_events.push(spark_event);
            }

            store.insert_events_batch(spark_events).await.unwrap();
        }

        let duration = start_time.elapsed();
        let performance_snapshot = monitor.stop_monitoring();

        // Verify results
        let final_count = store.count_events().await.unwrap();
        let applications = store
            .get_applications(Some(100), None, None, None)
            .await
            .unwrap();

        println!("Results for {} apps:", num_apps);
        println!("  Events inserted: {}", final_count);
        println!("  Applications found: {}", applications.len());
        println!("  Duration: {:.2}s", duration.as_secs_f64());
        println!(
            "  Throughput: {:.0} events/sec",
            total_events as f64 / duration.as_secs_f64()
        );
        println!(
            "  Peak memory: {:.1} MB",
            performance_snapshot.max_memory_usage as f64 / 1024.0 / 1024.0
        );

        if let Ok(metadata) = fs::metadata(&db_path) {
            println!(
                "  DB size: {:.1} MB",
                metadata.len() as f64 / 1024.0 / 1024.0
            );
        }
        println!();
    }
}
