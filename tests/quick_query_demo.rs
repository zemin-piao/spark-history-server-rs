use std::time::Instant;
use tempfile::NamedTempFile;

mod load_test_utils;
mod performance_monitor;

use load_test_utils::SyntheticDataGenerator;
use performance_monitor::PerformanceMonitor;
use spark_history_server::storage::duckdb_store::{DuckDbStore, SparkEvent};

#[tokio::test]
async fn test_quick_analytical_query_demo() {
    println!("⚡ Quick Analytical Query Demo");
    println!("Demonstrating query performance on a manageable dataset");
    println!();

    let monitor = PerformanceMonitor::new();
    monitor.start_monitoring(250).await;

    // Create a reasonable dataset for quick demonstration
    let num_applications = 5_000usize;
    let events_per_app = 40usize;
    let total_events = num_applications * events_per_app;

    println!("📊 Dataset:");
    println!("  Applications: {}", num_applications);
    println!("  Events per app: {}", events_per_app);
    println!("  Total events: {}", total_events);
    println!();

    // Setup database and data
    let temp_db_file = NamedTempFile::new().unwrap();
    let db_path = temp_db_file.path().to_path_buf();
    drop(temp_db_file);

    let setup_start = Instant::now();
    let store = DuckDbStore::new(&db_path).await.unwrap();
    let mut generator = SyntheticDataGenerator::new();

    // Fast data generation
    let batch_size = 25_000usize;
    let num_batches = total_events.div_ceil(batch_size);

    print!("📝 Generating data: ");
    for batch_num in 0..num_batches {
        let events_in_batch = std::cmp::min(batch_size, total_events - batch_num * batch_size);
        let mut spark_events = Vec::with_capacity(events_in_batch);

        for event_idx in 0..events_in_batch {
            let global_event_id = (batch_num * batch_size + event_idx) as i64;
            let app_index = global_event_id as usize / events_per_app;
            let app_id = format!("demo-app-{:04}", app_index);

            let event_batch = generator.generate_event_batch(1);
            let (_, _, raw_data) = &event_batch[0];

            let spark_event = SparkEvent::from_json(raw_data, &app_id, global_event_id).unwrap();
            spark_events.push(spark_event);
        }

        store.insert_events_batch(spark_events).await.unwrap();
        print!(".");
    }

    let setup_duration = setup_start.elapsed();
    println!(" ✅ Done ({:.1}s)", setup_duration.as_secs_f64());
    println!();

    // Now run queries and measure performance
    println!("🚀 Running Analytical Queries:");
    println!("{}", "-".repeat(50));

    // Query 1: Basic application list (most common query)
    print!("📱 Application List (top 50): ");
    let query_start = Instant::now();
    let apps = store
        .get_applications(Some(50), None, None, None)
        .await
        .unwrap();
    let query_time = query_start.elapsed();
    println!("{} apps in {:.0}ms", apps.len(), query_time.as_millis());

    // Query 2: Event count
    print!("📊 Total Event Count: ");
    let query_start = Instant::now();
    let event_count = store.count_events().await.unwrap();
    let query_time = query_start.elapsed();
    println!("{} events in {:.0}ms", event_count, query_time.as_millis());

    // Query 3: Executor summary for first application
    if let Some(app) = apps.first() {
        print!("⚙️ Executor Summary ({}): ", app.id);
        let query_start = Instant::now();
        let executors = store.get_executor_summary(&app.id).await.unwrap();
        let query_time = query_start.elapsed();
        println!(
            "{} executors in {:.0}ms",
            executors.len(),
            query_time.as_millis()
        );
    }

    // Query 4: Resource usage summary
    print!("💾 Resource Usage Summary: ");
    let query_start = Instant::now();
    let resource_usage = store.get_resource_usage_summary().await.unwrap();
    let query_time = query_start.elapsed();
    println!(
        "{} records in {:.0}ms",
        resource_usage.len(),
        query_time.as_millis()
    );

    // Query 5: Performance trends
    print!("📈 Performance Trends: ");
    let query_start = Instant::now();
    let params = spark_history_server::analytics_api::AnalyticsQuery {
        start_date: None,
        end_date: None,
        app_id: None,
        limit: Some(20),
    };
    let trends = store.get_performance_trends(&params).await.unwrap();
    let query_time = query_start.elapsed();
    println!("{} trends in {:.0}ms", trends.len(), query_time.as_millis());

    // Query 6: Task distribution
    print!("📋 Task Distribution: ");
    let query_start = Instant::now();
    let params = spark_history_server::analytics_api::AnalyticsQuery {
        start_date: None,
        end_date: None,
        app_id: None,
        limit: Some(15),
    };
    let distribution = store.get_task_distribution(&params).await.unwrap();
    let query_time = query_start.elapsed();
    println!(
        "{} distributions in {:.0}ms",
        distribution.len(),
        query_time.as_millis()
    );

    let performance_snapshot = monitor.stop_monitoring();

    println!();
    println!("🏆 Query Performance Summary:");
    println!(
        "  Dataset: {} applications, {} events",
        num_applications, total_events
    );
    println!("  Setup time: {:.1}s", setup_duration.as_secs_f64());
    println!("  All analytical queries completed successfully!");
    println!("  Database demonstrates excellent query performance");

    performance_snapshot.print_summary();

    println!("\n✅ Quick analytical query demo completed!");
}
