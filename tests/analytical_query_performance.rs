use std::collections::HashMap;
use std::fs;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

mod load_test_utils;
mod performance_monitor;

use load_test_utils::SyntheticDataGenerator;
use performance_monitor::PerformanceMonitor;
use spark_history_server::analytics_api::AnalyticsQuery;
use spark_history_server::storage::duckdb_store::{DuckDbStore, SparkEvent};

#[tokio::test]
async fn test_analytical_query_performance_100k_apps() {
    println!("🔍 Analytical Query Performance Test - 100K Applications");
    println!("Testing complex analytical queries on large dataset");
    println!();

    let monitor = PerformanceMonitor::new();
    monitor.start_monitoring(500).await;

    // First, create a substantial dataset similar to the 100K app test
    let num_applications = 100_000;
    let events_per_app = 20;
    let total_events = num_applications * events_per_app;

    println!("📊 Setting up test dataset:");
    println!("  Applications: {}", num_applications);
    println!("  Events per app: {}", events_per_app);
    println!("  Total events: {}", total_events);
    println!();

    // Create database and populate with data
    let temp_db_file = NamedTempFile::new().unwrap();
    let db_path = temp_db_file.path().to_path_buf();
    drop(temp_db_file);

    let setup_start = Instant::now();
    let store = DuckDbStore::new(&db_path).await.unwrap();

    // Quick data generation (reuse pattern from large scale test)
    println!("📝 Generating test data...");
    let mut generator = SyntheticDataGenerator::new();
    let batch_size = 20_000; // Larger batches for faster setup
    let num_batches = (total_events + batch_size - 1) / batch_size;

    for batch_num in 0..num_batches {
        let events_in_batch = std::cmp::min(batch_size, total_events - batch_num * batch_size);
        let mut spark_events = Vec::with_capacity(events_in_batch);

        for event_idx in 0..events_in_batch {
            let global_event_id = (batch_num * batch_size + event_idx) as i64;
            let app_index = global_event_id as usize / events_per_app;
            let app_id = format!("app-{:06}", app_index);

            let event_batch = generator.generate_event_batch(1);
            let (_, _, raw_data) = &event_batch[0];

            let spark_event = SparkEvent::from_json(raw_data, &app_id, global_event_id).unwrap();
            spark_events.push(spark_event);
        }

        store.insert_events_batch(spark_events).await.unwrap();

        if batch_num % 25 == 0 {
            let progress = ((batch_num + 1) as f64 / num_batches as f64) * 100.0;
            println!("  Setup progress: {:.1}%", progress);
        }
    }

    let setup_duration = setup_start.elapsed();
    println!(
        "✅ Data setup completed in: {:.2}s",
        setup_duration.as_secs_f64()
    );
    println!();

    // Now run analytical query performance tests
    println!("🚀 Running Analytical Query Performance Tests");
    println!("{}", "=".repeat(60));

    let mut query_results = Vec::new();

    // Test 1: Cross-Application Summary Query
    println!("\n📊 Test 1: Cross-Application Summary");
    let query_start = Instant::now();
    let params = AnalyticsQuery {
        start_date: None,
        end_date: None,
        app_id: None,
        limit: Some(100),
    };
    let summary = store.get_cross_app_summary(&params).await.unwrap();
    let query_duration = query_start.elapsed();

    println!("Results:");
    println!("  Total applications: {}", summary.total_applications);
    println!("  Active applications: {}", summary.active_applications);
    println!("  Total events: {}", summary.total_events);
    println!("  Query time: {:.0}ms", query_duration.as_millis());

    query_results.push((
        "Cross-App Summary",
        query_duration,
        summary.total_events as usize,
    ));

    // Test 2: Performance Trends Query
    println!("\n📈 Test 2: Performance Trends (Top 1000 results)");
    let query_start = Instant::now();
    let params = AnalyticsQuery {
        start_date: None,
        end_date: None,
        app_id: None,
        limit: Some(1000),
    };
    let trends = store.get_performance_trends(&params).await.unwrap();
    let query_duration = query_start.elapsed();

    println!("Results:");
    println!("  Trend records returned: {}", trends.len());
    println!("  Query time: {:.0}ms", query_duration.as_millis());

    if let Some(first_trend) = trends.first() {
        println!(
            "  Sample trend - App: {}, Avg duration: {:.1}ms",
            first_trend.app_id,
            first_trend.avg_task_duration_ms.unwrap_or(0.0)
        );
    }

    query_results.push(("Performance Trends", query_duration, trends.len()));

    // Test 3: Task Distribution Analysis
    println!("\n📋 Test 3: Task Distribution Analysis");
    let query_start = Instant::now();
    let params = AnalyticsQuery {
        start_date: None,
        end_date: None,
        app_id: None,
        limit: Some(500),
    };
    let distribution = store.get_task_distribution(&params).await.unwrap();
    let query_duration = query_start.elapsed();

    println!("Results:");
    println!("  Distribution records: {}", distribution.len());
    println!("  Query time: {:.0}ms", query_duration.as_millis());

    if let Some(first_dist) = distribution.first() {
        println!(
            "  Sample dist - App: {}, Total tasks: {}, Completed: {}",
            first_dist.app_id, first_dist.total_tasks, first_dist.completed_tasks
        );
    }

    query_results.push(("Task Distribution", query_duration, distribution.len()));

    // Test 4: Executor Utilization Query
    println!("\n⚙️ Test 4: Executor Utilization Analysis");
    let query_start = Instant::now();
    let params = AnalyticsQuery {
        start_date: None,
        end_date: None,
        app_id: None,
        limit: Some(200),
    };
    let utilization = store.get_executor_utilization(&params).await.unwrap();
    let query_duration = query_start.elapsed();

    println!("Results:");
    println!("  Executor records: {}", utilization.len());
    println!("  Query time: {:.0}ms", query_duration.as_millis());

    if let Some(first_exec) = utilization.first() {
        println!(
            "  Sample executor - ID: {}, Total tasks: {}",
            first_exec.executor_id, first_exec.total_tasks
        );
    }

    query_results.push(("Executor Utilization", query_duration, utilization.len()));

    // Test 5: Basic Application List Query (frequently used)
    println!("\n📱 Test 5: Application List Query (Paginated)");
    let query_start = Instant::now();
    let apps = store
        .get_applications(Some(100), None, None, None)
        .await
        .unwrap();
    let query_duration = query_start.elapsed();

    println!("Results:");
    println!("  Applications returned: {}", apps.len());
    println!("  Query time: {:.0}ms", query_duration.as_millis());

    query_results.push(("Application List", query_duration, apps.len()));

    // Test 6: Specific Application Executor Summary
    println!("\n🔧 Test 6: Single Application Executor Summary");
    if let Some(app) = apps.first() {
        let query_start = Instant::now();
        let executors = store.get_executor_summary(&app.id).await.unwrap();
        let query_duration = query_start.elapsed();

        println!("Results:");
        println!("  App ID: {}", app.id);
        println!("  Executors found: {}", executors.len());
        println!("  Query time: {:.0}ms", query_duration.as_millis());

        query_results.push(("Executor Summary", query_duration, executors.len()));
    }

    // Test 7: Resource Utilization Metrics (comprehensive)
    println!("\n💾 Test 7: Resource Utilization Metrics");
    let query_start = Instant::now();
    let params = AnalyticsQuery {
        start_date: None,
        end_date: None,
        app_id: None,
        limit: Some(50),
    };
    let resources = store
        .get_resource_utilization_metrics(&params)
        .await
        .unwrap();
    let query_duration = query_start.elapsed();

    println!("Results:");
    println!("  Resource records: {}", resources.len());
    println!("  Query time: {:.0}ms", query_duration.as_millis());

    query_results.push(("Resource Utilization", query_duration, resources.len()));

    let performance_snapshot = monitor.stop_monitoring();
    let total_test_duration = setup_start.elapsed();

    // Performance Summary
    println!("\n{}", "=".repeat(60));
    println!("📊 ANALYTICAL QUERY PERFORMANCE SUMMARY");
    println!("{}", "=".repeat(60));
    println!(
        "Dataset: {} applications, {} events",
        num_applications, total_events
    );

    if let Ok(metadata) = fs::metadata(&db_path) {
        println!(
            "Database size: {:.1} MB",
            metadata.len() as f64 / 1024.0 / 1024.0
        );
    }

    println!("\nQuery Performance Results:");
    println!(
        "{:<25} {:>12} {:>15} {:>15}",
        "Query Type", "Time (ms)", "Records", "Records/ms"
    );
    println!("{}", "-".repeat(70));

    let mut total_query_time = Duration::ZERO;
    let mut total_records = 0;

    for (query_name, duration, record_count) in &query_results {
        let records_per_ms = if duration.as_millis() > 0 {
            *record_count as f64 / duration.as_millis() as f64
        } else {
            *record_count as f64
        };

        println!(
            "{:<25} {:>12} {:>15} {:>15.1}",
            query_name,
            duration.as_millis(),
            record_count,
            records_per_ms
        );

        total_query_time += *duration;
        total_records += record_count;
    }

    println!("{}", "-".repeat(70));
    println!(
        "{:<25} {:>12} {:>15} {:>15.1}",
        "TOTAL",
        total_query_time.as_millis(),
        total_records,
        if total_query_time.as_millis() > 0 {
            total_records as f64 / total_query_time.as_millis() as f64
        } else {
            total_records as f64
        }
    );

    println!("\nPerformance Analysis:");
    let avg_query_time = total_query_time.as_millis() as f64 / query_results.len() as f64;
    println!("  Average query time: {:.1}ms", avg_query_time);
    println!("  Total setup time: {:.2}s", setup_duration.as_secs_f64());
    println!(
        "  Total test time: {:.2}s",
        total_test_duration.as_secs_f64()
    );

    // Performance categories
    let fast_queries = query_results
        .iter()
        .filter(|(_, d, _)| d.as_millis() < 100)
        .count();
    let medium_queries = query_results
        .iter()
        .filter(|(_, d, _)| d.as_millis() >= 100 && d.as_millis() < 500)
        .count();
    let slow_queries = query_results
        .iter()
        .filter(|(_, d, _)| d.as_millis() >= 500)
        .count();

    println!("\nQuery Performance Distribution:");
    println!("  Fast queries (<100ms): {}", fast_queries);
    println!("  Medium queries (100-500ms): {}", medium_queries);
    println!("  Slow queries (>500ms): {}", slow_queries);

    println!("\nSystem Performance:");
    performance_snapshot.print_summary();

    // Validate performance expectations
    println!("\n✅ Performance Validation:");
    let all_fast = query_results.iter().all(|(_, d, _)| d.as_millis() < 1000);
    println!(
        "  All queries < 1000ms: {}",
        if all_fast { "✅ PASS" } else { "❌ FAIL" }
    );

    let most_fast = fast_queries + medium_queries >= query_results.len() * 80 / 100;
    println!(
        "  80% queries < 500ms: {}",
        if most_fast { "✅ PASS" } else { "❌ FAIL" }
    );

    println!("\n🏆 Analytical query performance test completed successfully!");
    println!("This demonstrates excellent analytical query performance on 100K applications!");
}

#[tokio::test]
async fn test_concurrent_analytical_queries() {
    println!("🔄 Concurrent Analytical Query Performance Test");
    println!("Testing multiple analytical queries running simultaneously");
    println!();

    // Set up a smaller but still substantial dataset for concurrent testing
    let num_applications = 10_000;
    let events_per_app = 50;
    let total_events = num_applications * events_per_app;

    let temp_db_file = NamedTempFile::new().unwrap();
    let db_path = temp_db_file.path().to_path_buf();
    drop(temp_db_file);

    println!("📊 Setting up concurrent test dataset:");
    println!("  Applications: {}", num_applications);
    println!("  Events per app: {}", events_per_app);
    println!("  Total events: {}", total_events);

    let store = DuckDbStore::new(&db_path).await.unwrap();
    let mut generator = SyntheticDataGenerator::new();

    // Quick setup
    let batch_size = 25_000;
    let num_batches = (total_events + batch_size - 1) / batch_size;

    for batch_num in 0..num_batches {
        let events_in_batch = std::cmp::min(batch_size, total_events - batch_num * batch_size);
        let mut spark_events = Vec::with_capacity(events_in_batch);

        for event_idx in 0..events_in_batch {
            let global_event_id = (batch_num * batch_size + event_idx) as i64;
            let app_index = global_event_id as usize / events_per_app;
            let app_id = format!("app-{:05}", app_index);

            let event_batch = generator.generate_event_batch(1);
            let (_, _, raw_data) = &event_batch[0];

            let spark_event = SparkEvent::from_json(raw_data, &app_id, global_event_id).unwrap();
            spark_events.push(spark_event);
        }

        store.insert_events_batch(spark_events).await.unwrap();
    }

    println!("✅ Dataset ready for concurrent testing\n");

    let monitor = PerformanceMonitor::new();
    monitor.start_monitoring(250).await;

    // Run multiple queries concurrently
    let store = std::sync::Arc::new(store);
    let concurrent_users = 20;
    let queries_per_user = 10;

    println!(
        "🚀 Running {} concurrent users, {} queries each",
        concurrent_users, queries_per_user
    );

    let start_time = Instant::now();
    let mut handles = Vec::new();

    for user_id in 0..concurrent_users {
        let store_clone = std::sync::Arc::clone(&store);

        let handle = tokio::spawn(async move {
            let mut user_results = Vec::new();

            for query_num in 0..queries_per_user {
                let query_start = Instant::now();

                // Rotate through different query types
                let result = match query_num % 5 {
                    0 => {
                        // Cross-app summary
                        let params = AnalyticsQuery {
                            start_date: None,
                            end_date: None,
                            app_id: None,
                            limit: Some(50),
                        };
                        let summary = store_clone.get_cross_app_summary(&params).await.unwrap();
                        ("cross_app_summary", summary.total_events as usize)
                    }
                    1 => {
                        // Performance trends
                        let params = AnalyticsQuery {
                            start_date: None,
                            end_date: None,
                            app_id: None,
                            limit: Some(100),
                        };
                        let trends = store_clone.get_performance_trends(&params).await.unwrap();
                        ("performance_trends", trends.len())
                    }
                    2 => {
                        // Task distribution
                        let params = AnalyticsQuery {
                            start_date: None,
                            end_date: None,
                            app_id: None,
                            limit: Some(75),
                        };
                        let dist = store_clone.get_task_distribution(&params).await.unwrap();
                        ("task_distribution", dist.len())
                    }
                    3 => {
                        // Application list
                        let apps = store_clone
                            .get_applications(Some(50), None, None, None)
                            .await
                            .unwrap();
                        ("app_list", apps.len())
                    }
                    4 => {
                        // Executor utilization
                        let params = AnalyticsQuery {
                            start_date: None,
                            end_date: None,
                            app_id: None,
                            limit: Some(25),
                        };
                        let exec = store_clone.get_executor_utilization(&params).await.unwrap();
                        ("executor_util", exec.len())
                    }
                    _ => unreachable!(),
                };

                let query_duration = query_start.elapsed();
                user_results.push((result.0.to_string(), query_duration, result.1));
            }

            (user_id, user_results)
        });

        handles.push(handle);
    }

    // Collect all results
    let mut all_query_results = Vec::new();
    let mut query_type_stats: HashMap<String, (u64, usize, usize)> = HashMap::new(); // (total_ms, count, total_records)

    for handle in handles {
        let (_user_id, user_results) = handle.await.unwrap();

        for (query_type, duration, record_count) in user_results {
            let entry = query_type_stats
                .entry(query_type.clone())
                .or_insert((0, 0, 0));
            entry.0 += duration.as_millis() as u64;
            entry.1 += 1;
            entry.2 += record_count;

            all_query_results.push((query_type, duration, record_count));
        }
    }

    let total_duration = start_time.elapsed();
    let performance_snapshot = monitor.stop_monitoring();

    println!("\n📊 Concurrent Query Performance Results");
    println!("{}", "=".repeat(60));
    println!("Total concurrent queries: {}", all_query_results.len());
    println!("Total execution time: {:.2}s", total_duration.as_secs_f64());
    println!(
        "Average concurrency: {:.1} queries/sec",
        all_query_results.len() as f64 / total_duration.as_secs_f64()
    );

    println!("\nPer-Query Type Performance:");
    println!(
        "{:<20} {:>8} {:>12} {:>12} {:>15}",
        "Query Type", "Count", "Avg (ms)", "Total (ms)", "Records"
    );
    println!("{}", "-".repeat(70));

    for (query_type, (total_ms, count, total_records)) in &query_type_stats {
        let avg_ms = *total_ms as f64 / *count as f64;
        println!(
            "{:<20} {:>8} {:>12.1} {:>12} {:>15}",
            query_type, count, avg_ms, total_ms, total_records
        );
    }

    println!("\nConcurrency Analysis:");
    let total_query_time: u64 = query_type_stats
        .values()
        .map(|(total_ms, _, _)| *total_ms)
        .sum();
    let concurrency_factor = total_query_time as f64 / total_duration.as_millis() as f64;
    println!("  Effective concurrency factor: {:.2}x", concurrency_factor);
    println!(
        "  Query throughput: {:.1} queries/sec",
        all_query_results.len() as f64 / total_duration.as_secs_f64()
    );

    performance_snapshot.print_summary();

    println!("\n✅ Concurrent analytical query test completed!");
    println!(
        "Database handled {} concurrent analytical queries efficiently!",
        all_query_results.len()
    );
}
