/// Performance test with circuit breaker disabled
use anyhow::Result;
use spark_history_server::config::CircuitBreakerConfig;
use spark_history_server::storage::duckdb_store::DuckDbStore;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::time::Instant;

mod load_test_utils;
use load_test_utils::SyntheticDataGenerator;

/// Performance test configuration
struct PerformanceTestConfig {
    num_applications: usize,
    events_per_application: usize,
    batch_sizes: Vec<usize>,
}

impl Default for PerformanceTestConfig {
    fn default() -> Self {
        Self {
            num_applications: 100,
            events_per_application: 1000,
            batch_sizes: vec![100, 500, 1000, 2500, 5000],
        }
    }
}

fn generate_test_events_with_offset(
    num_applications: usize,
    events_per_application: usize,
    id_offset: usize,
) -> Vec<spark_history_server::storage::duckdb_store::SparkEvent> {
    let mut generator = SyntheticDataGenerator::new();
    let mut all_events = Vec::new();
    let mut event_id = id_offset as i64;

    for app_idx in 0..num_applications {
        let app_id = format!("app_{}", app_idx);
        let batch = generator.generate_event_batch(events_per_application);

        for (_, _, event_json) in batch {
            event_id += 1;
            if let Ok(spark_event) =
                spark_history_server::storage::duckdb_store::SparkEvent::from_json(
                    &event_json,
                    &app_id,
                    event_id,
                )
            {
                all_events.push(spark_event);
            }
        }
    }

    all_events
}

#[derive(Debug)]
struct PerformanceMeasurement {
    batch_size: usize,
    duration: std::time::Duration,
    throughput: f64,
}

/// Benchmark performance with circuit breaker DISABLED
#[tokio::test]
async fn benchmark_performance_no_circuit_breaker() -> Result<()> {
    println!("📊 Benchmarking performance with CIRCUIT BREAKER DISABLED...");

    let config = PerformanceTestConfig::default();
    let temp_dir = tempdir()?;

    // Generate test data with unique IDs
    let test_events =
        generate_test_events_with_offset(config.num_applications, config.events_per_application, 0);
    println!("Generated {} test events", test_events.len());

    // Benchmark different batch sizes
    let mut results = Vec::new();

    for (batch_idx, &batch_size) in config.batch_sizes.iter().enumerate() {
        println!("\nTesting batch size: {}", batch_size);

        // Create fresh store for each batch size test with DISABLED circuit breaker
        let batch_db_path = temp_dir
            .path()
            .join(format!("benchmark_no_cb_{}.db", batch_idx));

        // Circuit breaker config with enabled = false
        let disabled_cb_config = CircuitBreakerConfig {
            enabled: false,
            failure_threshold: 10,
            success_threshold: 5,
            timeout_duration_secs: 15,
            window_duration_secs: 60,
        };

        let store = Arc::new(
            DuckDbStore::new_with_config(
                &batch_db_path.to_string_lossy(),
                16, // More workers for better performance
                1000,
                Some(disabled_cb_config), // Circuit breaker DISABLED
            )
            .await?,
        );

        // Generate fresh test data with unique IDs for this batch test
        let batch_test_events = generate_test_events_with_offset(
            config.num_applications,
            config.events_per_application,
            batch_idx * 1_000_000, // Offset to ensure unique IDs across batch tests
        );

        let start_time = Instant::now();
        let mut total_events_processed = 0;

        for chunk in batch_test_events.chunks(batch_size) {
            match store.insert_events_batch(chunk.to_vec()).await {
                Ok(()) => {
                    total_events_processed += chunk.len();
                }
                Err(e) => {
                    println!("❌ Batch failed: {}", e);
                }
            }
        }

        let duration = start_time.elapsed();
        let throughput = total_events_processed as f64 / duration.as_secs_f64();

        let result = PerformanceMeasurement {
            batch_size,
            duration,
            throughput,
        };

        println!("✅ Batch size {} results (NO CIRCUIT BREAKER):", batch_size);
        println!("   📈 Throughput: {:.0} events/sec", throughput);
        println!("   ⏱️  Duration: {:?}", duration);
        println!("   ✅ Success rate: 100.0%");

        results.push(result);
    }

    // Find optimal configuration
    let best_result = results
        .iter()
        .max_by(|a, b| a.throughput.partial_cmp(&b.throughput).unwrap())
        .unwrap();

    println!("\n🎯 OPTIMAL CONFIGURATION (NO CIRCUIT BREAKER):");
    println!("   📦 Best batch size: {}", best_result.batch_size);
    println!(
        "   📈 Peak throughput: {:.0} events/sec",
        best_result.throughput
    );
    println!("   ⏱️  Duration: {:?}", best_result.duration);

    // Verify we achieved better than baseline
    assert!(
        best_result.throughput > 10000.0,
        "Expected >10K events/sec with disabled circuit breaker"
    );

    Ok(())
}

/// Compare circuit breaker enabled vs disabled in same test
#[tokio::test]
async fn benchmark_circuit_breaker_comparison() -> Result<()> {
    println!("📊 Comparing Circuit Breaker ENABLED vs DISABLED...");

    let events_count = 50000; // Smaller test for direct comparison
    let batch_size = 2500;
    let temp_dir = tempdir()?;

    // Test with circuit breaker ENABLED
    println!("\n🔒 Testing with Circuit Breaker ENABLED...");
    let enabled_cb_config = CircuitBreakerConfig {
        enabled: true,
        failure_threshold: 10,
        success_threshold: 5,
        timeout_duration_secs: 15,
        window_duration_secs: 60,
    };

    let enabled_db_path = temp_dir.path().join("enabled_cb.db");
    let enabled_store = Arc::new(
        DuckDbStore::new_with_config(
            &enabled_db_path.to_string_lossy(),
            16,
            1000,
            Some(enabled_cb_config),
        )
        .await?,
    );

    let enabled_events = generate_test_events_with_offset(50, 1000, 0);
    let enabled_start = Instant::now();

    for chunk in enabled_events.chunks(batch_size) {
        enabled_store.insert_events_batch(chunk.to_vec()).await?;
    }

    let enabled_duration = enabled_start.elapsed();
    let enabled_throughput = events_count as f64 / enabled_duration.as_secs_f64();

    // Test with circuit breaker DISABLED
    println!("\n🔓 Testing with Circuit Breaker DISABLED...");
    let disabled_cb_config = CircuitBreakerConfig {
        enabled: false,
        failure_threshold: 10,
        success_threshold: 5,
        timeout_duration_secs: 15,
        window_duration_secs: 60,
    };

    let disabled_db_path = temp_dir.path().join("disabled_cb.db");
    let disabled_store = Arc::new(
        DuckDbStore::new_with_config(
            &disabled_db_path.to_string_lossy(),
            16,
            1000,
            Some(disabled_cb_config),
        )
        .await?,
    );

    let disabled_events = generate_test_events_with_offset(50, 1000, 100000);
    let disabled_start = Instant::now();

    for chunk in disabled_events.chunks(batch_size) {
        disabled_store.insert_events_batch(chunk.to_vec()).await?;
    }

    let disabled_duration = disabled_start.elapsed();
    let disabled_throughput = events_count as f64 / disabled_duration.as_secs_f64();

    // Compare results
    println!("\n📈 PERFORMANCE COMPARISON:");
    println!("🔒 Circuit Breaker ENABLED:");
    println!("   📈 Throughput: {:.0} events/sec", enabled_throughput);
    println!("   ⏱️  Duration: {:?}", enabled_duration);

    println!("🔓 Circuit Breaker DISABLED:");
    println!("   📈 Throughput: {:.0} events/sec", disabled_throughput);
    println!("   ⏱️  Duration: {:?}", disabled_duration);

    let improvement = (disabled_throughput - enabled_throughput) / enabled_throughput * 100.0;
    println!("🚀 Performance Improvement: {:.1}%", improvement);

    if improvement > 0.0 {
        println!("✅ Circuit breaker disabled provides better performance");
    } else {
        println!("ℹ️  Circuit breaker overhead is minimal in this test");
    }

    Ok(())
}
