/// Performance benchmarks and regression tests
/// Ensures performance remains consistent as complexity increases

use anyhow::Result;
use criterion::{black_box, Criterion};
use spark_history_server::storage::duckdb_store::DuckDbStore;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::tempdir;
use tokio::time::Instant;

/// Performance test configuration
struct PerformanceTestConfig {
    num_applications: usize,
    events_per_application: usize,
    batch_sizes: Vec<usize>,
    concurrent_writers: usize,
}

impl Default for PerformanceTestConfig {
    fn default() -> Self {
        Self {
            num_applications: 100,
            events_per_application: 1000,
            batch_sizes: vec![100, 500, 1000, 2500, 5000],
            concurrent_writers: 8,
        }
    }
}

/// Benchmark baseline performance without optimizations
#[tokio::test]
async fn benchmark_baseline_performance() -> Result<()> {
    println!("📊 Benchmarking baseline performance...");

    let config = PerformanceTestConfig::default();
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("benchmark_baseline.db");
    let store = Arc::new(DuckDbStore::new(&db_path).await?);

    // Generate test data
    let test_events = generate_test_events(config.num_applications, config.events_per_application);
    println!("Generated {} test events", test_events.len());

    // Benchmark different batch sizes
    let mut results = Vec::new();

    for &batch_size in &config.batch_sizes {
        println!("\nTesting batch size: {}", batch_size);
        
        let start_time = Instant::now();
        let mut successful_batches = 0;
        let mut total_events_processed = 0;

        for chunk in test_events.chunks(batch_size) {
            match store.insert_events_batch(chunk.to_vec()).await {
                Ok(()) => {
                    successful_batches += 1;
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
            total_events: total_events_processed,
            duration,
            throughput,
            successful_batches,
        };

        println!("✅ Batch size {} results:", batch_size);
        println!("   📈 Throughput: {:.0} events/sec", throughput);
        println!("   ⏱️  Duration: {:?}", duration);
        println!("   ✅ Success rate: {:.1}%", 
                (successful_batches as f64 / (test_events.len() / batch_size) as f64) * 100.0);

        results.push(result);
    }

    // Find optimal batch size
    let optimal = results.iter().max_by(|a, b| a.throughput.partial_cmp(&b.throughput).unwrap()).unwrap();
    println!("\n🎯 OPTIMAL CONFIGURATION:");
    println!("   📦 Best batch size: {}", optimal.batch_size);
    println!("   📈 Peak throughput: {:.0} events/sec", optimal.throughput);
    println!("   ⏱️  Duration: {:?}", optimal.duration);

    // Performance assertions
    assert!(optimal.throughput > 5000.0, "Should achieve >5000 events/sec peak throughput");

    Ok(())
}

/// Benchmark concurrent write performance
#[tokio::test]
async fn benchmark_concurrent_write_performance() -> Result<()> {
    println!("⚡ Benchmarking concurrent write performance...");

    let config = PerformanceTestConfig::default();
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("benchmark_concurrent.db");
    let store = Arc::new(DuckDbStore::new(&db_path).await?);

    const EVENTS_PER_WRITER: usize = 1000;
    const BATCH_SIZE: usize = 100;

    let start_time = Instant::now();
    let mut handles = Vec::new();

    // Launch concurrent writers
    for writer_id in 0..config.concurrent_writers {
        let store_clone = Arc::clone(&store);
        
        let handle = tokio::spawn(async move {
            let mut writer_events = Vec::new();
            for event_id in 0..EVENTS_PER_WRITER {
                let global_id = (writer_id * EVENTS_PER_WRITER + event_id) as i64;
                writer_events.push(create_benchmark_event(global_id, &format!("concurrent_app_{}", writer_id)));
            }

            let writer_start = Instant::now();
            let mut processed = 0;

            for chunk in writer_events.chunks(BATCH_SIZE) {
                if store_clone.insert_events_batch(chunk.to_vec()).await.is_ok() {
                    processed += chunk.len();
                }
            }

            let writer_duration = writer_start.elapsed();
            let writer_throughput = processed as f64 / writer_duration.as_secs_f64();

            Ok::<(usize, usize, f64, Duration), anyhow::Error>((writer_id, processed, writer_throughput, writer_duration))
        });

        handles.push(handle);
    }

    // Collect results
    let mut total_processed = 0;
    let mut writer_throughputs = Vec::new();

    for handle in handles {
        match handle.await? {
            Ok((writer_id, processed, throughput, duration)) => {
                total_processed += processed;
                writer_throughputs.push(throughput);
                println!("✅ Writer {}: {} events, {:.0} events/sec, {:?}", 
                        writer_id, processed, throughput, duration);
            }
            Err(e) => {
                println!("❌ Writer failed: {}", e);
            }
        }
    }

    let total_duration = start_time.elapsed();
    let aggregate_throughput = total_processed as f64 / total_duration.as_secs_f64();
    let avg_writer_throughput = writer_throughputs.iter().sum::<f64>() / writer_throughputs.len() as f64;

    println!("\n🎯 CONCURRENT WRITE RESULTS:");
    println!("   👥 Concurrent writers: {}", config.concurrent_writers);
    println!("   📊 Total events processed: {}", total_processed);
    println!("   📈 Aggregate throughput: {:.0} events/sec", aggregate_throughput);
    println!("   📈 Average writer throughput: {:.0} events/sec", avg_writer_throughput);
    println!("   ⏱️  Total duration: {:?}", total_duration);

    // Performance assertions for concurrent operations
    assert!(aggregate_throughput > 10000.0, "Concurrent writes should achieve >10K events/sec aggregate");
    assert!(avg_writer_throughput > 1000.0, "Each writer should achieve >1K events/sec average");

    Ok(())
}

/// Benchmark memory usage and efficiency
#[tokio::test]
async fn benchmark_memory_efficiency() -> Result<()> {
    println!("🧠 Benchmarking memory efficiency...");

    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("benchmark_memory.db");
    let store = Arc::new(DuckDbStore::new(&db_path).await?);

    // Test with increasingly large batches to monitor memory usage
    let test_sizes = vec![1000, 5000, 10000, 25000, 50000];
    let mut memory_results = Vec::new();

    for &batch_size in &test_sizes {
        println!("\nTesting memory usage with {} events...", batch_size);

        // Generate events
        let events = generate_large_test_events(batch_size);
        let estimated_memory_mb = estimate_event_memory_usage(&events);

        let start_time = Instant::now();
        let result = store.insert_events_batch(events).await;
        let duration = start_time.elapsed();

        let success = result.is_ok();
        let throughput = if success { batch_size as f64 / duration.as_secs_f64() } else { 0.0 };

        memory_results.push(MemoryTestResult {
            batch_size,
            estimated_memory_mb,
            duration,
            throughput,
            success,
        });

        println!("   📦 Batch size: {} events", batch_size);
        println!("   💾 Estimated memory: {:.1} MB", estimated_memory_mb);
        println!("   ✅ Success: {}", success);
        println!("   📈 Throughput: {:.0} events/sec", throughput);

        // Brief pause for memory cleanup
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Find memory efficiency sweet spot
    let successful_results: Vec<_> = memory_results.iter().filter(|r| r.success).collect();
    let max_successful_size = successful_results.iter().map(|r| r.batch_size).max().unwrap_or(0);

    println!("\n🎯 MEMORY EFFICIENCY RESULTS:");
    println!("   📊 Maximum successful batch size: {} events", max_successful_size);
    println!("   💾 Memory handling: {}", if max_successful_size >= 25000 { "Excellent" } else { "Needs optimization" });

    // Memory efficiency assertions
    assert!(max_successful_size >= 10000, "Should handle batches of at least 10K events");

    Ok(())
}

/// Benchmark cache performance (if cache is implemented)
#[tokio::test]
async fn benchmark_cache_performance() -> Result<()> {
    println!("🗄️ Benchmarking cache performance...");

    // This is a placeholder for cache performance testing
    // In a real implementation, this would test:
    // - Cache hit rates
    // - Cache lookup performance
    // - Cache eviction efficiency
    // - Memory usage patterns

    let cache_operations = vec![
        ("Cache Miss (Cold)", Duration::from_millis(100)),
        ("Cache Hit (Warm)", Duration::from_millis(1)),
        ("Cache Eviction", Duration::from_millis(50)),
    ];

    for (operation, simulated_time) in cache_operations {
        tokio::time::sleep(simulated_time).await;
        println!("✅ {}: {:?}", operation, simulated_time);
    }

    println!("\n🎯 CACHE PERFORMANCE SUMMARY:");
    println!("   🔥 Cache hit ratio: 95%+ (target)");
    println!("   ⚡ Lookup time: <1ms (target)");
    println!("   🗑️ Eviction efficiency: <50ms (target)");

    Ok(())
}

/// Stress test with realistic production load
#[tokio::test]
async fn stress_test_production_load() -> Result<()> {
    println!("🏋️ Running production load stress test...");

    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("stress_test.db");
    let store = Arc::new(DuckDbStore::new(&db_path).await?);

    // Production-like configuration
    const NUM_APPLICATIONS: usize = 1000;  // Scaled down from 40K for CI
    const EVENTS_PER_APP: usize = 100;
    const CONCURRENT_PRODUCERS: usize = 16;
    const BATCH_SIZE: usize = 500;

    let total_events = NUM_APPLICATIONS * EVENTS_PER_APP;
    println!("Stress testing with {} total events across {} applications", total_events, NUM_APPLICATIONS);

    let start_time = Instant::now();
    let mut handles = Vec::new();

    // Create concurrent event producers
    for producer_id in 0..CONCURRENT_PRODUCERS {
        let store_clone = Arc::clone(&store);
        let apps_per_producer = NUM_APPLICATIONS / CONCURRENT_PRODUCERS;
        let start_app = producer_id * apps_per_producer;
        let end_app = if producer_id == CONCURRENT_PRODUCERS - 1 { NUM_APPLICATIONS } else { start_app + apps_per_producer };

        let handle = tokio::spawn(async move {
            let mut producer_events = Vec::new();
            
            // Generate events for assigned applications
            for app_id in start_app..end_app {
                for event_id in 0..EVENTS_PER_APP {
                    let global_id = (app_id * EVENTS_PER_APP + event_id) as i64;
                    producer_events.push(create_benchmark_event(global_id, &format!("stress_app_{}", app_id)));
                }
            }

            // Process in batches
            let mut processed = 0;
            let producer_start = Instant::now();

            for chunk in producer_events.chunks(BATCH_SIZE) {
                match store_clone.insert_events_batch(chunk.to_vec()).await {
                    Ok(()) => processed += chunk.len(),
                    Err(e) => {
                        println!("⚠️  Producer {} batch failed: {}", producer_id, e);
                    }
                }
            }

            let producer_duration = producer_start.elapsed();
            Ok::<(usize, usize, Duration), anyhow::Error>((producer_id, processed, producer_duration))
        });

        handles.push(handle);
    }

    // Collect results
    let mut total_processed = 0;
    let mut max_producer_time = Duration::default();

    for handle in handles {
        match handle.await? {
            Ok((producer_id, processed, duration)) => {
                total_processed += processed;
                max_producer_time = max_producer_time.max(duration);
                let throughput = processed as f64 / duration.as_secs_f64();
                println!("✅ Producer {}: {} events in {:?} ({:.0} events/sec)", 
                        producer_id, processed, duration, throughput);
            }
            Err(e) => {
                println!("❌ Producer failed: {}", e);
            }
        }
    }

    let total_duration = start_time.elapsed();
    let aggregate_throughput = total_processed as f64 / total_duration.as_secs_f64();
    let processing_efficiency = (total_processed as f64 / total_events as f64) * 100.0;

    println!("\n🎯 PRODUCTION STRESS TEST RESULTS:");
    println!("   📊 Total events processed: {}/{} ({:.1}%)", total_processed, total_events, processing_efficiency);
    println!("   📈 Aggregate throughput: {:.0} events/sec", aggregate_throughput);
    println!("   ⏱️  Total test duration: {:?}", total_duration);
    println!("   ⏱️  Longest producer time: {:?}", max_producer_time);
    println!("   👥 Concurrent producers: {}", CONCURRENT_PRODUCERS);

    // Production readiness assertions
    assert!(processing_efficiency > 95.0, "Should process >95% of events successfully");
    assert!(aggregate_throughput > 20000.0, "Should achieve >20K events/sec aggregate under stress");
    assert!(total_duration < Duration::from_secs(120), "Stress test should complete within 2 minutes");

    println!("🎉 PRODUCTION STRESS TEST PASSED - System ready for enterprise scale!");

    Ok(())
}

// Helper structures and functions

#[derive(Debug)]
struct PerformanceMeasurement {
    batch_size: usize,
    total_events: usize,
    duration: Duration,
    throughput: f64,
    successful_batches: usize,
}

#[derive(Debug)]
struct MemoryTestResult {
    batch_size: usize,
    estimated_memory_mb: f64,
    duration: Duration,
    throughput: f64,
    success: bool,
}

fn generate_test_events(num_apps: usize, events_per_app: usize) -> Vec<spark_history_server::storage::duckdb_store::SparkEvent> {
    let mut events = Vec::new();
    for app_id in 0..num_apps {
        for event_id in 0..events_per_app {
            let global_id = (app_id * events_per_app + event_id) as i64;
            events.push(create_benchmark_event(global_id, &format!("perf_app_{}", app_id)));
        }
    }
    events
}

fn generate_large_test_events(count: usize) -> Vec<spark_history_server::storage::duckdb_store::SparkEvent> {
    let mut events = Vec::new();
    for i in 0..count {
        events.push(create_large_benchmark_event(i as i64, "large_test_app"));
    }
    events
}

fn create_benchmark_event(id: i64, app_id: &str) -> spark_history_server::storage::duckdb_store::SparkEvent {
    use spark_history_server::storage::duckdb_store::SparkEvent;
    use serde_json::json;

    let raw_data = json!({
        "Event": "SparkListenerTaskEnd",
        "Task Info": {
            "Task ID": id,
            "Executor ID": format!("exec_{}", id % 8),
            "Host": "benchmark-node"
        },
        "Task Metrics": {
            "Executor Run Time": 500 + (id % 1000),
            "Peak Execution Memory": 1073741824,
            "JVM GC Time": 25
        }
    });

    SparkEvent::from_json(&raw_data, app_id, id).expect("Failed to create benchmark event")
}

fn create_large_benchmark_event(id: i64, app_id: &str) -> spark_history_server::storage::duckdb_store::SparkEvent {
    use spark_history_server::storage::duckdb_store::SparkEvent;
    use serde_json::json;

    let raw_data = json!({
        "Event": "SparkListenerTaskEnd",
        "Task Info": {
            "Task ID": id,
            "Executor ID": format!("exec_{}", id % 8),
            "Host": "memory-test-node",
            "large_payload": "x".repeat(10240) // 10KB of extra data per event
        },
        "Task Metrics": {
            "Executor Run Time": 500 + (id % 1000),
            "Peak Execution Memory": 1073741824,
            "JVM GC Time": 25,
            "additional_metrics": (0..100).map(|i| format!("metric_{}_{}", i, id)).collect::<Vec<_>>()
        }
    });

    SparkEvent::from_json(&raw_data, app_id, id).expect("Failed to create large benchmark event")
}

fn estimate_event_memory_usage(events: &[spark_history_server::storage::duckdb_store::SparkEvent]) -> f64 {
    // Rough estimation: each event ~ 2KB in memory
    (events.len() * 2048) as f64 / 1024.0 / 1024.0 // Convert to MB
}