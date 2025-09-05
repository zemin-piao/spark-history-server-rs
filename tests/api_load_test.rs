#![cfg(feature = "performance-tests")]

use reqwest::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::{NamedTempFile, TempDir};
use tokio::sync::Semaphore;

mod load_test_utils;
mod performance_monitor;

use load_test_utils::SyntheticDataGenerator;
use performance_monitor::PerformanceMonitor;
use spark_history_server::api::create_app;
use spark_history_server::config::HistoryConfig;
use spark_history_server::storage::duckdb_store::{DuckDbStore, SparkEvent};
use spark_history_server::storage::HistoryProvider;

#[derive(Debug, Clone)]
pub struct LoadTestResult {
    pub endpoint: String,
    pub total_requests: usize,
    pub successful_requests: usize,
    pub failed_requests: usize,
    pub total_duration: Duration,
    pub avg_response_time: Duration,
    pub min_response_time: Duration,
    pub max_response_time: Duration,
    pub requests_per_second: f64,
    pub response_times: Vec<Duration>,
}

impl LoadTestResult {
    pub fn print_summary(&self) {
        println!("\n=== Load Test Results: {} ===", self.endpoint);
        println!("Total requests: {}", self.total_requests);
        println!("Successful: {}", self.successful_requests);
        println!("Failed: {}", self.failed_requests);
        println!(
            "Success rate: {:.1}%",
            (self.successful_requests as f64 / self.total_requests as f64) * 100.0
        );
        println!("Total duration: {:.2}s", self.total_duration.as_secs_f64());
        println!("Requests/sec: {:.1}", self.requests_per_second);
        println!("Response times:");
        println!("  Average: {:.0}ms", self.avg_response_time.as_millis());
        println!("  Min: {:.0}ms", self.min_response_time.as_millis());
        println!("  Max: {:.0}ms", self.max_response_time.as_millis());

        // Calculate percentiles
        let mut sorted_times = self.response_times.clone();
        sorted_times.sort();

        if !sorted_times.is_empty() {
            let p50 = sorted_times[sorted_times.len() * 50 / 100];
            let p90 = sorted_times[sorted_times.len() * 90 / 100];
            let p95 = sorted_times[sorted_times.len() * 95 / 100];
            let p99 = sorted_times[sorted_times.len() * 99 / 100];

            println!("  P50: {:.0}ms", p50.as_millis());
            println!("  P90: {:.0}ms", p90.as_millis());
            println!("  P95: {:.0}ms", p95.as_millis());
            println!("  P99: {:.0}ms", p99.as_millis());
        }
        println!("=========================================\n");
    }
}

async fn setup_test_server_with_data(num_events: usize) -> (String, TempDir) {
    // Create temporary directory and database
    let temp_dir = TempDir::new().unwrap();
    let temp_db_file = NamedTempFile::new().unwrap();
    let db_path = temp_db_file.path();

    // Generate test data
    println!("Setting up test server with {} events...", num_events);
    let store = DuckDbStore::new(db_path).await.unwrap();
    let mut generator = SyntheticDataGenerator::new();

    let batch_size = 10_000;
    let num_batches = num_events.div_ceil(batch_size);

    for batch_idx in 0..num_batches {
        let events_in_batch = std::cmp::min(batch_size, num_events - batch_idx * batch_size);
        let event_batch = generator.generate_event_batch(events_in_batch);

        let spark_events: Vec<SparkEvent> = event_batch
            .into_iter()
            .map(|(event_id, app_id, raw_data)| {
                SparkEvent::from_json(&raw_data, &app_id, event_id).unwrap()
            })
            .collect();

        store.insert_events_batch(spark_events).await.unwrap();
    }

    // Create history provider and app
    let history_settings = HistoryConfig {
        log_directory: temp_dir.path().to_string_lossy().to_string(),
        max_applications: 1000,
        update_interval_seconds: 60,
        max_apps_per_request: 100,
        compression_enabled: true,
        database_directory: Some(temp_dir.path().to_string_lossy().to_string()),
        hdfs: None,
        s3: None,
    };

    let history_provider = HistoryProvider::new(history_settings).await.unwrap();
    let app = create_app(history_provider).await.unwrap();

    // Start server
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_url = format!("http://127.0.0.1:{}", addr.port());

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Wait for server to be ready
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("Test server started at: {}", server_url);
    (server_url, temp_dir)
}

async fn load_test_endpoint(
    base_url: &str,
    endpoint: &str,
    num_requests: usize,
    concurrent_requests: usize,
) -> LoadTestResult {
    let client = Client::new();
    let url = format!("{}{}", base_url, endpoint);
    let semaphore = Arc::new(Semaphore::new(concurrent_requests));

    let start_time = Instant::now();
    let mut handles = Vec::new();
    let mut response_times = Vec::with_capacity(num_requests);

    for _ in 0..num_requests {
        let client = client.clone();
        let url = url.clone();
        let semaphore = Arc::clone(&semaphore);

        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();

            let request_start = Instant::now();
            let result = client.get(&url).send().await;
            let request_duration = request_start.elapsed();

            match result {
                Ok(response) => {
                    if response.status().is_success() {
                        (true, request_duration)
                    } else {
                        (false, request_duration)
                    }
                }
                Err(_) => (false, request_duration),
            }
        });

        handles.push(handle);
    }

    // Collect results
    let mut successful_requests = 0;
    let mut failed_requests = 0;

    for handle in handles {
        let (success, duration) = handle.await.unwrap();
        if success {
            successful_requests += 1;
        } else {
            failed_requests += 1;
        }
        response_times.push(duration);
    }

    let total_duration = start_time.elapsed();
    let avg_response_time = if !response_times.is_empty() {
        response_times.iter().sum::<Duration>() / response_times.len() as u32
    } else {
        Duration::ZERO
    };

    let min_response_time = response_times
        .iter()
        .min()
        .cloned()
        .unwrap_or(Duration::ZERO);
    let max_response_time = response_times
        .iter()
        .max()
        .cloned()
        .unwrap_or(Duration::ZERO);
    let requests_per_second = num_requests as f64 / total_duration.as_secs_f64();

    LoadTestResult {
        endpoint: endpoint.to_string(),
        total_requests: num_requests,
        successful_requests,
        failed_requests,
        total_duration,
        avg_response_time,
        min_response_time,
        max_response_time,
        requests_per_second,
        response_times,
    }
}

#[tokio::test]
async fn test_api_load_performance() {
    println!("=== API Load Performance Test ===");

    let monitor = PerformanceMonitor::new();
    monitor.start_monitoring(500).await;

    // Setup server with substantial data
    let (server_url, _temp_dir) = setup_test_server_with_data(500_000).await;

    let endpoints_to_test = vec![
        "/api/v1/applications",
        "/api/v1/applications?limit=10",
        "/api/v1/applications?limit=100",
        "/api/v1/analytics/summary",
        "/api/v1/analytics/performance-trends?limit=50",
        "/api/v1/analytics/cross-app-summary",
    ];

    let num_requests_per_endpoint = 100;
    let concurrent_requests = 10;

    let mut all_results = Vec::new();

    for endpoint in endpoints_to_test {
        println!("Testing endpoint: {}", endpoint);

        let result = load_test_endpoint(
            &server_url,
            endpoint,
            num_requests_per_endpoint,
            concurrent_requests,
        )
        .await;

        result.print_summary();
        all_results.push(result);
    }

    let performance_snapshot = monitor.stop_monitoring();

    println!("=== Overall API Performance Summary ===");
    let total_requests: usize = all_results.iter().map(|r| r.total_requests).sum();
    let total_successful: usize = all_results.iter().map(|r| r.successful_requests).sum();
    let overall_success_rate = (total_successful as f64 / total_requests as f64) * 100.0;

    println!("Total API requests: {}", total_requests);
    println!("Overall success rate: {:.1}%", overall_success_rate);
    println!("Endpoints tested: {}", all_results.len());

    // Find best and worst performing endpoints
    if let Some(fastest) = all_results.iter().min_by_key(|r| r.avg_response_time) {
        println!(
            "Fastest endpoint: {} ({:.0}ms avg)",
            fastest.endpoint,
            fastest.avg_response_time.as_millis()
        );
    }

    if let Some(slowest) = all_results.iter().max_by_key(|r| r.avg_response_time) {
        println!(
            "Slowest endpoint: {} ({:.0}ms avg)",
            slowest.endpoint,
            slowest.avg_response_time.as_millis()
        );
    }

    println!("=====================================");
    performance_snapshot.print_summary();
}

#[tokio::test]
async fn test_concurrent_user_simulation() {
    println!("=== Concurrent User Simulation Test ===");

    let monitor = PerformanceMonitor::new();
    monitor.start_monitoring(250).await;

    // Setup server
    let (server_url, _temp_dir) = setup_test_server_with_data(1_000_000).await;

    let num_users = 50;
    let requests_per_user = 20;
    let user_think_time_ms = 1000; // 1 second between requests

    println!(
        "Simulating {} concurrent users, {} requests each",
        num_users, requests_per_user
    );
    println!("Think time between requests: {}ms", user_think_time_ms);

    let client = Arc::new(Client::new());
    let semaphore = Arc::new(Semaphore::new(num_users)); // Limit concurrent users

    let endpoints = vec![
        "/api/v1/applications",
        "/api/v1/applications?limit=20",
        "/api/v1/analytics/summary",
        "/api/v1/analytics/performance-trends?limit=10",
    ];

    let start_time = Instant::now();
    let mut handles = Vec::new();

    for user_id in 0..num_users {
        let client = Arc::clone(&client);
        let semaphore = Arc::clone(&semaphore);
        let server_url = server_url.clone();
        let endpoints = endpoints.clone();

        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();

            let mut user_results = Vec::new();

            for request_num in 0..requests_per_user {
                // Pick a random endpoint
                let endpoint = &endpoints[request_num % endpoints.len()];
                let url = format!("{}{}", server_url, endpoint);

                let request_start = Instant::now();
                let result = client.get(&url).send().await;
                let request_duration = request_start.elapsed();

                let success = match result {
                    Ok(response) => response.status().is_success(),
                    Err(_) => false,
                };

                user_results.push(((*endpoint).to_string(), success, request_duration));

                // Simulate user think time (except for last request)
                if request_num < requests_per_user - 1 {
                    tokio::time::sleep(Duration::from_millis(user_think_time_ms)).await;
                }
            }

            (user_id, user_results)
        });

        handles.push(handle);
    }

    // Collect all user results
    let mut endpoint_stats: HashMap<String, (usize, usize, Vec<Duration>)> = HashMap::new();
    let mut total_requests = 0;
    let mut total_successful = 0;

    for handle in handles {
        let (_user_id, user_results) = handle.await.unwrap();

        for (endpoint, success, duration) in user_results {
            total_requests += 1;
            if success {
                total_successful += 1;
            }

            let entry = endpoint_stats
                .entry(endpoint.to_string())
                .or_insert((0, 0, Vec::new()));
            entry.0 += 1; // total requests
            if success {
                entry.1 += 1; // successful requests
            }
            entry.2.push(duration); // response times
        }
    }

    let total_duration = start_time.elapsed();
    let performance_snapshot = monitor.stop_monitoring();

    println!("\n=== Concurrent User Simulation Results ===");
    println!(
        "Total simulation time: {:.2}s",
        total_duration.as_secs_f64()
    );
    println!("Total requests: {}", total_requests);
    println!("Successful requests: {}", total_successful);
    println!(
        "Success rate: {:.1}%",
        (total_successful as f64 / total_requests as f64) * 100.0
    );
    println!(
        "Overall throughput: {:.1} requests/sec",
        total_requests as f64 / total_duration.as_secs_f64()
    );
    println!();

    // Print per-endpoint statistics
    for (endpoint, (total, successful, times)) in endpoint_stats {
        let avg_time = if !times.is_empty() {
            times.iter().sum::<Duration>() / times.len() as u32
        } else {
            Duration::ZERO
        };

        println!(
            "{}: {} requests, {:.1}% success, {:.0}ms avg",
            endpoint,
            total,
            (successful as f64 / total as f64) * 100.0,
            avg_time.as_millis()
        );
    }

    println!("==========================================");
    performance_snapshot.print_summary();
}

#[tokio::test]
async fn test_stress_test_scaling() {
    println!("=== API Stress Test - Scaling Performance ===");

    let (server_url, _temp_dir) = setup_test_server_with_data(2_000_000).await;

    let concurrency_levels = vec![1, 5, 10, 25, 50, 100];
    let requests_per_level = 100;
    let endpoint = "/api/v1/applications?limit=50";

    println!("Testing endpoint: {}", endpoint);
    println!("Requests per concurrency level: {}", requests_per_level);
    println!();

    for concurrency in concurrency_levels {
        println!("Testing concurrency level: {}", concurrency);

        let monitor = PerformanceMonitor::new();
        monitor.start_monitoring(250).await;

        let result =
            load_test_endpoint(&server_url, endpoint, requests_per_level, concurrency).await;

        let performance_snapshot = monitor.stop_monitoring();

        println!(
            "Concurrency {}: {:.1} req/sec, {:.0}ms avg, {:.1}% success, {:.1} MB peak memory",
            concurrency,
            result.requests_per_second,
            result.avg_response_time.as_millis(),
            (result.successful_requests as f64 / result.total_requests as f64) * 100.0,
            performance_snapshot.max_memory_usage as f64 / 1024.0 / 1024.0
        );
    }

    println!(
        "\nStress test completed - check for performance degradation at higher concurrency levels"
    );
}
