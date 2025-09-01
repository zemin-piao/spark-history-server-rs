use serde_json::json;
use std::fs;
use std::time::{Duration, Instant};
use tempfile::{NamedTempFile, TempDir};

mod load_test_utils;
mod performance_monitor;

use load_test_utils::SyntheticDataGenerator;
use performance_monitor::{PerformanceMonitor, PerformanceSnapshot};
use spark_history_server::api::create_app;
use spark_history_server::config::HistoryConfig;
use spark_history_server::storage::duckdb_store::{DuckDbStore, SparkEvent};
use spark_history_server::storage::HistoryProvider;

#[derive(Debug)]
pub struct BenchmarkReport {
    pub test_name: String,
    pub total_events: usize,
    pub duration: Duration,
    pub events_per_second: f64,
    pub performance_snapshot: PerformanceSnapshot,
    pub additional_metrics: std::collections::HashMap<String, String>,
}

impl BenchmarkReport {
    pub fn print_summary(&self) {
        println!("\n=== Benchmark Report: {} ===", self.test_name);
        println!("Total events: {}", self.total_events);
        println!("Duration: {:.2}s", self.duration.as_secs_f64());
        println!("Throughput: {:.0} events/sec", self.events_per_second);

        for (key, value) in &self.additional_metrics {
            println!("{}: {}", key, value);
        }

        self.performance_snapshot.print_summary();
    }

    pub fn to_json(&self) -> serde_json::Value {
        json!({
            "test_name": self.test_name,
            "total_events": self.total_events,
            "duration_seconds": self.duration.as_secs_f64(),
            "events_per_second": self.events_per_second,
            "max_cpu_usage": self.performance_snapshot.max_cpu_usage,
            "avg_cpu_usage": self.performance_snapshot.avg_cpu_usage,
            "max_memory_usage_mb": self.performance_snapshot.max_memory_usage as f64 / 1024.0 / 1024.0,
            "avg_memory_usage_mb": self.performance_snapshot.avg_memory_usage as f64 / 1024.0 / 1024.0,
            "additional_metrics": self.additional_metrics,
        })
    }
}

pub struct ComprehensiveBenchmark {
    reports: Vec<BenchmarkReport>,
}

impl ComprehensiveBenchmark {
    pub fn new() -> Self {
        Self {
            reports: Vec::new(),
        }
    }

    pub async fn run_write_performance_benchmark(&mut self, events: usize) -> anyhow::Result<()> {
        println!(
            "Running write performance benchmark with {} events...",
            events
        );

        let monitor = PerformanceMonitor::new();
        monitor.start_monitoring(500).await;

        let temp_db_file = NamedTempFile::new()?;
        let db_path = temp_db_file.path().to_path_buf();
        drop(temp_db_file); // Close the file so DuckDB can use it

        let start_time = Instant::now();

        // Setup database
        let store = DuckDbStore::new(&db_path).await?;

        // Generate and insert data
        let mut generator = SyntheticDataGenerator::new();
        let batch_size = 10_000;
        let num_batches = (events + batch_size - 1) / batch_size;

        for batch_idx in 0..num_batches {
            let events_in_batch = std::cmp::min(batch_size, events - batch_idx * batch_size);
            let event_batch = generator.generate_event_batch(events_in_batch);

            let spark_events: Vec<SparkEvent> = event_batch
                .into_iter()
                .map(|(event_id, app_id, raw_data)| {
                    SparkEvent::from_json(&raw_data, &app_id, event_id).unwrap()
                })
                .collect();

            store.insert_events_batch(spark_events).await?;
        }

        let duration = start_time.elapsed();
        let performance_snapshot = monitor.stop_monitoring();

        // Verify count
        let final_count = store.count_events().await?;

        let mut additional_metrics = std::collections::HashMap::new();
        additional_metrics.insert("batch_size".to_string(), batch_size.to_string());
        additional_metrics.insert("num_batches".to_string(), num_batches.to_string());
        additional_metrics.insert("final_db_count".to_string(), final_count.to_string());

        let report = BenchmarkReport {
            test_name: "Write Performance".to_string(),
            total_events: events,
            duration,
            events_per_second: events as f64 / duration.as_secs_f64(),
            performance_snapshot,
            additional_metrics,
        };

        report.print_summary();
        self.reports.push(report);

        Ok(())
    }

    pub async fn run_read_performance_benchmark(&mut self, events: usize) -> anyhow::Result<()> {
        println!(
            "Running read performance benchmark with {} events in database...",
            events
        );

        // First, populate database
        let temp_db_file = NamedTempFile::new()?;
        let db_path = temp_db_file.path().to_path_buf();
        drop(temp_db_file); // Close the file so DuckDB can use it
        let store = DuckDbStore::new(&db_path).await?;

        // Populate with data
        let mut generator = SyntheticDataGenerator::new();
        let batch_size = 10_000;
        let num_batches = (events + batch_size - 1) / batch_size;

        for batch_idx in 0..num_batches {
            let events_in_batch = std::cmp::min(batch_size, events - batch_idx * batch_size);
            let event_batch = generator.generate_event_batch(events_in_batch);

            let spark_events: Vec<SparkEvent> = event_batch
                .into_iter()
                .map(|(event_id, app_id, raw_data)| {
                    SparkEvent::from_json(&raw_data, &app_id, event_id).unwrap()
                })
                .collect();

            store.insert_events_batch(spark_events).await?;
        }

        println!("Database populated, starting read performance test...");

        let monitor = PerformanceMonitor::new();
        monitor.start_monitoring(250).await;

        let start_time = Instant::now();

        // Run various read operations
        let num_queries = 100;
        let mut total_results = 0;

        for _ in 0..num_queries {
            // Test different query patterns
            let apps = store.get_applications(Some(50), None, None, None).await?;
            total_results += apps.len();

            if let Some(first_app) = apps.first() {
                let _executors = store.get_executor_summary(&first_app.id).await?;
            }

            // Test analytics queries
            let params = spark_history_server::analytics_api::AnalyticsQuery {
                start_date: None,
                end_date: None,
                app_id: None,
                limit: Some(20),
            };

            let _trends = store.get_performance_trends(&params).await?;
            let _summary = store.get_cross_app_summary(&params).await?;
        }

        let duration = start_time.elapsed();
        let performance_snapshot = monitor.stop_monitoring();

        let mut additional_metrics = std::collections::HashMap::new();
        additional_metrics.insert("num_queries".to_string(), num_queries.to_string());
        additional_metrics.insert(
            "total_results_returned".to_string(),
            total_results.to_string(),
        );
        additional_metrics.insert(
            "queries_per_second".to_string(),
            format!("{:.1}", num_queries as f64 / duration.as_secs_f64()),
        );

        let report = BenchmarkReport {
            test_name: "Read Performance".to_string(),
            total_events: events,
            duration,
            events_per_second: (num_queries * events) as f64 / duration.as_secs_f64(), // Adjusted metric for read operations
            performance_snapshot,
            additional_metrics,
        };

        report.print_summary();
        self.reports.push(report);

        Ok(())
    }

    pub async fn run_file_processing_benchmark(&mut self, events: usize) -> anyhow::Result<()> {
        println!(
            "Running file processing benchmark with {} events...",
            events
        );

        let monitor = PerformanceMonitor::new();
        monitor.start_monitoring(500).await;

        let temp_dir = TempDir::new()?;
        let temp_db_file = NamedTempFile::new()?;
        let db_path = temp_db_file.path().to_path_buf();
        drop(temp_db_file); // Close the file so DuckDB can use it

        let start_time = Instant::now();

        // Phase 1: Generate event log files
        let mut generator = SyntheticDataGenerator::new();
        let events_per_file = 10_000;
        let num_files = (events + events_per_file - 1) / events_per_file;

        let mut generated_files = Vec::new();
        let mut file_generation_time = Duration::ZERO;

        for file_idx in 0..num_files {
            let file_start = Instant::now();

            let file_name = format!("app-{:04}_1.gz", file_idx % 10); // 10 different apps
            let file_path = temp_dir.path().join(&file_name);

            let file = fs::File::create(&file_path)?;
            let mut encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());

            let events_in_file =
                std::cmp::min(events_per_file, events - file_idx * events_per_file);
            for _ in 0..events_in_file {
                let batch = generator.generate_event_batch(1);
                let (_, _, event_json) = &batch[0];
                let event_line = format!("{}\n", serde_json::to_string(event_json)?);
                std::io::Write::write_all(&mut encoder, event_line.as_bytes())?;
            }

            encoder.finish()?;
            generated_files.push(file_path);
            file_generation_time += file_start.elapsed();
        }

        // Phase 2: Process files into database
        let store = DuckDbStore::new(&db_path).await?;
        let processing_start = Instant::now();

        let mut total_processed = 0;
        for (file_idx, file_path) in generated_files.iter().enumerate() {
            // Read and decompress
            let compressed_data = fs::read(file_path)?;
            let mut decoder = flate2::read::GzDecoder::new(compressed_data.as_slice());
            let mut content = String::new();
            std::io::Read::read_to_string(&mut decoder, &mut content)?;

            // Parse and insert
            let mut events_batch = Vec::new();
            for (line_idx, line) in content.lines().enumerate() {
                if line.trim().is_empty() {
                    continue;
                }

                let event_json: serde_json::Value = serde_json::from_str(line)?;
                let event_id = (file_idx * events_per_file + line_idx) as i64;
                let app_id = format!("app-{:04}", file_idx % 10);

                let spark_event = SparkEvent::from_json(&event_json, &app_id, event_id)?;
                events_batch.push(spark_event);
            }

            store.insert_events_batch(events_batch).await?;
            total_processed += events_per_file.min(events - file_idx * events_per_file);
        }

        let processing_time = processing_start.elapsed();
        let total_duration = start_time.elapsed();
        let performance_snapshot = monitor.stop_monitoring();

        // Calculate file sizes
        let total_file_size: u64 = generated_files
            .iter()
            .map(|path| fs::metadata(path).unwrap().len())
            .sum();

        let mut additional_metrics = std::collections::HashMap::new();
        additional_metrics.insert("num_files".to_string(), generated_files.len().to_string());
        additional_metrics.insert(
            "total_file_size_mb".to_string(),
            format!("{:.2}", total_file_size as f64 / 1024.0 / 1024.0),
        );
        additional_metrics.insert(
            "file_generation_time".to_string(),
            format!("{:.2}s", file_generation_time.as_secs_f64()),
        );
        additional_metrics.insert(
            "processing_time".to_string(),
            format!("{:.2}s", processing_time.as_secs_f64()),
        );
        additional_metrics.insert(
            "processing_throughput".to_string(),
            format!(
                "{:.0} events/sec",
                total_processed as f64 / processing_time.as_secs_f64()
            ),
        );

        let report = BenchmarkReport {
            test_name: "File Processing (End-to-End)".to_string(),
            total_events: events,
            duration: total_duration,
            events_per_second: events as f64 / total_duration.as_secs_f64(),
            performance_snapshot,
            additional_metrics,
        };

        report.print_summary();
        self.reports.push(report);

        Ok(())
    }

    pub async fn run_api_performance_benchmark(&mut self, events: usize) -> anyhow::Result<()> {
        println!(
            "Running API performance benchmark with {} events in database...",
            events
        );

        let monitor = PerformanceMonitor::new();
        monitor.start_monitoring(500).await;

        // Setup database and server
        let temp_dir = TempDir::new()?;
        let temp_db_file = NamedTempFile::new()?;
        let db_path = temp_db_file.path().to_path_buf();
        drop(temp_db_file); // Close the file so DuckDB can use it

        // Populate database
        let store = DuckDbStore::new(&db_path).await?;
        let mut generator = SyntheticDataGenerator::new();
        let batch_size = 10_000;
        let num_batches = (events + batch_size - 1) / batch_size;

        for batch_idx in 0..num_batches {
            let events_in_batch = std::cmp::min(batch_size, events - batch_idx * batch_size);
            let event_batch = generator.generate_event_batch(events_in_batch);

            let spark_events: Vec<SparkEvent> = event_batch
                .into_iter()
                .map(|(event_id, app_id, raw_data)| {
                    SparkEvent::from_json(&raw_data, &app_id, event_id).unwrap()
                })
                .collect();

            store.insert_events_batch(spark_events).await?;
        }

        // Setup API server
        let history_settings = HistoryConfig {
            log_directory: temp_dir.path().to_string_lossy().to_string(),
            max_applications: 1000,
            update_interval_seconds: 60,
            max_apps_per_request: 100,
            compression_enabled: true,
            cache_directory: Some(temp_dir.path().to_string_lossy().to_string()),
            enable_cache: false,
        };

        let history_provider = HistoryProvider::new(history_settings).await?;
        let app = create_app(history_provider).await?;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server_url = format!("http://127.0.0.1:{}", addr.port());

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(100)).await; // Let server start

        // Run API performance test
        let start_time = Instant::now();
        let client = reqwest::Client::new();

        let endpoints = vec![
            "/api/v1/applications",
            "/api/v1/applications?limit=50",
            "/api/v1/analytics/summary",
            "/api/v1/analytics/performance-trends?limit=100",
            "/api/v1/analytics/cross-app-summary",
        ];

        let requests_per_endpoint = 50;
        let mut total_requests = 0;
        let mut successful_requests = 0;
        let mut total_response_time = Duration::ZERO;

        for endpoint in &endpoints {
            for _ in 0..requests_per_endpoint {
                let url = format!("{}{}", server_url, endpoint);
                let request_start = Instant::now();

                match client.get(&url).send().await {
                    Ok(response) => {
                        total_requests += 1;
                        let response_time = request_start.elapsed();
                        total_response_time += response_time;

                        if response.status().is_success() {
                            successful_requests += 1;
                        }
                    }
                    Err(_) => {
                        total_requests += 1;
                        total_response_time += request_start.elapsed();
                    }
                }
            }
        }

        let duration = start_time.elapsed();
        let performance_snapshot = monitor.stop_monitoring();

        let avg_response_time = if total_requests > 0 {
            total_response_time / total_requests as u32
        } else {
            Duration::ZERO
        };

        let mut additional_metrics = std::collections::HashMap::new();
        additional_metrics.insert("total_api_requests".to_string(), total_requests.to_string());
        additional_metrics.insert(
            "successful_requests".to_string(),
            successful_requests.to_string(),
        );
        additional_metrics.insert(
            "success_rate".to_string(),
            format!(
                "{:.1}%",
                (successful_requests as f64 / total_requests as f64) * 100.0
            ),
        );
        additional_metrics.insert(
            "avg_response_time_ms".to_string(),
            format!("{:.0}", avg_response_time.as_millis()),
        );
        additional_metrics.insert(
            "requests_per_second".to_string(),
            format!("{:.1}", total_requests as f64 / duration.as_secs_f64()),
        );

        let report = BenchmarkReport {
            test_name: "API Performance".to_string(),
            total_events: events,
            duration,
            events_per_second: total_requests as f64 / duration.as_secs_f64(), // API requests per second
            performance_snapshot,
            additional_metrics,
        };

        report.print_summary();
        self.reports.push(report);

        Ok(())
    }

    pub fn generate_final_report(&self) -> serde_json::Value {
        let reports_json: Vec<serde_json::Value> =
            self.reports.iter().map(|r| r.to_json()).collect();

        let total_events: usize = self.reports.iter().map(|r| r.total_events).sum();
        let total_duration: f64 = self.reports.iter().map(|r| r.duration.as_secs_f64()).sum();

        json!({
            "benchmark_summary": {
                "total_tests": self.reports.len(),
                "total_events_processed": total_events,
                "total_test_time_seconds": total_duration,
                "timestamp": chrono::Utc::now().to_rfc3339(),
            },
            "individual_tests": reports_json,
        })
    }

    pub fn print_comprehensive_summary(&self) {
        println!("\n{}", "=".repeat(60));
        println!("              COMPREHENSIVE BENCHMARK SUMMARY");
        println!("{}", "=".repeat(60));

        for report in &self.reports {
            println!(
                "{}: {:.0} events/sec, {:.1} MB peak memory",
                report.test_name,
                report.events_per_second,
                report.performance_snapshot.max_memory_usage as f64 / 1024.0 / 1024.0
            );
        }

        if let (Some(fastest), Some(slowest)) = (
            self.reports.iter().max_by(|a, b| {
                a.events_per_second
                    .partial_cmp(&b.events_per_second)
                    .unwrap()
            }),
            self.reports.iter().min_by(|a, b| {
                a.events_per_second
                    .partial_cmp(&b.events_per_second)
                    .unwrap()
            }),
        ) {
            println!();
            println!(
                "Fastest: {} ({:.0} events/sec)",
                fastest.test_name, fastest.events_per_second
            );
            println!(
                "Slowest: {} ({:.0} events/sec)",
                slowest.test_name, slowest.events_per_second
            );
        }

        println!("{}", "=".repeat(60));
    }
}

#[tokio::test]
async fn run_comprehensive_benchmark_suite() {
    println!("Starting Comprehensive Benchmark Suite");
    println!("This will test write, read, file processing, and API performance");
    println!();

    let mut benchmark = ComprehensiveBenchmark::new();

    // Run all benchmarks with different data sizes
    let test_sizes = vec![100_000, 500_000, 1_000_000];

    for &size in &test_sizes {
        println!("\n🔄 Running benchmarks with {} events...", size);

        // Write performance
        if let Err(e) = benchmark.run_write_performance_benchmark(size).await {
            println!("Write benchmark failed: {}", e);
        }

        // Read performance
        if let Err(e) = benchmark.run_read_performance_benchmark(size).await {
            println!("Read benchmark failed: {}", e);
        }

        // File processing performance
        if let Err(e) = benchmark.run_file_processing_benchmark(size).await {
            println!("File processing benchmark failed: {}", e);
        }

        // API performance
        if let Err(e) = benchmark.run_api_performance_benchmark(size).await {
            println!("API benchmark failed: {}", e);
        }
    }

    // Generate final report
    benchmark.print_comprehensive_summary();

    let report_json = benchmark.generate_final_report();
    let report_str = serde_json::to_string_pretty(&report_json).unwrap();

    std::fs::write("comprehensive_benchmark_report.json", &report_str).unwrap();
    println!("\n📊 Detailed benchmark report saved to: comprehensive_benchmark_report.json");
}
