#![cfg(feature = "performance-tests")]

use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs;
use std::io::Write;
use std::time::Instant;
use tempfile::{NamedTempFile, TempDir};

mod load_test_utils;
mod performance_monitor;

use load_test_utils::SyntheticDataGenerator;
use performance_monitor::PerformanceMonitor;
use spark_history_server::storage::duckdb_store::{DuckDbStore, SparkEvent};

/// Test that generates actual event log files on disk and processes them through the full pipeline
#[tokio::test]
async fn test_full_pipeline_10m_events() {
    println!("=== Full Pipeline Test: 10M Events ===");
    println!("This test will:");
    println!("1. Generate realistic Spark event log files on disk");
    println!("2. Test file reading, decompression, and parsing");
    println!("3. Measure end-to-end performance including disk I/O");
    println!();

    let monitor = PerformanceMonitor::new();
    monitor.start_monitoring(500).await; // Sample every 500ms for longer test

    let total_events = 10_000_000;
    let events_per_file = 50_000; // Realistic event log file size
    let num_files = total_events / events_per_file;

    // Create temporary directory for event log files
    let temp_dir = TempDir::new().unwrap();
    let log_dir = temp_dir.path();

    // Create temporary database file
    let temp_db_file = NamedTempFile::new().unwrap();
    let db_path = temp_db_file.path();

    println!(
        "Phase 1: Generating {} event log files ({} events each)...",
        num_files, events_per_file
    );
    let file_generation_start = Instant::now();

    let mut generator = SyntheticDataGenerator::new();
    let mut generated_files = Vec::new();

    // Generate event log files
    for file_idx in 0..num_files {
        let app_id = format!("app-{:04}", file_idx % 20); // 20 different applications
        let file_name = format!("{}_1.gz", app_id);
        let file_path = log_dir.join(&file_name);

        // Create compressed event log file
        let file = fs::File::create(&file_path).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());

        for _ in 0..events_per_file {
            let batch = generator.generate_event_batch(1);
            let (_, _, event_json) = &batch[0];
            let event_line = format!("{}\n", serde_json::to_string(event_json).unwrap());
            encoder.write_all(event_line.as_bytes()).unwrap();
        }

        encoder.finish().unwrap();
        generated_files.push(file_path);

        if file_idx % 20 == 0 || file_idx == num_files - 1 {
            let progress = ((file_idx + 1) as f64 / num_files as f64) * 100.0;
            println!(
                "Generated {}/{} files ({:.1}%)",
                file_idx + 1,
                num_files,
                progress
            );
        }
    }

    let file_generation_duration = file_generation_start.elapsed();
    let total_file_size: u64 = generated_files
        .iter()
        .map(|path| fs::metadata(path).unwrap().len())
        .sum();

    println!(
        "File generation completed in {:.2}s",
        file_generation_duration.as_secs_f64()
    );
    println!(
        "Total file size: {:.2} MB",
        total_file_size as f64 / 1024.0 / 1024.0
    );
    println!();

    println!("Phase 2: Processing event log files through full pipeline...");
    let processing_start = Instant::now();

    // Initialize DuckDB store
    let store = DuckDbStore::new(db_path).await.unwrap();

    let mut total_events_processed = 0;
    let batch_size = 5_000;

    // Process each file
    for (file_idx, file_path) in generated_files.iter().enumerate() {
        let file_start = Instant::now();

        // Read and decompress file
        let compressed_data = fs::read(file_path).unwrap();
        let mut decoder = flate2::read::GzDecoder::new(compressed_data.as_slice());
        let mut decompressed_content = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut decompressed_content).unwrap();

        // Parse events and batch insert
        let mut events_batch = Vec::with_capacity(batch_size);
        let mut event_id = (file_idx * events_per_file) as i64;

        for line in decompressed_content.lines() {
            if line.trim().is_empty() {
                continue;
            }

            let event_json: serde_json::Value = serde_json::from_str(line).unwrap();
            let app_id = format!("app-{:04}", file_idx % 20);

            event_id += 1;
            let spark_event = SparkEvent::from_json(&event_json, &app_id, event_id).unwrap();
            events_batch.push(spark_event);

            // Insert batch when full
            if events_batch.len() >= batch_size {
                store
                    .insert_events_batch(events_batch.clone())
                    .await
                    .unwrap();
                total_events_processed += events_batch.len();
                events_batch.clear();
            }
        }

        // Insert remaining events
        if !events_batch.is_empty() {
            store
                .insert_events_batch(events_batch.clone())
                .await
                .unwrap();
            total_events_processed += events_batch.len();
        }

        let file_duration = file_start.elapsed();

        if file_idx % 20 == 0 || file_idx == generated_files.len() - 1 {
            let progress = ((file_idx + 1) as f64 / generated_files.len() as f64) * 100.0;
            let overall_elapsed = processing_start.elapsed().as_secs_f64();
            let events_per_sec = total_events_processed as f64 / overall_elapsed;

            println!(
                "Processed {}/{} files ({:.1}%), {} events total, {:.0} events/sec, file time: {:.3}s",
                file_idx + 1,
                generated_files.len(),
                progress,
                total_events_processed,
                events_per_sec,
                file_duration.as_secs_f64()
            );
        }
    }

    let processing_duration = processing_start.elapsed();
    let total_duration = file_generation_start.elapsed();

    // Stop monitoring and get results
    let performance_snapshot = monitor.stop_monitoring();

    // Verify final count
    let final_count = store.count_events().await.unwrap();

    println!("\n=== Full Pipeline Performance Results ===");
    println!("Total events: {}", total_events);
    println!("Events processed: {}", total_events_processed);
    println!("Final DB count: {}", final_count);
    println!("Files generated: {}", generated_files.len());
    println!(
        "Total file size: {:.2} MB",
        total_file_size as f64 / 1024.0 / 1024.0
    );
    println!();
    println!("Timing breakdown:");
    println!(
        "  File generation: {:.2}s",
        file_generation_duration.as_secs_f64()
    );
    println!(
        "  File processing: {:.2}s",
        processing_duration.as_secs_f64()
    );
    println!("  Total duration: {:.2}s", total_duration.as_secs_f64());
    println!();
    println!("Throughput:");
    println!(
        "  Processing only: {:.0} events/sec",
        total_events_processed as f64 / processing_duration.as_secs_f64()
    );
    println!(
        "  End-to-end: {:.0} events/sec",
        total_events as f64 / total_duration.as_secs_f64()
    );
    println!("==========================================");

    performance_snapshot.print_summary();

    // Save performance metrics
    let csv_data = performance_snapshot.to_csv_string();
    std::fs::write("full_pipeline_performance_metrics.csv", csv_data).unwrap();
    println!("Performance metrics saved to: full_pipeline_performance_metrics.csv");
}

/// Test different compression formats and their impact on performance
#[tokio::test]
async fn test_compression_performance() {
    println!("Testing different compression formats...");

    let events_per_test = 100_000;
    let events_per_file = 10_000;
    let num_files = events_per_test / events_per_file;

    let compression_methods = vec![
        ("uncompressed", None),
        ("gzip", Some("gz")),
        // Note: lz4 would require additional dependency
    ];

    for (method_name, extension) in compression_methods {
        println!("\n--- Testing {} ---", method_name);

        let monitor = PerformanceMonitor::new();
        monitor.start_monitoring(250).await;

        let temp_dir = TempDir::new().unwrap();
        let temp_db_file = NamedTempFile::new().unwrap();
        let db_path = temp_db_file.path().to_path_buf();
        drop(temp_db_file); // Close the file so DuckDB can use it
        let store = DuckDbStore::new(&db_path).await.unwrap();

        let mut generator = SyntheticDataGenerator::new();
        let file_generation_start = Instant::now();
        let mut generated_files = Vec::new();

        // Generate files with specific compression
        for file_idx in 0..num_files {
            let file_name = match extension {
                Some(ext) => format!("app_{}.{}", file_idx, ext),
                None => format!("app_{}.log", file_idx),
            };
            let file_path = temp_dir.path().join(&file_name);

            match extension {
                Some("gz") => {
                    let file = fs::File::create(&file_path).unwrap();
                    let mut encoder = GzEncoder::new(file, Compression::default());

                    for _ in 0..events_per_file {
                        let batch = generator.generate_event_batch(1);
                        let (_, _, event_json) = &batch[0];
                        let event_line =
                            format!("{}\n", serde_json::to_string(event_json).unwrap());
                        encoder.write_all(event_line.as_bytes()).unwrap();
                    }
                    encoder.finish().unwrap();
                }
                None => {
                    let mut file = fs::File::create(&file_path).unwrap();
                    for _ in 0..events_per_file {
                        let batch = generator.generate_event_batch(1);
                        let (_, _, event_json) = &batch[0];
                        let event_line =
                            format!("{}\n", serde_json::to_string(event_json).unwrap());
                        file.write_all(event_line.as_bytes()).unwrap();
                    }
                }
                _ => panic!("Unsupported compression"),
            }

            generated_files.push(file_path);
        }

        let file_generation_duration = file_generation_start.elapsed();
        let total_size: u64 = generated_files
            .iter()
            .map(|p| fs::metadata(p).unwrap().len())
            .sum();

        // Process files
        let processing_start = Instant::now();
        let mut total_processed = 0;

        for (file_idx, file_path) in generated_files.iter().enumerate() {
            let content = match extension {
                Some("gz") => {
                    let compressed_data = fs::read(file_path).unwrap();
                    let mut decoder = flate2::read::GzDecoder::new(compressed_data.as_slice());
                    let mut decompressed = String::new();
                    std::io::Read::read_to_string(&mut decoder, &mut decompressed).unwrap();
                    decompressed
                }
                None => fs::read_to_string(file_path).unwrap(),
                _ => panic!("Unsupported compression"),
            };

            let mut events_batch = Vec::new();
            for (line_idx, line) in content.lines().enumerate() {
                if line.trim().is_empty() {
                    continue;
                }

                let event_json: serde_json::Value = serde_json::from_str(line).unwrap();
                let event_id = (file_idx * events_per_file + line_idx) as i64;
                let app_id = format!("app-{}", file_idx);

                let spark_event = SparkEvent::from_json(&event_json, &app_id, event_id).unwrap();
                events_batch.push(spark_event);
            }

            store.insert_events_batch(events_batch).await.unwrap();
            total_processed += events_per_file;
        }

        let processing_duration = processing_start.elapsed();
        let performance_snapshot = monitor.stop_monitoring();

        println!("Method: {}", method_name);
        println!("  File size: {:.2} MB", total_size as f64 / 1024.0 / 1024.0);
        println!(
            "  Generation time: {:.2}s",
            file_generation_duration.as_secs_f64()
        );
        println!(
            "  Processing time: {:.2}s",
            processing_duration.as_secs_f64()
        );
        println!(
            "  Processing rate: {:.0} events/sec",
            total_processed as f64 / processing_duration.as_secs_f64()
        );
        println!(
            "  Max memory: {:.1} MB",
            performance_snapshot.max_memory_usage as f64 / 1024.0 / 1024.0
        );
    }
}
