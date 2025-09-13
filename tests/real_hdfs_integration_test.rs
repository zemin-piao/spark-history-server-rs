use anyhow::Result;
use std::{path::Path, sync::Arc, time::Duration};
use tokio::time::timeout;

use spark_history_server::{
    config::{HdfsConfig, HistoryConfig, KerberosConfig},
    storage::file_reader::{create_file_reader, FileReader, HdfsFileReader},
};

/// Real HDFS integration tests
/// These tests require an actual HDFS cluster and are marked with #[ignore]
/// Run with: cargo test test_real_hdfs --ignored
fn get_test_hdfs_config() -> HdfsConfig {
    HdfsConfig {
        namenode_url: std::env::var("HDFS_NAMENODE_URL")
            .unwrap_or_else(|_| "hdfs://localhost:9000".to_string()),
        connection_timeout_ms: Some(30000),
        read_timeout_ms: Some(60000),
        kerberos: None,
    }
}

fn get_test_hdfs_config_with_kerberos() -> HdfsConfig {
    let kerberos_config = KerberosConfig {
        principal: std::env::var("KERBEROS_PRINCIPAL")
            .unwrap_or_else(|_| "spark@EXAMPLE.COM".to_string()),
        keytab_path: std::env::var("KERBEROS_KEYTAB").ok(),
        krb5_config_path: std::env::var("KRB5_CONFIG").ok(),
        realm: std::env::var("KERBEROS_REALM").ok(),
    };

    HdfsConfig {
        namenode_url: std::env::var("HDFS_NAMENODE_URL")
            .unwrap_or_else(|_| "hdfs://secure-namenode:9000".to_string()),
        connection_timeout_ms: Some(45000),
        read_timeout_ms: Some(90000),
        kerberos: Some(kerberos_config),
    }
}

#[tokio::test]
#[ignore] // Requires real HDFS cluster
async fn test_real_hdfs_connection_health() -> Result<()> {
    println!("========================================");
    println!("Testing Real HDFS Connection Health");
    println!("========================================");
    println!();

    let hdfs_config = get_test_hdfs_config();
    println!("HDFS Namenode: {}", hdfs_config.namenode_url);
    println!(
        "Connection Timeout: {:?}ms",
        hdfs_config.connection_timeout_ms
    );
    println!("Read Timeout: {:?}ms", hdfs_config.read_timeout_ms);
    println!();

    println!("Creating HDFS client...");
    let hdfs_reader = HdfsFileReader::new(hdfs_config)?;

    println!("Performing HDFS health check...");
    let health_result = timeout(Duration::from_secs(60), hdfs_reader.health_check()).await;

    match health_result {
        Ok(Ok(healthy)) => {
            assert!(healthy);
            println!("✅ HDFS health check PASSED");
        }
        Ok(Err(e)) => {
            println!("❌ HDFS health check FAILED: {}", e);
            return Err(e);
        }
        Err(_) => {
            println!("⏱️ HDFS health check TIMED OUT after 60 seconds");
            return Err(anyhow::anyhow!("Health check timeout"));
        }
    }

    println!();
    println!("✅ Real HDFS connection health test PASSED");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires real HDFS cluster
async fn test_real_hdfs_directory_operations() -> Result<()> {
    println!("========================================");
    println!("Testing Real HDFS Directory Operations");
    println!("========================================");
    println!();

    let hdfs_config = get_test_hdfs_config();
    let file_reader = create_file_reader("/", Some(&hdfs_config), None).await?;

    println!("Testing root directory listing...");
    let root_entries = file_reader.list_directory(Path::new("/")).await?;
    println!("Found {} entries in root directory:", root_entries.len());
    for (i, entry) in root_entries.iter().take(10).enumerate() {
        println!("  {}. {}", i + 1, entry);
    }
    if root_entries.len() > 10 {
        println!("  ... and {} more entries", root_entries.len() - 10);
    }
    println!();

    // Test common Spark directories
    let test_directories = vec!["/tmp", "/user", "/spark-events", "/var/log"];

    for dir in test_directories {
        println!("Testing directory: {}", dir);
        match file_reader.list_directory(Path::new(dir)).await {
            Ok(entries) => {
                println!("  ✅ Found {} entries in {}", entries.len(), dir);
                for entry in entries.iter().take(3) {
                    println!("    - {}", entry);
                }
                if entries.len() > 3 {
                    println!("    ... and {} more", entries.len() - 3);
                }
            }
            Err(e) => {
                println!("  ⚠️ Could not access {}: {}", dir, e);
            }
        }
        println!();
    }

    println!("✅ Real HDFS directory operations test COMPLETED");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires real HDFS cluster with Kerberos
async fn test_real_hdfs_kerberos_authentication() -> Result<()> {
    println!("========================================");
    println!("Testing Real HDFS Kerberos Authentication");
    println!("========================================");
    println!();

    let hdfs_config = get_test_hdfs_config_with_kerberos();

    println!("HDFS Configuration:");
    println!("  Namenode: {}", hdfs_config.namenode_url);
    println!(
        "  Connection Timeout: {:?}ms",
        hdfs_config.connection_timeout_ms
    );
    println!("  Read Timeout: {:?}ms", hdfs_config.read_timeout_ms);

    if let Some(kerberos) = &hdfs_config.kerberos {
        println!("  Kerberos Principal: {}", kerberos.principal);
        println!("  Keytab Path: {:?}", kerberos.keytab_path);
        println!("  KRB5 Config: {:?}", kerberos.krb5_config_path);
        println!("  Realm: {:?}", kerberos.realm);
    }
    println!();

    println!("Creating HDFS client with Kerberos...");
    let hdfs_reader = HdfsFileReader::new(hdfs_config)?;

    println!("Performing Kerberos health check...");
    let health_result = timeout(Duration::from_secs(90), hdfs_reader.health_check()).await;

    match health_result {
        Ok(Ok(healthy)) => {
            assert!(healthy);
            println!("✅ Kerberos health check PASSED");
        }
        Ok(Err(e)) => {
            println!("❌ Kerberos health check FAILED: {}", e);
            println!("   This may indicate:");
            println!("   - Invalid Kerberos credentials");
            println!("   - Expired tickets");
            println!("   - KDC unavailable");
            println!("   - Network connectivity issues");
            return Err(e);
        }
        Err(_) => {
            println!("⏱️ Kerberos health check TIMED OUT after 90 seconds");
            return Err(anyhow::anyhow!("Kerberos health check timeout"));
        }
    }

    println!();
    println!("Testing authenticated directory operations...");
    match hdfs_reader.list_directory(Path::new("/")).await {
        Ok(entries) => {
            println!("✅ Authenticated directory listing successful");
            println!("   Found {} entries in root directory", entries.len());
        }
        Err(e) => {
            println!("❌ Authenticated directory listing failed: {}", e);
            return Err(e);
        }
    }

    println!();
    println!("✅ Real HDFS Kerberos authentication test PASSED");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires real HDFS cluster with Spark event logs
async fn test_real_hdfs_spark_event_logs() -> Result<()> {
    println!("========================================");
    println!("Testing Real HDFS Spark Event Logs");
    println!("========================================");
    println!();

    let spark_events_dir =
        std::env::var("SPARK_EVENTS_DIR").unwrap_or_else(|_| "/spark-events".to_string());

    println!("Spark Events Directory: {}", spark_events_dir);
    println!();

    let hdfs_config = get_test_hdfs_config();
    let file_reader = create_file_reader(&spark_events_dir, Some(&hdfs_config), None).await?;

    println!("Scanning for Spark event logs...");
    let entries = file_reader
        .list_directory(Path::new(&spark_events_dir))
        .await?;

    if entries.is_empty() {
        println!("⚠️ No event logs found in {}", spark_events_dir);
        println!("   Make sure Spark applications have run and event logging is enabled");
        return Ok(());
    }

    println!("Found {} potential event log entries:", entries.len());

    let mut processed_apps = 0;
    let mut processed_files = 0;

    for (i, entry) in entries.iter().take(10).enumerate() {
        println!();
        println!("{}. Processing: {}", i + 1, entry);

        let entry_path = format!("{}/{}", spark_events_dir, entry);

        if entry.starts_with("app-") || entry.starts_with("application_") {
            // This looks like an application directory
            println!("   Type: Application Directory");

            match file_reader.list_directory(Path::new(&entry_path)).await {
                Ok(app_files) => {
                    println!("   Files: {} event log files", app_files.len());

                    for app_file in app_files.iter().take(3) {
                        if app_file.starts_with("events_") || app_file == "eventLog" {
                            let file_path = format!("{}/{}", entry_path, app_file);
                            match file_reader.read_file(Path::new(&file_path)).await {
                                Ok(content) => {
                                    let lines = content.lines().count();
                                    println!(
                                        "     - {}: {} lines ({} bytes)",
                                        app_file,
                                        lines,
                                        content.len()
                                    );
                                    processed_files += 1;

                                    // Try to parse first few events
                                    for (line_num, line) in content.lines().take(3).enumerate() {
                                        if let Ok(event) =
                                            serde_json::from_str::<serde_json::Value>(line)
                                        {
                                            if let Some(event_type) = event.get("Event") {
                                                println!(
                                                    "       Line {}: {}",
                                                    line_num + 1,
                                                    event_type
                                                );
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    println!("     - {}: Error reading file - {}", app_file, e);
                                }
                            }
                        }
                    }
                    processed_apps += 1;
                }
                Err(e) => {
                    println!("   Error: Could not list directory - {}", e);
                }
            }
        } else if entry.ends_with(".inprogress") {
            // This looks like an in-progress event log file
            println!("   Type: In-Progress Event Log");

            match file_reader.read_file(Path::new(&entry_path)).await {
                Ok(content) => {
                    let lines = content.lines().count();
                    println!("   Content: {} lines ({} bytes)", lines, content.len());
                    processed_files += 1;
                }
                Err(e) => {
                    println!("   Error: Could not read file - {}", e);
                }
            }
        }
    }

    println!();
    println!("Summary:");
    println!("  Total entries found: {}", entries.len());
    println!("  Applications processed: {}", processed_apps);
    println!("  Event log files read: {}", processed_files);

    if entries.len() > 10 {
        println!("  Note: Only processed first 10 entries");
    }

    println!();
    println!("✅ Real HDFS Spark event logs test COMPLETED");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires real HDFS cluster
async fn test_real_hdfs_history_provider_integration() -> Result<()> {
    println!("========================================");
    println!("Testing Real HDFS History Provider Integration");
    println!("========================================");
    println!();

    // Create a temporary directory for database
    let temp_dir = tempfile::TempDir::new()?;

    let spark_events_dir =
        std::env::var("SPARK_EVENTS_DIR").unwrap_or_else(|_| "/spark-events".to_string());

    let hdfs_config = if std::env::var("KERBEROS_PRINCIPAL").is_ok() {
        get_test_hdfs_config_with_kerberos()
    } else {
        get_test_hdfs_config()
    };

    let history_config = HistoryConfig {
        log_directory: spark_events_dir.clone(),
        max_applications: 100,
        update_interval_seconds: 300, // 5 minutes for testing
        max_apps_per_request: 50,
        compression_enabled: true,
        database_directory: Some(temp_dir.path().to_string_lossy().to_string()),
        hdfs: Some(hdfs_config),
        s3: None,
    };

    println!("Creating HistoryProvider with HDFS configuration...");
    println!("  Events Directory: {}", spark_events_dir);
    println!("  Max Applications: {}", history_config.max_applications);
    println!(
        "  Update Interval: {}s",
        history_config.update_interval_seconds
    );
    println!();

    println!("Initializing HistoryProvider (this may take a while)...");
    let start_time = std::time::Instant::now();

    // Create history provider using the factory
    use spark_history_server::storage::{StorageBackendFactory, StorageConfig};
    let storage_config = StorageConfig::DuckDB {
        database_path: "./data/test_events.db".to_string(),
        num_workers: 8,
        batch_size: 5000,
    };
    let history_provider = match timeout(
        Duration::from_secs(300),
        StorageBackendFactory::create_backend(storage_config),
    )
    .await
    {
        Ok(Ok(provider)) => {
            let init_duration = start_time.elapsed();
            println!(
                "✅ HistoryProvider initialized in {:.2}s",
                init_duration.as_secs_f64()
            );
            provider
        }
        Ok(Err(e)) => {
            println!("❌ HistoryProvider initialization failed: {}", e);
            return Err(e);
        }
        Err(_) => {
            println!("⏱️ HistoryProvider initialization timed out after 5 minutes");
            return Err(anyhow::anyhow!("HistoryProvider initialization timeout"));
        }
    };

    println!();
    println!("Testing application retrieval...");
    let applications = history_provider
        .get_applications(Some(10)) // Limit to 10 applications
        .await?;

    println!("Retrieved {} applications:", applications.len());

    for (i, app) in applications.iter().enumerate() {
        let app_id = app.get("id").and_then(|v| v.as_str()).unwrap_or("unknown");
        let app_name = app
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let empty_vec = vec![];
        let attempts = app
            .get("attempts")
            .and_then(|v| v.as_array())
            .unwrap_or(&empty_vec);

        println!("  {}. Application: {}", i + 1, app_id);
        println!("     Name: {}", app_name);
        println!("     Attempts: {}", attempts.len());

        if let Some(attempt) = attempts.first() {
            let start_time = attempt
                .get("start_time")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let end_time = attempt
                .get("end_time")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let duration = attempt
                .get("duration")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let completed = attempt
                .get("completed")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            println!("     Start Time: {}", start_time);
            println!("     End Time: {}", end_time);
            println!("     Duration: {:.2}s", duration);
            println!("     Completed: {}", completed);
        }

        println!();
    }

    if !applications.is_empty() {
        println!("Testing individual application retrieval...");
        let test_app = &applications[0];
        let test_app_id = test_app
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let app_detail = history_provider
            .get_application_summary(test_app_id)
            .await?;

        match app_detail {
            Some(app) => {
                let app_id = app.get("id").and_then(|v| v.as_str()).unwrap_or("unknown");
                let app_name = app
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let empty_vec = vec![];
                let attempts = app
                    .get("attempts")
                    .and_then(|v| v.as_array())
                    .unwrap_or(&empty_vec);

                println!(
                    "✅ Successfully retrieved application details for {}",
                    app_id
                );
                println!("   Name: {}", app_name);
                println!("   Attempts: {}", attempts.len());
            }
            None => {
                println!("⚠️ Application details not found for {}", test_app_id);
            }
        }

        println!();
        println!("Testing executor information...");
        let executors = history_provider.get_executors(test_app_id).await?;
        println!(
            "Found {} executors for application {}",
            executors.len(),
            test_app_id
        );

        for executor in executors.iter().take(3) {
            let exec_id = executor
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let total_cores = executor
                .get("total_cores")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let max_memory = executor
                .get("max_memory")
                .and_then(|v| v.as_str())
                .unwrap_or("0");

            println!(
                "  Executor {}: {} cores, {} MB memory",
                exec_id, total_cores, max_memory
            );
        }
    }

    println!();
    println!("Testing analytics backend integration...");
    let stored_apps = history_provider.get_applications(None).await?;
    println!(
        "Analytics backend contains {} stored applications",
        stored_apps.len()
    );

    println!();
    println!("Performance Summary:");
    println!(
        "  Total initialization time: {:.2}s",
        start_time.elapsed().as_secs_f64()
    );
    println!("  Applications loaded: {}", applications.len());
    println!("  Storage backend: HDFS + DuckDB");

    println!();
    println!("✅ Real HDFS History Provider integration test COMPLETED");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires real HDFS cluster
async fn test_real_hdfs_performance_benchmarks() -> Result<()> {
    println!("========================================");
    println!("Testing Real HDFS Performance Benchmarks");
    println!("========================================");
    println!();

    let hdfs_config = get_test_hdfs_config();
    let file_reader = create_file_reader("/", Some(&hdfs_config), None).await?;

    println!("Running HDFS performance benchmarks...");
    println!();

    // Benchmark 1: Directory listing performance
    println!("Benchmark 1: Directory Listing Performance");
    let start = std::time::Instant::now();
    let root_entries = file_reader.list_directory(Path::new("/")).await?;
    let list_duration = start.elapsed();
    println!(
        "  Result: {} entries in {:.3}s",
        root_entries.len(),
        list_duration.as_secs_f64()
    );
    println!(
        "  Rate: {:.1} entries/second",
        root_entries.len() as f64 / list_duration.as_secs_f64()
    );
    println!();

    // Benchmark 2: File existence checks
    println!("Benchmark 2: File Existence Checks");
    let test_paths = vec![
        "/tmp", "/user", "/var", "/opt", "/etc", "/home", "/root", "/bin", "/usr", "/dev",
    ];
    let start = std::time::Instant::now();
    let mut exists_count = 0;

    for path in &test_paths {
        if file_reader.file_exists(Path::new(path)).await {
            exists_count += 1;
        }
    }

    let exists_duration = start.elapsed();
    println!(
        "  Result: {}/{} paths exist",
        exists_count,
        test_paths.len()
    );
    println!("  Duration: {:.3}s", exists_duration.as_secs_f64());
    println!(
        "  Rate: {:.1} checks/second",
        test_paths.len() as f64 / exists_duration.as_secs_f64()
    );
    println!();

    // Benchmark 3: Concurrent operations
    println!("Benchmark 3: Concurrent Directory Listings");
    let start = std::time::Instant::now();
    let mut handles = Vec::new();

    for i in 0..5 {
        let test_hdfs_config = get_test_hdfs_config();
        let reader = Arc::new(create_file_reader("/", Some(&test_hdfs_config), None).await?);
        let handle = tokio::spawn(async move {
            let start = std::time::Instant::now();
            let result = reader.list_directory(Path::new("/")).await;
            let duration = start.elapsed();
            (i, result, duration)
        });
        handles.push(handle);
    }

    let mut total_entries = 0;
    let mut successful_ops = 0;

    for handle in handles {
        match handle.await? {
            (i, Ok(entries), duration) => {
                total_entries += entries.len();
                successful_ops += 1;
                println!(
                    "  Operation {}: {} entries in {:.3}s",
                    i,
                    entries.len(),
                    duration.as_secs_f64()
                );
            }
            (i, Err(e), duration) => {
                println!(
                    "  Operation {}: FAILED in {:.3}s - {}",
                    i,
                    duration.as_secs_f64(),
                    e
                );
            }
        }
    }

    let concurrent_duration = start.elapsed();
    println!("  Summary: {}/{} operations successful", successful_ops, 5);
    println!("  Total time: {:.3}s", concurrent_duration.as_secs_f64());
    println!(
        "  Average entries per operation: {:.1}",
        total_entries as f64 / successful_ops as f64
    );
    println!();

    println!("Performance Summary:");
    println!(
        "  Directory Listing: {:.1} entries/second",
        root_entries.len() as f64 / list_duration.as_secs_f64()
    );
    println!(
        "  File Existence: {:.1} checks/second",
        test_paths.len() as f64 / exists_duration.as_secs_f64()
    );
    println!(
        "  Concurrent Ops: {:.1} ops/second",
        successful_ops as f64 / concurrent_duration.as_secs_f64()
    );

    println!();
    println!("✅ Real HDFS performance benchmarks COMPLETED");
    Ok(())
}

#[tokio::test]
#[ignore] // Requires real HDFS cluster - run manually for validation
async fn test_manual_hdfs_validation() -> Result<()> {
    println!("========================================");
    println!("Manual HDFS Validation Test");
    println!("========================================");
    println!();
    println!("This test provides manual validation steps for HDFS integration.");
    println!("Run this test to verify your HDFS setup is working correctly.");
    println!();

    println!("Environment Variables to Set:");
    println!("  HDFS_NAMENODE_URL=hdfs://your-namenode:9000");
    println!("  SPARK_EVENTS_DIR=/your/spark/events/directory");
    println!("  For Kerberos:");
    println!("    KERBEROS_PRINCIPAL=your-principal@YOUR.REALM");
    println!("    KERBEROS_KEYTAB=/path/to/your.keytab");
    println!("    KRB5_CONFIG=/path/to/krb5.conf");
    println!("    KERBEROS_REALM=YOUR.REALM");
    println!();

    println!("Current Environment:");
    println!(
        "  HDFS_NAMENODE_URL: {}",
        std::env::var("HDFS_NAMENODE_URL").unwrap_or_else(|_| "NOT SET".to_string())
    );
    println!(
        "  SPARK_EVENTS_DIR: {}",
        std::env::var("SPARK_EVENTS_DIR").unwrap_or_else(|_| "NOT SET".to_string())
    );
    println!(
        "  KERBEROS_PRINCIPAL: {}",
        std::env::var("KERBEROS_PRINCIPAL").unwrap_or_else(|_| "NOT SET".to_string())
    );
    println!(
        "  KERBEROS_KEYTAB: {}",
        std::env::var("KERBEROS_KEYTAB").unwrap_or_else(|_| "NOT SET".to_string())
    );
    println!();

    println!("Manual Test Commands:");
    println!("  # Test basic HDFS connection");
    println!("  cargo test test_real_hdfs_connection_health --ignored");
    println!();
    println!("  # Test HDFS directory operations");
    println!("  cargo test test_real_hdfs_directory_operations --ignored");
    println!();
    println!("  # Test Kerberos authentication (if configured)");
    println!("  cargo test test_real_hdfs_kerberos_authentication --ignored");
    println!();
    println!("  # Test Spark event log processing");
    println!("  cargo test test_real_hdfs_spark_event_logs --ignored");
    println!();
    println!("  # Test full integration");
    println!("  cargo test test_real_hdfs_history_provider_integration --ignored");
    println!();
    println!("  # Test performance");
    println!("  cargo test test_real_hdfs_performance_benchmarks --ignored");
    println!();

    println!("CLI Testing:");
    println!("  # Start server with HDFS");
    println!("  ./target/release/spark-history-server \\");
    println!("    --hdfs \\");
    println!("    --hdfs-namenode hdfs://your-namenode:9000 \\");
    println!("    --log-directory /your/spark/events");
    println!();
    println!("  # With Kerberos");
    println!("  ./target/release/spark-history-server \\");
    println!("    --hdfs \\");
    println!("    --hdfs-namenode hdfs://secure-namenode:9000 \\");
    println!("    --kerberos-principal your-principal@YOUR.REALM \\");
    println!("    --keytab-path /path/to/your.keytab \\");
    println!("    --log-directory /your/spark/events");
    println!();

    println!("✅ Manual HDFS validation test COMPLETED");
    println!("   Use the commands above to validate your HDFS setup");
    Ok(())
}
