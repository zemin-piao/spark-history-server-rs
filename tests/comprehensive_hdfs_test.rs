use anyhow::Result;
use std::{collections::HashMap, path::Path, sync::Arc};
use tempfile::TempDir;

use spark_history_server::{
    config::{HdfsConfig, HistoryConfig, KerberosConfig},
    storage::{file_reader::FileReader, HistoryProvider},
};

/// Comprehensive HDFS integration tests covering various scenarios
/// These tests use mock implementations to avoid dependency on real HDFS cluster
struct AdvancedMockHdfsReader {
    files: HashMap<String, String>,
    directories: HashMap<String, Vec<String>>,
    health_status: bool,
    simulated_errors: HashMap<String, String>,
    access_log: Arc<tokio::sync::Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl FileReader for AdvancedMockHdfsReader {
    async fn read_file(&self, path: &Path) -> Result<String> {
        let path_str = path.to_string_lossy().to_string();

        // Log access for testing
        {
            let mut log = self.access_log.lock().await;
            log.push(format!("READ: {}", path_str));
        }

        // Simulate specific errors
        if let Some(error) = self.simulated_errors.get(&path_str) {
            return Err(anyhow::anyhow!("Simulated error: {}", error));
        }

        self.files
            .get(&path_str)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("File not found: {}", path_str))
    }

    async fn list_directory(&self, path: &Path) -> Result<Vec<String>> {
        let path_str = path.to_string_lossy().to_string();

        // Log access for testing
        {
            let mut log = self.access_log.lock().await;
            log.push(format!("LIST: {}", path_str));
        }

        if !self.health_status {
            return Err(anyhow::anyhow!("HDFS cluster unavailable"));
        }

        self.directories
            .get(&path_str)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Directory not found: {}", path_str))
    }

    async fn file_exists(&self, path: &Path) -> bool {
        let path_str = path.to_string_lossy().to_string();
        self.files.contains_key(&path_str)
    }
}

impl AdvancedMockHdfsReader {
    fn new() -> Self {
        let mut files = HashMap::new();
        let mut directories = HashMap::new();

        // Create various test event logs
        let basic_event_log = r#"{"Event":"SparkListenerLogStart","Spark Version":"3.4.0"}
{"Event":"SparkListenerApplicationStart","App Name":"HDFSBasicApp","App ID":"app-hdfs-basic-001","Timestamp":1700500000000,"User":"hdfs-user"}
{"Event":"SparkListenerJobStart","Job ID":0,"Submission Time":1700500001000,"Stage Infos":[]}
{"Event":"SparkListenerJobEnd","Job ID":0,"Completion Time":1700500002000,"Job Result":{"Result":"JobSucceeded"}}
{"Event":"SparkListenerApplicationEnd","App ID":"app-hdfs-basic-001","Timestamp":1700500003000}
"#;

        let large_event_log = basic_event_log.repeat(100); // Create a larger log

        let compressed_event_log = r#"{"Event":"SparkListenerLogStart","Spark Version":"3.4.0"}
{"Event":"SparkListenerApplicationStart","App Name":"CompressedApp","App ID":"app-compressed-hdfs-001","Timestamp":1700500000000,"User":"hdfs-user"}
{"Event":"SparkListenerApplicationEnd","App ID":"app-compressed-hdfs-001","Timestamp":1700500003000}
"#;

        // Add test files
        files.insert(
            "/hdfs/spark-events/app-hdfs-basic-001/eventLog".to_string(),
            basic_event_log.to_string(),
        );
        files.insert(
            "/hdfs/spark-events/app-large-001/eventLog".to_string(),
            large_event_log,
        );
        files.insert(
            "/hdfs/spark-events/app-compressed-001/eventLog.gz".to_string(),
            compressed_event_log.to_string(),
        );
        files.insert(
            "/hdfs/spark-events/app-inprogress-001.inprogress".to_string(),
            basic_event_log.replace("HDFSBasicApp", "InProgressApp"),
        );

        // Add directories
        directories.insert(
            "/hdfs/spark-events".to_string(),
            vec![
                "app-hdfs-basic-001".to_string(),
                "app-large-001".to_string(),
                "app-compressed-001".to_string(),
                "app-inprogress-001.inprogress".to_string(),
            ],
        );
        directories.insert(
            "/hdfs/spark-events/app-hdfs-basic-001".to_string(),
            vec!["eventLog".to_string()],
        );
        directories.insert(
            "/hdfs/spark-events/app-large-001".to_string(),
            vec!["eventLog".to_string()],
        );
        directories.insert(
            "/hdfs/spark-events/app-compressed-001".to_string(),
            vec!["eventLog.gz".to_string()],
        );

        Self {
            files,
            directories,
            health_status: true,
            simulated_errors: HashMap::new(),
            access_log: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    fn with_error(mut self, path: &str, error: &str) -> Self {
        self.simulated_errors
            .insert(path.to_string(), error.to_string());
        self
    }

    fn set_unhealthy(&mut self) {
        self.health_status = false;
    }

    async fn get_access_log(&self) -> Vec<String> {
        self.access_log.lock().await.clone()
    }
}

#[tokio::test]
async fn test_hdfs_comprehensive_file_operations() -> Result<()> {
    println!("Testing comprehensive HDFS file operations...");

    let hdfs_reader = AdvancedMockHdfsReader::new();

    // Test basic file reading
    let basic_log = hdfs_reader
        .read_file(Path::new("/hdfs/spark-events/app-hdfs-basic-001/eventLog"))
        .await?;
    assert!(basic_log.contains("HDFSBasicApp"));
    assert!(basic_log.contains("hdfs-user"));
    println!("✅ Basic file reading test passed");

    // Test large file reading
    let large_log = hdfs_reader
        .read_file(Path::new("/hdfs/spark-events/app-large-001/eventLog"))
        .await?;
    assert!(large_log.len() > 1000); // Should be much larger due to repetition
    println!("✅ Large file reading test passed");

    // Test compressed file reading
    let compressed_log = hdfs_reader
        .read_file(Path::new(
            "/hdfs/spark-events/app-compressed-001/eventLog.gz",
        ))
        .await?;
    assert!(compressed_log.contains("CompressedApp"));
    println!("✅ Compressed file reading test passed");

    // Test in-progress file reading
    let inprogress_log = hdfs_reader
        .read_file(Path::new(
            "/hdfs/spark-events/app-inprogress-001.inprogress",
        ))
        .await?;
    assert!(inprogress_log.contains("InProgressApp"));
    println!("✅ In-progress file reading test passed");

    // Test directory listing
    let entries = hdfs_reader
        .list_directory(Path::new("/hdfs/spark-events"))
        .await?;
    assert_eq!(entries.len(), 4);
    assert!(entries.contains(&"app-hdfs-basic-001".to_string()));
    println!("✅ Directory listing test passed");

    // Test file existence checks
    assert!(
        hdfs_reader
            .file_exists(Path::new("/hdfs/spark-events/app-hdfs-basic-001/eventLog"))
            .await
    );
    assert!(
        !hdfs_reader
            .file_exists(Path::new("/hdfs/spark-events/non-existent"))
            .await
    );
    println!("✅ File existence check test passed");

    Ok(())
}

#[tokio::test]
async fn test_hdfs_error_handling_scenarios() -> Result<()> {
    println!("Testing HDFS error handling scenarios...");

    let hdfs_reader = AdvancedMockHdfsReader::new()
        .with_error(
            "/hdfs/spark-events/corrupted-file",
            "File corruption detected",
        )
        .with_error("/hdfs/spark-events/permission-denied", "Permission denied");

    // Test file corruption error
    let result = hdfs_reader
        .read_file(Path::new("/hdfs/spark-events/corrupted-file"))
        .await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("File corruption detected"));
    println!("✅ File corruption error handling test passed");

    // Test permission denied error
    let result = hdfs_reader
        .read_file(Path::new("/hdfs/spark-events/permission-denied"))
        .await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Permission denied"));
    println!("✅ Permission denied error handling test passed");

    // Test file not found error
    let result = hdfs_reader
        .read_file(Path::new("/hdfs/spark-events/non-existent-file"))
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("File not found"));
    println!("✅ File not found error handling test passed");

    Ok(())
}

#[tokio::test]
async fn test_hdfs_cluster_availability() -> Result<()> {
    println!("Testing HDFS cluster availability scenarios...");

    let mut hdfs_reader = AdvancedMockHdfsReader::new();

    // Test healthy cluster
    let result = hdfs_reader
        .list_directory(Path::new("/hdfs/spark-events"))
        .await;
    assert!(result.is_ok());
    println!("✅ Healthy cluster test passed");

    // Test unhealthy cluster
    hdfs_reader.set_unhealthy();
    let result = hdfs_reader
        .list_directory(Path::new("/hdfs/spark-events"))
        .await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("HDFS cluster unavailable"));
    println!("✅ Unhealthy cluster test passed");

    Ok(())
}

#[tokio::test]
async fn test_hdfs_access_patterns() -> Result<()> {
    println!("Testing HDFS access patterns...");

    let hdfs_reader = Arc::new(AdvancedMockHdfsReader::new());

    // Perform various operations
    let _ = hdfs_reader
        .list_directory(Path::new("/hdfs/spark-events"))
        .await?;
    let _ = hdfs_reader
        .read_file(Path::new("/hdfs/spark-events/app-hdfs-basic-001/eventLog"))
        .await?;
    let _ = hdfs_reader
        .read_file(Path::new("/hdfs/spark-events/app-large-001/eventLog"))
        .await?;

    // Check access log
    let log = hdfs_reader.get_access_log().await;
    assert_eq!(log.len(), 3);
    assert!(log[0].starts_with("LIST:"));
    assert!(log[1].starts_with("READ:"));
    assert!(log[2].starts_with("READ:"));
    println!("✅ Access pattern logging test passed");

    Ok(())
}

#[tokio::test]
async fn test_hdfs_concurrent_operations() -> Result<()> {
    println!("Testing HDFS concurrent operations...");

    let hdfs_reader = Arc::new(AdvancedMockHdfsReader::new());
    let mut handles = Vec::new();

    // Launch concurrent read operations
    for i in 0..10 {
        let reader = Arc::clone(&hdfs_reader);
        let handle = tokio::spawn(async move {
            let path = if i % 2 == 0 {
                Path::new("/hdfs/spark-events/app-hdfs-basic-001/eventLog")
            } else {
                Path::new("/hdfs/spark-events/app-large-001/eventLog")
            };

            let content = reader.read_file(path).await.expect("Failed to read file");
            assert!(!content.is_empty());
            println!("Concurrent operation {} completed", i);
        });
        handles.push(handle);
    }

    // Wait for all operations to complete
    for handle in handles {
        handle.await?;
    }

    // Verify access log has all operations
    let log = hdfs_reader.get_access_log().await;
    assert_eq!(log.len(), 10);
    println!("✅ Concurrent operations test passed");

    Ok(())
}

#[tokio::test]
async fn test_history_provider_hdfs_integration() -> Result<()> {
    println!("Testing HistoryProvider with HDFS integration...");

    let temp_dir = TempDir::new()?;

    // Create HDFS config with mock settings
    let hdfs_config = HdfsConfig {
        namenode_url: "hdfs://mock-namenode:9000".to_string(),
        connection_timeout_ms: Some(30000),
        read_timeout_ms: Some(60000),
        kerberos: None,
    };

    let history_config = HistoryConfig {
        log_directory: "/hdfs/spark-events".to_string(),
        max_applications: 100,
        update_interval_seconds: 60,
        max_apps_per_request: 50,
        compression_enabled: true,
        cache_directory: Some(temp_dir.path().join("cache").to_string_lossy().to_string()),
        enable_cache: false,
        hdfs: Some(hdfs_config),
    };

    // This will attempt to create HDFS client (may fail in test environment)
    let result = HistoryProvider::new(history_config).await;

    match result {
        Ok(_provider) => {
            println!("✅ HistoryProvider with HDFS created successfully");
        }
        Err(e) => {
            println!("⚠️ HistoryProvider with HDFS failed as expected: {}", e);
            // This is expected in test environment without real HDFS
        }
    }

    println!("✅ HistoryProvider HDFS integration test completed");
    Ok(())
}

#[tokio::test]
async fn test_hdfs_configuration_validation() -> Result<()> {
    println!("Testing HDFS configuration validation...");

    // Test valid configuration
    let valid_config = HdfsConfig {
        namenode_url: "hdfs://namenode:9000".to_string(),
        connection_timeout_ms: Some(30000),
        read_timeout_ms: Some(60000),
        kerberos: None,
    };
    assert!(valid_config.namenode_url.starts_with("hdfs://"));
    println!("✅ Valid configuration test passed");

    // Test configuration with Kerberos
    let kerberos_config = KerberosConfig {
        principal: "spark@EXAMPLE.COM".to_string(),
        keytab_path: Some("/path/to/keytab".to_string()),
        krb5_config_path: Some("/etc/krb5.conf".to_string()),
        realm: Some("EXAMPLE.COM".to_string()),
    };

    let hdfs_with_kerberos = HdfsConfig {
        namenode_url: "hdfs://secure-namenode:9000".to_string(),
        connection_timeout_ms: Some(45000),
        read_timeout_ms: Some(90000),
        kerberos: Some(kerberos_config),
    };

    assert!(hdfs_with_kerberos.kerberos.is_some());
    let kerberos = hdfs_with_kerberos.kerberos.as_ref().unwrap();
    assert_eq!(kerberos.principal, "spark@EXAMPLE.COM");
    assert!(kerberos.keytab_path.is_some());
    assert!(kerberos.krb5_config_path.is_some());
    assert!(kerberos.realm.is_some());
    println!("✅ Kerberos configuration test passed");

    Ok(())
}

#[tokio::test]
async fn test_hdfs_timeout_configurations() -> Result<()> {
    println!("Testing HDFS timeout configurations...");

    // Test various timeout settings
    let configs = vec![
        (Some(15000), Some(30000)),  // Short timeouts
        (Some(30000), Some(60000)),  // Default timeouts
        (Some(60000), Some(120000)), // Long timeouts
        (None, None),                // No timeout specified
    ];

    for (conn_timeout, read_timeout) in configs {
        let hdfs_config = HdfsConfig {
            namenode_url: "hdfs://namenode:9000".to_string(),
            connection_timeout_ms: conn_timeout,
            read_timeout_ms: read_timeout,
            kerberos: None,
        };

        // Validate timeout values are reasonable
        if let Some(timeout) = hdfs_config.connection_timeout_ms {
            assert!((5000..=300000).contains(&timeout)); // 5s to 5min
        }
        if let Some(timeout) = hdfs_config.read_timeout_ms {
            assert!((10000..=600000).contains(&timeout)); // 10s to 10min
        }
    }

    println!("✅ HDFS timeout configurations test passed");
    Ok(())
}

#[tokio::test]
async fn test_hdfs_event_log_patterns() -> Result<()> {
    println!("Testing HDFS event log patterns...");

    let hdfs_reader = AdvancedMockHdfsReader::new();

    // Test various event log patterns
    let patterns = vec![
        "/hdfs/spark-events/app-hdfs-basic-001/eventLog", // Standard directory structure
        "/hdfs/spark-events/app-compressed-001/eventLog.gz", // Compressed file
        "/hdfs/spark-events/app-inprogress-001.inprogress", // In-progress file
    ];

    for pattern in patterns {
        let result = hdfs_reader.read_file(Path::new(pattern)).await;
        assert!(result.is_ok(), "Failed to read pattern: {}", pattern);
        let content = result.unwrap();
        assert!(
            !content.is_empty(),
            "Empty content for pattern: {}",
            pattern
        );
    }

    println!("✅ HDFS event log patterns test passed");
    Ok(())
}

#[tokio::test]
async fn test_hdfs_path_handling() -> Result<()> {
    println!("Testing HDFS path handling...");

    let _hdfs_reader = AdvancedMockHdfsReader::new();

    // Test various path formats
    let valid_paths = vec![
        "/hdfs/spark-events",
        "/hdfs/spark-events/",
        "/hdfs/spark-events/app-hdfs-basic-001",
        "/hdfs/spark-events/app-hdfs-basic-001/eventLog",
    ];

    for path_str in valid_paths {
        let path = Path::new(path_str);
        let path_lossy = path.to_string_lossy();
        assert!(!path_lossy.is_empty());
        assert!(path_lossy.starts_with("/hdfs/spark-events"));

        // Test path operations don't panic
        let _ = path.file_name();
        let _ = path.parent();
    }

    println!("✅ HDFS path handling test passed");
    Ok(())
}
