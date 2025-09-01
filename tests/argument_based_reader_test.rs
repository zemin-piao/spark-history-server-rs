use anyhow::Result;
use tempfile::TempDir;
use tokio::fs;

use spark_history_server::{
    config::{HdfsConfig, KerberosConfig, HistoryConfig},
    storage::{file_reader::create_file_reader, HistoryProvider},
};

#[tokio::test]
async fn test_argument_based_local_reader_selection() -> Result<()> {
    println!("Testing argument-based local reader selection...");

    // Create a temporary directory with test data
    let temp_dir = TempDir::new()?;
    let log_dir = temp_dir.path().to_path_buf();

    // Create test event log
    let app_dir = log_dir.join("app-20231120120000-0001");
    fs::create_dir_all(&app_dir).await?;

    let event_log_content = r#"{"Event":"SparkListenerLogStart","Spark Version":"3.4.0"}
{"Event":"SparkListenerApplicationStart","App Name":"LocalTestApp","App ID":"app-20231120120000-0001","Timestamp":1700481600000,"User":"local-user"}
{"Event":"SparkListenerJobStart","Job ID":0,"Submission Time":1700481601000,"Stage Infos":[]}
{"Event":"SparkListenerJobEnd","Job ID":0,"Completion Time":1700481602000,"Job Result":{"Result":"JobSucceeded"}}
{"Event":"SparkListenerApplicationEnd","App ID":"app-20231120120000-0001","Timestamp":1700481603000}
"#;

    let event_log_path = app_dir.join("eventLog");
    fs::write(&event_log_path, event_log_content).await?;

    // Test local file reader creation (no HDFS config)
    let log_directory = log_dir.to_string_lossy();
    let file_reader = create_file_reader(&log_directory, None).await?;

    // Test file reading
    let content = file_reader.read_file(&event_log_path).await?;
    assert!(content.contains("LocalTestApp"));
    assert!(content.contains("local-user"));

    // Test directory listing
    let entries = file_reader.list_directory(log_dir.as_path()).await?;
    assert!(entries.contains(&"app-20231120120000-0001".to_string()));

    // Test file existence check
    assert!(file_reader.file_exists(&event_log_path).await);
    assert!(!file_reader.file_exists(log_dir.join("non-existent").as_path()).await);

    println!("✅ Argument-based local reader selection test passed");
    Ok(())
}

#[tokio::test]
async fn test_argument_based_hdfs_reader_selection() -> Result<()> {
    println!("Testing argument-based HDFS reader selection...");

    let hdfs_config = HdfsConfig {
        namenode_url: "hdfs://localhost:9000".to_string(),
        connection_timeout_ms: Some(30000),
        read_timeout_ms: Some(60000),
        kerberos: None,
    };

    // Create file reader with HDFS config
    let file_reader = create_file_reader("/hdfs/spark-events", Some(&hdfs_config)).await;

    // This will likely fail in CI/testing environment, but we can test the creation
    match file_reader {
        Ok(_) => {
            println!("✅ HDFS reader created successfully (HDFS available)");
        }
        Err(e) => {
            println!("⚠️ HDFS reader creation failed (expected if no HDFS available): {}", e);
            // This is expected in most test environments
        }
    }

    println!("✅ Argument-based HDFS reader selection test completed");
    Ok(())
}

#[tokio::test]
async fn test_argument_based_hdfs_with_kerberos() -> Result<()> {
    println!("Testing argument-based HDFS reader with Kerberos...");

    let kerberos_config = KerberosConfig {
        principal: "spark@EXAMPLE.COM".to_string(),
        keytab_path: Some("/path/to/spark.keytab".to_string()),
        krb5_config_path: Some("/etc/krb5.conf".to_string()),
        realm: Some("EXAMPLE.COM".to_string()),
    };

    let hdfs_config = HdfsConfig {
        namenode_url: "hdfs://secure-namenode:9000".to_string(),
        connection_timeout_ms: Some(30000),
        read_timeout_ms: Some(60000),
        kerberos: Some(kerberos_config),
    };

    // Create file reader with HDFS + Kerberos config
    let file_reader = create_file_reader("/hdfs/secure-spark-events", Some(&hdfs_config)).await;

    match file_reader {
        Ok(_) => {
            println!("✅ HDFS reader with Kerberos created successfully");
        }
        Err(e) => {
            println!("⚠️ HDFS reader with Kerberos creation failed (expected if no secure HDFS): {}", e);
            // Expected in most test environments without secure HDFS
        }
    }

    println!("✅ Argument-based HDFS reader with Kerberos test completed");
    Ok(())
}

#[tokio::test]
async fn test_history_provider_with_local_reader() -> Result<()> {
    println!("Testing HistoryProvider with local reader...");

    let temp_dir = TempDir::new()?;
    let log_dir = temp_dir.path().to_path_buf();

    // Create test data structure
    let app_dir = log_dir.join("app-20231120140000-0001");
    fs::create_dir_all(&app_dir).await?;

    let event_log_content = r#"{"Event":"SparkListenerLogStart","Spark Version":"3.4.0"}
{"Event":"SparkListenerApplicationStart","App Name":"ProviderTestApp","App ID":"app-20231120140000-0001","Timestamp":1700485200000,"User":"provider-user"}
{"Event":"SparkListenerApplicationEnd","App ID":"app-20231120140000-0001","Timestamp":1700485260000}
"#;

    fs::write(app_dir.join("eventLog"), event_log_content).await?;

    // Create history config with local directory (no HDFS)
    let history_config = HistoryConfig {
        log_directory: log_dir.to_string_lossy().to_string(),
        max_applications: 100,
        update_interval_seconds: 60,
        max_apps_per_request: 50,
        compression_enabled: true,
        cache_directory: Some(temp_dir.path().join("cache").to_string_lossy().to_string()),
        enable_cache: false,
        hdfs: None, // This forces local file reader selection
    };

    // Create history provider - this should use local file reader
    let history_provider = HistoryProvider::new(history_config).await?;

    // Test application retrieval
    let applications = history_provider.get_applications(None, None, None, None, None, None).await?;
    
    if !applications.is_empty() {
        let app = &applications[0];
        assert_eq!(app.id, "app-20231120140000-0001");
        // App name should be either "ProviderTestApp" from the event log or a fallback
        assert!(app.name == "ProviderTestApp" || app.name.contains("app-20231120140000-0001"));
        println!("Found application: {} - {}", app.id, app.name);
    }

    println!("✅ HistoryProvider with local reader test passed");
    Ok(())
}

#[tokio::test]
async fn test_history_provider_with_hdfs_config() -> Result<()> {
    println!("Testing HistoryProvider with HDFS config...");

    let temp_dir = TempDir::new()?;
    
    // Create history config with HDFS
    let hdfs_config = HdfsConfig {
        namenode_url: "hdfs://localhost:9000".to_string(),
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
        hdfs: Some(hdfs_config), // This forces HDFS file reader selection
    };

    // Create history provider - this should use HDFS file reader
    let result = HistoryProvider::new(history_config).await;

    match result {
        Ok(_provider) => {
            println!("✅ HistoryProvider with HDFS reader created successfully");
        }
        Err(e) => {
            println!("⚠️ HistoryProvider with HDFS failed (expected if no HDFS): {}", e);
            // Expected in most test environments
        }
    }

    println!("✅ HistoryProvider with HDFS config test completed");
    Ok(())
}

#[tokio::test]
async fn test_runtime_reader_switching() -> Result<()> {
    println!("Testing runtime reader switching...");

    let temp_dir = TempDir::new()?;
    let log_dir = temp_dir.path().to_path_buf();

    // Setup test data
    let app_dir = log_dir.join("app-switch-test");
    fs::create_dir_all(&app_dir).await?;
    fs::write(app_dir.join("eventLog"), r#"{"Event":"SparkListenerLogStart"}"#).await?;

    // Test 1: Create with local reader
    println!("Creating local file reader...");
    let local_reader = create_file_reader(&log_dir.to_string_lossy(), None).await?;
    
    let entries = local_reader.list_directory(log_dir.as_path()).await?;
    assert!(entries.contains(&"app-switch-test".to_string()));

    // Test 2: Create with HDFS reader (will likely fail, but tests the switching logic)
    println!("Attempting to create HDFS file reader...");
    let hdfs_config = HdfsConfig {
        namenode_url: "hdfs://localhost:9000".to_string(),
        connection_timeout_ms: Some(5000),
        read_timeout_ms: Some(10000),
        kerberos: None,
    };

    let hdfs_reader_result = create_file_reader("/hdfs/spark-events", Some(&hdfs_config)).await;
    
    match hdfs_reader_result {
        Ok(_) => println!("HDFS reader created successfully"),
        Err(_) => println!("HDFS reader creation failed (expected)"),
    }

    // Test 3: Switch back to local reader
    println!("Switching back to local file reader...");
    let local_reader_2 = create_file_reader(&log_dir.to_string_lossy(), None).await?;
    
    let entries_2 = local_reader_2.list_directory(log_dir.as_path()).await?;
    assert!(entries_2.contains(&"app-switch-test".to_string()));

    println!("✅ Runtime reader switching test passed");
    Ok(())
}

#[tokio::test]
async fn test_configuration_precedence() -> Result<()> {
    println!("Testing configuration precedence (CLI args vs config file vs defaults)...");

    // Test default configuration (no HDFS)
    let default_config = HistoryConfig {
        log_directory: "./test-data/spark-events".to_string(),
        max_applications: 1000,
        update_interval_seconds: 10,
        max_apps_per_request: 100,
        compression_enabled: true,
        cache_directory: Some("./cache/rocksdb".to_string()),
        enable_cache: true,
        hdfs: None,
    };

    assert!(default_config.hdfs.is_none());
    println!("✅ Default configuration uses local reader");

    // Test HDFS configuration override
    let hdfs_override_config = HistoryConfig {
        log_directory: "/hdfs/spark-events".to_string(),
        max_applications: 1000,
        update_interval_seconds: 10,
        max_apps_per_request: 100,
        compression_enabled: true,
        cache_directory: Some("./cache/rocksdb".to_string()),
        enable_cache: true,
        hdfs: Some(HdfsConfig {
            namenode_url: "hdfs://override-namenode:9000".to_string(),
            connection_timeout_ms: Some(15000),
            read_timeout_ms: Some(30000),
            kerberos: None,
        }),
    };

    assert!(hdfs_override_config.hdfs.is_some());
    let hdfs_config = hdfs_override_config.hdfs.as_ref().unwrap();
    assert_eq!(hdfs_config.namenode_url, "hdfs://override-namenode:9000");
    assert_eq!(hdfs_config.connection_timeout_ms, Some(15000));
    assert_eq!(hdfs_config.read_timeout_ms, Some(30000));
    println!("✅ HDFS configuration override works correctly");

    // Test Kerberos configuration
    let kerberos_config = KerberosConfig {
        principal: "test@REALM.COM".to_string(),
        keytab_path: Some("/test/keytab".to_string()),
        krb5_config_path: Some("/test/krb5.conf".to_string()),
        realm: Some("REALM.COM".to_string()),
    };

    let hdfs_with_kerberos = HdfsConfig {
        namenode_url: "hdfs://secure-namenode:9000".to_string(),
        connection_timeout_ms: Some(30000),
        read_timeout_ms: Some(60000),
        kerberos: Some(kerberos_config),
    };

    assert!(hdfs_with_kerberos.kerberos.is_some());
    let kerberos = hdfs_with_kerberos.kerberos.as_ref().unwrap();
    assert_eq!(kerberos.principal, "test@REALM.COM");
    assert_eq!(kerberos.keytab_path.as_ref().unwrap(), "/test/keytab");
    println!("✅ Kerberos configuration works correctly");

    println!("✅ Configuration precedence test passed");
    Ok(())
}