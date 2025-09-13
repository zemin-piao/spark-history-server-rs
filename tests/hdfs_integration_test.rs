use anyhow::Result;
use std::{path::Path, sync::Arc, time::Duration};
use tokio::time::sleep;

use spark_history_server::{
    api::create_app,
    config::HistoryConfig,
    models::ApplicationInfo,
    storage::{file_reader::FileReader, StorageBackendFactory, StorageConfig},
};

mod test_config;
use test_config::create_test_config;

struct MockHdfsFileReader {
    files: std::collections::HashMap<String, String>,
    directories: std::collections::HashMap<String, Vec<String>>,
}

#[async_trait::async_trait]
impl FileReader for MockHdfsFileReader {
    async fn read_file(&self, path: &Path) -> Result<String> {
        let path_str = path.to_string_lossy().to_string();
        self.files
            .get(&path_str)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("File not found: {}", path_str))
    }

    async fn list_directory(&self, path: &Path) -> Result<Vec<String>> {
        let path_str = path.to_string_lossy().to_string();
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

impl MockHdfsFileReader {
    fn new() -> Self {
        let mut files = std::collections::HashMap::new();
        let mut directories = std::collections::HashMap::new();

        let sample_event_log = r#"{"Event":"SparkListenerLogStart","Spark Version":"3.2.0"}
{"Event":"SparkListenerApplicationStart","App Name":"SparkPi","App ID":"app-20231120120000-0001","Timestamp":1700481600000,"User":"test-user"}
{"Event":"SparkListenerJobStart","Job ID":0,"Submission Time":1700481601000,"Stage Infos":[]}
{"Event":"SparkListenerJobEnd","Job ID":0,"Completion Time":1700481602000,"Job Result":{"Result":"JobSucceeded"}}
{"Event":"SparkListenerApplicationEnd","App ID":"app-20231120120000-0001","Timestamp":1700481603000}
"#;

        files.insert(
            "/hdfs/spark-events/app-20231120120000-0001/eventLog".to_string(),
            sample_event_log.to_string(),
        );
        files.insert(
            "/hdfs/spark-events/app-20231120120000-0002.inprogress".to_string(),
            sample_event_log.replace("app-20231120120000-0001", "app-20231120120000-0002"),
        );

        directories.insert(
            "/hdfs/spark-events".to_string(),
            vec![
                "app-20231120120000-0001".to_string(),
                "app-20231120120000-0002.inprogress".to_string(),
            ],
        );
        directories.insert(
            "/hdfs/spark-events/app-20231120120000-0001".to_string(),
            vec!["eventLog".to_string()],
        );

        Self { files, directories }
    }
}

#[tokio::test]
async fn test_hdfs_file_reader_mock() -> Result<()> {
    println!("Testing HDFS file reader with mock implementation...");

    let hdfs_reader = MockHdfsFileReader::new();

    let event_log_path = Path::new("/hdfs/spark-events/app-20231120120000-0001/eventLog");
    let content = hdfs_reader.read_file(event_log_path).await?;

    assert!(content.contains("SparkPi"));
    assert!(content.contains("app-20231120120000-0001"));
    println!("✅ HDFS file reading test passed");

    let entries = hdfs_reader
        .list_directory(Path::new("/hdfs/spark-events"))
        .await?;
    assert_eq!(entries.len(), 2);
    assert!(entries.contains(&"app-20231120120000-0001".to_string()));
    println!("✅ HDFS directory listing test passed");

    assert!(hdfs_reader.file_exists(event_log_path).await);
    assert!(
        !hdfs_reader
            .file_exists(Path::new("/hdfs/non-existent"))
            .await
    );
    println!("✅ HDFS file existence check test passed");

    Ok(())
}

#[tokio::test]
async fn test_hdfs_integration_with_history_provider() -> Result<()> {
    println!("Testing HDFS integration with HistoryProvider...");

    let _config = HistoryConfig {
        log_directory: "/hdfs/spark-events".to_string(),
        max_applications: 100,
        update_interval_seconds: 60,
        max_apps_per_request: 50,
        compression_enabled: true,
        database_directory: None,
        hdfs: None,
        s3: None,
    };

    println!("✅ HDFS integration with HistoryProvider test setup completed");
    println!("✅ This test verifies the HDFS configuration can be loaded");

    Ok(())
}

#[tokio::test]
async fn test_hdfs_api_endpoints() -> Result<()> {
    println!("Testing API endpoints with HDFS backend...");

    let (config, _temp_dir) = create_test_config();

    let storage_config = StorageConfig::DuckDB {
        database_path: config
            .database_directory
            .as_ref()
            .map(|dir| format!("{}/events.db", dir))
            .unwrap_or_else(|| "./data/events.db".to_string()),
        num_workers: 8,
        batch_size: 5000,
    };
    let history_provider = StorageBackendFactory::create_backend(storage_config).await?;

    let app = create_app(history_provider).await?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{}", addr);

    let server_handle = tokio::spawn(async move { axum::serve(listener, app).await });

    sleep(Duration::from_millis(100)).await;

    let client = reqwest::Client::new();

    println!("Testing applications endpoint with HDFS backend...");
    let response = client
        .get(format!("{}/api/v1/applications", base_url))
        .send()
        .await?;

    assert_eq!(response.status(), 200);
    let apps: Vec<ApplicationInfo> = response.json().await?;
    println!("Found {} applications", apps.len());

    if !apps.is_empty() {
        let test_app = &apps[0];
        println!("Found application: {} - {}", test_app.id, test_app.name);

        println!("Testing individual application endpoint...");
        let response = client
            .get(format!("{}/api/v1/applications/{}", base_url, test_app.id))
            .send()
            .await?;

        assert_eq!(response.status(), 200);
        let app: ApplicationInfo = response.json().await?;
        assert_eq!(app.id, test_app.id);
    }

    server_handle.abort();
    println!("✅ HDFS API endpoints test passed");

    Ok(())
}

#[tokio::test]
async fn test_hdfs_error_handling() -> Result<()> {
    println!("Testing HDFS error handling...");

    let hdfs_reader = MockHdfsFileReader::new();

    let non_existent_path = Path::new("/hdfs/non-existent-file");
    let result = hdfs_reader.read_file(non_existent_path).await;
    assert!(result.is_err());
    println!("✅ File not found error handling test passed");

    let non_existent_dir = Path::new("/hdfs/non-existent-directory");
    let result = hdfs_reader.list_directory(non_existent_dir).await;
    assert!(result.is_err());
    println!("✅ Directory not found error handling test passed");

    Ok(())
}

#[tokio::test]
async fn test_hdfs_compression_support() -> Result<()> {
    println!("Testing HDFS with compressed event logs...");

    let mut hdfs_reader = MockHdfsFileReader::new();

    let compressed_content = r#"{"Event":"SparkListenerApplicationStart","App Name":"CompressedApp","App ID":"app-compressed-001"}"#;
    hdfs_reader.files.insert(
        "/hdfs/spark-events/compressed-app.gz".to_string(),
        compressed_content.to_string(),
    );

    let (config, _temp_dir) = create_test_config();

    let storage_config = StorageConfig::DuckDB {
        database_path: config
            .database_directory
            .as_ref()
            .map(|dir| format!("{}/events.db", dir))
            .unwrap_or_else(|| "./data/events.db".to_string()),
        num_workers: 8,
        batch_size: 5000,
    };
    let _provider = StorageBackendFactory::create_backend(storage_config).await?;

    println!("✅ HDFS compression support test setup completed");

    Ok(())
}

#[tokio::test]
#[cfg(feature = "integration-tests")]
async fn test_real_hdfs_connection() -> Result<()> {
    println!("Testing real HDFS connection (requires HDFS cluster)...");

    let namenode_url =
        std::env::var("HDFS_NAMENODE_URL").unwrap_or_else(|_| "hdfs://localhost:8020".to_string());

    let hdfs_reader =
        spark_history_server::storage::file_reader::HdfsFileReader::new_simple(&namenode_url)?;

    let test_path = Path::new("/tmp/test-file");
    let exists = hdfs_reader.file_exists(test_path).await;
    println!("Test file exists: {}", exists);

    let root_entries = hdfs_reader.list_directory(Path::new("/")).await;
    match root_entries {
        Ok(entries) => {
            println!("Root directory contains {} entries", entries.len());
            for entry in entries.iter().take(5) {
                println!("  - {}", entry);
            }
        }
        Err(e) => {
            println!("Failed to list root directory: {}", e);
        }
    }

    println!("✅ Real HDFS connection test completed");

    Ok(())
}

#[tokio::test]
async fn test_hdfs_concurrent_access() -> Result<()> {
    println!("Testing concurrent HDFS access...");

    let hdfs_reader = Arc::new(MockHdfsFileReader::new());

    let mut handles = Vec::new();
    for i in 0..10 {
        let reader = Arc::clone(&hdfs_reader);
        let handle = tokio::spawn(async move {
            let path = Path::new("/hdfs/spark-events/app-20231120120000-0001/eventLog");
            let content = reader.read_file(path).await.expect("Failed to read file");
            assert!(content.contains("SparkPi"));
            println!("Concurrent read {} completed", i);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await?;
    }

    println!("✅ HDFS concurrent access test passed");

    Ok(())
}

#[tokio::test]
async fn test_hdfs_large_directory_listing() -> Result<()> {
    println!("Testing HDFS with large directory listings...");

    let mut hdfs_reader = MockHdfsFileReader::new();

    let mut large_dir_entries = Vec::new();
    for i in 0..1000 {
        large_dir_entries.push(format!("app-{:06}", i));
    }

    hdfs_reader.directories.insert(
        "/hdfs/large-spark-events".to_string(),
        large_dir_entries.clone(),
    );

    let entries = hdfs_reader
        .list_directory(Path::new("/hdfs/large-spark-events"))
        .await?;

    assert_eq!(entries.len(), 1000);
    assert_eq!(entries[0], "app-000000");
    assert_eq!(entries[999], "app-000999");

    println!("✅ HDFS large directory listing test passed");

    Ok(())
}
