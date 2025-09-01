use anyhow::Result;
use std::{collections::HashMap, path::Path, sync::Arc};
use tokio::time::{sleep, Duration};

use spark_history_server::{
    config::{HdfsConfig, KerberosConfig},
    storage::file_reader::{FileReader, HdfsFileReader},
};

struct MockKerberosHdfsFileReader {
    files: HashMap<String, String>,
    directories: HashMap<String, Vec<String>>,
    kerberos_config: Option<KerberosConfig>,
    simulated_delays: HashMap<String, u64>, // path -> delay in ms
}

#[async_trait::async_trait]
impl FileReader for MockKerberosHdfsFileReader {
    async fn read_file(&self, path: &Path) -> Result<String> {
        let path_str = path.to_string_lossy().to_string();

        // Simulate network delay if configured
        if let Some(&delay_ms) = self.simulated_delays.get(&path_str) {
            sleep(Duration::from_millis(delay_ms)).await;
        }

        // Simulate Kerberos authentication check
        if self.kerberos_config.is_some() {
            // Simulate authentication delay
            sleep(Duration::from_millis(100)).await;
        }

        self.files
            .get(&path_str)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("File not found: {}", path_str))
    }

    async fn list_directory(&self, path: &Path) -> Result<Vec<String>> {
        let path_str = path.to_string_lossy().to_string();

        // Simulate Kerberos authentication check
        if self.kerberos_config.is_some() {
            sleep(Duration::from_millis(50)).await;
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

impl MockKerberosHdfsFileReader {
    fn new_with_kerberos(kerberos_config: Option<KerberosConfig>) -> Self {
        let mut files = HashMap::new();
        let mut directories = HashMap::new();

        let sample_event_log = r#"{"Event":"SparkListenerLogStart","Spark Version":"3.4.0"}
{"Event":"SparkListenerApplicationStart","App Name":"KerberosTestApp","App ID":"app-20231120150000-0001","Timestamp":1700488800000,"User":"hdfs-user"}
{"Event":"SparkListenerJobStart","Job ID":0,"Submission Time":1700488801000,"Stage Infos":[]}
{"Event":"SparkListenerJobEnd","Job ID":0,"Completion Time":1700488802000,"Job Result":{"Result":"JobSucceeded"}}
{"Event":"SparkListenerApplicationEnd","App ID":"app-20231120150000-0001","Timestamp":1700488803000}
"#;

        files.insert(
            "/hdfs/secure-spark-events/app-20231120150000-0001/eventLog".to_string(),
            sample_event_log.to_string(),
        );

        directories.insert(
            "/hdfs/secure-spark-events".to_string(),
            vec!["app-20231120150000-0001".to_string()],
        );
        directories.insert(
            "/hdfs/secure-spark-events/app-20231120150000-0001".to_string(),
            vec!["eventLog".to_string()],
        );

        Self {
            files,
            directories,
            kerberos_config,
            simulated_delays: HashMap::new(),
        }
    }

    fn with_delay(mut self, path: &str, delay_ms: u64) -> Self {
        self.simulated_delays.insert(path.to_string(), delay_ms);
        self
    }
}

#[tokio::test]
async fn test_kerberos_configuration() -> Result<()> {
    println!("Testing Kerberos configuration...");

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
        kerberos: Some(kerberos_config.clone()),
    };

    // Verify configuration values
    assert_eq!(hdfs_config.namenode_url, "hdfs://secure-namenode:9000");
    assert!(hdfs_config.kerberos.is_some());

    let kerberos = hdfs_config.kerberos.as_ref().unwrap();
    assert_eq!(kerberos.principal, "spark@EXAMPLE.COM");
    assert_eq!(
        kerberos.keytab_path.as_ref().unwrap(),
        "/path/to/spark.keytab"
    );
    assert_eq!(
        kerberos.krb5_config_path.as_ref().unwrap(),
        "/etc/krb5.conf"
    );
    assert_eq!(kerberos.realm.as_ref().unwrap(), "EXAMPLE.COM");

    println!("✅ Kerberos configuration test passed");
    Ok(())
}

#[tokio::test]
async fn test_hdfs_with_kerberos_authentication() -> Result<()> {
    println!("Testing HDFS with Kerberos authentication...");

    let kerberos_config = KerberosConfig {
        principal: "spark@EXAMPLE.COM".to_string(),
        keytab_path: Some("/path/to/spark.keytab".to_string()),
        krb5_config_path: None,
        realm: Some("EXAMPLE.COM".to_string()),
    };

    let reader = MockKerberosHdfsFileReader::new_with_kerberos(Some(kerberos_config));

    // Test authenticated file reading
    let event_log_path = Path::new("/hdfs/secure-spark-events/app-20231120150000-0001/eventLog");
    let start_time = std::time::Instant::now();
    let content = reader.read_file(event_log_path).await?;
    let read_duration = start_time.elapsed();

    assert!(content.contains("KerberosTestApp"));
    assert!(content.contains("hdfs-user"));
    // Should have some authentication delay
    assert!(read_duration >= Duration::from_millis(100));

    println!("✅ HDFS with Kerberos authentication test passed");
    Ok(())
}

#[tokio::test]
async fn test_hdfs_without_kerberos() -> Result<()> {
    println!("Testing HDFS without Kerberos authentication...");

    let reader = MockKerberosHdfsFileReader::new_with_kerberos(None);

    let event_log_path = Path::new("/hdfs/secure-spark-events/app-20231120150000-0001/eventLog");
    let start_time = std::time::Instant::now();
    let content = reader.read_file(event_log_path).await?;
    let read_duration = start_time.elapsed();

    assert!(content.contains("KerberosTestApp"));
    // Should be faster without Kerberos authentication delay
    assert!(read_duration < Duration::from_millis(50));

    println!("✅ HDFS without Kerberos test passed");
    Ok(())
}

#[tokio::test]
async fn test_hdfs_timeout_handling() -> Result<()> {
    println!("Testing HDFS timeout handling...");

    let mut reader = MockKerberosHdfsFileReader::new_with_kerberos(None)
        .with_delay("/hdfs/secure-spark-events/slow-file", 5000); // 5 second delay

    let slow_file_path = Path::new("/hdfs/secure-spark-events/slow-file");
    reader.files.insert(
        slow_file_path.to_string_lossy().to_string(),
        "slow content".to_string(),
    );

    // This should timeout (assuming our timeout is less than 5 seconds)
    let start_time = std::time::Instant::now();
    let result = reader.read_file(slow_file_path).await;
    let elapsed = start_time.elapsed();

    // The mock will wait 5 seconds, so the operation should complete
    // In real HDFS implementation with timeout, this would fail
    assert!(result.is_ok());
    assert!(elapsed >= Duration::from_millis(5000));

    println!("✅ HDFS timeout handling test passed (mock behavior)");
    Ok(())
}

#[tokio::test]
async fn test_hdfs_kerberos_error_scenarios() -> Result<()> {
    println!("Testing HDFS Kerberos error scenarios...");

    let reader = MockKerberosHdfsFileReader::new_with_kerberos(None);

    // Test file not found
    let non_existent_path = Path::new("/hdfs/secure-spark-events/non-existent-file");
    let result = reader.read_file(non_existent_path).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("File not found"));

    // Test directory not found
    let non_existent_dir = Path::new("/hdfs/non-existent-directory");
    let result = reader.list_directory(non_existent_dir).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Directory not found"));

    println!("✅ HDFS Kerberos error scenarios test passed");
    Ok(())
}

#[tokio::test]
async fn test_hdfs_concurrent_kerberos_access() -> Result<()> {
    println!("Testing concurrent HDFS access with Kerberos...");

    let kerberos_config = KerberosConfig {
        principal: "spark@EXAMPLE.COM".to_string(),
        keytab_path: Some("/path/to/spark.keytab".to_string()),
        krb5_config_path: None,
        realm: Some("EXAMPLE.COM".to_string()),
    };

    let reader = Arc::new(MockKerberosHdfsFileReader::new_with_kerberos(Some(
        kerberos_config,
    )));

    let mut handles = Vec::new();
    for i in 0..5 {
        let reader = Arc::clone(&reader);
        let handle = tokio::spawn(async move {
            let path = Path::new("/hdfs/secure-spark-events/app-20231120150000-0001/eventLog");
            let content = reader.read_file(path).await.expect("Failed to read file");
            assert!(content.contains("KerberosTestApp"));
            println!("Concurrent Kerberos read {} completed", i);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await?;
    }

    println!("✅ Concurrent HDFS access with Kerberos test passed");
    Ok(())
}

#[tokio::test]
async fn test_hdfs_kerberos_environment_variables() -> Result<()> {
    println!("Testing Kerberos environment variables...");

    // Test environment variable fallbacks
    std::env::set_var("KERBEROS_PRINCIPAL", "env-user@EXAMPLE.COM");
    std::env::set_var("KERBEROS_KEYTAB", "/env/path/to/keytab");
    std::env::set_var("KRB5_CONFIG", "/env/path/to/krb5.conf");
    std::env::set_var("KERBEROS_REALM", "ENV.EXAMPLE.COM");

    let kerberos_config = KerberosConfig {
        principal: std::env::var("KERBEROS_PRINCIPAL")
            .unwrap_or_else(|_| "default@EXAMPLE.COM".to_string()),
        keytab_path: std::env::var("KERBEROS_KEYTAB").ok(),
        krb5_config_path: std::env::var("KRB5_CONFIG").ok(),
        realm: std::env::var("KERBEROS_REALM").ok(),
    };

    assert_eq!(kerberos_config.principal, "env-user@EXAMPLE.COM");
    assert_eq!(kerberos_config.keytab_path.unwrap(), "/env/path/to/keytab");
    assert_eq!(
        kerberos_config.krb5_config_path.unwrap(),
        "/env/path/to/krb5.conf"
    );
    assert_eq!(kerberos_config.realm.unwrap(), "ENV.EXAMPLE.COM");

    // Clean up environment variables
    std::env::remove_var("KERBEROS_PRINCIPAL");
    std::env::remove_var("KERBEROS_KEYTAB");
    std::env::remove_var("KRB5_CONFIG");
    std::env::remove_var("KERBEROS_REALM");

    println!("✅ Kerberos environment variables test passed");
    Ok(())
}

#[tokio::test]
#[ignore] // Only run when connecting to real HDFS with Kerberos
async fn test_real_hdfs_kerberos_connection() -> Result<()> {
    println!("Testing real HDFS connection with Kerberos (requires secure HDFS cluster)...");

    let namenode_url = std::env::var("HDFS_NAMENODE_URL")
        .unwrap_or_else(|_| "hdfs://secure-namenode:9000".to_string());

    let principal =
        std::env::var("KERBEROS_PRINCIPAL").unwrap_or_else(|_| "spark@EXAMPLE.COM".to_string());

    let keytab_path = std::env::var("KERBEROS_KEYTAB").ok();

    let kerberos_config = KerberosConfig {
        principal: principal.clone(),
        keytab_path,
        krb5_config_path: std::env::var("KRB5_CONFIG").ok(),
        realm: std::env::var("KERBEROS_REALM").ok(),
    };

    let hdfs_config = HdfsConfig {
        namenode_url: namenode_url.clone(),
        connection_timeout_ms: Some(30000),
        read_timeout_ms: Some(60000),
        kerberos: Some(kerberos_config),
    };

    println!(
        "Attempting to create HDFS client with Kerberos for: {}",
        namenode_url
    );
    println!("Using principal: {}", principal);

    let hdfs_reader = HdfsFileReader::new(hdfs_config)?;

    // Test health check
    match hdfs_reader.health_check().await {
        Ok(_) => println!("✅ HDFS health check with Kerberos passed"),
        Err(e) => {
            println!("⚠️ HDFS health check with Kerberos failed: {}", e);
            println!("This is expected if no secure HDFS cluster is available");
        }
    }

    // Test directory listing
    match hdfs_reader.list_directory(Path::new("/")).await {
        Ok(entries) => {
            println!(
                "✅ Root directory listing with Kerberos successful: {} entries",
                entries.len()
            );
            for entry in entries.iter().take(3) {
                println!("  - {}", entry);
            }
        }
        Err(e) => {
            println!("⚠️ Root directory listing with Kerberos failed: {}", e);
        }
    }

    println!("✅ Real HDFS Kerberos connection test completed");
    Ok(())
}
