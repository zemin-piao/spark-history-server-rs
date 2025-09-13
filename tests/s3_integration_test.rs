use spark_history_server::config::S3Config;
use spark_history_server::storage::file_reader::{FileReader, S3FileReader};
use std::path::Path;

/// Mock S3 tests - these tests work without requiring actual S3 infrastructure
mod mock_tests {
    use super::*;

    #[tokio::test]
    async fn test_s3_file_reader_creation() {
        let config = S3Config {
            bucket_name: "test-bucket".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint_url: None,
            access_key_id: Some("test-key".to_string()),
            secret_access_key: Some("test-secret".to_string()),
            session_token: None,
            connection_timeout_ms: Some(30000),
            read_timeout_ms: Some(60000),
        };

        // Test creation - this should not fail even without real S3 access
        let result = S3FileReader::new(config, "spark-events".to_string()).await;
        assert!(result.is_ok(), "S3FileReader creation should succeed");

        let reader = result.unwrap();

        // Test simple getter methods
        assert!(reader
            .s3_key_from_path(Path::new("test/path"))
            .contains("spark-events"));
    }

    #[tokio::test]
    async fn test_s3_file_reader_simple_creation() {
        let result = S3FileReader::new_simple("test-bucket", Some("us-west-2"), "events").await;
        assert!(
            result.is_ok(),
            "Simple S3FileReader creation should succeed"
        );
    }

    #[tokio::test]
    async fn test_s3_key_from_path() {
        let config = S3Config {
            bucket_name: "test-bucket".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint_url: None,
            access_key_id: Some("test-key".to_string()),
            secret_access_key: Some("test-secret".to_string()),
            session_token: None,
            connection_timeout_ms: Some(30000),
            read_timeout_ms: Some(60000),
        };

        let reader = S3FileReader::new(config, "spark-events".to_string())
            .await
            .unwrap();

        // Test path conversion
        let key = reader.s3_key_from_path(Path::new("app1/events"));
        assert_eq!(key, "spark-events/app1/events");

        let key2 = reader.s3_key_from_path(Path::new("test.log"));
        assert_eq!(key2, "spark-events/test.log");
    }

    #[tokio::test]
    async fn test_empty_prefix() {
        let config = S3Config {
            bucket_name: "test-bucket".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint_url: None,
            access_key_id: Some("test-key".to_string()),
            secret_access_key: Some("test-secret".to_string()),
            session_token: None,
            connection_timeout_ms: Some(30000),
            read_timeout_ms: Some(60000),
        };

        let reader = S3FileReader::new(config, "".to_string()).await.unwrap();

        // Test path conversion with empty prefix
        let key = reader.s3_key_from_path(Path::new("app1/events"));
        assert_eq!(key, "app1/events");
    }

    #[tokio::test]
    async fn test_s3_config_validation() {
        // Test minimal config
        let minimal_config = S3Config {
            bucket_name: "test".to_string(),
            region: None,
            endpoint_url: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            connection_timeout_ms: None,
            read_timeout_ms: None,
        };

        let result = S3FileReader::new(minimal_config, "prefix".to_string()).await;
        assert!(result.is_ok(), "Minimal S3 config should work");

        // Test with custom endpoint (MinIO style)
        let minio_config = S3Config {
            bucket_name: "test-bucket".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint_url: Some("http://localhost:9000".to_string()),
            access_key_id: Some("minioadmin".to_string()),
            secret_access_key: Some("minioadmin".to_string()),
            session_token: None,
            connection_timeout_ms: Some(10000),
            read_timeout_ms: Some(30000),
        };

        let result = S3FileReader::new(minio_config, "data".to_string()).await;
        assert!(result.is_ok(), "MinIO-style config should work");
    }
}

/// Integration tests that require real S3 infrastructure
/// These are marked with #[ignore] and can be run with: cargo test test_real_s3 --ignored
mod real_s3_tests {
    use super::*;
    use std::env;

    #[allow(dead_code)]
    fn get_test_s3_config() -> Option<S3Config> {
        let bucket = env::var("S3_TEST_BUCKET").ok()?;
        let region = env::var("AWS_REGION").ok();
        let endpoint = env::var("AWS_ENDPOINT_URL").ok();
        let access_key = env::var("AWS_ACCESS_KEY_ID").ok();
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").ok();
        let session_token = env::var("AWS_SESSION_TOKEN").ok();

        Some(S3Config {
            bucket_name: bucket,
            region,
            endpoint_url: endpoint,
            access_key_id: access_key,
            secret_access_key: secret_key,
            session_token,
            connection_timeout_ms: Some(30000),
            read_timeout_ms: Some(60000),
        })
    }

    #[tokio::test]
    #[cfg(feature = "integration-tests")]
    async fn test_real_s3_health_check() {
        let config = match get_test_s3_config() {
            Some(config) => config,
            None => {
                eprintln!("Skipping real S3 test - environment variables not set");
                eprintln!("Set S3_TEST_BUCKET and AWS credentials to run this test");
                return;
            }
        };

        println!(
            "Testing S3 health check with bucket: {}",
            config.bucket_name
        );

        let reader = S3FileReader::new(config, "test-prefix".to_string())
            .await
            .expect("Failed to create S3 reader");

        let health_result = reader.health_check().await;
        match health_result {
            Ok(healthy) => {
                println!("S3 health check result: {}", healthy);
                assert!(healthy, "S3 should be healthy");
            }
            Err(e) => {
                println!("S3 health check failed: {}", e);
                // Don't fail the test if it's a permission issue, just log it
                if e.to_string().contains("AccessDenied") {
                    println!("Access denied - this is expected if bucket doesn't exist or lacks permissions");
                } else {
                    panic!("Unexpected S3 health check error: {}", e);
                }
            }
        }
    }

    #[tokio::test]
    #[cfg(feature = "integration-tests")]
    async fn test_real_s3_list_directory() {
        let config = match get_test_s3_config() {
            Some(config) => config,
            None => {
                eprintln!("Skipping real S3 test - environment variables not set");
                return;
            }
        };

        println!(
            "Testing S3 directory listing with bucket: {}",
            config.bucket_name
        );

        let reader = S3FileReader::new(config, "".to_string())
            .await
            .expect("Failed to create S3 reader");

        let list_result = reader.list_directory(Path::new("")).await;
        match list_result {
            Ok(entries) => {
                println!("Found {} entries in S3 bucket", entries.len());
                for entry in entries.iter().take(10) {
                    println!("  - {}", entry);
                }
            }
            Err(e) => {
                println!("S3 list directory failed: {}", e);
                // Don't fail if it's an expected error
                if e.to_string().contains("AccessDenied") || e.to_string().contains("NoSuchBucket")
                {
                    println!("This is expected if bucket doesn't exist or lacks permissions");
                } else {
                    panic!("Unexpected S3 list error: {}", e);
                }
            }
        }
    }

    #[tokio::test]
    #[cfg(feature = "integration-tests")]
    async fn test_real_s3_read_file() {
        let config = match get_test_s3_config() {
            Some(config) => config,
            None => {
                eprintln!("Skipping real S3 test - environment variables not set");
                return;
            }
        };

        // This test expects a test file to exist in the S3 bucket
        let test_key = env::var("S3_TEST_KEY").unwrap_or("test.txt".to_string());

        println!("Testing S3 file read with key: {}", test_key);

        let reader = S3FileReader::new(config, "".to_string())
            .await
            .expect("Failed to create S3 reader");

        let read_result = reader.read_file(Path::new(&test_key)).await;
        match read_result {
            Ok(content) => {
                println!("Successfully read {} bytes from S3", content.len());
                println!("First 100 chars: {}", &content[..content.len().min(100)]);
            }
            Err(e) => {
                println!("S3 read file failed: {}", e);
                // Expected if test file doesn't exist
                if e.to_string().contains("NoSuchKey") {
                    println!(
                        "Test file doesn't exist - create '{}' in bucket to test file reading",
                        test_key
                    );
                } else {
                    panic!("Unexpected S3 read error: {}", e);
                }
            }
        }
    }

    #[tokio::test]
    #[cfg(feature = "integration-tests")]
    async fn test_real_s3_file_exists() {
        let config = match get_test_s3_config() {
            Some(config) => config,
            None => {
                eprintln!("Skipping real S3 test - environment variables not set");
                return;
            }
        };

        let reader = S3FileReader::new(config, "".to_string())
            .await
            .expect("Failed to create S3 reader");

        // Test with a key that likely doesn't exist
        let nonexistent_exists = reader
            .file_exists(Path::new("nonexistent-file-12345.txt"))
            .await;
        println!("Non-existent file exists: {}", nonexistent_exists);
        assert!(!nonexistent_exists, "Non-existent file should not exist");

        // Test with a key that might exist (don't assert, just log)
        let test_key = env::var("S3_TEST_KEY").unwrap_or("test.txt".to_string());
        let test_exists = reader.file_exists(Path::new(&test_key)).await;
        println!("Test file '{}' exists: {}", test_key, test_exists);
    }
}

/// Circuit breaker tests
mod circuit_breaker_tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_breaker_integration() {
        // Create S3 reader with invalid config to trigger circuit breaker
        let bad_config = S3Config {
            bucket_name: "nonexistent-bucket-12345".to_string(),
            region: Some("invalid-region".to_string()),
            endpoint_url: Some("http://invalid-endpoint:9000".to_string()),
            access_key_id: Some("invalid".to_string()),
            secret_access_key: Some("invalid".to_string()),
            session_token: None,
            connection_timeout_ms: Some(1000), // Short timeout
            read_timeout_ms: Some(1000),
        };

        let reader = S3FileReader::new(bad_config, "test".to_string()).await;
        assert!(
            reader.is_ok(),
            "Reader creation should succeed even with bad config"
        );

        let reader = reader.unwrap();

        // Multiple failed operations should eventually trigger circuit breaker
        for i in 0..5 {
            let result = reader.file_exists(Path::new(&format!("test-{}", i))).await;
            println!("Attempt {}: file_exists result = {}", i + 1, result);

            // All should fail, but we're testing that the circuit breaker doesn't panic
            assert!(!result, "Operations should fail with invalid config");
        }

        println!("Circuit breaker test completed - no panics occurred");
    }
}

/// Performance tests
#[cfg(feature = "performance-tests")]
mod performance_tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    #[ignore = "Performance test - run manually"]
    async fn test_s3_reader_performance() {
        let config = S3Config {
            bucket_name: "test-bucket".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint_url: None,
            access_key_id: Some("test-key".to_string()),
            secret_access_key: Some("test-secret".to_string()),
            session_token: None,
            connection_timeout_ms: Some(30000),
            read_timeout_ms: Some(60000),
        };

        let start = Instant::now();

        // Create multiple readers in parallel
        let mut handles = vec![];
        for i in 0..10 {
            let config_clone = config.clone();
            let handle = tokio::spawn(async move {
                let reader = S3FileReader::new(config_clone, format!("prefix-{}", i)).await;
                assert!(reader.is_ok(), "Reader creation should succeed");
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let duration = start.elapsed();
        println!("Created 10 S3 readers in {:?}", duration);

        // Should complete reasonably quickly
        assert!(duration.as_secs() < 30, "Reader creation should be fast");
    }
}
