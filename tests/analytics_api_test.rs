use anyhow::Result;
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;

use spark_history_server::{
    api::create_app,
    storage::{HistoryProvider, StorageBackendFactory, StorageConfig},
};
mod test_config;
use test_config::create_test_config;

/// Setup test environment with sample data
async fn setup_test_environment() -> Result<(HistoryProvider, tempfile::TempDir)> {
    let (mut config, temp_dir) = create_test_config();

    // Create a test event log directory
    let test_events_dir = temp_dir.path().join("spark-events");
    std::fs::create_dir_all(&test_events_dir)?;
    config.log_directory = test_events_dir.to_string_lossy().to_string();

    // Create test event log file with resource optimization focused events
    // This includes multiple applications with different resource patterns for testing
    let event_log_content = r#"{"Event":"SparkListenerApplicationStart","Timestamp":1609459200000,"App ID":"memory-hog-app","App Name":"Memory Heavy Application"}
{"Event":"SparkListenerExecutorAdded","Timestamp":1609459201000,"Executor ID":"1","Executor Info":{"Host":"worker-1:12345","Total Cores":4,"Max Memory":8589934592}}
{"Event":"SparkListenerTaskEnd","Timestamp":1609459205000,"Stage ID":0,"Task Info":{"Task ID":1,"Executor ID":"1","Host":"worker-1","Locality":"PROCESS_LOCAL"},"Task End Reason":{"Reason":"Success"},"Task Metrics":{"Executor Run Time":2000,"Executor CPU Time":1800000000,"JVM GC Time":150,"Input Metrics":{"Bytes Read":10485760},"Output Metrics":{"Bytes Written":5242880},"Shuffle Read Metrics":{"Total Bytes Read":2097152},"Shuffle Write Metrics":{"Bytes Written":1048576},"Peak Execution Memory":4294967296,"Disk Bytes Spilled":0,"Memory Bytes Spilled":0}}
{"Event":"SparkListenerTaskEnd","Timestamp":1609459206000,"Stage ID":0,"Task Info":{"Task ID":2,"Executor ID":"1","Host":"worker-1","Locality":"PROCESS_LOCAL"},"Task End Reason":{"Reason":"Success"},"Task Metrics":{"Executor Run Time":1800,"Executor CPU Time":1600000000,"JVM GC Time":120,"Input Metrics":{"Bytes Read":20971520},"Output Metrics":{"Bytes Written":10485760},"Shuffle Read Metrics":{"Total Bytes Read":4194304},"Shuffle Write Metrics":{"Bytes Written":2097152},"Peak Execution Memory":3758096384,"Disk Bytes Spilled":0,"Memory Bytes Spilled":0}}
{"Event":"SparkListenerTaskEnd","Timestamp":1609459207000,"Stage ID":0,"Task Info":{"Task ID":3,"Executor ID":"1","Host":"worker-1","Locality":"PROCESS_LOCAL"},"Task End Reason":{"Reason":"Success"},"Task Metrics":{"Executor Run Time":2200,"Executor CPU Time":2000000000,"JVM GC Time":180,"Input Metrics":{"Bytes Read":15728640},"Output Metrics":{"Bytes Written":7864320},"Shuffle Read Metrics":{"Total Bytes Read":3145728},"Shuffle Write Metrics":{"Bytes Written":1572864},"Peak Execution Memory":5368709120,"Disk Bytes Spilled":0,"Memory Bytes Spilled":0}}
{"Event":"SparkListenerApplicationEnd","Timestamp":1609459210000,"App ID":"memory-hog-app"}
{"Event":"SparkListenerApplicationStart","Timestamp":1609459220000,"App ID":"cpu-heavy-app","App Name":"CPU Intensive Application"}  
{"Event":"SparkListenerExecutorAdded","Timestamp":1609459221000,"Executor ID":"1","Executor Info":{"Host":"worker-2:12345","Total Cores":8,"Max Memory":4294967296}}
{"Event":"SparkListenerTaskEnd","Timestamp":1609459225000,"Stage ID":0,"Task Info":{"Task ID":1,"Executor ID":"1","Host":"worker-2","Locality":"PROCESS_LOCAL"},"Task End Reason":{"Reason":"Success"},"Task Metrics":{"Executor Run Time":5000,"Executor CPU Time":4800000000,"JVM GC Time":50,"Input Metrics":{"Bytes Read":1048576},"Output Metrics":{"Bytes Written":524288},"Shuffle Read Metrics":{"Total Bytes Read":262144},"Shuffle Write Metrics":{"Bytes Written":131072},"Peak Execution Memory":268435456,"Disk Bytes Spilled":0,"Memory Bytes Spilled":0}}
{"Event":"SparkListenerTaskEnd","Timestamp":1609459226000,"Stage ID":0,"Task Info":{"Task ID":2,"Executor ID":"1","Host":"worker-2","Locality":"PROCESS_LOCAL"},"Task End Reason":{"Reason":"Success"},"Task Metrics":{"Executor Run Time":4800,"Executor CPU Time":4600000000,"JVM GC Time":45,"Input Metrics":{"Bytes Read":2097152},"Output Metrics":{"Bytes Written":1048576},"Shuffle Read Metrics":{"Total Bytes Read":524288},"Shuffle Write Metrics":{"Bytes Written":262144},"Peak Execution Memory":536870912,"Disk Bytes Spilled":0,"Memory Bytes Spilled":0}}
{"Event":"SparkListenerApplicationEnd","Timestamp":1609459230000,"App ID":"cpu-heavy-app"}
{"Event":"SparkListenerApplicationStart","Timestamp":1609459240000,"App ID":"spill-heavy-app","App Name":"Disk Spilling Application"}
{"Event":"SparkListenerExecutorAdded","Timestamp":1609459241000,"Executor ID":"1","Executor Info":{"Host":"worker-3:12345","Total Cores":2,"Max Memory":1073741824}}
{"Event":"SparkListenerTaskEnd","Timestamp":1609459245000,"Stage ID":0,"Task Info":{"Task ID":1,"Executor ID":"1","Host":"worker-3","Locality":"PROCESS_LOCAL"},"Task End Reason":{"Reason":"Success"},"Task Metrics":{"Executor Run Time":3000,"Executor CPU Time":900000000,"JVM GC Time":400,"Input Metrics":{"Bytes Read":104857600},"Output Metrics":{"Bytes Written":52428800},"Shuffle Read Metrics":{"Total Bytes Read":20971520},"Shuffle Write Metrics":{"Bytes Written":10485760},"Peak Execution Memory":1073741824,"Disk Bytes Spilled":2147483648,"Memory Bytes Spilled":536870912}}
{"Event":"SparkListenerTaskEnd","Timestamp":1609459246000,"Stage ID":0,"Task Info":{"Task ID":2,"Executor ID":"1","Host":"worker-3","Locality":"PROCESS_LOCAL"},"Task End Reason":{"Reason":"Success"},"Task Metrics":{"Executor Run Time":3200,"Executor CPU Time":950000000,"JVM GC Time":420,"Input Metrics":{"Bytes Read":125829120},"Output Metrics":{"Bytes Written":62914560},"Shuffle Read Metrics":{"Total Bytes Read":25165824},"Shuffle Write Metrics":{"Bytes Written":12582912},"Peak Execution Memory":1073741824,"Disk Bytes Spilled":1610612736,"Memory Bytes Spilled":805306368}}
{"Event":"SparkListenerApplicationEnd","Timestamp":1609459250000,"App ID":"spill-heavy-app"}"#;

    std::fs::write(
        test_events_dir.join("test-app-1.inprogress"),
        event_log_content,
    )?;

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

    // Give it a moment to process the events
    sleep(Duration::from_millis(200)).await;

    Ok((history_provider, temp_dir))
}

#[tokio::test]
async fn test_platform_engineering_endpoints() -> Result<()> {
    let (history_provider, _temp_dir) = setup_test_environment().await?;

    // Create the app
    let app = create_app(history_provider).await?;

    // Start server on a test port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{}", addr);

    // Spawn server in background
    let _server_handle = tokio::spawn(async move { axum::serve(listener, app).await });

    // Give server time to start
    sleep(Duration::from_millis(100)).await;

    // Create HTTP client
    let client = reqwest::Client::new();

    // Test 1: TOP Resource Consumers (Resource Hogs)
    println!("🔍 Testing TOP resource consumers endpoint...");
    let response = client
        .get(format!("{}/api/v1/optimization/resource-hogs", base_url))
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    let json: Value = response.json().await?;
    assert!(json.is_array());
    println!("✅ Resource hogs endpoint passed");

    // Test 2: TOP Resource Consumers with query parameters
    println!("🔍 Testing resource hogs with limit parameter...");
    let response = client
        .get(format!(
            "{}/api/v1/optimization/resource-hogs?limit=5",
            base_url
        ))
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    let json: Value = response.json().await?;
    assert!(json.is_array());
    if let Some(first_result) = json.as_array().and_then(|arr| arr.first()) {
        assert!(first_result.get("app_id").is_some());
        assert!(first_result.get("resource_type").is_some());
        assert!(first_result.get("consumption_value").is_some());
        assert!(first_result.get("efficiency_score").is_some());
        assert!(first_result.get("recommendation").is_some());
    }
    println!("✅ Resource hogs with parameters passed");

    // Test 3: Application Efficiency Analysis
    println!("📊 Testing application efficiency analysis endpoint...");
    let response = client
        .get(format!(
            "{}/api/v1/optimization/efficiency-analysis",
            base_url
        ))
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    let json: Value = response.json().await?;
    assert!(json.is_array());
    if let Some(first_result) = json.as_array().and_then(|arr| arr.first()) {
        assert!(first_result.get("app_id").is_some());
        assert!(first_result.get("efficiency_category").is_some());
        assert!(first_result.get("memory_efficiency").is_some());
        assert!(first_result.get("cpu_efficiency").is_some());
        assert!(first_result.get("optimization_actions").is_some());
    }
    println!("✅ Efficiency analysis endpoint passed");

    // Test 4: Capacity Usage Trends
    println!("📈 Testing capacity usage trends endpoint...");
    let response = client
        .get(format!("{}/api/v1/capacity/usage-trends", base_url))
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    let json: Value = response.json().await?;
    assert!(json.is_array());
    if let Some(first_result) = json.as_array().and_then(|arr| arr.first()) {
        assert!(first_result.get("date").is_some());
        assert!(first_result.get("total_memory_gb_used").is_some());
        assert!(first_result.get("total_cpu_cores_used").is_some());
        assert!(first_result.get("peak_concurrent_applications").is_some());
    }
    println!("✅ Capacity usage trends endpoint passed");

    // Test 5: Cost Optimization Opportunities
    println!("💰 Testing cost optimization opportunities endpoint...");
    let response = client
        .get(format!("{}/api/v1/capacity/cost-optimization", base_url))
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    let json: Value = response.json().await?;
    assert!(json.is_object());
    assert!(json.get("app_id").is_some());
    assert!(json.get("optimization_type").is_some());
    assert!(json.get("current_cost").is_some());
    assert!(json.get("savings_percentage").is_some());
    assert!(json.get("confidence_score").is_some());
    println!("✅ Cost optimization endpoint passed");

    // All platform engineering endpoint tests passed!
    println!("🎉 All platform engineering endpoints are working correctly!");
    // Old analytics tests removed - analytics functionality has been removed

    Ok(())
}

#[tokio::test]
async fn test_deprecated_endpoints_return_404() -> Result<()> {
    let (history_provider, _temp_dir) = setup_test_environment().await?;

    // Create the app
    let app = create_app(history_provider).await?;

    // Start server on a test port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{}", addr);

    // Spawn server in background
    let _server_handle = tokio::spawn(async move { axum::serve(listener, app).await });

    // Give server time to start
    sleep(Duration::from_millis(100)).await;

    // Create HTTP client
    let client = reqwest::Client::new();

    // Test deprecated endpoints return 404 Not Found
    let deprecated_endpoints = vec![
        "/api/v1/analytics/test",
        "/api/v1/analytics/gc-time-trends",
        "/api/v1/analytics/cpu-utilization-analysis",
        "/api/v1/analytics/memory-usage-analysis",
        "/api/v1/analytics/task-distribution",
        "/api/v1/analytics/executor-utilization",
    ];

    for endpoint in deprecated_endpoints {
        println!("🚫 Testing deprecated endpoint: {}", endpoint);
        let response = client
            .get(format!("{}{}", base_url, endpoint))
            .send()
            .await?;
        assert_eq!(response.status(), 404);
        println!(
            "✅ Deprecated endpoint {} correctly returns 404 Not Found",
            endpoint
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_platform_engineering_data_quality() -> Result<()> {
    let (history_provider, _temp_dir) = setup_test_environment().await?;

    // Create the app
    let app = create_app(history_provider).await?;

    // Start server on a test port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{}", addr);

    // Spawn server in background
    let _server_handle = tokio::spawn(async move { axum::serve(listener, app).await });

    // Give server time to start and process events
    sleep(Duration::from_millis(500)).await;

    // Create HTTP client
    let client = reqwest::Client::new();

    // Test that resource hogs contain expected data patterns
    println!("🔍 Testing resource hogs data quality...");
    let response = client
        .get(format!(
            "{}/api/v1/optimization/resource-hogs?limit=10",
            base_url
        ))
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    let json: Value = response.json().await?;

    if let Some(resource_hogs) = json.as_array() {
        for hog in resource_hogs {
            // Validate required fields
            assert!(hog.get("app_id").and_then(|v| v.as_str()).is_some());
            assert!(hog.get("resource_type").and_then(|v| v.as_str()).is_some());
            assert!(hog
                .get("consumption_value")
                .and_then(|v| v.as_f64())
                .is_some());
            assert!(hog
                .get("efficiency_score")
                .and_then(|v| v.as_f64())
                .is_some());

            // Validate data ranges
            let efficiency_score = hog
                .get("efficiency_score")
                .and_then(|v| v.as_f64())
                .unwrap();
            assert!((0.0..=100.0).contains(&efficiency_score));

            let consumption_value = hog
                .get("consumption_value")
                .and_then(|v| v.as_f64())
                .unwrap();
            assert!(consumption_value > 0.0);
        }
        println!("✅ Resource hogs data quality validated");
    }

    // Test efficiency analysis data quality
    println!("📊 Testing efficiency analysis data quality...");
    let response = client
        .get(format!(
            "{}/api/v1/optimization/efficiency-analysis?limit=10",
            base_url
        ))
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    let json: Value = response.json().await?;

    if let Some(efficiency_results) = json.as_array() {
        for result in efficiency_results {
            // Validate efficiency categories
            let category = result
                .get("efficiency_category")
                .and_then(|v| v.as_str())
                .unwrap();
            assert!(["OverProvisioned", "WellTuned", "UnderProvisioned"].contains(&category));

            // Validate efficiency percentages
            if let Some(memory_eff) = result.get("memory_efficiency").and_then(|v| v.as_f64()) {
                assert!((0.0..=100.0).contains(&memory_eff));
            }
            if let Some(cpu_eff) = result.get("cpu_efficiency").and_then(|v| v.as_f64()) {
                assert!(
                    (0.0..=100.0).contains(&cpu_eff),
                    "CPU efficiency should be between 0-100%, got: {}%",
                    cpu_eff
                );
            }

            // Validate risk level
            let risk = result.get("risk_level").and_then(|v| v.as_str()).unwrap();
            assert!(["Low", "Medium", "High"].contains(&risk));
        }
        println!("✅ Efficiency analysis data quality validated");
    }

    Ok(())
}
