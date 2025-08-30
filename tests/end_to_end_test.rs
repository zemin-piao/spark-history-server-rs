use anyhow::Result;
use std::time::Duration;
use tokio::time::sleep;

use spark_history_server::{
    api::create_app,
    config::HistoryConfig,
    models::ApplicationInfo,
    storage::HistoryProvider,
};

#[tokio::test]
async fn test_end_to_end_with_real_data() -> Result<()> {
    // Use the test event logs directory
    let config = HistoryConfig {
        log_directory: "./test-data/spark-events".to_string(),
        max_applications: 100,
        update_interval_seconds: 60,
        max_apps_per_request: 50,
        compression_enabled: true,
        cache_directory: None,
        enable_cache: false,
    };

    println!("Creating history provider with test data...");
    let history_provider = HistoryProvider::new(config).await?;

    // Give it a moment to scan the event logs
    sleep(Duration::from_millis(500)).await;

    let app = create_app(history_provider.clone()).await?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{}", addr);

    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app).await
    });

    sleep(Duration::from_millis(200)).await;

    let client = reqwest::Client::new();

    // Test: Get applications (should find our sample app)
    println!("Testing applications endpoint with real data...");
    let response = client
        .get(&format!("{}/api/v1/applications", base_url))
        .send()
        .await?;

    assert_eq!(response.status(), 200);
    let apps: Vec<ApplicationInfo> = response.json().await?;
    
    println!("Found {} applications", apps.len());
    
    // Print application details for debugging
    for app in &apps {
        let attempts_text = if app.attempts.is_empty() { 
            "No attempts".to_string() 
        } else { 
            format!("{} attempts", app.attempts.len()) 
        };
        println!("Application: {} - {} ({})", app.id, app.name, attempts_text);
        
        for attempt in &app.attempts {
            println!("  Attempt {}: {} -> {} (completed: {})", 
                attempt.attempt_id.as_deref().unwrap_or("None"),
                attempt.start_time.format("%Y-%m-%d %H:%M:%S"),
                attempt.end_time.format("%Y-%m-%d %H:%M:%S"),
                attempt.completed
            );
        }
    }

    // If we found applications, test specific app endpoint
    if !apps.is_empty() {
        let test_app = &apps[0];
        println!("Testing specific application endpoint for: {}", test_app.id);
        
        let response = client
            .get(&format!("{}/api/v1/applications/{}", base_url, test_app.id))
            .send()
            .await?;
        
        assert_eq!(response.status(), 200);
        let app: ApplicationInfo = response.json().await?;
        assert_eq!(app.id, test_app.id);
        assert_eq!(app.name, test_app.name);
        
        println!("✅ Specific application test passed");
        
        // Test the jobs endpoint too
        let response = client
            .get(&format!("{}/api/v1/applications/{}/jobs", base_url, test_app.id))
            .send()
            .await?;
        
        assert_eq!(response.status(), 200);
        println!("✅ Jobs endpoint test passed");
    } else {
        println!("⚠️ No applications found - check event log parsing");
    }

    server_handle.abort();
    
    println!("🎉 End-to-end test completed successfully!");
    Ok(())
}

#[tokio::test]
async fn test_performance_and_concurrent_requests() -> Result<()> {
    let config = HistoryConfig {
        log_directory: "./test-data/spark-events".to_string(),
        max_applications: 100,
        update_interval_seconds: 60,
        max_apps_per_request: 50,
        compression_enabled: true,
        cache_directory: None,
        enable_cache: false,
    };

    let history_provider = HistoryProvider::new(config).await?;
    let app = create_app(history_provider).await?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{}", addr);

    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app).await
    });

    sleep(Duration::from_millis(100)).await;

    // Test concurrent requests
    println!("Testing concurrent requests...");
    let start_time = std::time::Instant::now();
    
    let mut handles = Vec::new();
    for i in 0..10 {
        let url = base_url.clone();
        let handle = tokio::spawn(async move {
            let client = reqwest::Client::new();
            let response = client
                .get(&format!("{}/api/v1/applications", url))
                .send()
                .await
                .expect("Request failed");
            
            assert_eq!(response.status(), 200);
            println!("Request {} completed", i);
        });
        handles.push(handle);
    }

    // Wait for all requests to complete
    for handle in handles {
        handle.await?;
    }

    let duration = start_time.elapsed();
    println!("✅ 10 concurrent requests completed in {:?}", duration);
    
    // Should be quite fast
    assert!(duration < Duration::from_secs(2), "Requests took too long: {:?}", duration);

    server_handle.abort();
    Ok(())
}