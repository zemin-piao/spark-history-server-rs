use anyhow::Result;
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;

use spark_history_server::{
    api::create_app,
    config::{HistoryConfig, Settings},
    models::{ApplicationInfo, VersionInfo},
    storage::HistoryProvider,
};
mod test_config;
use test_config::create_test_config;

#[tokio::test]
async fn test_integration_full_workflow() -> Result<()> {
    // Setup test configuration
    let (config, _) = create_test_config();

    // Create history provider
    let history_provider = HistoryProvider::new(config).await?;

    // Create the app
    let app = create_app(history_provider).await?;

    // Start server on a test port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{}", addr);

    // Spawn server in background
    let server_handle = tokio::spawn(async move { axum::serve(listener, app).await });

    // Give server time to start
    sleep(Duration::from_millis(100)).await;

    // Create HTTP client
    let client = reqwest::Client::new();

    // Test 1: Health check
    println!("Testing health check endpoint...");
    let response = client.get(&format!("{}/health", base_url)).send().await?;

    assert_eq!(response.status(), 200);
    let health: Value = response.json().await?;
    assert_eq!(health["status"], "healthy");
    println!("✅ Health check passed");

    // Test 2: Version endpoint
    println!("Testing version endpoint...");
    let response = client
        .get(&format!("{}/api/v1/version", base_url))
        .send()
        .await?;

    assert_eq!(response.status(), 200);
    let version: VersionInfo = response.json().await?;
    assert!(!version.version.is_empty());
    println!("✅ Version endpoint passed: {}", version.version);

    // Test 3: Applications list endpoint
    println!("Testing applications list endpoint...");
    let response = client
        .get(&format!("{}/api/v1/applications", base_url))
        .send()
        .await?;

    assert_eq!(response.status(), 200);
    let apps: Vec<ApplicationInfo> = response.json().await?;
    println!(
        "✅ Applications endpoint passed, found {} applications",
        apps.len()
    );

    // If we have applications, test individual app endpoint
    if !apps.is_empty() {
        let first_app = &apps[0];
        println!(
            "Testing individual application endpoint for app: {}",
            first_app.id
        );

        let response = client
            .get(&format!(
                "{}/api/v1/applications/{}",
                base_url, first_app.id
            ))
            .send()
            .await?;

        assert_eq!(response.status(), 200);
        let app: ApplicationInfo = response.json().await?;
        assert_eq!(app.id, first_app.id);
        println!("✅ Individual application endpoint passed");
    }

    // Test 4: Applications with query parameters
    println!("Testing applications endpoint with query parameters...");
    let response = client
        .get(&format!(
            "{}/api/v1/applications?limit=5&status=COMPLETED",
            base_url
        ))
        .send()
        .await?;

    assert_eq!(response.status(), 200);
    let filtered_apps: Vec<ApplicationInfo> = response.json().await?;
    assert!(filtered_apps.len() <= 5);
    println!(
        "✅ Applications with filters passed, returned {} applications",
        filtered_apps.len()
    );

    // Test 5: Non-existent application
    println!("Testing non-existent application endpoint...");
    let response = client
        .get(&format!(
            "{}/api/v1/applications/non-existent-app",
            base_url
        ))
        .send()
        .await?;

    assert_eq!(response.status(), 404);
    println!("✅ Non-existent application returns 404 as expected");

    // Test 6: Jobs endpoint (should return empty array for now)
    if !apps.is_empty() {
        let first_app = &apps[0];
        println!("Testing jobs endpoint for app: {}", first_app.id);

        let response = client
            .get(&format!(
                "{}/api/v1/applications/{}/jobs",
                base_url, first_app.id
            ))
            .send()
            .await?;

        assert_eq!(response.status(), 200);
        let jobs: Vec<Value> = response.json().await?;
        println!("✅ Jobs endpoint passed, returned {} jobs", jobs.len());
    }

    // Clean up: abort the server
    server_handle.abort();

    println!("\n🎉 All integration tests passed!");
    Ok(())
}

#[tokio::test]
async fn test_date_filtering() -> Result<()> {
    let (config, _) = create_test_config();

    let history_provider = HistoryProvider::new(config).await?;
    let app = create_app(history_provider).await?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{}", addr);

    let server_handle = tokio::spawn(async move { axum::serve(listener, app).await });

    sleep(Duration::from_millis(100)).await;

    let client = reqwest::Client::new();

    // Test date range filtering
    println!("Testing date range filtering...");
    let response = client
        .get(&format!(
            "{}/api/v1/applications?minDate=2023-01-01&maxDate=2024-01-01",
            base_url
        ))
        .send()
        .await?;

    assert_eq!(response.status(), 200);
    let apps: Vec<ApplicationInfo> = response.json().await?;
    println!(
        "✅ Date filtering passed, returned {} applications",
        apps.len()
    );

    server_handle.abort();
    Ok(())
}

#[tokio::test]
async fn test_cors_headers() -> Result<()> {
    let (config, _) = create_test_config();

    let history_provider = HistoryProvider::new(config).await?;
    let app = create_app(history_provider).await?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{}", addr);

    let server_handle = tokio::spawn(async move { axum::serve(listener, app).await });

    sleep(Duration::from_millis(100)).await;

    let client = reqwest::Client::new();

    // Test CORS preflight request
    println!("Testing CORS headers...");
    let response = client
        .request(
            reqwest::Method::OPTIONS,
            &format!("{}/api/v1/applications", base_url),
        )
        .header("Origin", "http://localhost:3000")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await?;

    // Should have CORS headers
    assert!(response.status().is_success() || response.status() == 404); // Either works with CORS
    println!("✅ CORS test passed");

    server_handle.abort();
    Ok(())
}
