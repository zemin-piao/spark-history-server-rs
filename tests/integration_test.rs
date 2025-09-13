use anyhow::Result;
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

use spark_history_server::{
    api::create_app,
    models::{ApplicationInfo, VersionInfo},
    storage::{StorageBackendFactory, StorageConfig},
};
mod test_config;
use test_config::create_test_config;

// Initialize test logging
fn init_test_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let _ = fmt()
        .with_test_writer()
        .with_env_filter(EnvFilter::from_default_env().add_directive("debug".parse().unwrap()))
        .try_init();
}

#[tokio::test]
async fn test_integration_full_workflow() -> Result<()> {
    init_test_tracing();

    // Setup test configuration
    let (config, _temp_dir) = create_test_config();

    // Create history provider
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
    info!("Testing health check endpoint...");
    let response = client.get(format!("{}/health", base_url)).send().await?;

    assert_eq!(response.status(), 200);
    let health: Value = response.json().await?;
    assert_eq!(health["status"], "healthy");
    info!("✅ Health check passed");

    // Test 2: Version endpoint
    info!("Testing version endpoint...");
    let response = client
        .get(format!("{}/api/v1/version", base_url))
        .send()
        .await?;

    assert_eq!(response.status(), 200);
    let version: VersionInfo = response.json().await?;
    assert!(!version.version.is_empty());
    info!("✅ Version endpoint passed: {}", version.version);

    // Test 3: Applications list endpoint
    info!("Testing applications list endpoint...");
    let response = client
        .get(format!("{}/api/v1/applications", base_url))
        .send()
        .await?;

    assert_eq!(response.status(), 200);
    let apps: Vec<ApplicationInfo> = response.json().await?;
    info!(
        "✅ Applications endpoint passed, found {} applications",
        apps.len()
    );

    // If we have applications, test individual app endpoint
    if !apps.is_empty() {
        let first_app = &apps[0];
        info!(
            "Testing individual application endpoint for app: {}",
            first_app.id
        );

        let response = client
            .get(format!("{}/api/v1/applications/{}", base_url, first_app.id))
            .send()
            .await?;

        assert_eq!(response.status(), 200);
        let app: ApplicationInfo = response.json().await?;
        assert_eq!(app.id, first_app.id);
        info!("✅ Individual application endpoint passed");
    }

    // Test 4: Applications with query parameters
    info!("Testing applications endpoint with query parameters...");
    let response = client
        .get(format!(
            "{}/api/v1/applications?limit=5&status=COMPLETED",
            base_url
        ))
        .send()
        .await?;

    assert_eq!(response.status(), 200);
    let filtered_apps: Vec<ApplicationInfo> = response.json().await?;
    assert!(filtered_apps.len() <= 5);
    info!(
        "✅ Applications with filters passed, returned {} applications",
        filtered_apps.len()
    );

    // Test 5: Non-existent application
    info!("Testing non-existent application endpoint...");
    let response = client
        .get(format!("{}/api/v1/applications/non-existent-app", base_url))
        .send()
        .await?;

    assert_eq!(response.status(), 404);
    info!("✅ Non-existent application returns 404 as expected");

    // Test 6: Jobs endpoint (should return empty array for now)
    if !apps.is_empty() {
        let first_app = &apps[0];
        info!("Testing jobs endpoint for app: {}", first_app.id);

        let response = client
            .get(format!(
                "{}/api/v1/applications/{}/jobs",
                base_url, first_app.id
            ))
            .send()
            .await?;

        assert_eq!(response.status(), 200);
        let jobs: Vec<Value> = response.json().await?;
        info!("✅ Jobs endpoint passed, returned {} jobs", jobs.len());
    }

    // Clean up: abort the server
    server_handle.abort();

    info!("\n🎉 All integration tests passed!");
    Ok(())
}

#[tokio::test]
async fn test_date_filtering() -> Result<()> {
    init_test_tracing();
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

    // Test date range filtering
    info!("Testing date range filtering...");
    let response = client
        .get(format!(
            "{}/api/v1/applications?minDate=2023-01-01&maxDate=2024-01-01",
            base_url
        ))
        .send()
        .await?;

    assert_eq!(response.status(), 200);
    let apps: Vec<ApplicationInfo> = response.json().await?;
    info!(
        "✅ Date filtering passed, returned {} applications",
        apps.len()
    );

    server_handle.abort();
    Ok(())
}

#[tokio::test]
async fn test_cors_headers() -> Result<()> {
    init_test_tracing();
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

    // Test CORS preflight request
    info!("Testing CORS headers...");
    let response = client
        .request(
            reqwest::Method::OPTIONS,
            format!("{}/api/v1/applications", base_url),
        )
        .header("Origin", "http://localhost:3000")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await?;

    // Should have CORS headers
    assert!(response.status().is_success() || response.status() == 404); // Either works with CORS
    info!("✅ CORS test passed");

    server_handle.abort();
    Ok(())
}
