use spark_history_server::{
    api::create_app,
    config::{HistoryConfig, ServerConfig, Settings},
    storage::{StorageBackendFactory, StorageConfig},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Create a sample configuration
    let settings = Settings {
        server: ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 18080,
            max_applications: 100,
        },
        history: HistoryConfig {
            log_directory: "./examples".to_string(),
            max_applications: 100,
            update_interval_seconds: 10,
            max_apps_per_request: 50,
            compression_enabled: true,
            database_directory: None,
            hdfs: None,
            s3: None,
        },
    };

    println!("Starting Spark History Server example...");
    println!(
        "Server will start on http://{}:{}",
        settings.server.host, settings.server.port
    );
    println!("API endpoints:");
    println!("  GET /api/v1/applications");
    println!("  GET /api/v1/applications/{{app_id}}");
    println!("  GET /api/v1/version");
    println!("  GET /health");

    // Initialize history provider
    let storage_config = StorageConfig::DuckDB {
        database_path: settings.history.database_directory.as_ref()
            .map(|dir| format!("{}/events.db", dir))
            .unwrap_or_else(|| "./data/events.db".to_string()),
        num_workers: 8,
        batch_size: 5000,
    };
    let history_provider = StorageBackendFactory::create_backend(storage_config).await?;

    // Create the web application
    let app = create_app(history_provider).await?;

    // Start the server
    let addr = format!("{}:{}", settings.server.host, settings.server.port);
    let listener = tokio::net::TcpListener::bind(addr).await?;

    println!("Server listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;

    Ok(())
}
