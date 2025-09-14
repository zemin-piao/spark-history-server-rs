use anyhow::Result;
use clap::Parser;
use std::collections::HashSet;
use std::net::SocketAddr;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod analytics_api;
mod api;
mod circuit_breaker;
mod config;
mod dashboard;
mod models;
mod s3_reader;
mod storage;

use crate::api::create_app;
use crate::config::{HdfsConfig, KerberosConfig, S3Config, Settings};

#[derive(Parser, Debug)]
#[command(name = "spark-history-server")]
#[command(about = "A high-performance read-only Spark History Server API")]
struct Args {
    /// Configuration file path
    #[arg(short, long, default_value = "config/settings.toml")]
    config: String,

    /// Server host
    #[arg(long)]
    host: Option<String>,

    /// Server port
    #[arg(short, long)]
    port: Option<u16>,

    /// Log directory path
    #[arg(short, long)]
    log_directory: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Use HDFS for reading event logs (default: local filesystem)
    #[arg(long)]
    hdfs: bool,

    /// HDFS namenode URL (e.g., hdfs://namenode:9000)
    #[arg(long)]
    hdfs_namenode: Option<String>,

    /// Kerberos principal for HDFS authentication
    #[arg(long)]
    kerberos_principal: Option<String>,

    /// Path to Kerberos keytab file
    #[arg(long)]
    keytab_path: Option<String>,

    /// Path to krb5.conf configuration file
    #[arg(long)]
    krb5_config: Option<String>,

    /// Kerberos realm
    #[arg(long)]
    kerberos_realm: Option<String>,

    /// HDFS connection timeout in milliseconds
    #[arg(long, default_value = "30000")]
    hdfs_connection_timeout: u64,

    /// HDFS read timeout in milliseconds
    #[arg(long, default_value = "60000")]
    hdfs_read_timeout: u64,

    /// Use S3 for reading event logs
    #[arg(long)]
    s3: bool,

    /// S3 bucket name
    #[arg(long)]
    s3_bucket: Option<String>,

    /// AWS region (e.g., us-east-1)
    #[arg(long)]
    s3_region: Option<String>,

    /// Custom S3 endpoint URL (for S3-compatible services like MinIO)
    #[arg(long)]
    s3_endpoint: Option<String>,

    /// AWS access key ID
    #[arg(long)]
    aws_access_key_id: Option<String>,

    /// AWS secret access key
    #[arg(long)]
    aws_secret_access_key: Option<String>,

    /// AWS session token (for temporary credentials)
    #[arg(long)]
    aws_session_token: Option<String>,

    /// S3 connection timeout in milliseconds
    #[arg(long, default_value = "30000")]
    s3_connection_timeout: u64,

    /// S3 read timeout in milliseconds
    #[arg(long, default_value = "60000")]
    s3_read_timeout: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    let log_level = match args.log_level.as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(false)
        .compact()
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    // Load configuration
    let mut settings = Settings::load(&args.config)?;

    // Override with command line arguments
    if let Some(host) = args.host {
        settings.server.host = host;
    }
    if let Some(port) = args.port {
        settings.server.port = port;
    }
    if let Some(log_directory) = args.log_directory {
        settings.history.log_directory = log_directory;
    }

    // Handle HDFS configuration from CLI arguments
    if args.hdfs {
        let namenode_url = args
            .hdfs_namenode
            .or_else(|| std::env::var("HDFS_NAMENODE_URL").ok())
            .unwrap_or_else(|| "hdfs://localhost:9000".to_string());

        let kerberos_config = if args.kerberos_principal.is_some()
            || args.keytab_path.is_some()
            || args.krb5_config.is_some()
            || args.kerberos_realm.is_some()
        {
            Some(KerberosConfig {
                principal: args
                    .kerberos_principal
                    .or_else(|| std::env::var("KERBEROS_PRINCIPAL").ok())
                    .unwrap_or_else(|| "spark@EXAMPLE.COM".to_string()),
                keytab_path: args
                    .keytab_path
                    .or_else(|| std::env::var("KERBEROS_KEYTAB").ok()),
                krb5_config_path: args
                    .krb5_config
                    .or_else(|| std::env::var("KRB5_CONFIG").ok()),
                realm: args
                    .kerberos_realm
                    .or_else(|| std::env::var("KERBEROS_REALM").ok()),
            })
        } else {
            None
        };

        settings.history.hdfs = Some(HdfsConfig {
            namenode_url: namenode_url.clone(),
            connection_timeout_ms: Some(args.hdfs_connection_timeout),
            read_timeout_ms: Some(args.hdfs_read_timeout),
            kerberos: kerberos_config,
        });

        info!("HDFS mode enabled with namenode: {}", namenode_url);
        if settings.history.hdfs.as_ref().unwrap().kerberos.is_some() {
            info!("Kerberos authentication configured");
        }
    }

    // Handle S3 configuration from CLI arguments
    if args.s3 {
        let bucket_name = args
            .s3_bucket
            .or_else(|| std::env::var("AWS_S3_BUCKET").ok())
            .expect("S3 bucket name is required when using S3 mode");

        settings.history.s3 = Some(S3Config {
            bucket_name: bucket_name.clone(),
            region: args.s3_region.or_else(|| std::env::var("AWS_REGION").ok()),
            endpoint_url: args
                .s3_endpoint
                .or_else(|| std::env::var("AWS_ENDPOINT_URL").ok()),
            access_key_id: args
                .aws_access_key_id
                .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok()),
            secret_access_key: args
                .aws_secret_access_key
                .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok()),
            session_token: args
                .aws_session_token
                .or_else(|| std::env::var("AWS_SESSION_TOKEN").ok()),
            connection_timeout_ms: Some(args.s3_connection_timeout),
            read_timeout_ms: Some(args.s3_read_timeout),
        });

        info!("S3 mode enabled with bucket: {}", bucket_name);
        if let Some(endpoint) = &settings.history.s3.as_ref().unwrap().endpoint_url {
            info!("Using custom S3 endpoint: {}", endpoint);
        }
    }

    info!("Starting Spark History Server");
    info!("Log directory: {}", settings.history.log_directory);
    if args.s3 {
        info!("Storage backend: S3");
    } else if args.hdfs {
        info!("Storage backend: HDFS");
    } else {
        info!("Storage backend: Local filesystem");
    }

    // Initialize history provider using the factory
    use crate::storage::{StorageBackendFactory, StorageConfig};
    let storage_config = StorageConfig::DuckDB {
        database_path: settings
            .history
            .database_directory
            .as_ref()
            .map(|dir| format!("{}/events.db", dir))
            .unwrap_or_else(|| "./data/events.db".to_string()),
        num_workers: 8,
        batch_size: 5000,
    };
    let history_provider = StorageBackendFactory::create_backend(storage_config).await?;

    // Start background event processing task
    let processing_provider = history_provider.clone();
    let log_directory = settings.history.log_directory.clone();
    let update_interval = settings.history.update_interval_seconds;

    tokio::spawn(async move {
        event_processing_task(processing_provider, log_directory, update_interval).await;
    });

    // Create the web application
    let app = create_app(history_provider).await?;

    // Start the server
    let addr: SocketAddr = format!("{}:{}", settings.server.host, settings.server.port)
        .parse()
        .expect("Invalid server address");

    info!("Server listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

/// Background task that continuously scans the log directory and processes events into DuckDB
async fn event_processing_task(
    provider: storage::HistoryProvider,
    log_directory: String,
    update_interval_seconds: u64,
) {
    use std::time::Duration;
    use tokio::time::sleep;

    info!(
        "Starting background event processing task for directory: {}",
        log_directory
    );
    info!("Update interval: {} seconds", update_interval_seconds);

    let mut processed_files = HashSet::new();
    let mut event_id_counter = 1i64;

    loop {
        match scan_and_process_events(
            &provider,
            &log_directory,
            &mut processed_files,
            &mut event_id_counter,
        )
        .await
        {
            Ok(events_processed) => {
                if events_processed > 0 {
                    info!("Processed {} events in this scan", events_processed);
                }
            }
            Err(e) => {
                tracing::error!("Error during event processing: {}", e);
            }
        }

        sleep(Duration::from_secs(update_interval_seconds)).await;
    }
}

/// Scan the log directory and process new events
async fn scan_and_process_events(
    provider: &storage::HistoryProvider,
    log_directory: &str,
    processed_files: &mut HashSet<String>,
    event_id_counter: &mut i64,
) -> Result<usize> {
    use std::fs;

    let mut total_events_processed = 0;

    // Read directory contents
    let entries = match fs::read_dir(log_directory) {
        Ok(entries) => entries,
        Err(e) => {
            tracing::warn!("Failed to read log directory {}: {}", log_directory, e);
            return Ok(0);
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) => {
                tracing::warn!("Failed to read directory entry: {}", e);
                continue;
            }
        };

        let path = entry.path();

        // Skip if it's a directory or already processed
        if path.is_dir() || processed_files.contains(&path.to_string_lossy().to_string()) {
            continue;
        }

        // Skip hidden files and non-event files
        if let Some(file_name) = path.file_name() {
            let file_name_str = file_name.to_string_lossy();
            if file_name_str.starts_with('.') {
                continue;
            }
        }

        let file_path = path.to_string_lossy().to_string();
        info!("Processing event file: {}", file_path);

        match process_event_file(&file_path, provider, event_id_counter).await {
            Ok(events_count) => {
                processed_files.insert(file_path.clone());
                total_events_processed += events_count;
                info!(
                    "Successfully processed {} events from {}",
                    events_count, file_path
                );
            }
            Err(e) => {
                tracing::error!("Failed to process file {}: {}", file_path, e);
            }
        }
    }

    Ok(total_events_processed)
}

/// Process a single event file and insert events into DuckDB
async fn process_event_file(
    file_path: &str,
    provider: &storage::HistoryProvider,
    event_id_counter: &mut i64,
) -> Result<usize> {
    use crate::storage::SparkEvent;
    use serde_json::Value;
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    let mut events = Vec::new();

    // Extract app_id from filename (e.g., "app-20241201-160000-hog" from the filename)
    let app_id = std::path::Path::new(file_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string();

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();

        if trimmed.is_empty() {
            continue;
        }

        match serde_json::from_str::<Value>(trimmed) {
            Ok(json_event) => {
                match SparkEvent::from_json(&json_event, &app_id, *event_id_counter) {
                    Ok(spark_event) => {
                        events.push(spark_event);
                        *event_id_counter += 1;
                    }
                    Err(e) => {
                        tracing::warn!("Failed to parse event from {}: {}", file_path, e);
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Failed to parse JSON from {}: {}", file_path, e);
            }
        }
    }

    // Insert events in batch
    if !events.is_empty() {
        provider.insert_events_batch(events.clone()).await?;
    }

    Ok(events.len())
}
