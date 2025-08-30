use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod api;
mod config;
mod models;
mod storage;

use crate::api::create_app;
use crate::config::Settings;
use crate::storage::HistoryProvider;

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

    info!("Starting Spark History Server");
    info!("Log directory: {}", settings.history.log_directory);

    // Initialize history provider
    let history_provider = HistoryProvider::new(settings.history.clone()).await?;

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