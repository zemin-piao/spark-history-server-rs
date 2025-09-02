use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod analytics_api;
mod api;
mod config;
mod dashboard;
mod models;
mod storage;

use crate::api::create_app;
use crate::config::{HdfsConfig, KerberosConfig, Settings};
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

    info!("Starting Spark History Server");
    info!("Log directory: {}", settings.history.log_directory);
    if args.hdfs {
        info!("Storage backend: HDFS");
    } else {
        info!("Storage backend: Local filesystem");
    }

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
