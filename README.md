# Spark History Server (Rust)

A high-performance, read-only Spark History Server API implementation in Rust. This server provides REST API endpoints for querying Spark application history without serving the web UI components.

## Features

- **API-Only**: Exposes only REST API endpoints (no web UI)
- **Read-Only**: Safe for production environments
- **High Performance**: Built with Rust and async I/O
- **Compatible**: API-compatible with Apache Spark History Server v1 API
- **Event Log Support**: Reads compressed and uncompressed Spark event logs
- **Hadoop Integration**: Supports HDFS and local file systems

## API Endpoints

The server implements the Spark History Server REST API v1:

### Application List
- `GET /api/v1/applications` - List all applications
  - Query parameters: `status`, `minDate`, `maxDate`, `minEndDate`, `maxEndDate`, `limit`

### Application Details
- `GET /api/v1/applications/{appId}` - Get application info
- `GET /api/v1/applications/{appId}/jobs` - List jobs for application
- `GET /api/v1/applications/{appId}/stages` - List stages for application
- `GET /api/v1/applications/{appId}/executors` - List executors for application
- `GET /api/v1/applications/{appId}/environment` - Get application environment
- `GET /api/v1/applications/{appId}/storage/rdd` - List RDD storage info

### System
- `GET /api/v1/version` - Get Spark version info

## Configuration

Configuration is provided via `config/settings.toml`:

```toml
[server]
host = "0.0.0.0"
port = 18080

[history]
# Path to Spark event logs directory
log_directory = "/tmp/spark-events"
# Maximum number of applications to retain in memory
max_applications = 1000
# Update interval in seconds
update_interval_seconds = 10
```

## Usage

```bash
# Install and run
cargo build --release
./target/release/spark-history-server

# With custom config
./target/release/spark-history-server --config config/settings.toml

# Enable HDFS support
cargo build --release --features hdfs
```

## Architecture

- **Event Log Reader**: Parses Spark event logs in JSON format
- **Application Cache**: In-memory cache of application metadata
- **REST API**: Axum-based web server with JSON responses
- **File System Abstraction**: Supports local FS and HDFS