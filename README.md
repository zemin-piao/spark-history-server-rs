# Spark History Server (Rust)

⚠️ **This project is a Work In Progress (WIP) and not production ready** ⚠️

A high-performance Spark History Server implementation in Rust with advanced analytics capabilities. Built with DuckDB for powerful cross-application insights and optimized for large-scale Spark deployments.

## Current Status

- ✅ **Core Infrastructure**: Event log parsing, DuckDB integration, incremental scanning
- ✅ **Standard Spark History Server API v1**: Basic application endpoints implemented
- ✅ **Analytics Engine**: Advanced cross-application analytics with DuckDB
- ✅ **Storage Layer**: DuckDB-based storage with batched writes and caching
- ✅ **HDFS Support**: Native HDFS integration via `hdfs-native`
- 🚧 **Web UI**: Not implemented (API-only service)
- 🚧 **Production Features**: Metrics, monitoring, security features planned

## Features

- **Advanced Analytics**: Cross-application performance insights and resource usage analytics
- **DuckDB Backend**: Embedded analytical database for fast aggregations and complex queries
- **HDFS Native Support**: Direct HDFS integration using `hdfs-native` for optimal performance
- **Event Stream Processing**: Efficient batch processing of Spark event logs with compression support
- **REST API**: Comprehensive API endpoints including standard Spark History Server v1 compatibility
- **Zero Deployment**: Single binary with embedded database - no external dependencies
- **High Performance**: Async I/O with batched writes and in-memory caching for hot data

## API Endpoints

### Standard Spark History Server API v1

**✅ Fully Implemented**
- `GET /api/v1/applications` - List all applications with filtering support
  - Query parameters: `status`, `minDate`, `maxDate`, `minEndDate`, `maxEndDate`, `limit`
- `GET /api/v1/applications/{appId}` - Get application details
- `GET /api/v1/applications/{appId}/executors` - List executors for application
- `GET /api/v1/version` - Get version information

**⚠️ Placeholder Implementation (returns mock data)**
- `GET /api/v1/applications/{appId}/jobs` - List jobs for application
- `GET /api/v1/applications/{appId}/stages` - List stages for application  
- `GET /api/v1/applications/{appId}/environment` - Get application environment
- `GET /api/v1/applications/{appId}/storage/rdd` - List RDD storage info

### Advanced Analytics Endpoints

**✅ Implemented and Active**
- `GET /api/v1/analytics/resource-usage` - Resource usage trends across applications
- `GET /api/v1/analytics/performance-trends` - Performance metrics over time
- `GET /api/v1/analytics/cross-app-summary` - Cross-application summary statistics  
- `GET /api/v1/analytics/task-distribution` - Task distribution and locality analysis
- `GET /api/v1/analytics/executor-utilization` - Executor utilization metrics

### System Health

**✅ Fully Implemented**
- `GET /health` - Health check endpoint

## Configuration

Configuration is provided via `config/settings.toml`:

```toml
[server]
host = "127.0.0.1"
port = 18080
max_applications = 1000

[history]
# Path to Spark event logs directory (supports HDFS and local paths)
log_directory = "/tmp/spark-events"  # or "hdfs://namenode:9000/spark-events"
max_applications = 1000
# Update interval for polling new event logs (seconds)
update_interval_seconds = 60
max_apps_per_request = 100
compression_enabled = true
# Local cache for processed data
cache_directory = "./data"
enable_cache = true
```

### Storage Configuration

The DuckDB storage is automatically configured and managed internally. Key features:
- **Database Location**: `./data/spark_history.duckdb` (created automatically)
- **Batch Processing**: Events are processed in configurable batches for optimal write performance
- **Incremental Scanning**: Only new/modified event logs are processed
- **Schema Evolution**: Flexible schema handles diverse Spark event types

## Quick Start

### 1. Build and Run

```bash
# Build the project
cargo build --release

# Run with default configuration
./target/release/spark-history-server

# Run with custom configuration
./target/release/spark-history-server --config config/settings.toml

# For development
cargo run
```

### 2. Test the API

```bash
# Health check
curl http://localhost:18080/health

# List applications
curl "http://localhost:18080/api/v1/applications?limit=10"

# Get specific application
curl "http://localhost:18080/api/v1/applications/{app_id}"

# Advanced analytics
curl "http://localhost:18080/api/v1/analytics/cross-app-summary"
curl "http://localhost:18080/api/v1/analytics/performance-trends"
curl "http://localhost:18080/api/v1/analytics/resource-usage"
```

### 3. HDFS Setup (Optional)

For HDFS integration:

```toml
# Update config/settings.toml
[history]
log_directory = "hdfs://namenode:9000/spark-events"
```

See [HDFS_INTEGRATION.md](HDFS_INTEGRATION.md) for detailed HDFS configuration.

## Architecture

The Spark History Server is built around a modern, analytics-first architecture designed for large-scale Spark deployments:

### Core Components

1. **Log Processing Engine**
   - **HDFS Integration**: Native HDFS support using `hdfs-native` for efficient access to event logs
   - **Event Stream Processing**: Asynchronous processing of compressed event logs (.lz4, .snappy)
   - **Batch Processing**: Optimized batch writes to DuckDB for high throughput
   - **Schema Flexibility**: Handles 10+ Spark event types with hot field extraction + JSON fallback

2. **DuckDB Storage Layer** 
   - **Embedded Analytical Database**: Single-file database optimized for analytics workloads
   - **Columnar Storage**: Fast aggregations and time-series queries across applications
   - **Flexible Schema**: Combined structured fields + JSON column for event diversity
   - **Cross-Application Queries**: SQL analytics across all Spark applications simultaneously

3. **REST API Server**
   - **Dual API Support**: Standard Spark History Server v1 + advanced analytics endpoints  
   - **In-Memory Caching**: Hot data caching for frequently accessed applications
   - **Async I/O**: Built with Axum and Tokio for high concurrency

### Data Flow

```
┌─────────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   HDFS/Storage  │ -> │  Event Processor │ -> │   DuckDB Store   │ -> │   API Server    │
│                 │    │                  │    │                  │    │                 │
│ • Event Logs    │    │ • Incremental    │    │ • Events Table   │    │ • REST API      │
│ • .lz4/.snappy  │    │   Scanning       │    │ • JSON + Hot     │    │ • Analytics     │
│ • .inprogress   │    │ • Batch Writing  │    │   Fields         │    │ • Cross-App     │
└─────────────────┘    └──────────────────┘    └──────────────────┘    └─────────────────┘
```

### Key Advantages

- **Zero Deployment Complexity**: Single binary, embedded database, no external dependencies
- **Cross-Application Analytics**: SQL-powered insights across all Spark applications simultaneously
- **High Write Performance**: Incremental scanning + batched processing for optimal throughput
- **Analytical Power**: DuckDB's columnar storage optimized for complex aggregations and time-series
- **Schema Flexibility**: Hot field extraction + JSON fallback handles diverse event schemas
- **Production Ready**: Comprehensive testing, error handling, and monitoring hooks

## Testing

```bash
# Run all tests
cargo test

# Run specific test suites
cargo test --test integration_test
cargo test --test analytics_api_test
cargo test --test incremental_scan_test

# Run with detailed logging
RUST_LOG=info cargo test test_full_incremental_scan_workflow --test incremental_scan_test -- --nocapture
```

## Development

### Key Components

- **`src/storage/duckdb_store.rs`**: Core DuckDB integration and analytics engine
- **`src/event_processor.rs`**: Spark event log parsing and processing
- **`src/api.rs`**: Standard Spark History Server API v1 endpoints
- **`src/analytics_api.rs`**: Advanced cross-application analytics endpoints
- **`src/storage/file_reader.rs`**: File system abstraction (local + HDFS)

### Adding New Analytics

1. Add SQL query to `src/analytics_api.rs`
2. Define response model in `src/models.rs`
3. Add tests to `tests/analytics_api_test.rs`
4. Update API documentation

### Performance Tuning

- **Batch Size**: Adjust event processing batch size via configuration
- **Update Interval**: Balance freshness vs. system load
- **Cache Settings**: Enable local caching for frequently accessed data
- **DuckDB Settings**: Tune memory usage and query optimization