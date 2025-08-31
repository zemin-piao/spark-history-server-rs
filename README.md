# Spark History Server (Rust)

⚠️ **This project is a Work In Progress (WIP) and not production ready** ⚠️

A high-performance Spark History Server implementation in Rust with advanced analytics capabilities. Built with DuckDB for powerful cross-application insights and optimized for large-scale Spark deployments.

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

**⚠️ Placeholder Implementation (returns empty data)**
- `GET /api/v1/applications/{appId}/jobs` - List jobs for application
- `GET /api/v1/applications/{appId}/stages` - List stages for application  
- `GET /api/v1/applications/{appId}/environment` - Get application environment
- `GET /api/v1/applications/{appId}/storage/rdd` - List RDD storage info

### Advanced Analytics Endpoints

**❌ Not Yet Integrated (code exists but not wired to router)**
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
host = "0.0.0.0"
port = 18080

[history]
# Path to Spark event logs directory (supports HDFS and local paths)
log_directory = "hdfs://namenode:9000/spark-events"
# Update interval for polling new event logs (seconds)
update_interval_seconds = 10

[storage]
# DuckDB database file path
database_path = "data/spark_history.duckdb"
# Batch size for event processing (optimizes write performance)
batch_size = 1000
# Background flush interval (seconds)
flush_interval_seconds = 30
```

## Usage

```bash
# Build and run (HDFS support included by default)
cargo build --release
./target/release/spark-history-server

# With custom configuration
./target/release/spark-history-server --config config/settings.toml

# Example: Query applications
curl "http://localhost:18080/api/v1/applications?limit=10"

# Example: Get cross-application analytics
curl "http://localhost:18080/api/v1/analytics/cross-app-summary"

# Example: Get performance trends
curl "http://localhost:18080/api/v1/analytics/performance-trends?limit=50"
```

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
HDFS Event Logs → Stream Processing → Batch Processing → DuckDB Storage
                                                              ↓
                                    REST API ← In-Memory Cache ← 
```

### Key Advantages

- **Zero Deployment Complexity**: Single binary, embedded database, no external dependencies
- **Cross-Application Analytics**: Powerful insights across all Spark applications 
- **High Write Performance**: Batched processing handles write-heavy workloads efficiently
- **Analytical Power**: DuckDB's columnar storage optimized for complex aggregations
- **Schema Evolution**: JSON flexibility handles diverse Spark event schemas gracefully