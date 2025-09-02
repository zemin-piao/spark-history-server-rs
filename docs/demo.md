# Spark History Server (Rust) - Demo Guide

## Overview

This Rust-based Spark History Server provides a high-performance, analytics-focused API for accessing Spark application history. Built with DuckDB for advanced cross-application analytics, it offers both standard Spark History Server API v1 compatibility and powerful new analytics endpoints.

## Current Implementation Status

✅ **Analytics Engine**
- DuckDB-based storage for cross-application analytics
- Incremental event log scanning and processing
- Batched writes for optimal performance
- Schema flexibility with hot field extraction + JSON fallback

✅ **Standard API Endpoints**
- `GET /api/v1/applications` - List applications with filtering
- `GET /api/v1/applications/{app_id}` - Get application details
- `GET /api/v1/applications/{app_id}/executors` - List executors
- `GET /api/v1/version` - Server version
- `GET /health` - Health check

✅ **Advanced Analytics Endpoints**
- `GET /api/v1/analytics/cross-app-summary` - Cross-application statistics
- `GET /api/v1/analytics/performance-trends` - Performance metrics over time
- `GET /api/v1/analytics/resource-usage` - Resource utilization trends
- `GET /api/v1/analytics/task-distribution` - Task distribution analysis
- `GET /api/v1/analytics/executor-utilization` - Executor metrics

✅ **Storage & Processing**
- DuckDB embedded analytical database
- Local filesystem and HDFS support
- Comprehensive Spark event parsing
- Incremental processing of new event logs

## Architecture

```
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│ Event Log Sources    │    │   Processing Engine   │    │    API Layer         │
│ (Local/HDFS)        │───►│                     │───►│                     │
│                     │    │  • Incremental Scan  │    │  • Standard API v1   │
│  • Spark Event Logs │    │  • Event Parsing     │    │  • Analytics API     │
│  • .lz4/.snappy     │    │  • Batch Processing  │    │  • Health Endpoints  │
│  • Compressed       │    │                     │    │                     │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
                                                      │
                               ┌─────────────────────────────────────────────┐
                               │           DuckDB Storage Engine              │
                               │                                             │
                               │  • Columnar storage for fast analytics   │
                               │  • Hot fields + JSON for flexibility     │
                               │  • Cross-application SQL queries        │
                               │  • Time-series optimized indexing       │
                               └─────────────────────────────────────────────┘
```

## Quick Demo

### 1. Build and Run

```bash
# Build the project
cargo build --release

# Run with default configuration (uses /tmp/spark-events)
./target/release/spark-history-server

# Run with custom config
./target/release/spark-history-server --config config/settings.toml --port 18080
```

### 2. Test the API

```bash
# Health check
curl http://localhost:18080/health

# List all applications
curl http://localhost:18080/api/v1/applications

# Get specific application
curl http://localhost:18080/api/v1/applications/app-20231120120000-0001

# Get version info
curl http://localhost:18080/api/v1/version

# Advanced analytics endpoints
curl http://localhost:18080/api/v1/analytics/cross-app-summary
curl http://localhost:18080/api/v1/analytics/performance-trends
curl http://localhost:18080/api/v1/analytics/resource-usage
curl http://localhost:18080/api/v1/analytics/task-distribution
curl http://localhost:18080/api/v1/analytics/executor-utilization
```

### 3. Sample Responses

#### Standard Application Listing

```json
[
  {
    "id": "app-20231120120000-0001",
    "name": "SparkPi",
    "coresGranted": null,
    "maxCores": null,
    "coresPerExecutor": null,
    "memoryPerExecutorMB": null,
    "attempts": [
      {
        "attemptId": "1",
        "startTime": "2023-11-20T12:00:00.000Z",
        "endTime": "2023-11-20T12:00:04.000Z",
        "lastUpdated": "2023-11-20T12:00:04.000Z",
        "duration": 4000,
        "sparkUser": "spark",
        "completed": true,
        "appSparkVersion": "3.5.0",
        "startTimeEpoch": 1700486400000,
        "endTimeEpoch": 1700486404000,
        "lastUpdatedEpoch": 1700486404000
      }
    ]
  }
]
```

#### Advanced Analytics Response

```json
{
  "cross_app_summary": {
    "total_applications": 47,
    "total_jobs": 234,
    "total_stages": 1156,
    "total_tasks": 8934,
    "avg_duration_ms": 28945,
    "total_cpu_time_ms": 1356780000,
    "avg_cpu_time_per_task_ms": 151853
  },
  "performance_trends": [
    {
      "date": "2023-11-20",
      "avg_duration_ms": 25678,
      "task_count": 1247,
      "success_rate": 0.987
    }
  ],
  "resource_usage": {
    "total_executor_hours": 1247.5,
    "avg_cpu_utilization": 0.78,
    "peak_memory_usage_gb": 156.7
  }
}
```

## Configuration

The server can be configured via `config/settings.toml`:

```toml
[server]
host = "127.0.0.1"
port = 18080
max_applications = 1000

[history]
log_directory = "/tmp/spark-events"  # or "hdfs://namenode:9000/spark-events"
max_applications = 1000
update_interval_seconds = 60
max_apps_per_request = 100
compression_enabled = true
cache_directory = "./data"
enable_cache = true
```

## Performance Benefits

- **Analytical Power**: DuckDB's columnar storage enables complex cross-application analytics
- **Incremental Processing**: Only processes new/modified event logs for optimal efficiency
- **Batched Writes**: Optimized batch processing for high-throughput scenarios
- **Schema Flexibility**: Hot field extraction + JSON fallback handles diverse event types
- **Fast Queries**: Indexed storage for sub-second response times
- **Memory Efficient**: Embedded database with intelligent memory management
- **Zero Dependencies**: Single binary deployment with no external requirements

## Comparison with Original Spark History Server

| Feature | Original Spark History Server | Rust Implementation |
|---------|-------------------------------|-------------------|
| **Web UI** | ✅ Full HTML interface | ❌ API only |
| **REST API** | ✅ Standard v1 API | ✅ v1 + Advanced Analytics |
| **Cross-App Analytics** | ❌ Limited | ✅ Full SQL analytics |
| **Storage Backend** | 📋 KV store/Memory | ✅ DuckDB (analytical) |
| **Event Processing** | 🐌 Full parsing | ✅ Incremental + batched |
| **Performance** | 🐌 JVM overhead | ⚡ Native Rust |
| **Memory Usage** | 🐘 High JVM heap | 🪶 Minimal footprint |
| **Startup Time** | 🐌 30-60s | ⚡ <5s |
| **Deployment** | 📦 Complex (Java + deps) | ✅ Single binary |
| **Analytics Queries** | ❌ Basic aggregations | ✅ Complex SQL analysis |

## Testing the Analytics Features

### Cross-Application Analytics

```bash
# Get summary statistics across all applications
curl "http://localhost:18080/api/v1/analytics/cross-app-summary" | jq

# Example response:
{
  "total_applications": 150,
  "total_jobs": 1247,
  "total_stages": 5893,
  "total_tasks": 28456,
  "avg_duration_ms": 45230,
  "total_cpu_time_ms": 3567890,
  "total_shuffle_read_bytes": 1048576000,
  "total_shuffle_write_bytes": 524288000
}
```

### Performance Trends

```bash
# Get performance trends over time
curl "http://localhost:18080/api/v1/analytics/performance-trends?limit=50" | jq

# Analyze resource utilization patterns
curl "http://localhost:18080/api/v1/analytics/resource-usage?time_range=7d" | jq
```

### Task Distribution Analysis

```bash
# Analyze task locality and distribution
curl "http://localhost:18080/api/v1/analytics/task-distribution" | jq

# Get executor utilization metrics
curl "http://localhost:18080/api/v1/analytics/executor-utilization" | jq
```

## Production Deployment

### Docker Setup

```dockerfile
FROM rust:1.70 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/spark-history-server /usr/local/bin/
EXPOSE 18080
CMD ["spark-history-server"]
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spark-history-server
spec:
  replicas: 1
  selector:
    matchLabels:
      app: spark-history-server
  template:
    metadata:
      labels:
        app: spark-history-server
    spec:
      containers:
      - name: spark-history-server
        image: spark-history-server:latest
        ports:
        - containerPort: 18080
        env:
        - name: RUST_LOG
          value: "info"
        volumeMounts:
        - name: config
          mountPath: /app/config
        - name: data
          mountPath: /app/data
      volumes:
      - name: config
        configMap:
          name: spark-history-config
      - name: data
        persistentVolumeClaim:
          claimName: spark-history-data
```

## Why Rust + DuckDB?

### Rust Advantages
- **Performance**: Native performance without JVM overhead
- **Memory Safety**: Zero-cost abstractions with compile-time guarantees
- **Concurrency**: Excellent async/await support via Tokio
- **Reliability**: Type system prevents common runtime errors
- **Ecosystem**: Rich crates for web services, databases, serialization
- **Deployment**: Single binary with no runtime dependencies

### DuckDB Benefits
- **Analytical Workloads**: Optimized for OLAP queries and aggregations
- **Columnar Storage**: Efficient compression and query performance
- **SQL Compatibility**: Full SQL support for complex analytics
- **Embedded**: No separate database server required
- **Cross-Platform**: Works on all major operating systems
- **Memory Efficient**: Intelligent memory management and query optimization

### The Perfect Combination
- **Zero Deployment**: Single binary + embedded database
- **High Performance**: Native code + analytical database engine
- **Rich Analytics**: Complex SQL queries across all Spark applications
- **Production Ready**: Type safety + comprehensive testing
- **Maintainable**: Clean architecture with excellent tooling