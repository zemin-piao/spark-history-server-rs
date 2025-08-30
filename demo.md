# Spark History Server (Rust) - Demo

## Quick Start

This Rust-based Spark History Server provides a high-performance, read-only API for accessing Spark application history. It's designed to be API-compatible with the original Spark History Server but focuses only on serving REST endpoints without the web UI.

## Key Features Implemented

✅ **Core Infrastructure**
- Event log parsing from JSON format
- Application metadata extraction
- In-memory caching with background refresh
- REST API with JSON responses
- Configuration management
- Local filesystem support
- Optional HDFS support (via hdfs-native crate)

✅ **API Endpoints**
- `GET /api/v1/applications` - List all applications with filtering
- `GET /api/v1/applications/{app_id}` - Get specific application info
- `GET /api/v1/version` - Get server version
- `GET /health` - Health check endpoint

✅ **Data Models**
- ApplicationInfo with attempts
- ApplicationAttemptInfo with timestamps
- ApplicationStatus enum (Running/Completed)
- Comprehensive application metadata

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   File System   │    │ History Provider │    │   REST API      │
│   (Local/HDFS)  │◄──►│                 │◄──►│   (Axum)        │
│                 │    │  - Event Parser │    │                 │
│  Event Logs     │    │  - App Cache    │    │  JSON Responses │
└─────────────────┘    └─────────────────┘    └─────────────────┘
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
# List all applications
curl http://localhost:18080/api/v1/applications

# Get specific application
curl http://localhost:18080/api/v1/applications/app-20231120120000-0001

# Get version info
curl http://localhost:18080/api/v1/version

# Health check
curl http://localhost:18080/health
```

### 3. Sample Response

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

## Configuration

The server can be configured via `config/settings.toml`:

```toml
[server]
host = "0.0.0.0"
port = 18080
max_applications = 1000

[history]
log_directory = "/tmp/spark-events"
max_applications = 1000
update_interval_seconds = 10
max_apps_per_request = 100
compression_enabled = true
```

## Performance Benefits

- **Memory Efficient**: Only stores application metadata, not full event histories
- **Fast Startup**: Background scanning with immediate API availability
- **Concurrent**: Async I/O with configurable thread pools
- **Scalable**: Designed to handle thousands of applications
- **Safe**: Read-only operations ensure data integrity

## Comparison with Original

| Feature | Original Spark History Server | Rust Implementation |
|---------|-------------------------------|-------------------|
| Web UI | ✅ Full HTML interface | ❌ API only |
| REST API | ✅ Complete v1 API | ✅ Core endpoints |
| Event Log Parsing | ✅ Full parsing | ✅ Metadata extraction |
| Storage | ✅ Local FS + HDFS | ✅ Local FS + HDFS |
| Performance | 🐌 JVM overhead | ⚡ Native performance |
| Memory Usage | 🐘 High | 🪶 Low |
| Startup Time | 🐌 Slow | ⚡ Fast |

## Next Steps for Full Feature Parity

1. **Extended Event Parsing**
   - Job information extraction
   - Stage and task details
   - Executor metrics
   - RDD storage information

2. **Advanced Filtering**
   - Date range filtering
   - Status-based filtering
   - User-based filtering

3. **Additional Endpoints**
   - Jobs, stages, executors endpoints
   - Environment information
   - Event log download

4. **Production Features**
   - Metrics and monitoring
   - Security (ACLs)
   - Event log compaction
   - Hybrid storage (disk + memory)

## Why Rust?

- **Performance**: Near-C performance with memory safety
- **Concurrency**: Excellent async/await support via Tokio
- **Reliability**: Type safety prevents common bugs
- **Ecosystem**: Rich crates for web services, serialization, etc.
- **Deployment**: Single binary with no runtime dependencies