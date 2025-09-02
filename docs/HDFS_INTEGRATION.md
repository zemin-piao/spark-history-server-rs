# HDFS Integration for Spark History Server (Rust)

This document describes how to configure and use HDFS integration with the Spark History Server (Rust).

## Overview

The Spark History Server (Rust) supports reading Spark event logs from HDFS through the `hdfs-native` crate. This allows you to serve Spark application history directly from your HDFS cluster.

## Features

- ✅ Read event logs from HDFS directories
- ✅ List applications stored on HDFS  
- ✅ Support for compressed event logs (.lz4, .snappy, .gz)
- ✅ Concurrent HDFS operations with connection pooling
- ✅ Error handling for network issues and missing files
- ✅ Large directory support (thousands of applications)
- ✅ Incremental scanning - only process new/modified files
- ✅ Background refresh from HDFS with configurable intervals
- ✅ DuckDB storage integration for analytics
- ✅ Comprehensive testing framework

## Configuration

### HDFS Support

HDFS support is built-in by default via the `hdfs-native` crate:

```bash
# Build with HDFS support (default)
cargo build --release

# Run tests
cargo test
```

### HDFS Configuration

Update your `settings.toml` to point to HDFS:

```toml
[history]
log_directory = "hdfs://namenode:9000/spark-events"
max_applications = 1000
update_interval_seconds = 300  # 5 minutes
max_apps_per_request = 100
compression_enabled = true
```

### Environment Variables

Optional environment variables for HDFS configuration:

```bash
# HDFS namenode URL (can also be set in config)
export HDFS_NAMENODE_URL="hdfs://your-namenode:9000"

# Enable debug logging for HDFS operations
export RUST_LOG="spark_history_server=debug,hdfs_native=debug"
```

## Usage

### Running with HDFS

```bash
# Build the server
cargo build --release

# Run with HDFS configuration
./target/release/spark-history-server --config config/settings.toml

# For development with debug logging
RUST_LOG=info cargo run -- --config config/settings.toml
```

### HDFS Path Formats

The server supports various HDFS path formats:

```
hdfs://namenode:9000/spark-events/
hdfs://namenode:9000/user/spark/logs/
hdfs://cluster/spark-history/
```

### Directory Structure

The server expects one of these directory structures on HDFS:

```
/spark-events/
├── app-20231120120000-0001/
│   └── eventLog
├── app-20231120120001-0002/
│   └── eventLog
└── app-20231120120002-0003.inprogress
```

Or flat structure:

```
/spark-events/
├── app-20231120120000-0001_eventLog
├── app-20231120120001-0002_eventLog.gz
└── app-20231120120002-0003.inprogress
```

## Testing

### Mock HDFS Testing

Run HDFS tests without a real cluster:

```bash
./scripts/run_hdfs_tests.sh
```

### Real HDFS Testing

To test with a real HDFS cluster:

```bash
# Set your HDFS namenode URL in config or environment
export HDFS_NAMENODE_URL="hdfs://your-namenode:9000"

# Run HDFS integration tests
cargo test --test hdfs_integration_test

# Run specific HDFS tests with logging
RUST_LOG=debug cargo test test_hdfs_operations --test hdfs_integration_test -- --nocapture
```

### Test Coverage

The HDFS integration tests cover:

- HDFS file operations and directory listing
- Event log parsing from HDFS sources
- Incremental scanning and change detection
- Error handling (connection failures, missing files, timeouts)
- Concurrent access patterns and connection pooling
- Large directory listings (1000+ applications)
- Compressed event log support (.lz4, .snappy, .gz)
- DuckDB integration with HDFS data sources
- API endpoint integration and response validation
- Performance testing with large datasets

## Performance Considerations

### Caching

- Event logs are cached in memory after first read
- Background refresh respects the `update_interval_seconds` setting
- Large directories are handled efficiently with streaming

### Network Optimization

- Use connection pooling in hdfs-native
- Set appropriate timeout values
- Consider HDFS local reads when possible

### Configuration Tuning

```toml
[server]
host = "0.0.0.0"
port = 18080
max_applications = 5000

[history]
log_directory = "hdfs://namenode:9000/spark-events"
max_applications = 5000
max_apps_per_request = 500
# Reduce refresh frequency for stable clusters
update_interval_seconds = 300  # 5 minutes
compression_enabled = true
cache_directory = "./data"
enable_cache = true
```

## Troubleshooting

### Connection Issues

```
Error: Failed to connect to HDFS namenode
```

- Check namenode URL is correct
- Verify network connectivity
- Ensure HDFS cluster is running

### Permission Issues

```
Error: Access denied to HDFS path
```

- Check HDFS permissions for the user running the server
- Verify the service user has read access to event log directories

### Performance Issues

```
Warning: HDFS operations are slow
```

- Check network latency to HDFS cluster
- Verify HDFS cluster health and load
- Consider tuning `update_interval_seconds` for less frequent scans
- Enable local caching with `enable_cache = true`
- Monitor DuckDB write performance with `RUST_LOG=debug`

### Missing Applications

```
Info: No applications found in HDFS directory
```

- Verify the directory path is correct
- Check if applications are in subdirectories
- Ensure event logs are in expected format

## Migration from Local Storage

To migrate from local filesystem to HDFS:

1. **Copy existing event logs to HDFS:**
   ```bash
   hdfs dfs -put /local/spark-events/* /hdfs/spark-events/
   ```

2. **Update configuration:**
   ```toml
   log_directory = "hdfs://namenode:9000/spark-events"
   ```

3. **Test the migration:**
   ```bash
   cargo test --features hdfs
   ./scripts/run_hdfs_tests.sh
   ```

4. **Start the server:**
   ```bash
   cargo run -- --config config/settings.toml
   ```

## API Compatibility

All existing REST API endpoints work unchanged with HDFS backend:

- `GET /api/v1/applications` - List applications from HDFS
- `GET /api/v1/applications/{id}` - Get specific application from HDFS  
- `GET /api/v1/applications/{id}/jobs` - Get jobs for HDFS application
- `GET /health` - Health check includes HDFS connectivity
- `GET /api/v1/version` - Version info unchanged

## Security

### Authentication

HDFS authentication is handled by the underlying `hdfs-native` crate:

- Kerberos authentication supported
- Username/password authentication
- Token-based authentication

### Authorization

Ensure proper HDFS ACLs are set:

```bash
# Example: Grant read access to spark-history user
hdfs dfsadmin -setSpaceQuota /spark-events spark-history:r-x
```

## Examples

### Basic HDFS Setup with Analytics

```rust
use spark_history_server::{
    config::{HistoryConfig, ServerConfig, Settings},
    storage::duckdb_store::DuckDBStore,
};
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings {
        server: ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 18080,
            max_applications: 1000,
        },
        history: HistoryConfig {
            log_directory: "hdfs://namenode:9000/spark-events".to_string(),
            max_applications: 1000,
            update_interval_seconds: 300,
            max_apps_per_request: 100,
            compression_enabled: true,
            cache_directory: "./data".to_string(),
            enable_cache: true,
        },
    };

    // Initialize DuckDB store with HDFS backend
    let store = DuckDBStore::new("./data/spark_history.duckdb").await?;
    
    // Start incremental scanning from HDFS
    // This will process event logs and populate DuckDB for analytics
    
    println!("Spark History Server ready with HDFS + DuckDB analytics");
    Ok(())
}
```

### HDFS File Reader Integration

```rust
use spark_history_server::storage::file_reader::{FileReader, create_file_reader};
use std::path::Path;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Create appropriate file reader based on URI scheme
    let reader = create_file_reader("hdfs://namenode:9000/spark-events").await?;
    
    // List all event log files
    let files = reader.list_directory(Path::new("/spark-events")).await?;
    println!("Found {} event log files in HDFS", files.len());
    
    // Read a specific event log file
    if let Some(first_file) = files.first() {
        let content = reader.read_file_to_string(&first_file.path).await?;
        println!("First event log size: {} bytes", content.len());
    }
    
    Ok(())
}
```

## Contributing

To contribute HDFS-related features:

1. Add tests to `tests/hdfs_integration_test.rs`
2. Update this documentation
3. Run the full test suite: `./scripts/run_hdfs_tests.sh`
4. Ensure backwards compatibility with local filesystem

## Current Limitations

- Read-only operations (no event log writes or modifications)
- Requires network connectivity to HDFS cluster
- Limited to `hdfs-native` crate capabilities
- No built-in HDFS authentication management (relies on system configuration)
- Single-threaded incremental scanning (parallelization planned)

## Supported Features

- ✅ Multiple compression formats (.lz4, .snappy, .gz)
- ✅ Large directory handling with streaming
- ✅ Connection pooling and error recovery
- ✅ Incremental processing of only changed files
- ✅ DuckDB integration for cross-application analytics
- ✅ Comprehensive monitoring and logging

## Future Enhancements

### Storage Backends
- S3 and other cloud storage support
- Azure Blob Storage integration
- Google Cloud Storage support
- Local filesystem optimizations

### Performance & Scalability
- Parallel event log processing
- Advanced caching strategies
- HDFS federation support
- Connection pooling improvements
- Query result caching

### Enterprise Features
- HDFS authentication integration (Kerberos)
- Access control and security policies
- Metrics and monitoring dashboards
- Event log retention policies
- Multi-tenant support

### Analytics Enhancements
- Real-time streaming analytics
- Custom metric definitions
- Advanced visualization endpoints
- Historical trend analysis
- Predictive performance insights