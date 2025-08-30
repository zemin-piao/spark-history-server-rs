# HDFS Integration for Spark History Server (Rust)

This document describes how to configure and use HDFS integration with the Spark History Server (Rust).

## Overview

The Spark History Server (Rust) supports reading Spark event logs from HDFS through the `hdfs-native` crate. This allows you to serve Spark application history directly from your HDFS cluster.

## Features

- ✅ Read event logs from HDFS directories
- ✅ List applications stored on HDFS
- ✅ Support for compressed event logs (gz format)
- ✅ Concurrent HDFS operations
- ✅ Error handling for network issues and missing files
- ✅ Large directory support (thousands of applications)
- ✅ Background refresh from HDFS
- ✅ Mock HDFS for testing

## Configuration

### Enable HDFS Feature

Add the HDFS feature when building:

```bash
cargo build --features hdfs
cargo test --features hdfs
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

Set the HDFS namenode URL:

```bash
export HDFS_NAMENODE_URL="hdfs://your-namenode:9000"
```

## Usage

### Running with HDFS

```bash
# Build with HDFS support
cargo build --features hdfs

# Run the server
./target/debug/spark-history-server --config config/settings.toml
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
./run_hdfs_tests.sh
```

### Real HDFS Testing

To test with a real HDFS cluster:

```bash
# Set your HDFS namenode URL
export HDFS_NAMENODE_URL="hdfs://your-namenode:9000"

# Run real HDFS tests (ignored by default)
cargo test test_real_hdfs_connection --features hdfs --test hdfs_integration_test -- --ignored
```

### Test Coverage

The HDFS integration tests cover:

- Mock HDFS file operations
- Directory listing and file existence checks
- Error handling (connection failures, missing files)
- Concurrent access patterns
- Large directory listings (1000+ applications)
- Compressed event log support
- API endpoint integration

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
[history]
# Increase for better performance with large clusters
max_applications = 5000
max_apps_per_request = 500

# Reduce refresh frequency for stable clusters  
update_interval_seconds = 600  # 10 minutes

# Enable compression to save bandwidth
compression_enabled = true
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
- Consider increasing connection pool size
- Verify HDFS cluster health

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
   ./run_hdfs_tests.sh
   ```

4. **Start the server:**
   ```bash
   cargo run --features hdfs -- --config config/settings.toml
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

### Basic HDFS Setup

```rust
use spark_history_server::{
    config::HistoryConfig,
    storage::HistoryProvider,
};

#[tokio::main]
async fn main() -> Result<()> {
    let config = HistoryConfig {
        log_directory: "hdfs://namenode:9000/spark-events".to_string(),
        max_applications: 1000,
        update_interval_seconds: 300,
        max_apps_per_request: 100,
        compression_enabled: true,
    };

    let history_provider = HistoryProvider::new(config).await?;
    let apps = history_provider.get_applications(None, None, None, None, None, None).await?;
    
    println!("Found {} applications in HDFS", apps.len());
    Ok(())
}
```

### Custom HDFS FileReader

```rust
use spark_history_server::storage::file_reader::{FileReader, HdfsFileReader};

#[tokio::main]
async fn main() -> Result<()> {
    let hdfs_reader = HdfsFileReader::new("hdfs://namenode:9000")?;
    
    let files = hdfs_reader.list_directory(&Path::new("/spark-events")).await?;
    println!("Found {} event log files", files.len());
    
    Ok(())
}
```

## Contributing

To contribute HDFS-related features:

1. Add tests to `tests/hdfs_integration_test.rs`
2. Update this documentation
3. Run the full test suite: `./run_hdfs_tests.sh`
4. Ensure backwards compatibility with local filesystem

## Limitations

- Currently only supports HDFS (not S3, GCS, etc.)
- Requires `hdfs-native` crate compatibility
- No write operations (read-only history server)
- Limited compression format support (gz only)

## Future Enhancements

- S3 and other cloud storage backends
- Advanced compression support (lz4, snappy)
- HDFS federation support
- Metrics and monitoring integration
- Advanced caching strategies