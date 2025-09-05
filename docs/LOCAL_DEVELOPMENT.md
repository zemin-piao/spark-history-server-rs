# Local Development Setup

This guide helps you set up a complete local development environment with S3 (MinIO) and HDFS for testing the Spark History Server.

## Quick Start

```bash
# Start the development environment
./scripts/setup-dev-environment.sh

# Verify integrations
./scripts/verify-integrations.sh

# Clean up when done
./scripts/cleanup-dev-environment.sh
```

## Services Overview

### MinIO (S3-Compatible Storage)
- **Console**: http://localhost:9001 (UI for managing buckets)
- **API**: http://localhost:9000 (S3-compatible endpoint)
- **Credentials**: `minioadmin` / `minioadmin123`
- **Bucket**: `spark-events` (auto-created)

### HDFS (Hadoop Distributed File System)
- **Namenode UI**: http://localhost:9870 (cluster status and file browser)
- **Datanode UI**: http://localhost:9864 (datanode status)
- **RPC Endpoint**: `hdfs://localhost:8020`
- **Event Directory**: `/spark-events`

## Prerequisites

- Docker and Docker Compose
- Rust and Cargo
- curl (for health checks)

## Manual Setup Steps

### 1. Start Services

```bash
docker-compose up -d
```

Wait for all services to be healthy:
- MinIO: http://localhost:9000/minio/health/live
- HDFS Namenode: http://localhost:9870
- HDFS Datanode: http://localhost:9864

### 2. Initialize Storage

```bash
# Create HDFS directories
docker exec spark-history-hdfs-namenode hdfs dfs -mkdir -p /spark-events

# Upload test data
./scripts/upload-test-data.sh
```

### 3. Run Application

#### With MinIO (S3):
```bash
cargo run -- \
  --s3 \
  --s3-bucket spark-events \
  --s3-endpoint http://localhost:9000 \
  --s3-access-key minioadmin \
  --s3-secret-key minioadmin123
```

#### With HDFS:
```bash
cargo run -- \
  --hdfs \
  --hdfs-namenode hdfs://localhost:8020 \
  --log-directory /spark-events
```

## Testing

### Unit Tests
```bash
# S3 integration tests (mock)
cargo test s3_integration_test

# HDFS integration tests (mock)
cargo test hdfs_integration_test
```

### Integration Tests with Real Services

#### MinIO (S3) Tests:
```bash
export S3_TEST_BUCKET=spark-events
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin123

cargo test test_real_s3 --ignored
```

#### HDFS Tests:
```bash
export HDFS_NAMENODE_URL=hdfs://localhost:8020
export SPARK_EVENTS_DIR=/spark-events

cargo test test_real_hdfs --ignored
```

### Full Integration Verification
```bash
./scripts/verify-integrations.sh
```

## Configuration Examples

### config/local-s3.toml
```toml
[server]
host = "0.0.0.0"
port = 18080

[s3]
bucket_name = "spark-events"
region = "us-east-1"
endpoint_url = "http://localhost:9000"
access_key_id = "minioadmin"
secret_access_key = "minioadmin123"
connection_timeout_ms = 30000
read_timeout_ms = 60000

[history]
update_interval_seconds = 30
```

### config/local-hdfs.toml
```toml
[server]
host = "0.0.0.0"
port = 18080

[hdfs]
namenode_url = "hdfs://localhost:8020"
connection_timeout_ms = 30000
read_timeout_ms = 60000

[history]
log_directory = "/spark-events"
update_interval_seconds = 30
```

## Troubleshooting

### MinIO Issues

**Connection Refused:**
```bash
# Check MinIO status
docker logs spark-history-minio
curl http://localhost:9000/minio/health/live
```

**Authentication Errors:**
- Verify credentials: `minioadmin` / `minioadmin123`
- Check endpoint URL: `http://localhost:9000`

### HDFS Issues

**Namenode Not Ready:**
```bash
# Check HDFS status
docker logs spark-history-hdfs-namenode
curl http://localhost:9870
```

**Connection Timeout:**
- Ensure both namenode and datanode are running
- Check port mapping: `8020` for RPC, `9870` for web UI

### Application Issues

**Circuit Breaker Open:**
- Wait for circuit breaker to reset (30 seconds)
- Check service health endpoints
- Verify network connectivity

**File Not Found Errors:**
```bash
# Verify test data exists in MinIO
mc ls local-minio/spark-events --recursive

# Verify test data exists in HDFS  
docker exec spark-history-hdfs-namenode hdfs dfs -ls -R /spark-events
```

## Development Workflow

1. **Start Environment**: `./scripts/setup-dev-environment.sh`
2. **Code Changes**: Make your changes to the Rust code
3. **Test Locally**: 
   ```bash
   # Test with MinIO
   cargo run -- --s3 --s3-bucket spark-events --s3-endpoint http://localhost:9000 --s3-access-key minioadmin --s3-secret-key minioadmin123
   
   # Or test with HDFS
   cargo run -- --hdfs --hdfs-namenode hdfs://localhost:8020 --log-directory /spark-events
   ```
4. **Run Tests**: `./scripts/verify-integrations.sh`
5. **Clean Up**: `./scripts/cleanup-dev-environment.sh`

## Performance Testing

Load test your changes against the local environment:

```bash
# Start with larger dataset
./scripts/upload-test-data.sh

# Run performance tests
cargo test --release test_100k_applications_load
./scripts/run_load_tests.sh
```

## Data Management

### Adding Custom Test Data

1. Place Spark event logs in `test-data/spark-events/`
2. Run `./scripts/upload-test-data.sh` to sync to MinIO and HDFS
3. Restart application to pick up new data

### Monitoring Storage

**MinIO Console**: http://localhost:9001
- View buckets, objects, and usage metrics
- Upload/download files via web interface

**HDFS Web UI**: http://localhost:9870  
- Browse HDFS filesystem
- Monitor cluster health and storage usage

## Environment Variables

For testing and configuration:

```bash
# S3/MinIO
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin123
export S3_TEST_BUCKET=spark-events

# HDFS
export HDFS_NAMENODE_URL=hdfs://localhost:8020
export SPARK_EVENTS_DIR=/spark-events

# Application
export RUST_LOG=info
export DATABASE_DIRECTORY=./data
```