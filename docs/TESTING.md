# Testing Strategy

This project uses a multi-tiered testing approach to ensure reliability while maintaining fast CI builds.

## Test Categories

### 🟢 **Unit & Mock Tests** (Always Run)
- **Unit tests**: Individual component testing
- **Mock integration tests**: S3 and HDFS integration with in-memory mocks
- **API tests**: HTTP endpoint testing with test data
- **Performance tests**: Benchmarking and load testing

**Run with**: `cargo test` or `./scripts/test-ci.sh`

### 🔵 **Docker Integration Tests** (Optional)
- **Real S3 tests**: MinIO container integration
- **Real HDFS tests**: Hadoop cluster integration  
- **End-to-end tests**: Full system testing

**Run with**: `cargo test --features integration-tests` or `./scripts/test-local.sh`

## Quick Start

### CI/Development Testing (Fast)
```bash
# Run all tests that work without Docker
./scripts/test-ci.sh

# Or manually:
cargo test
cargo test --features performance-tests
```

### Full Local Testing (Complete)
```bash
# Run everything including Docker-dependent tests
./scripts/test-local.sh

# Or manually:
docker-compose -f docker-compose.minio.yml up -d
docker-compose -f docker-compose.hdfs.yml up -d
export S3_TEST_BUCKET=spark-events AWS_ENDPOINT_URL=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin123
cargo test --features integration-tests
```

## Test Organization

### Mock Tests (No Docker Required)
```rust
#[tokio::test]
async fn test_s3_file_reader_creation() {
    // Uses mock S3 implementation
}

#[tokio::test]  
async fn test_hdfs_file_reader_mock() {
    // Uses mock HDFS implementation
}
```

### Docker Integration Tests (Feature Gated)
```rust
#[tokio::test]
#[cfg(feature = "integration-tests")]
async fn test_real_s3_health_check() {
    // Requires MinIO container
}

#[tokio::test]
#[cfg(feature = "integration-tests")]
async fn test_real_hdfs_connection() {
    // Requires HDFS cluster
}
```

## CI Configuration

### GitHub Actions
- **Default CI**: Runs mock tests only (fast, no Docker)
- **Integration CI**: Runs with Docker services (triggered by `[run-integration]` in commit message or on schedule)
- **Manual trigger**: Add `[run-integration]` to your commit message

### Commands by Environment

| Environment | Command | Duration | Coverage |
|-------------|---------|----------|----------|
| **CI/CD** | `./scripts/test-ci.sh` | ~2min | Unit + Mock |
| **Local Dev** | `./scripts/test-local.sh` | ~5min | Full Coverage |
| **Manual Integration** | `cargo test --features integration-tests` | ~3min | Docker Tests Only |

## Test Data

### Mock Test Data
- **Location**: `test-data/spark-events/`
- **Format**: Sample Spark event logs
- **Usage**: Automatically loaded by mock tests

### Docker Test Data
- **MinIO**: Auto-uploaded via scripts
- **HDFS**: Created during container startup
- **Configuration**: Environment variables for connection details

## Environment Variables

### For Real Integration Tests
```bash
# S3/MinIO
export S3_TEST_BUCKET=spark-events
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin123
export AWS_REGION=us-east-1

# HDFS  
export HDFS_NAMENODE_URL=hdfs://localhost:8020
export SPARK_EVENTS_DIR=/spark-events
```

## Performance Testing

### Load Tests
```bash
# Basic performance tests
cargo test --features performance-tests

# Large scale tests (100K applications)
cargo test --release test_100k_applications_load

# Comprehensive benchmarks
./scripts/run_load_tests.sh
```

### Benchmarking
```bash
# Run with criterion benchmarks
cargo bench

# Monitor performance
./scripts/performance-monitor.sh
```

## Troubleshooting

### CI Tests Failing
1. **Formatting**: `cargo fmt --all -- --check`
2. **Clippy**: `cargo clippy --all-targets --all-features -- -D warnings`
3. **Mock tests**: Check test data in `test-data/`

### Docker Tests Failing
1. **Services not ready**: Increase wait times in scripts
2. **Port conflicts**: Check `docker ps` for conflicting services
3. **Network issues**: Restart Docker Desktop

### Performance Tests Failing
1. **Resource constraints**: Run on dedicated hardware
2. **Timing issues**: Increase test timeouts
3. **Memory limits**: Check available system memory

## Test Metrics

### Current Coverage
- **Unit Tests**: 45+ tests
- **Mock Integration**: 16+ tests  
- **Docker Integration**: 5+ tests
- **Performance Tests**: 10+ tests
- **Total**: 75+ automated tests

### Success Criteria
- **CI Tests**: Must pass for PR merge
- **Integration Tests**: Optional but recommended
- **Performance Tests**: Run on release candidates
- **Load Tests**: Required for major versions