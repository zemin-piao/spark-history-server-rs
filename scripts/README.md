# Scripts Directory

This directory contains all the bash scripts for testing and development of the Spark History Server.

## Script Overview

### 🧪 Testing Scripts

| Script | Purpose | Usage |
|--------|---------|-------|
| `run-hdfs-tests.sh` | **HDFS Integration Tests** | Comprehensive HDFS testing including Kerberos authentication, mock tests, and real cluster validation |
| `run_hdfs_tests.sh` | **Legacy HDFS Test Runner** | Older version of HDFS test runner (deprecated, use `run-hdfs-tests.sh`) |
| `run_integration_tests.sh` | **Integration Test Suite** | End-to-end integration testing for the entire application |
| `run_load_tests.sh` | **Load Testing & Performance** | Large-scale performance testing with up to 100K applications |
| `test-hdfs-args.sh` | **HDFS CLI Demo** | Demonstration script showing HDFS command-line argument usage |

## Quick Start

```bash
# Make all scripts executable (if needed)
chmod +x scripts/*.sh

# Run HDFS integration tests
./scripts/run-hdfs-tests.sh

# Run load testing
./scripts/run_load_tests.sh

# Run integration tests
./scripts/run_integration_tests.sh

# View HDFS CLI examples
./scripts/test-hdfs-args.sh
```

## Script Details

### 🔗 HDFS Integration Testing (`run-hdfs-tests.sh`)

**Comprehensive HDFS testing suite covering:**
- Mock HDFS operations (no real cluster needed)
- Kerberos authentication scenarios
- Argument-based reader selection
- Real HDFS cluster validation (optional)

**Features:**
- 25+ automated tests
- Error scenario testing
- Performance benchmarks
- CLI usage examples

### 📊 Load Testing (`run_load_tests.sh`)

**Enterprise-scale performance testing:**
- Tests with 100K+ applications
- 2M+ events processing
- Write performance validation
- Memory usage monitoring

**Test Categories:**
- Application generation and processing
- Write performance scaling
- API load testing
- Analytics query performance

### 🔄 Integration Testing (`run_integration_tests.sh`)

**End-to-end application testing:**
- Full application workflow
- API endpoint validation
- Data consistency checks
- Cross-component integration

### 🎯 HDFS CLI Demo (`test-hdfs-args.sh`)

**Command-line usage examples:**
- Local filesystem mode
- Basic HDFS connection
- Kerberos authentication
- Environment variable usage
- Custom timeout settings

## Environment Variables

Many scripts support environment variables for configuration:

### HDFS Configuration
```bash
export HDFS_NAMENODE_URL=hdfs://your-namenode:9000
export SPARK_EVENTS_DIR=/your/spark/events/directory
```

### Kerberos Authentication
```bash
export KERBEROS_PRINCIPAL=your-principal@YOUR.REALM
export KERBEROS_KEYTAB=/path/to/your.keytab
export KRB5_CONFIG=/path/to/krb5.conf
export KERBEROS_REALM=YOUR.REALM
```

### Load Testing
```bash
export TEST_SCALE=large           # Test scale: small, medium, large
export MAX_APPLICATIONS=100000    # Maximum applications to test
```

## Development

### Adding New Scripts

When adding new scripts to this directory:

1. **Make executable**: `chmod +x scripts/your-script.sh`
2. **Add header comment**: Include purpose and usage
3. **Update this README**: Add entry to the table above
4. **Follow naming convention**: 
   - Use `run_` prefix for test runners
   - Use `-` for multi-word names where appropriate
   - Use descriptive names

### Script Standards

All scripts should:
- Include a descriptive header with purpose
- Use proper error handling (`set -e` if appropriate)
- Provide usage examples
- Support common environment variables
- Include progress indicators for long-running operations

## Troubleshooting

### Permission Issues
```bash
# Make all scripts executable
chmod +x scripts/*.sh
```

### HDFS Connection Issues
```bash
# Verify HDFS connectivity
hdfs dfs -ls /

# Check Kerberos tickets
klist
```

### Test Failures
```bash
# Run with verbose output
RUST_LOG=debug ./scripts/run-hdfs-tests.sh

# Run individual test categories
cargo test hdfs --release -- --nocapture
```

## Related Documentation

- [Main README](../README.md) - Project overview and setup
- [HDFS Integration Guide](../README.md#hdfs-integration) - Detailed HDFS setup
- [Load Testing Guide](../README.md#load-testing--performance) - Performance testing details