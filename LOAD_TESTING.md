# Load Testing Suite

This load testing suite provides comprehensive performance testing for the Spark History Server implementation, focusing on 10 million event scale testing with realistic data patterns.

## Overview

The load testing suite includes:

1. **Synthetic Data Generation**: Creates realistic Spark event data with proper JSON structure
2. **Performance Monitoring**: Tracks CPU, memory, and disk I/O during tests
3. **Full Pipeline Testing**: Tests complete event log file processing (file → decompression → parsing → DuckDB)
4. **Write Performance**: Benchmarks DuckDB insertion performance with various batch sizes
5. **API Load Testing**: Tests REST API endpoints under concurrent load
6. **Comprehensive Benchmarking**: Automated test suite with detailed reporting

## Key Features

### 🎯 Realistic Test Data
- Generates authentic Spark event types (ApplicationStart, JobEnd, TaskEnd, etc.)
- Includes realistic task metrics (execution time, memory usage, shuffle data)
- Supports multiple applications and executors
- Creates compressed event log files (.gz format)

### 📊 Performance Monitoring
- Real-time CPU and memory usage tracking
- Disk I/O monitoring for read/write operations
- Detailed performance snapshots and CSV export
- Process-level and system-level metrics

### 🔄 Full Pipeline Testing
- **File Generation**: Creates compressed Spark event log files on disk
- **File Processing**: Reads, decompresses, and parses event files
- **Database Operations**: Batched insertion into DuckDB
- **End-to-End Performance**: Complete workflow benchmarking

### 🚀 API Load Testing
- Concurrent user simulation
- Multiple endpoint testing
- Response time analysis (P50, P90, P95, P99 percentiles)
- Scalability testing with increasing concurrency

## Quick Start

### Run All Tests
```bash
# Run the complete test suite (30-60 minutes)
./run_load_tests.sh --all
```

### Run Individual Tests
```bash
# Interactive mode - select specific tests
./run_load_tests.sh

# Or run specific test modules
cargo test test_full_pipeline_10m_events --release -- --nocapture
cargo test test_api_load_performance --release -- --nocapture
cargo test run_comprehensive_benchmark_suite --release -- --nocapture
```

## Available Tests

### 1. Full Pipeline Test (10M events)
**File**: `tests/write_performance_test.rs`
- Generates 10 million Spark events across multiple applications
- Creates compressed event log files (realistic file sizes)
- Tests complete pipeline: file I/O → decompression → parsing → DuckDB insertion
- Measures end-to-end throughput and resource usage

### 2. Write Performance Tests
**File**: `tests/write_performance_test.rs`
- Tests DuckDB write performance with various batch sizes (1K, 5K, 10K, 20K, 50K)
- Measures pure insertion throughput (events/second)
- Concurrent write testing (multiple threads)
- Memory usage optimization analysis

### 3. API Load Tests
**File**: `tests/api_load_test.rs`
- Tests all REST API endpoints under load
- Concurrent user simulation (realistic usage patterns)
- Response time analysis with percentiles
- Scalability testing (1, 5, 10, 25, 50, 100 concurrent users)

### 4. Comprehensive Benchmark Suite
**File**: `tests/comprehensive_benchmark.rs`
- Automated testing with multiple data sizes (100K, 500K, 1M events)
- Tests all components: write, read, file processing, API performance
- Generates detailed JSON reports
- Performance comparison across test scenarios

### 5. Compression Performance
**File**: `tests/write_performance_test.rs`
- Compares uncompressed vs compressed event log processing
- File size vs processing speed analysis
- Memory usage comparison

## Test Data Characteristics

### Event Types Generated
- `SparkListenerApplicationStart/End`
- `SparkListenerJobStart/End`
- `SparkListenerStageSubmitted/Completed`
- `SparkListenerTaskStart/End`
- `SparkListenerExecutorAdded/Removed`

### Realistic Metrics Included
- Task execution times (100ms - 30s)
- Memory usage (1MB - 1GB peak execution memory)
- Shuffle read/write metrics
- Input/output data sizes
- GC times and CPU usage
- Data locality information

### Data Volume Examples
- **10M events**: ~200 event log files, ~500MB compressed data
- **1M events**: ~20 event log files, ~50MB compressed data
- **100K events**: ~2 event log files, ~5MB compressed data

## Performance Metrics Collected

### System Level
- CPU utilization (%)
- Memory usage (MB)
- Disk I/O (read/write bytes)
- System load

### Application Level
- Events processed per second
- Database write throughput
- API response times
- Error rates and success rates

### Database Level
- DuckDB insert performance
- Query response times
- Storage utilization
- Index effectiveness

## Expected Performance Baselines

### Write Performance
- **Target**: 50,000+ events/second for batched writes
- **Memory**: <2GB peak usage for 10M events
- **Storage**: ~1GB final DuckDB size for 10M events

### API Performance
- **Response Time**: <100ms for typical queries
- **Concurrency**: 100+ concurrent users supported
- **Success Rate**: >99% under normal load

### File Processing
- **Throughput**: 20,000+ events/second including decompression
- **Memory**: <1GB for file processing pipeline
- **I/O**: Efficient streaming processing

## Output Files

After running tests, check the `load_test_results/` directory:

- `test_summary.txt` - Overview of all test results
- `comprehensive_benchmark_report.json` - Detailed JSON report
- `*_performance_metrics.csv` - Raw performance data
- `*.log` - Individual test outputs with detailed metrics

## Interpreting Results

### Key Metrics to Watch
1. **Events/Second**: Higher is better for write performance
2. **Memory Usage**: Should remain stable, avoid memory leaks
3. **Response Times**: P99 < 1000ms for good user experience
4. **Success Rate**: Should be >99% for production readiness

### Performance Regression Detection
- Compare throughput against previous runs
- Monitor memory usage trends
- Check for increased error rates
- Validate response time percentiles

## Architecture Integration

This testing suite validates the key architectural decisions:

### DuckDB Choice
- Validates analytical query performance
- Tests cross-application analytics
- Verifies storage efficiency

### Batched Write Strategy
- Confirms optimal batch sizes
- Tests write throughput under load
- Validates transaction handling

### REST API Design
- Tests endpoint performance
- Validates concurrent user support
- Checks caching effectiveness

## Extending the Test Suite

### Adding New Test Scenarios
1. Create test function in appropriate file
2. Add to `run_load_tests.sh` script
3. Update this documentation

### Custom Event Types
1. Extend `SyntheticDataGenerator` in `load_test_utils.rs`
2. Add new JSON event structures
3. Update SparkEvent parsing logic

### New Performance Metrics
1. Extend `PerformanceMetrics` struct
2. Add collection logic in `PerformanceMonitor`
3. Update CSV export format

## Troubleshooting

### Common Issues
- **DuckDB file exists error**: Tests create unique temp files automatically
- **Memory issues**: Reduce batch sizes or test data volume
- **Timeout errors**: Increase test timeout values
- **Compilation errors**: Ensure all dependencies are up to date

### Debug Mode
```bash
# Run with debug output
RUST_LOG=debug cargo test test_name --release -- --nocapture

# Enable DuckDB cleanup for testing
ENABLE_DB_CLEANUP=true cargo test test_name
```

## Performance Optimization Tips

Based on load testing results:

1. **Batch Size**: 10,000-20,000 events per batch optimal for writes
2. **Memory**: Enable DuckDB's memory optimization features
3. **Indexing**: Ensure proper indexes on app_id, timestamp, event_type
4. **Caching**: Enable in-memory caching for frequently accessed data
5. **Connection Pooling**: Use connection pooling for API endpoints

## Contributing

When adding new tests:
1. Follow the existing pattern for test structure
2. Include performance monitoring
3. Add comprehensive logging
4. Update test documentation
5. Ensure tests clean up resources properly

---

This load testing suite ensures the Spark History Server can handle production-scale workloads with 10+ million events while maintaining good performance and resource utilization.