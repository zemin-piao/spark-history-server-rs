# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **high-performance, analytics-first Spark History Server** implementation in Rust, purpose-built for **cross-application insights** and **enterprise-scale analytics**. Unlike traditional Spark History Servers focused on individual application details, this server excels at aggregations and analytics across multiple applications using DuckDB's analytical power.

**Proven Performance**: Successfully tested with 100,000 applications and 2M events at 10,700 events/sec.

## Recent Improvements (2024)

### ✅ **Production-Ready HDFS Integration**
- **Real HDFS Native Support**: Replaced mock implementation with production-ready `hdfs-native` integration
- **Full Kerberos Authentication**: Enterprise-grade security with keytab and ticket cache support
- **Connection Management**: Proper timeout handling, connection pooling, and health checks
- **Comprehensive Testing**: 25+ tests covering mock, integration, and real HDFS cluster scenarios

### ✅ **Simplified DuckDB-Only Architecture**
- **Removed Hybrid Storage Complexity**: Eliminated confusing hybrid storage layer for cleaner architecture
- **Single Source of Truth**: All data flows through DuckDB for consistent analytics and reporting
- **Configuration Simplified**: Updated from `cache_directory`/`enable_cache` to single `database_directory` parameter

### ✅ **Circuit Breaker Fault Tolerance**
- **External Dependency Protection**: Circuit breakers for HDFS and DuckDB operations
- **Automatic Recovery**: Self-healing system that automatically retries failed operations
- **Failure Tracking**: Comprehensive monitoring of failure rates, success rates, and system health
- **Production-Ready**: Configurable thresholds, timeouts, and recovery windows

### ✅ **Code Quality & Testing Excellence**
- **Strict Clippy Compliance**: All code passes `cargo clippy --all-targets --all-features -- -D warnings`
- **Comprehensive Test Suite**: 45+ tests covering all major functionality and edge cases
- **Zero Technical Debt**: All deprecated patterns removed, clean modern Rust architecture

## Architecture Context

You are working on a principal-level system design for a Spark History Server that emphasizes:

- **Analytics-First Design**: Cross-application performance insights, not individual app debugging
- **DuckDB Backend**: Embedded columnar database optimized for aggregations and time-series analysis  
- **Real HDFS Integration**: Production-ready HDFS access via `hdfs-native` with full Kerberos authentication support
- **Circuit Breaker Protection**: Fault-tolerant operations with automatic recovery for external dependencies
- **Zero Deployment Complexity**: Single binary with embedded database
- **Enterprise Scale**: Load tested with 100K+ applications

### Core Components

1. **Log Processing Engine** (`src/event_processor.rs`, `src/storage/`)
   - **Production HDFS integration** via `hdfs-native` with Kerberos authentication
   - **Circuit breaker protection** for HDFS operations with automatic recovery
   - Event stream processing with batch optimization (1000+ events/batch)
   - Schema flexibility: hot field extraction + JSON fallback for 10+ event types

2. **DuckDB Storage Layer** (`src/storage/duckdb_store.rs`)
   - **Simplified DuckDB-only architecture** (hybrid storage removed)
   - **Circuit breaker protection** for database operations
   - Single events table with common fields + JSON column
   - Optimized for cross-application analytics and aggregations
   - Batched writes with background flushing

3. **REST API & Dashboard** (`src/api.rs`, `src/analytics_api.rs`, `src/dashboard.rs`)
   - Standard Spark History Server v1 API compatibility
   - Advanced analytics endpoints for cross-application insights
   - Built-in web dashboard with server-side rendering (Askama templates)

## Development Commands

### Building & Running
```bash
# Build for development
cargo build

# Build optimized release
cargo build --release

# Run with default configuration
cargo run

# Run with custom config
cargo run -- --config config/settings.toml

# Run with HDFS support
cargo run -- --hdfs --hdfs-namenode hdfs://namenode:9000 --log-directory /spark-events

# Run with Kerberos authentication
cargo run -- --hdfs --hdfs-namenode hdfs://secure-namenode:9000 \
  --kerberos-principal spark@EXAMPLE.COM \
  --keytab-path /etc/security/keytabs/spark.keytab \
  --log-directory /hdfs/spark-events
```

### Testing Commands

```bash
# Run all tests
cargo test

# NEW: Platform Engineering Focused Tests
cargo test --test analytics_api_test                # Platform engineering endpoint tests
cargo test test_platform_engineering_endpoints      # Core optimization endpoint tests  
cargo test test_deprecated_endpoints                # Deprecated endpoint validation
cargo test test_platform_engineering_data_quality   # Data quality validation

# Core Integration Tests
cargo test --test integration_test                  # Core API functionality
cargo test --test incremental_scan_test            # Event processing tests

# HDFS & Infrastructure Tests
./scripts/run-hdfs-tests.sh                        # HDFS integration tests
cargo test --test hdfs_integration_test            # HDFS functionality
cargo test --test kerberos_advanced_test           # Security tests

# Performance & Load Testing
cargo test --test load_test_utils                  # Load testing utilities
cargo test --test large_scale_test                 # Enterprise scale tests
cargo test test_100k_applications_load --release   # 100K application load test
./scripts/run_load_tests.sh                        # Comprehensive load tests

# Endpoint Specific Tests
RUST_LOG=info cargo test test_platform_engineering_endpoints -- --nocapture
RUST_LOG=debug cargo test test_deprecated_endpoints -- --nocapture

# Real Environment Tests  
export HDFS_NAMENODE_URL=hdfs://your-namenode:9000
export SPARK_EVENTS_DIR=/your/spark/events
cargo test test_real_hdfs --ignored
```

### Linting & Code Quality
```bash
# Run clippy for linting (strict mode)
cargo clippy --all-targets --all-features -- -D warnings

# Format code
cargo fmt

# Check for unused dependencies
cargo machete
```

## Key File Structure

### Core Application Files
- `src/main.rs` - Main entry point with CLI argument parsing and server startup
- `src/api.rs` - Standard Spark History Server v1 API endpoints + main router
- `src/analytics_api.rs` - Advanced cross-application analytics endpoints  
- `src/dashboard.rs` - Web dashboard controllers and template rendering
- `src/config.rs` - Configuration management (TOML + CLI args + env vars)
- `src/models.rs` - Data models and API response structures

### Storage & Processing
- `src/storage/duckdb_store.rs` - Core DuckDB integration and analytics engine with circuit breaker protection
- `src/storage/file_reader.rs` - File system abstraction (local + HDFS) with circuit breaker protection
- `src/event_processor.rs` - Spark event log parsing and processing
- `src/hdfs_reader.rs` - Production HDFS client with Kerberos authentication
- `src/circuit_breaker.rs` - Circuit breaker implementation for fault tolerance

### Testing Infrastructure
- `tests/integration_test.rs` - End-to-end integration testing
- `tests/analytics_api_test.rs` - Analytics API endpoint testing
- `tests/hdfs_integration_test.rs` - Comprehensive HDFS testing
- `tests/real_hdfs_integration_test.rs` - Real HDFS cluster testing
- `tests/batch_writer_debug_test.rs` - Event processing and storage testing
- `tests/argument_based_reader_test.rs` - Reader selection and configuration testing
- `tests/incremental_scan_test.rs` - Incremental scanning and processing testing
- `tests/large_scale_test.rs` - Enterprise-scale load testing (100K apps)
- `tests/load_test_utils.rs` - Load testing utilities and helpers

### Configuration & Scripts
- `config/settings.toml` - Default configuration file
- `scripts/run-hdfs-tests.sh` - HDFS integration test runner
- `scripts/run_load_tests.sh` - Load testing and performance validation
- `templates/` - Askama HTML templates for web dashboard

## Configuration

The server uses a layered configuration approach:
1. **TOML file** (`config/settings.toml`) - Base configuration
2. **CLI arguments** - Override TOML settings  
3. **Environment variables** - Fallback for sensitive values

### Key Configuration Areas

**Server Settings**:
- `server.host` / `--host` - Server bind address (default: 0.0.0.0)
- `server.port` / `--port` - Server port (default: 18080)

**Event Log Storage**:
- `history.log_directory` / `--log-directory` - Path to Spark event logs
- `history.database_directory` - DuckDB database storage directory (replaces cache_directory)
- `history.update_interval_seconds` - Polling interval for new logs
- `history.compression_enabled` - Support for .lz4/.snappy files

**HDFS Configuration**:
- `--hdfs` - Enable HDFS mode
- `--hdfs-namenode` / `HDFS_NAMENODE_URL` - HDFS namenode URL
- `--kerberos-principal` / `KERBEROS_PRINCIPAL` - Kerberos principal
- `--keytab-path` / `KERBEROS_KEYTAB` - Path to keytab file

## Development Patterns

### Adding New Analytics Endpoints
1. Add SQL query to `src/analytics_api.rs`
2. Define response model in `src/models.rs` 
3. Add tests to `tests/analytics_api_test.rs`
4. Update API documentation

### Adding Dashboard Features
1. Add controller function to `src/dashboard.rs`
2. Create or modify HTML template in `templates/`
3. Add route to dashboard router
4. Test dashboard view at `http://localhost:18080`

### HDFS Integration Development
- Use mock HDFS tests for rapid development (`cargo test hdfs`)
- Test Kerberos scenarios with `cargo test kerberos`
- Validate with real HDFS cluster using `cargo test test_real_hdfs --ignored`

### Performance Optimization
- **Batch Processing**: Events processed in configurable batches for write optimization
- **DuckDB Tuning**: Columnar storage with proper indexing on hot query paths
- **Caching Strategy**: In-memory caching for frequently accessed applications
- **Load Testing**: Validate performance with `./scripts/run_load_tests.sh`

## API Design Philosophy

This server implements a **dual API strategy**:

### ✅ **Analytics-First APIs** (Our Strength)
- `/api/v1/analytics/cross-app-summary` - Enterprise-wide Spark metrics
- `/api/v1/analytics/performance-trends` - Time-series performance analysis
- `/api/v1/analytics/resource-usage` - Resource utilization patterns
- `/api/v1/analytics/task-distribution` - Cross-application task performance

### ✅ **Standard Spark History Server v1** (Basic Compatibility)
- `/api/v1/applications` - Application listing with filtering
- `/api/v1/applications/{appId}` - Basic application details
- `/api/v1/applications/{appId}/executors` - Executor information

### ❌ **Not Implemented** (Use Traditional Spark History Server)
- Detailed job/stage/task drill-down endpoints
- SQL query execution plan analysis
- Streaming batch analysis
- Real-time application monitoring

## Performance Characteristics

- **Write Throughput**: 10,700+ events/sec sustained
- **Query Performance**: <10ms for analytical queries
- **Scale**: 100K applications, 2M events tested
- **Storage Efficiency**: 229 bytes per event average
- **Memory Usage**: ~4.6KB per application in memory