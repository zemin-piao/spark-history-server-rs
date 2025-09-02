# Spark History Server (Rust)

[![Rust](https://img.shields.io/badge/rust-%23000000.svg?style=for-the-badge&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-FDEE21?style=for-the-badge&logo=apachespark&logoColor=black)](https://spark.apache.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)](https://duckdb.org/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg?style=for-the-badge)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://img.shields.io/github/actions/workflow/status/zemin-piao/spark-history-server-rs/rust.yml?branch=main&style=flat-square)](https://github.com/zemin-piao/spark-history-server-rs/actions/workflows/rust.yml)

⚠️ **This project is a Work In Progress (WIP) and not production ready** ⚠️

A **analytics-first** Spark History Server implementation in Rust, purpose-built for **cross-application insights** and **trend analysis**. Unlike traditional Spark History Servers that focus on individual application details, this server excels at **aggregations and analytics across multiple applications** using DuckDB's analytical power.

**🏆 Proven Performance: Successfully tested with 100,000 applications and 2M events at 10,700 events/sec**

## Current Status

- ✅ **Core Infrastructure**: Event log parsing, DuckDB integration, incremental scanning
- ✅ **Standard Spark History Server API v1**: Basic application endpoints implemented
- ✅ **Analytics Engine**: Advanced cross-application analytics with DuckDB
- ✅ **Storage Layer**: DuckDB-based storage with batched writes and caching
- ✅ **HDFS Support**: Native HDFS integration via `hdfs-native`
- ✅ **Enterprise Scale**: **Load tested with 100K applications, 2M events**
- ✅ **High Performance**: **10,700 events/sec sustained throughput**
- ✅ **Web Dashboard**: Analytics-focused web dashboard with cluster overview, performance insights, and optimization recommendations
- 🚧 **Production Features**: Metrics, monitoring, security features planned

## 🎯 **Analytics-First Design Philosophy**

This Spark History Server is **purpose-built for analytics**, not individual application monitoring. It excels where traditional history servers fall short:

### **✅ What We Excel At (Analytics & Trends)**
- 📊 **Cross-Application Analytics**: Query and aggregate metrics across thousands of Spark applications simultaneously
- 📈 **Performance Trend Analysis**: Time-series analysis of resource usage, task performance, and system health
- 🔍 **Resource Usage Patterns**: Identify underutilized executors, memory bottlenecks, and optimization opportunities across your entire Spark estate
- ⚡ **High-Scale Aggregations**: DuckDB's columnar storage powers complex analytical queries on millions of events
- 📉 **Historical Insights**: Long-term trend analysis for capacity planning and performance optimization
- 🎯 **Enterprise Dashboards**: Perfect backend for analytics dashboards showing cluster-wide Spark metrics

### **❌ What Traditional Servers Do Better (Individual App Details)**
- Individual job/stage/task drill-down details
- Real-time application monitoring and debugging  
- Task-level performance analysis within a single application
- Detailed executor thread dumps and live metrics
- SQL query execution plan analysis

## 🏗️ **Core Features**

- **🧮 Advanced Analytics Engine**: Cross-application performance insights and resource usage analytics
- **🦆 DuckDB Analytical Backend**: Embedded columnar database optimized for aggregations and complex analytical queries
- **📡 HDFS Native Integration**: Direct HDFS access using `hdfs-native` for enterprise-scale event log processing
- **⚡ High-Throughput Processing**: Batch processing of compressed event logs with 10K+ events/sec performance
- **🔌 Dual API Design**: Standard Spark History Server v1 compatibility + advanced analytics endpoints
- **📦 Zero Deployment Complexity**: Single binary with embedded database - no external dependencies
- **🎨 Built-in Web Dashboard**: Modern analytics dashboard with cluster overview, performance trends, and optimization insights
- **🚀 Enterprise Proven**: **Load tested with 100K+ applications and sub-10ms analytical query response times**

## 🌐 **API Endpoints & Capabilities**

### 🎯 **Analytics-First Endpoints (Our Strength)**

**✅ Advanced Cross-Application Analytics**
- `GET /api/v1/analytics/cross-app-summary` - **Enterprise-wide Spark metrics** across all applications
- `GET /api/v1/analytics/performance-trends` - **Time-series performance analysis** for capacity planning  
- `GET /api/v1/analytics/resource-usage` - **Resource utilization patterns** and optimization insights
- `GET /api/v1/analytics/task-distribution` - **Task performance distribution** and locality analysis across apps
- `GET /api/v1/analytics/executor-utilization` - **Cross-application executor efficiency** metrics

### 📋 **Standard Spark History Server API v1 (Basic Compatibility)**

**✅ Core Application Endpoints**
- `GET /api/v1/applications` - List all applications with filtering (`status`, `minDate`, `maxDate`, `limit`)
- `GET /api/v1/applications/{appId}` - Application details and summary metrics
- `GET /api/v1/applications/{appId}/executors` - Executor information for application
- `GET /api/v1/version` - Server version information

**⚠️ Limited Implementation (application-level only)**
- `GET /api/v1/applications/{appId}/jobs` - Basic job listing (no detailed drill-down)
- `GET /api/v1/applications/{appId}/stages` - Basic stage listing (no task-level details)  
- `GET /api/v1/applications/{appId}/environment` - Application environment summary
- `GET /api/v1/applications/{appId}/storage/rdd` - RDD storage summary

### ❌ **Not Implemented (Use Traditional Spark History Server)**

For detailed individual application analysis, use the standard Spark History Server:
- Job/Stage/Task detailed drill-down endpoints
- SQL query execution plan analysis (`/sql/*` endpoints)  
- Streaming batch analysis (`/streaming/*` endpoints)
- Executor thread dumps and live metrics
- Event log downloads and detailed task analysis

### 🎨 **Web Dashboard**

**✅ Analytics-Focused Dashboard Views**
- `GET /` - **Cluster Overview**: Real-time cluster status, active applications, and key metrics summary
- `GET /analytics` - **Analytics Dashboard**: Comprehensive performance analytics and cross-application insights
- `GET /optimize` - **Optimization View**: Performance trends, resource utilization, and task distribution analysis
- `GET /resources` - **Resource Management**: Executor utilization and capacity planning insights *(coming soon)*
- `GET /teams` - **Team Analytics**: User/team resource attribution and usage patterns *(coming soon)*

### 🏥 **System Health**
- `GET /health` - Health check and system status

## Configuration

Configuration is provided via `config/settings.toml`:

```toml
[server]
host = "127.0.0.1"
port = 18080
max_applications = 1000

[history]
# Path to Spark event logs directory (supports HDFS and local paths)
log_directory = "/tmp/spark-events"  # or "hdfs://namenode:9000/spark-events"
max_applications = 1000
# Update interval for polling new event logs (seconds)
update_interval_seconds = 60
max_apps_per_request = 100
compression_enabled = true
# Local cache for processed data
cache_directory = "./data"
enable_cache = true
```

### Storage Configuration

The DuckDB storage is automatically configured and managed internally. Key features:
- **Database Location**: `./data/spark_history.duckdb` (created automatically)
- **Batch Processing**: Events are processed in configurable batches for optimal write performance
- **Incremental Scanning**: Only new/modified event logs are processed
- **Schema Evolution**: Flexible schema handles diverse Spark event types

## Quick Start

### 1. Build and Run

```bash
# Build the project
cargo build --release

# Run with default configuration
./target/release/spark-history-server

# Run with custom configuration
./target/release/spark-history-server --config config/settings.toml

# For development
cargo run
```

### 2. Test the API

```bash
# Health check
curl http://localhost:18080/health

# Web Dashboard (open in browser)
open http://localhost:18080                    # Cluster overview dashboard
open http://localhost:18080/analytics          # Analytics dashboard  
open http://localhost:18080/optimize           # Optimization insights

# API endpoints
curl "http://localhost:18080/api/v1/applications?limit=10"
curl "http://localhost:18080/api/v1/applications/{app_id}"
curl "http://localhost:18080/api/v1/analytics/cross-app-summary"
curl "http://localhost:18080/api/v1/analytics/performance-trends"
curl "http://localhost:18080/api/v1/analytics/resource-usage"
```

### 3. HDFS Integration

The server supports both local filesystem and HDFS for event log storage with full Kerberos authentication:

#### 🏗️ **Argument-Based Reader Selection**

**Local Filesystem (Default):**
```bash
# Local filesystem mode
./target/release/spark-history-server --log-directory ./spark-events
```

**HDFS Without Authentication:**
```bash
# Basic HDFS mode
./target/release/spark-history-server \
  --hdfs \
  --hdfs-namenode hdfs://namenode:9000 \
  --log-directory /spark-events
```

**HDFS with Kerberos Authentication:**
```bash
# HDFS with Kerberos keytab
./target/release/spark-history-server \
  --hdfs \
  --hdfs-namenode hdfs://secure-namenode:9000 \
  --kerberos-principal spark@EXAMPLE.COM \
  --keytab-path /etc/security/keytabs/spark.keytab \
  --krb5-config /etc/krb5.conf \
  --kerberos-realm EXAMPLE.COM \
  --log-directory /hdfs/spark-events
```

**Custom Timeouts:**
```bash
# HDFS with custom timeout settings
./target/release/spark-history-server \
  --hdfs \
  --hdfs-namenode hdfs://namenode:9000 \
  --hdfs-connection-timeout 60000 \  # 60 seconds
  --hdfs-read-timeout 120000 \      # 2 minutes
  --log-directory /hdfs/spark-events
```

#### 🔐 **Environment Variable Support**

All HDFS settings support environment variable fallbacks:

```bash
# Set environment variables
export HDFS_NAMENODE_URL=hdfs://secure-namenode:9000
export KERBEROS_PRINCIPAL=spark@EXAMPLE.COM  
export KERBEROS_KEYTAB=/etc/security/keytabs/spark.keytab
export KRB5_CONFIG=/etc/krb5.conf
export KERBEROS_REALM=EXAMPLE.COM

# Start with minimal arguments
./target/release/spark-history-server --hdfs --log-directory /hdfs/spark-events
```

#### 📝 **Configuration File Integration**

HDFS can also be configured via `config/settings.toml`:

```toml
[history]
log_directory = "/hdfs/spark-events"

[history.hdfs]
namenode_url = "hdfs://secure-namenode:9000"
connection_timeout_ms = 30000
read_timeout_ms = 60000

[history.hdfs.kerberos]
principal = "spark@EXAMPLE.COM"
keytab_path = "/etc/security/keytabs/spark.keytab"
krb5_config_path = "/etc/krb5.conf"
realm = "EXAMPLE.COM"
```

#### ⚡ **Runtime Reader Switching**

Switch between local and HDFS readers at runtime without recompiling:

```bash
# Start with local reader
./target/release/spark-history-server --log-directory ./test-data

# Switch to HDFS by changing arguments
./target/release/spark-history-server --hdfs --hdfs-namenode hdfs://namenode:9000
```

#### 🧪 **HDFS Testing**

Comprehensive test suite with mock and real HDFS support:

```bash
# Run all HDFS tests (automated test runner)
./scripts/run-hdfs-tests.sh

# Individual test categories:
# 1. Basic Integration Tests
cargo test hdfs_integration_test --release

# 2. Comprehensive HDFS Operations
cargo test test_hdfs_comprehensive_file_operations --release
cargo test test_hdfs_error_handling_scenarios --release
cargo test test_hdfs_concurrent_operations --release

# 3. Kerberos Authentication Tests  
cargo test test_kerberos_configuration --release
cargo test test_kerberos_valid_authentication --release
cargo test test_kerberos_expired_tickets --release
cargo test test_kerberos_network_errors --release

# 4. Argument-Based Reader Selection
cargo test test_argument_based_local_reader_selection --release
cargo test test_runtime_reader_switching --release
cargo test test_configuration_precedence --release

# 5. Real HDFS Tests (requires actual HDFS cluster)
# Set environment variables first:
# export HDFS_NAMENODE_URL=hdfs://your-namenode:9000
# export SPARK_EVENTS_DIR=/your/spark/events
# For Kerberos: KERBEROS_PRINCIPAL, KERBEROS_KEYTAB, etc.

cargo test test_real_hdfs_connection_health --ignored
cargo test test_real_hdfs_kerberos_authentication --ignored  
cargo test test_real_hdfs_spark_event_logs --ignored
cargo test test_real_hdfs_performance_benchmarks --ignored
```

**Test Categories:**
- **Mock Tests**: 25+ tests using simulated HDFS - no infrastructure required
- **Integration Tests**: End-to-end testing with HistoryProvider
- **Kerberos Tests**: All authentication scenarios (valid, expired, invalid, keytab vs ticket cache)
- **Performance Tests**: Concurrent operations, timeout handling, error recovery
- **Real HDFS Tests**: Live cluster validation (marked `#[ignore]` - run manually)

## Architecture

The Spark History Server is built around a modern, analytics-first architecture designed for large-scale Spark deployments:

### Core Components

1. **Log Processing Engine**
   - **HDFS Integration**: Native HDFS support using `hdfs-native` with full Kerberos authentication
   - **Argument-Based Reader Selection**: Runtime switching between local filesystem and HDFS readers
   - **Kerberos Security**: Enterprise-grade authentication with keytab and ticket cache support
   - **Event Stream Processing**: Asynchronous processing of compressed event logs (.lz4, .snappy, .gz)
   - **Batch Processing**: Optimized batch writes to DuckDB with configurable timeouts
   - **Schema Flexibility**: Handles 10+ Spark event types with hot field extraction + JSON fallback
   - **Health Monitoring**: Automated HDFS connectivity and authentication health checks

2. **DuckDB Storage Layer** 
   - **Embedded Analytical Database**: Single-file database optimized for analytics workloads
   - **Columnar Storage**: Fast aggregations and time-series queries across applications
   - **Flexible Schema**: Combined structured fields + JSON column for event diversity
   - **Cross-Application Queries**: SQL analytics across all Spark applications simultaneously

3. **REST API Server & Web Dashboard**
   - **Dual API Support**: Standard Spark History Server v1 + advanced analytics endpoints  
   - **Built-in Web Dashboard**: Server-side rendered dashboard using Askama templates
   - **Multiple Dashboard Views**: Cluster overview, analytics, optimization insights, and resource management
   - **In-Memory Caching**: Hot data caching for frequently accessed applications
   - **Async I/O**: Built with Axum and Tokio for high concurrency

### Data Flow

```
┌─────────────────────────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│        Storage Layer            │ -> │  Event Processor │ -> │   DuckDB Store   │ -> │ API + Dashboard │
│                                 │    │                  │    │                  │    │                 │
│ HDFS (with Kerberos):           │    │ • Argument-based │    │ • Events Table   │    │ • REST API      │
│ • hdfs://namenode:9000          │    │   Reader Switch  │    │ • JSON + Hot     │    │ • Analytics     │ 
│ • /spark-events/*.lz4           │    │ • Incremental    │    │   Fields         │    │ • Web Dashboard │
│ • Kerberos Authentication       │    │   Scanning       │    │ • Timeout        │    │ • Multi-View UI │
│                                 │    │ • Batch Writing  │    │   Handling       │    │ • Health Check  │
│ Local FileSystem:               │    │ • Health Checks  │    │ • Schema Flex    │    │                 │
│ • ./spark-events/*.snappy       │    │                  │    │                  │    │                 │
│ • .inprogress files             │    │                  │    │                  │    │                 │
└─────────────────────────────────┘    └──────────────────┘    └──────────────────┘    └─────────────────┘
```

### 🏆 **Key Advantages for Analytics**

- **🎯 Analytics-First Architecture**: Purpose-built for cross-application insights and trend analysis, not individual app debugging
- **📊 SQL-Powered Analytics**: Complex aggregations and time-series analysis across thousands of Spark applications using DuckDB
- **⚡ High-Scale Performance**: **100K applications, 2M events processed at 10,700 events/sec** with sub-10ms analytical queries
- **🔄 Zero Deployment Complexity**: Single binary, embedded analytical database, no external infrastructure required
- **📈 Enterprise Trend Analysis**: Long-term performance patterns, capacity planning insights, and resource optimization opportunities
- **🏗️ Flexible Data Model**: Hot field extraction + JSON storage handles diverse Spark event schemas across versions
- **🚀 Production Proven**: Comprehensive load testing, error handling, and enterprise-scale validation

## Load Testing & Performance

### 🏆 **Proven Enterprise Scale Performance**

Our comprehensive load testing demonstrates exceptional performance at enterprise scale:

| **Metric** | **Result** | **Performance** |
|------------|------------|-----------------|
| **Applications Supported** | **100,000 applications** | ✅ Tested & Verified |
| **Total Events Processed** | **2,000,000 events** | ✅ Zero data loss |
| **Write Throughput** | **10,700 events/second** | ✅ Sustained performance |
| **Data Generation Time** | **193 seconds** | ⚡ ~3.2 minutes end-to-end |
| **Storage Efficiency** | **229 bytes/event** | 💾 Highly optimized |
| **Query Performance** | **<10ms response times** | 🚀 Sub-millisecond for basic queries |

### 📊 **Load Test Results Summary**

```
🎯 ENTERPRISE SCALE LOAD TEST - SUCCESS
==========================================
✅ Applications Created: 100,000
✅ Events Processed: 2,000,000  
✅ Write Throughput: 10,702 events/sec
✅ Database Size: 437 MB (229 bytes/event)
✅ Memory per App: 4,585 bytes/app
✅ Zero Data Loss: All events & apps verified
⚡ Total Duration: 186.88s (~3.1 minutes)

Query Performance Highlights:
📱 Application List (50 apps): 3ms
📊 Event Count (2M events): 0ms  
⚙️ Executor Summary: 11ms
💾 Perfect data integrity maintained
```

### 🚀 **Load Testing Suite**

Run comprehensive performance tests:

```bash
# Quick load testing demo
./scripts/run_load_tests.sh

# Full enterprise-scale test suite (30-60 minutes)
./scripts/run_load_tests.sh --all

# Specific tests
cargo test test_100k_applications_load --release -- --nocapture
cargo test test_write_performance_scaling --release -- --nocapture
```

**Available Load Tests:**
- **100K Applications Test**: Full enterprise-scale simulation
- **Write Performance**: Batch size optimization testing  
- **API Load Testing**: Concurrent user simulation
- **File Processing Pipeline**: End-to-end event log processing
- **Analytical Query Performance**: Complex cross-app analytics

See [docs/LOAD_TESTING.md](docs/LOAD_TESTING.md) for detailed performance analysis and benchmarking guide.

---

## Documentation

Additional documentation is available in the `docs/` folder:

- **[docs/LOAD_TESTING.md](docs/LOAD_TESTING.md)** - Comprehensive load testing and performance benchmarking guide
- **[docs/HDFS_INTEGRATION.md](docs/HDFS_INTEGRATION.md)** - Detailed HDFS integration and configuration guide  
- **[docs/SCRIPT_ORGANIZATION.md](docs/SCRIPT_ORGANIZATION.md)** - Development scripts and tooling documentation
- **[docs/demo.md](docs/demo.md)** - Demo and example usage scenarios

---

## Testing

```bash
# Run all tests
cargo test

# Run specific test suites
cargo test --test integration_test
cargo test --test analytics_api_test  
cargo test --test incremental_scan_test

# Run HDFS integration tests
./scripts/run-hdfs-tests.sh

# Run specific HDFS test categories
cargo test hdfs --release                    # All HDFS-related tests
cargo test kerberos --release               # Kerberos authentication tests
cargo test argument_based --release         # Runtime reader selection tests

# Run real HDFS tests (requires HDFS cluster)
export HDFS_NAMENODE_URL=hdfs://your-namenode:9000
cargo test test_real_hdfs --ignored

# Run with detailed logging
RUST_LOG=info cargo test test_full_incremental_scan_workflow --test incremental_scan_test -- --nocapture
RUST_LOG=debug cargo test test_kerberos_valid_authentication -- --nocapture
```

## Development

### Key Components

- **`src/storage/duckdb_store.rs`**: Core DuckDB integration and analytics engine
- **`src/event_processor.rs`**: Spark event log parsing and processing
- **`src/api.rs`**: Standard Spark History Server API v1 endpoints + main router
- **`src/analytics_api.rs`**: Advanced cross-application analytics endpoints
- **`src/dashboard.rs`**: Web dashboard controllers and template rendering
- **`src/storage/file_reader.rs`**: File system abstraction (local + HDFS)
- **`templates/`**: Askama HTML templates for dashboard views

### Adding New Analytics

1. Add SQL query to `src/analytics_api.rs`
2. Define response model in `src/models.rs`
3. Add tests to `tests/analytics_api_test.rs`
4. Update API documentation

### Adding Dashboard Features

1. Add controller function to `src/dashboard.rs`
2. Create or modify HTML template in `templates/`
3. Add route to dashboard router
4. Test dashboard view in browser at `http://localhost:18080`

### Performance Tuning

- **Batch Size**: Adjust event processing batch size via configuration
- **Update Interval**: Balance freshness vs. system load
- **Cache Settings**: Enable local caching for frequently accessed data
- **DuckDB Settings**: Tune memory usage and query optimization