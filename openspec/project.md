# Project Context

## Purpose

**High-performance, analytics-first Spark History Server** implementation in Rust, purpose-built for **cross-application insights** and **enterprise-scale analytics**. Unlike traditional Spark History Servers focused on individual application details, this server excels at aggregations and analytics across multiple applications using DuckDB's analytical power.

**Key Differentiators:**
- Cross-application performance insights (not individual app debugging)
- DuckDB-powered analytics engine for aggregations and time-series analysis
- Enterprise scale: tested with 100K+ applications, 2M events at 10,700 events/sec
- Zero deployment complexity: single binary with embedded database

## Tech Stack

**Core Technologies:**
- **Rust** - Systems programming language (stable toolchain)
- **DuckDB** - Embedded columnar database for analytics
- **Axum** - High-performance web framework
- **Tokio** - Async runtime

**Storage & Data Processing:**
- **hdfs-native** - Production HDFS integration with Kerberos support
- **DuckDB** - Single source of truth for all event data
- **Serde** - Serialization/deserialization (JSON event parsing)

**Web & Templates:**
- **Askama** - Type-safe HTML templating
- **Tower** - Middleware and service composition

**Testing & Quality:**
- **cargo test** - Unit and integration testing
- **cargo clippy** - Strict linting (all warnings denied)
- **cargo fmt** - Code formatting

## Project Conventions

### Code Style
- **Strict Clippy Compliance**: All code must pass `cargo clippy --all-targets --all-features -- -D warnings`
- **Formatting**: Use `cargo fmt` for consistent code style
- **Error Handling**: Prefer `Result<T, E>` with descriptive error types; use circuit breakers for external dependencies
- **Naming**:
  - Snake_case for functions, variables, modules
  - PascalCase for types, structs, enums
  - SCREAMING_SNAKE_CASE for constants
  - Descriptive names over abbreviations (e.g., `event_processor` not `evt_proc`)

### Architecture Patterns

**Simplified DuckDB-Only Architecture:**
- All data flows through DuckDB (hybrid storage removed in 2024)
- Single events table with common fields + JSON column for flexibility
- Configuration: `database_directory` (replaced cache_directory/enable_cache)

**Circuit Breaker Fault Tolerance:**
- External dependency protection (HDFS, DuckDB operations)
- Automatic recovery with self-healing
- Configurable thresholds, timeouts, recovery windows
- Failure tracking and health monitoring

**Batch Processing Optimization:**
- Events processed in batches (1000+ events/batch)
- Background flushing for write optimization
- Schema flexibility: hot field extraction + JSON fallback

**HDFS Integration:**
- Production-ready `hdfs-native` with Kerberos authentication
- Connection pooling and timeout handling
- Mock/integration/real cluster testing support

### Testing Strategy

**Test Coverage Requirements:**
- 45+ tests covering all major functionality
- Integration tests for end-to-end workflows
- Load tests for performance validation (100K apps baseline)

**Test Categories:**
1. **Platform Engineering**: `cargo test --test analytics_api_test`
2. **Core Integration**: `cargo test --test integration_test`
3. **HDFS & Infrastructure**: `./scripts/run-hdfs-tests.sh`
4. **Performance & Load**: `./scripts/run_load_tests.sh`
5. **Incremental Processing**: `cargo test --test incremental_scan_test`

**Quality Gates:**
- All tests must pass before merge
- Clippy warnings are errors
- Load test benchmarks must not regress
- Real HDFS tests for production readiness (when applicable)

### Git Workflow

**Branch Strategy:**
- `main` - Production-ready code
- Feature branches - Short-lived, merge via PR
- No long-lived development branches

**Commit Conventions:**
- Descriptive commit messages (imperative mood)
- Reference issue numbers when applicable
- Keep commits atomic and focused

**PR Requirements:**
- All tests passing
- Clippy compliance
- Code review approval
- No merge of failing builds

## Domain Context

**Spark Event Log Processing:**
- Spark applications write event logs (JSON) to a directory (local or HDFS)
- Event types: SparkListenerApplicationStart, JobStart, StageCompleted, TaskEnd, etc. (10+ types)
- Logs can be compressed (.lz4, .snappy) or uncompressed
- Events arrive continuously; server must handle incremental scanning

**Analytics Use Cases:**
- Cross-application performance trends (not debugging individual apps)
- Resource utilization patterns across the cluster
- Task distribution and executor metrics
- Time-series analysis of Spark job performance
- Enterprise-wide Spark metrics and reporting

**Performance Characteristics:**
- Write throughput: 10,700+ events/sec sustained
- Query performance: <10ms for analytical queries
- Scale: 100K applications, 2M events tested
- Storage efficiency: 229 bytes per event average
- Memory usage: ~4.6KB per application in memory

## Important Constraints

**Performance Requirements:**
- Must handle 100K+ applications without degradation
- Event processing: >10,000 events/sec sustained
- Query latency: <10ms for analytics endpoints
- Memory efficient: <5KB per application overhead

**Compatibility:**
- Spark History Server v1 API compatibility (basic endpoints)
- Support standard Spark event log formats
- Handle compressed event logs (.lz4, .snappy)

**Security:**
- Kerberos authentication for HDFS access
- Keytab and ticket cache support
- No authentication bypass in production mode

**Scalability:**
- Single binary deployment (no distributed components)
- Embedded database (no external DB dependency)
- Efficient incremental scanning (avoid full re-reads)

## External Dependencies

**HDFS Clusters:**
- Namenode URL configuration required for HDFS mode
- Kerberos principal and keytab for secure clusters
- Connection pooling and timeout handling
- Health checks and retry logic

**Spark Event Logs:**
- Source of truth: event log files written by Spark applications
- Location: configurable directory (local filesystem or HDFS)
- Format: JSON lines, optionally compressed
- Schema: Spark's event log schema (multiple event types)

**DuckDB Storage:**
- Embedded database (no external service)
- Database directory: configurable storage location
- Columnar storage optimized for analytics
- Batched writes with background flushing

**Operating Environment:**
- Linux/macOS for development
- Production: typically Linux servers
- Network access to HDFS namenode (if HDFS mode enabled)
- File system access to event log directory
