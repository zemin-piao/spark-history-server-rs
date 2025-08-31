Suppose you are a principal software engineer who has great system design skill and big data experience especially with spark and spark history server
# Spark History Server Rust Rewrite - Architecture Context

## Constraints
- Input data: Spark event logs stored in HDFS (`spark.eventLog.dir`)
- No cloud services (S3/DynamoDB) - self-hosted only
- Focus on core components: log processing & API serving
- UI layer is secondary priority

## Core Architecture Components

### 1. Log Processing Engine (ListenerBus Replacement)
**Purpose**: Poll HDFS, parse event logs, store in DuckDB for analytics

**Data Flow**:
1. Poll HDFS for new/updated `.lz4`/`.snappy` files
2. Stream → decompress → parse JSON events
3. Batch events (1000+ per batch) for write optimization
4. Store in DuckDB with extracted hot fields + raw JSON

**Storage Strategy (DuckDB Embedded)**:
- **Single events table**: All events with common fields + JSON column for flexibility
- **Optimized for analytics**: Cross-application queries, aggregations, time-series analysis
- **Write optimization**: Batched inserts, background flushing, single writer pattern
- **Schema flexibility**: Handle 10+ event types with hot field extraction + JSON fallback
- **Zero deployment**: Embedded database, single file storage

**Table Structure**:
```sql
CREATE TABLE events (
    id BIGINT PRIMARY KEY,
    app_id VARCHAR,
    event_type VARCHAR, 
    timestamp TIMESTAMP,
    raw_data JSON,
    -- Extracted hot fields for performance
    job_id BIGINT,
    stage_id BIGINT,
    task_id BIGINT, 
    duration_ms BIGINT
);

CREATE INDEX idx_app_time ON events(app_id, timestamp);
CREATE INDEX idx_event_type ON events(event_type);
```

**Tech Stack**:
- `duckdb` (embedded analytical database)
- `hdfs-native` (HDFS access)
- `lz4`/`snappy` (decompression)
- `serde_json` (event parsing)
- `tokio` (async runtime)

### 2. Data Serving API (Backend)
**Purpose**: Serve data from DuckDB with in-memory caching via REST API

**Endpoints**:
- `GET /api/v1/applications` (cross-app analytics enabled!)
- `GET /api/v1/applications/:app_id/jobs` (from DuckDB)
- `GET /api/v1/applications/:app_id/stages/:stage_id/tasks` (from DuckDB)
- `GET /api/v1/analytics/resource-usage` (new analytical endpoints)
- `GET /api/v1/analytics/performance-trends` (cross-application metrics)

**Performance**: 
- DuckDB columnar storage for fast aggregations
- JSON column for flexible event exploration  
- Proper indexing on hot query paths
- In-memory caching for recent applications and hot data
- Background batch processing for write optimization

**Tech Stack**:
- `axum` (HTTP server)
- `duckdb` (embedded analytical database)
- In-memory cache (dashmap/local) for hot data
- `tokio` (async runtime)

## Key Advantages
- **Cross-Application Analytics**: SQL queries across all Spark applications (major improvement over key-value stores)
- **Schema Flexibility**: JSON column + hot field extraction handles diverse event schemas (10+ types, 2-50+ fields each)
- **Zero Deployment Complexity**: Embedded DuckDB, single binary, single file storage
- **Analytical Power**: Columnar storage optimized for aggregations and time-series queries
- **Write Optimization**: Batched inserts and background processing handle write-heavy workloads
- **Type Safety**: Rust structs for common events, flexible JSON for edge cases

## Implementation Priority  
1. Define Spark event schemas in Rust (`spark-events` crate)
2. Build DuckDB integration and batched writer (`spark-storage` crate)
3. Implement HDFS log processor daemon
4. Build REST API server with DuckDB queries and caching

## File Structure Planned
