use anyhow::{anyhow, Result};
use duckdb::{params, Connection};
use serde_json::Value;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::analytics_api::{
    CapacityTrend, CostOptimization, DifficultyLevel, EfficiencyAnalysis, EfficiencyCategory,
    OptimizationType, ResourceHog, ResourceType, RiskLevel,
};
use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use crate::models::ApplicationInfo;

/// DuckDB-based storage for Spark events with analytics capabilities
pub struct DuckDbStore {
    connection: Mutex<Connection>,
    circuit_breaker: Arc<CircuitBreaker>,
}

impl DuckDbStore {
    /// Create a new DuckDB store with the database file
    pub async fn new(db_path: &Path) -> Result<Self> {
        let conn = Connection::open(db_path)?;

        // Install and load JSON extension first - handle potential failures gracefully
        let install_result = conn.execute_batch("INSTALL 'json'");
        let load_result = conn.execute_batch("LOAD 'json'");

        // Try alternative approaches if the standard method fails
        if install_result.is_err() || load_result.is_err() {
            warn!("Standard JSON extension loading failed, trying alternative approach");

            // Try installing from httpfs first
            if let Err(e) = conn.execute_batch("INSTALL httpfs") {
                debug!("HTTPFS extension install failed: {}", e);
            }
            if let Err(e) = conn.execute_batch("LOAD httpfs") {
                debug!("HTTPFS extension load failed: {}", e);
            }

            // Force reinstall JSON extension
            if let Err(e) = conn.execute_batch("FORCE INSTALL json") {
                debug!("Force install JSON failed: {}", e);
            }

            // Final attempt to load JSON
            if let Err(e) = conn.execute_batch("LOAD json") {
                warn!("Failed to load JSON extension after all attempts: {}", e);
                // Instead of failing, let's create a JSON-less version
                info!("Running in JSON-less mode - some functionality may be limited");
            }
        }

        // Create the events table with schema - use VARCHAR for JSON if JSON type fails
        let table_creation_result = conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS events (
                id BIGINT PRIMARY KEY,
                app_id VARCHAR NOT NULL,
                event_type VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                raw_data JSON,
                job_id BIGINT,
                stage_id BIGINT,
                task_id BIGINT,
                duration_ms BIGINT
            );

            CREATE INDEX IF NOT EXISTS idx_app_time ON events(app_id, timestamp);
            CREATE INDEX IF NOT EXISTS idx_event_type ON events(event_type);
            CREATE INDEX IF NOT EXISTS idx_job_stage ON events(job_id, stage_id);
            "#,
        );

        // If JSON type creation fails, create table with VARCHAR instead
        if let Err(e) = table_creation_result {
            warn!(
                "Failed to create table with JSON type, falling back to VARCHAR: {}",
                e
            );
            conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS events (
                    id BIGINT PRIMARY KEY,
                    app_id VARCHAR NOT NULL,
                    event_type VARCHAR NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    raw_data VARCHAR,
                    job_id BIGINT,
                    stage_id BIGINT,
                    task_id BIGINT,
                    duration_ms BIGINT
                );

                CREATE INDEX IF NOT EXISTS idx_app_time ON events(app_id, timestamp);
                CREATE INDEX IF NOT EXISTS idx_event_type ON events(event_type);
                CREATE INDEX IF NOT EXISTS idx_job_stage ON events(job_id, stage_id);
                "#,
            )?;
            info!("Created events table with VARCHAR fallback for JSON data");
        }

        info!("DuckDB initialized at: {:?}", db_path);

        // Create circuit breaker for DuckDB operations
        let circuit_breaker_config = CircuitBreakerConfig {
            failure_threshold: 5,
            success_threshold: 3,
            timeout_duration: std::time::Duration::from_secs(10),
            window_duration: std::time::Duration::from_secs(60),
        };
        let circuit_breaker = Arc::new(CircuitBreaker::new(
            format!("duckdb-{}", db_path.display()),
            circuit_breaker_config,
        ));

        Ok(Self {
            connection: Mutex::new(conn),
            circuit_breaker,
        })
    }

    /// Insert a batch of events for better write performance
    pub async fn insert_events_batch(&self, events: Vec<SparkEvent>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let result = self
            .circuit_breaker
            .call(async {
                let conn = self.connection.lock().await;
                let mut stmt = conn
                    .prepare(
                        r#"
                INSERT INTO events (
                    id, app_id, event_type, timestamp, raw_data, 
                    job_id, stage_id, task_id, duration_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
                    )
                    .map_err(|e| anyhow!("Failed to prepare statement: {}", e))?;

                // Use transaction for batch insert
                let transaction_result = (|| -> Result<()> {
                    conn.execute_batch("BEGIN TRANSACTION")?;

                    for event in &events {
                        stmt.execute(params![
                            event.id,
                            &event.app_id,
                            &event.event_type,
                            &event.timestamp,
                            &event.raw_data.to_string(),
                            event.job_id,
                            event.stage_id,
                            event.task_id,
                            event.duration_ms,
                        ])?;
                    }

                    conn.execute_batch("COMMIT")?;
                    Ok(())
                })();

                if let Err(e) = transaction_result {
                    // Rollback on any error
                    if let Err(rollback_err) = conn.execute_batch("ROLLBACK") {
                        warn!("Failed to rollback transaction: {}", rollback_err);
                    }
                    return Err(e);
                }

                debug!("Inserted {} events into DuckDB", events.len());
                Ok(())
            })
            .await;

        match result {
            Ok(db_result) => Ok(db_result),
            Err(e) => {
                if e.is_circuit_open() {
                    Err(anyhow!("Database insert failed: circuit breaker is open"))
                } else {
                    Err(e
                        .into_inner()
                        .unwrap_or_else(|| anyhow!("Unknown database error")))
                }
            }
        }
    }

    /// Get cross-application summary statistics
    pub async fn get_cross_app_summary(&self) -> Result<crate::dashboard::SimpleCrossAppSummary> {
        let result = self.circuit_breaker.call(async {
            let conn = self.connection.lock().await;

            let mut stmt = conn.prepare(r#"
                SELECT 
                    COUNT(DISTINCT app_id) as total_applications,
                    COUNT(DISTINCT CASE WHEN CAST(timestamp AS TIMESTAMP) >= CURRENT_TIMESTAMP::TIMESTAMP - '1 day'::INTERVAL THEN app_id END) as active_applications,
                    COUNT(*) as total_events,
                    COUNT(CASE WHEN event_type = 'SparkListenerTaskEnd' THEN 1 END) as total_tasks_completed,
                    COUNT(CASE WHEN event_type = 'SparkListenerTaskEnd' AND raw_data LIKE '%"failed":true%' THEN 1 END) as total_tasks_failed,
                    COALESCE(AVG(duration_ms), 0) as avg_task_duration_ms,
                    0 as peak_concurrent_executors
                FROM events
            "#).map_err(|e| anyhow!("Failed to prepare summary query: {}", e))?;

            let row = stmt.query_row(params![], |row| {
                Ok(crate::dashboard::SimpleCrossAppSummary {
                    total_applications: row.get::<_, i64>(0)?,
                    active_applications: row.get::<_, i64>(1)?,
                    total_events: row.get::<_, i64>(2)?,
                    total_tasks_completed: row.get::<_, i64>(3)?,
                    total_tasks_failed: row.get::<_, i64>(4)?,
                    avg_task_duration_ms: format!("{:.0}", row.get::<_, f64>(5)?),
                    total_data_processed_gb: "0".to_string(), // TODO: calculate from events
                    peak_concurrent_executors: row.get::<_, i64>(6)?,
                })
            }).map_err(|e| anyhow!("Failed to execute summary query: {}", e))?;

            Ok(row)
        }).await;

        match result {
            Ok(summary) => Ok(summary),
            Err(e) => {
                if e.is_circuit_open() {
                    Err(anyhow!("Database query failed: circuit breaker is open"))
                } else {
                    Err(e
                        .into_inner()
                        .unwrap_or_else(|| anyhow!("Unknown database error")))
                }
            }
        }
    }

    /// Get active applications summary for dashboard
    pub async fn get_active_applications(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<crate::dashboard::SimpleApplicationSummary>> {
        let conn = self.connection.lock().await;

        let limit_clause = limit.map(|l| format!(" LIMIT {}", l)).unwrap_or_default();

        let query = format!(
            r#"
            SELECT 
                app_id,
                'system' as user,
                COALESCE(EPOCH_MS(CAST(MAX(timestamp) AS TIMESTAMP) - CAST(MIN(timestamp) AS TIMESTAMP)), 0) as duration_ms,
                32 as cores,
                8192 as memory,
                CASE WHEN CAST(MAX(timestamp) AS TIMESTAMP) >= CURRENT_TIMESTAMP::TIMESTAMP - '1 day'::INTERVAL THEN 'RUNNING' ELSE 'FINISHED' END as status
            FROM events 
            GROUP BY app_id 
            ORDER BY MAX(timestamp) DESC
            {}
        "#,
            limit_clause
        );

        let mut stmt = conn.prepare(&query)?;
        let rows = stmt.query_map(params![], |row| {
            let duration_ms: i64 = row.get(2)?;
            let duration_str = if duration_ms > 0 {
                format!("{}s", duration_ms / 1000)
            } else {
                "0s".to_string()
            };

            Ok(crate::dashboard::SimpleApplicationSummary {
                id: row.get::<_, String>(0)?,
                user: row.get::<_, String>(1)?,
                duration: duration_str,
                cores: row.get::<_, u32>(3)?,
                memory: row.get::<_, u32>(4)?,
                status: row.get::<_, String>(5)?,
            })
        })?;

        let mut applications = Vec::new();
        for row in rows {
            applications.push(row?);
        }

        Ok(applications)
    }

    /// Get all applications with filtering support
    pub async fn get_applications(
        &self,
        limit: Option<usize>,
        min_date: Option<&str>,
        max_date: Option<&str>,
        _status: Option<&str>, // TODO: implement status filtering
    ) -> Result<Vec<ApplicationInfo>> {
        let conn = self.connection.lock().await;

        let mut query = String::from(
            r#"
            SELECT 
                app_id,
                MIN(timestamp) as start_time,
                MAX(timestamp) as end_time,
                COUNT(*) as event_count
            FROM events 
            WHERE 1=1
            "#,
        );

        let mut param_values = Vec::new();

        if let Some(min) = min_date {
            query.push_str(" AND timestamp >= ?");
            param_values.push(min.to_string());
        }

        if let Some(max) = max_date {
            query.push_str(" AND timestamp <= ?");
            param_values.push(max.to_string());
        }

        query.push_str(" GROUP BY app_id ORDER BY end_time DESC");

        if let Some(limit) = limit {
            query.push_str(&format!(" LIMIT {}", limit));
        }

        let mut stmt = conn.prepare(&query)?;
        let param_refs: Vec<&dyn duckdb::ToSql> = param_values
            .iter()
            .map(|p| p as &dyn duckdb::ToSql)
            .collect();
        let rows = stmt.query_map(param_refs.as_slice(), |row| {
            Ok(ApplicationInfo {
                id: row.get(0)?,
                name: format!("Application {}", row.get::<_, String>(0)?),
                cores_granted: Some(0),
                max_cores: Some(0),
                cores_per_executor: Some(1),
                memory_per_executor_mb: Some(1024),
                attempts: vec![], // TODO: Build from events
            })
        })?;

        let mut applications = Vec::new();
        for row in rows {
            applications.push(row?);
        }

        Ok(applications)
    }

    /// Get events for a specific application
    #[allow(dead_code)]
    pub async fn get_app_events(&self, app_id: &str) -> Result<Vec<Value>> {
        let conn = self.connection.lock().await;

        let mut stmt =
            conn.prepare("SELECT raw_data FROM events WHERE app_id = ? ORDER BY timestamp")?;

        let rows = stmt.query_map([app_id], |row| {
            let json_str: String = row.get(0)?;
            match serde_json::from_str::<Value>(&json_str) {
                Ok(val) => Ok(val),
                Err(e) => {
                    warn!("Failed to parse JSON in app events: {}", e);
                    Ok(Value::Null)
                }
            }
        })?;

        let mut events = Vec::new();
        for row in rows {
            events.push(row?);
        }

        Ok(events)
    }

    /// Cross-application analytics query example
    #[allow(dead_code)]
    pub async fn get_resource_usage_summary(&self) -> Result<Vec<ResourceUsage>> {
        let conn = self.connection.lock().await;

        let mut stmt = conn.prepare(
            r#"
            SELECT 
                app_id,
                event_type,
                COUNT(*) as event_count,
                AVG(duration_ms) as avg_duration_ms,
                DATE(timestamp) as event_date
            FROM events 
            WHERE event_type IN ('TaskEnd', 'JobEnd', 'StageCompleted')
            GROUP BY app_id, event_type, DATE(timestamp)
            ORDER BY event_date DESC, app_id
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(ResourceUsage {
                app_id: row.get(0)?,
                event_type: row.get(1)?,
                event_count: row.get(2)?,
                avg_duration_ms: row.get(3)?,
                event_date: row.get(4)?,
            })
        })?;

        let mut usage_data = Vec::new();
        for row in rows {
            usage_data.push(row?);
        }

        Ok(usage_data)
    }

    /// List all applications (compatibility method)
    pub async fn list(&self) -> Result<Vec<ApplicationInfo>> {
        self.get_applications(None, None, None, None).await
    }

    /// Get a specific application (compatibility method)
    pub async fn get(&self, app_id: &str) -> Result<Option<ApplicationInfo>> {
        let apps = self.get_applications(Some(1), None, None, None).await?;
        Ok(apps.into_iter().find(|app| app.id == app_id))
    }

    /// Store an application (compatibility method - simplified)
    pub async fn put(&self, _app_id: &str, _app_info: ApplicationInfo) -> Result<()> {
        // For now, we'll just log this since we're focusing on event-based storage
        debug!("Put application called for: {}", _app_id);
        Ok(())
    }

    /// Store a single event (helper method)
    pub async fn store_event(&self, event_id: i64, app_id: &str, raw_event: &Value) -> Result<()> {
        let spark_event = SparkEvent::from_json(raw_event, app_id, event_id)?;
        self.insert_events_batch(vec![spark_event]).await
    }

    /// Clear all data from the database (for testing purposes only)
    #[cfg(test)]
    #[allow(dead_code)]
    pub async fn cleanup_for_testing(&self) -> Result<()> {
        let conn = self.connection.lock().await;
        conn.execute_batch("DELETE FROM events")?;
        info!("Cleared all events from DuckDB for testing");
        Ok(())
    }

    /// Get executor summary for a specific application
    pub async fn get_executor_summary(
        &self,
        app_id: &str,
    ) -> Result<Vec<crate::models::ExecutorSummary>> {
        let conn = self.connection.lock().await;

        let query = r#"
            WITH executor_events AS (
                SELECT 
                    COALESCE(
                        JSON_EXTRACT_STRING(raw_data, '$.Executor ID'),
                        JSON_EXTRACT_STRING(raw_data, '$.Task Info.Executor ID')
                    ) as executor_id,
                    JSON_EXTRACT_STRING(raw_data, '$.Executor Info.Host') as host,
                    JSON_EXTRACT_STRING(raw_data, '$.Task Info.Host') as task_host,
                    JSON_EXTRACT(raw_data, '$.Executor Info.Total Cores') as total_cores,
                    JSON_EXTRACT(raw_data, '$.Executor Info.Max Memory') as max_memory,
                    JSON_EXTRACT(raw_data, '$.Task Metrics.Executor Run Time') as run_time,
                    JSON_EXTRACT(raw_data, '$.Task Metrics.JVM GC Time') as gc_time,
                    JSON_EXTRACT(raw_data, '$.Task Metrics.Input Metrics.Bytes Read') as input_bytes,
                    JSON_EXTRACT(raw_data, '$.Task Metrics.Shuffle Read Metrics.Total Bytes Read') as shuffle_read_bytes,
                    JSON_EXTRACT(raw_data, '$.Task Metrics.Shuffle Write Metrics.Bytes Written') as shuffle_write_bytes,
                    event_type,
                    timestamp
                FROM events 
                WHERE app_id = ?
                  AND (event_type LIKE '%Executor%' OR event_type LIKE '%Task%')
            ),
            executor_added AS (
                SELECT 
                    executor_id,
                    COALESCE(host, task_host) as host_port,
                    CAST(total_cores AS INTEGER) as total_cores,
                    CAST(max_memory AS BIGINT) as max_memory,
                    MIN(timestamp) as add_time
                FROM executor_events
                WHERE event_type = 'SparkListenerExecutorAdded'
                GROUP BY executor_id, host, task_host, total_cores, max_memory
            ),
            executor_removed AS (
                SELECT 
                    executor_id,
                    MAX(timestamp) as remove_time
                FROM executor_events
                WHERE event_type = 'SparkListenerExecutorRemoved'
                GROUP BY executor_id
            ),
            task_metrics AS (
                SELECT 
                    executor_id,
                    COUNT(*) as total_tasks,
                    COUNT(CASE WHEN event_type = 'SparkListenerTaskEnd' THEN 1 END) as completed_tasks,
                    SUM(CAST(run_time AS BIGINT)) as total_duration,
                    SUM(CAST(gc_time AS BIGINT)) as total_gc_time,
                    SUM(CAST(input_bytes AS BIGINT)) as total_input_bytes,
                    SUM(CAST(shuffle_read_bytes AS BIGINT)) as total_shuffle_read,
                    SUM(CAST(shuffle_write_bytes AS BIGINT)) as total_shuffle_write
                FROM executor_events
                WHERE event_type IN ('SparkListenerTaskStart', 'SparkListenerTaskEnd')
                  AND executor_id IS NOT NULL
                GROUP BY executor_id
            )
            SELECT 
                COALESCE(ea.executor_id, tm.executor_id, 'driver') as id,
                COALESCE(ea.host_port, 'localhost:0') as host_port,
                CASE WHEN er.executor_id IS NULL THEN true ELSE false END as is_active,
                0 as rdd_blocks,
                0 as memory_used,
                0 as disk_used,
                COALESCE(ea.total_cores, 1) as total_cores,
                COALESCE(ea.total_cores, 1) as max_tasks,
                0 as active_tasks,
                0 as failed_tasks,
                COALESCE(tm.completed_tasks, 0) as completed_tasks,
                COALESCE(tm.total_tasks, 0) as total_tasks,
                COALESCE(tm.total_duration, 0) as total_duration,
                COALESCE(tm.total_gc_time, 0) as total_gc_time,
                COALESCE(tm.total_input_bytes, 0) as total_input_bytes,
                COALESCE(tm.total_shuffle_read, 0) as total_shuffle_read,
                COALESCE(tm.total_shuffle_write, 0) as total_shuffle_write,
                false as is_excluded,
                COALESCE(ea.max_memory, 1073741824) as max_memory,
                0 as resource_profile_id
            FROM executor_added ea
            FULL OUTER JOIN executor_removed er ON ea.executor_id = er.executor_id
            FULL OUTER JOIN task_metrics tm ON COALESCE(ea.executor_id, er.executor_id) = tm.executor_id
            ORDER BY COALESCE(ea.executor_id, tm.executor_id, 'driver')
        "#;

        let mut stmt = conn.prepare(query)?;
        let rows = stmt.query_map([app_id], |row| {
            Ok(crate::models::ExecutorSummary {
                id: row.get(0)?,
                host_port: row.get(1)?,
                is_active: row.get(2)?,
                rdd_blocks: row.get(3)?,
                memory_used: row.get(4)?,
                disk_used: row.get(5)?,
                total_cores: row.get(6)?,
                max_tasks: row.get(7)?,
                active_tasks: row.get(8)?,
                failed_tasks: row.get(9)?,
                completed_tasks: row.get(10)?,
                total_tasks: row.get(11)?,
                total_duration: row.get(12)?,
                total_gc_time: row.get(13)?,
                total_input_bytes: row.get(14)?,
                total_shuffle_read: row.get(15)?,
                total_shuffle_write: row.get(16)?,
                is_excluded: row.get(17)?,
                max_memory: row.get(18)?,
                add_time: chrono::Utc::now(),
                remove_time: None,
                remove_reason: None,
                executor_logs: std::collections::HashMap::new(),
                memory_metrics: None,
                attributes: std::collections::HashMap::new(),
                resources: std::collections::HashMap::new(),
                resource_profile_id: row.get(19)?,
                excluded_in_stages: vec![],
            })
        })?;

        let mut executors = Vec::new();
        for row in rows {
            executors.push(row?);
        }

        Ok(executors)
    }

    /// Get total count of events in the database
    #[allow(dead_code)]
    pub async fn count_events(&self) -> Result<i64> {
        let conn = self.connection.lock().await;
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM events")?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count)
    }

    /// Get the maximum event ID from the database
    #[allow(dead_code)]
    pub async fn get_max_event_id(&self) -> Result<Option<i64>> {
        let conn = self.connection.lock().await;
        let mut stmt = conn.prepare("SELECT MAX(id) FROM events")?;
        let max_id: Option<i64> = stmt.query_row([], |row| row.get(0))?;
        Ok(max_id)
    }

    /// Clear all data from the database with explicit safety check
    /// Only works when ENABLE_DB_CLEANUP environment variable is set to "true"
    #[allow(dead_code)]
    pub async fn cleanup_database(&self) -> Result<()> {
        if std::env::var("ENABLE_DB_CLEANUP").unwrap_or_default() != "true" {
            return Err(anyhow::anyhow!(
                "Database cleanup disabled. Set ENABLE_DB_CLEANUP=true to enable for testing."
            ));
        }

        let conn = self.connection.lock().await;
        conn.execute_batch("DELETE FROM events")?;
        warn!("DANGER: Cleared all events from DuckDB database!");
        Ok(())
    }

    // ============================================================================
    // NEW: Platform Engineering Focused Query Methods
    // ============================================================================

    /// Get TOP resource consuming applications (Memory, CPU, Disk hogs)
    pub async fn get_top_resource_consumers(
        &self,
        params: &crate::analytics_api::AnalyticsQuery,
    ) -> anyhow::Result<Vec<ResourceHog>> {
        let conn = self.connection.lock().await;

        // Query for TOP memory consumers
        let query = r#"
            WITH resource_usage AS (
                SELECT 
                    app_id,
                    'app_' || app_id as app_name,
                    AVG(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Peak Execution Memory') AS BIGINT)) / 1048576.0 as avg_memory_mb,
                    MAX(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Peak Execution Memory') AS BIGINT)) / 1048576.0 as peak_memory_mb,
                    SUM(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Memory Bytes Spilled') AS BIGINT)) / 1048576.0 as total_memory_spill_mb,
                    SUM(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Disk Bytes Spilled') AS BIGINT)) / 1048576.0 as total_disk_spill_mb,
                    AVG(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.JVM GC Time') AS BIGINT)) / 1000.0 as avg_gc_time_s,
                    AVG(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Executor Run Time') AS BIGINT)) / 1000.0 as avg_run_time_s,
                    COUNT(*) as task_count,
                    MAX(timestamp) as last_seen
                FROM events 
                WHERE event_type = 'SparkListenerTaskEnd'
                AND (? IS NULL OR timestamp >= ?)
                AND (? IS NULL OR timestamp <= ?)
                AND (? IS NULL OR app_id = ?)
                GROUP BY app_id
            )
            SELECT 
                app_id,
                app_name,
                'Memory' as resource_type,
                peak_memory_mb as consumption_value,
                'MB' as unit,
                -- Real efficiency calculation based on spilling and GC overhead
                CASE 
                    WHEN total_memory_spill_mb > 2000 THEN 15.0  -- Heavy spilling
                    WHEN total_memory_spill_mb > 500 THEN 35.0   -- Moderate spilling  
                    WHEN (avg_gc_time_s / NULLIF(avg_run_time_s, 0)) > 0.15 THEN 25.0  -- High GC overhead
                    WHEN (avg_gc_time_s / NULLIF(avg_run_time_s, 0)) > 0.05 THEN 65.0  -- Moderate GC
                    ELSE 85.0  -- Good efficiency
                END as efficiency_score,
                -- Real efficiency explanation
                CASE 
                    WHEN total_memory_spill_mb > 2000 THEN CAST(15.0 AS VARCHAR) || '% (' || CAST(ROUND(total_memory_spill_mb/1024, 1) AS VARCHAR) || 'GB spilling)'
                    WHEN total_memory_spill_mb > 500 THEN CAST(35.0 AS VARCHAR) || '% (' || CAST(ROUND(total_memory_spill_mb, 0) AS VARCHAR) || 'MB spilling)'
                    WHEN (avg_gc_time_s / NULLIF(avg_run_time_s, 0)) > 0.15 THEN CAST(25.0 AS VARCHAR) || '% (high GC overhead: ' || CAST(ROUND((avg_gc_time_s / NULLIF(avg_run_time_s, 0)) * 100, 1) AS VARCHAR) || '%)'
                    WHEN (avg_gc_time_s / NULLIF(avg_run_time_s, 0)) > 0.05 THEN CAST(65.0 AS VARCHAR) || '% (moderate GC: ' || CAST(ROUND((avg_gc_time_s / NULLIF(avg_run_time_s, 0)) * 100, 1) AS VARCHAR) || '%)'
                    ELSE CAST(85.0 AS VARCHAR) || '% (well-tuned)'
                END as efficiency_explanation,
                peak_memory_mb * 0.001 as cost_impact, -- Rough cost estimate
                CASE
                    WHEN total_memory_spill_mb > 2000 THEN 'URGENT: Reduce executors or increase memory - massive spilling detected'
                    WHEN total_memory_spill_mb > 500 THEN 'Increase executor memory to reduce spilling'
                    WHEN (avg_gc_time_s / NULLIF(avg_run_time_s, 0)) > 0.15 THEN 'Tune GC settings or increase heap size'
                    WHEN peak_memory_mb > 8192 THEN 'Consider reducing executor memory'
                    ELSE 'Memory usage appears optimal'
                END as recommendation,
                last_seen::VARCHAR
            FROM resource_usage
            WHERE peak_memory_mb > 0
            ORDER BY peak_memory_mb DESC
            LIMIT ?
        "#;

        let limit = params.limit.unwrap_or(10);
        let mut stmt = conn.prepare(query)?;

        let rows = stmt.query_map(
            [
                &params.start_date,
                &params.start_date,
                &params.end_date,
                &params.end_date,
                &params.app_id,
                &params.app_id,
                &Some(limit.to_string()),
            ],
            |row| {
                Ok(ResourceHog {
                    app_id: row.get(0)?,
                    app_name: row.get(1)?,
                    resource_type: ResourceType::Memory,
                    consumption_value: row.get(3)?,
                    consumption_unit: row.get(4)?,
                    utilization_percentage: 0.0, // TODO: Calculate from allocated vs used
                    efficiency_score: row.get(5)?,
                    efficiency_explanation: row.get(6)?,
                    cost_impact: row.get(7)?,
                    recommendation: row.get(8)?,
                    last_seen: row.get(9)?,
                })
            },
        )?;

        let mut resource_hogs = Vec::new();
        for row in rows {
            resource_hogs.push(row?);
        }

        Ok(resource_hogs)
    }

    /// Get application efficiency analysis (over/under-provisioned apps)
    pub async fn get_efficiency_analysis(
        &self,
        params: &crate::analytics_api::AnalyticsQuery,
    ) -> anyhow::Result<Vec<EfficiencyAnalysis>> {
        let conn = self.connection.lock().await;

        let query = r#"
            WITH app_efficiency AS (
                SELECT 
                    app_id,
                    'app_' || app_id as app_name,
                    AVG(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Peak Execution Memory') AS BIGINT)) / 1048576.0 as avg_memory_usage_mb,
                    MAX(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Peak Execution Memory') AS BIGINT)) / 1048576.0 as peak_memory_usage_mb,
                    AVG(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Executor CPU Time') AS BIGINT)) / 1000.0 as avg_cpu_time_ms,
                    AVG(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Executor Run Time') AS BIGINT)) / 1000.0 as avg_wall_time_ms,
                    SUM(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Memory Bytes Spilled') AS BIGINT)) / 1048576.0 as total_memory_spill_mb,
                    AVG(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.JVM GC Time') AS BIGINT)) / 1000.0 as avg_gc_time_ms,
                    COUNT(*) as task_count
                FROM events 
                WHERE event_type = 'SparkListenerTaskEnd'
                AND (? IS NULL OR timestamp >= ?)
                AND (? IS NULL OR timestamp <= ?)
                AND (? IS NULL OR app_id = ?)
                GROUP BY app_id
                HAVING task_count > 1  -- Reduced threshold to catch more apps
            )
            SELECT 
                app_id,
                app_name,
                CASE 
                    WHEN total_memory_spill_mb > 1000 THEN 'OverProvisioned'  -- Memory spilling indicates over-provisioning
                    WHEN COALESCE((avg_cpu_time_ms / NULLIF(avg_wall_time_ms, 0) * 100), 0) < 30 THEN 'OverProvisioned'
                    WHEN COALESCE((avg_cpu_time_ms / NULLIF(avg_wall_time_ms, 0) * 100), 0) > 90 THEN 'UnderProvisioned'
                    ELSE 'WellTuned'
                END as efficiency_category,
                -- Memory efficiency with spilling consideration (rounded to 1 decimal place)
                ROUND(CASE 
                    WHEN total_memory_spill_mb > 1000 THEN 25.0
                    WHEN total_memory_spill_mb > 100 THEN 45.0
                    ELSE COALESCE(LEAST(100.0, (avg_memory_usage_mb / NULLIF(peak_memory_usage_mb, 0) * 100)), 0)
                END, 1) as memory_efficiency,
                -- Memory efficiency explanation
                COALESCE(
                    CASE 
                        WHEN total_memory_spill_mb > 1000 THEN CAST(25.0 AS VARCHAR) || '% (' || CAST(ROUND(total_memory_spill_mb/1024, 1) AS VARCHAR) || 'GB spilling)'
                        WHEN total_memory_spill_mb > 100 THEN CAST(45.0 AS VARCHAR) || '% (' || CAST(ROUND(total_memory_spill_mb, 0) AS VARCHAR) || 'MB spilling)'
                        WHEN (avg_gc_time_ms / NULLIF(avg_wall_time_ms, 0)) > 0.15 THEN CAST(ROUND(COALESCE(avg_memory_usage_mb / NULLIF(peak_memory_usage_mb, 0) * 100, 0), 0) AS VARCHAR) || '% (high GC overhead)'
                        ELSE CAST(ROUND(COALESCE(avg_memory_usage_mb / NULLIF(peak_memory_usage_mb, 0) * 100, 50), 0) AS VARCHAR) || '% (normal usage)'
                    END,
                    '50% (insufficient data)'
                ) as memory_efficiency_explanation,
                -- CPU efficiency (rounded to 1 decimal place)
                ROUND(COALESCE(LEAST(100.0, (avg_cpu_time_ms / NULLIF(avg_wall_time_ms, 0) * 100)), 0), 1) as cpu_efficiency,
                -- CPU efficiency explanation  
                COALESCE(
                    CASE 
                        WHEN COALESCE((avg_cpu_time_ms / NULLIF(avg_wall_time_ms, 0) * 100), 0) < 10 THEN CAST(ROUND(COALESCE((avg_cpu_time_ms / NULLIF(avg_wall_time_ms, 0) * 100), 0), 1) AS VARCHAR) || '% (serial processing)'
                        WHEN COALESCE((avg_cpu_time_ms / NULLIF(avg_wall_time_ms, 0) * 100), 0) > 95 THEN CAST(ROUND(COALESCE((avg_cpu_time_ms / NULLIF(avg_wall_time_ms, 0) * 100), 0), 1) AS VARCHAR) || '% (CPU bottleneck)'
                        ELSE CAST(ROUND(COALESCE((avg_cpu_time_ms / NULLIF(avg_wall_time_ms, 0) * 100), 50), 1) AS VARCHAR) || '% (good parallelism)'
                    END,
                    '50.0% (insufficient data)'
                ) as cpu_efficiency_explanation,
                peak_memory_usage_mb * 0.0007 as recommended_memory_gb, -- 70% of peak for buffer
                GREATEST(1, avg_cpu_time_ms / avg_wall_time_ms) as recommended_cpu_cores,
                CASE
                    WHEN total_memory_spill_mb > 1000 THEN peak_memory_usage_mb * 0.002
                    WHEN COALESCE((avg_cpu_time_ms / NULLIF(avg_wall_time_ms, 0) * 100), 0) < 30 THEN peak_memory_usage_mb * 0.0005
                    ELSE 0
                END as potential_cost_savings,
                CASE
                    WHEN task_count < 3 THEN 'High'
                    WHEN COALESCE((avg_cpu_time_ms / NULLIF(avg_wall_time_ms, 0) * 100), 0) < 20 THEN 'Low'
                    ELSE 'Medium'
                END as risk_level
            FROM app_efficiency
            ORDER BY potential_cost_savings DESC
            LIMIT ?
        "#;

        let limit = params.limit.unwrap_or(20);
        let mut stmt = conn.prepare(query)?;

        let rows = stmt.query_map(
            [
                &params.start_date,
                &params.start_date,
                &params.end_date,
                &params.end_date,
                &params.app_id,
                &params.app_id,
                &Some(limit.to_string()),
            ],
            |row| {
                let category_str: String = row.get(2)?;
                let efficiency_category = match category_str.as_str() {
                    "OverProvisioned" => EfficiencyCategory::OverProvisioned,
                    "UnderProvisioned" => EfficiencyCategory::UnderProvisioned,
                    _ => EfficiencyCategory::WellTuned,
                };

                let risk_str: String = row.get(10)?; // Updated position
                let risk_level = match risk_str.as_str() {
                    "Low" => RiskLevel::Low,
                    "High" => RiskLevel::High,
                    _ => RiskLevel::Medium,
                };

                let memory_efficiency: f64 = row.get(3)?;
                let cpu_efficiency: f64 = row.get(5)?; // Updated position

                let optimization_actions = match efficiency_category {
                    EfficiencyCategory::OverProvisioned => vec![
                        "Reduce executor memory allocation".to_string(),
                        "Decrease number of executor cores".to_string(),
                        "Consider smaller instance types".to_string(),
                    ],
                    EfficiencyCategory::UnderProvisioned => vec![
                        "Increase executor memory allocation".to_string(),
                        "Add more executor cores".to_string(),
                        "Monitor for OOM errors".to_string(),
                    ],
                    EfficiencyCategory::WellTuned => {
                        vec!["Configuration appears optimal".to_string()]
                    }
                };

                Ok(EfficiencyAnalysis {
                    app_id: row.get(0)?,
                    app_name: row.get(1)?,
                    efficiency_category,
                    memory_efficiency,
                    memory_efficiency_explanation: row.get(4)?,
                    cpu_efficiency,
                    cpu_efficiency_explanation: row.get(6)?,
                    recommended_memory_gb: Some(row.get(7)?),
                    recommended_cpu_cores: Some(row.get(8)?),
                    potential_cost_savings: row.get(9)?,
                    risk_level,
                    optimization_actions,
                })
            },
        )?;

        let mut analysis = Vec::new();
        for row in rows {
            analysis.push(row?);
        }

        Ok(analysis)
    }

    /// Get capacity usage trends for planning
    pub async fn get_capacity_usage_trends(
        &self,
        params: &crate::analytics_api::AnalyticsQuery,
    ) -> anyhow::Result<Vec<CapacityTrend>> {
        let conn = self.connection.lock().await;

        let query = r#"
            WITH daily_usage AS (
                SELECT 
                    DATE(timestamp) as date,
                    SUM(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Peak Execution Memory') AS BIGINT)) / 1073741824.0 as total_memory_gb,
                    COUNT(DISTINCT app_id) as concurrent_apps,
                    COUNT(*) as total_tasks,
                    AVG(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Executor CPU Time') AS BIGINT)) / 1000.0 as avg_cpu_usage
                FROM events 
                WHERE event_type = 'SparkListenerTaskEnd'
                AND (? IS NULL OR timestamp >= ?)
                AND (? IS NULL OR timestamp <= ?)
                GROUP BY DATE(timestamp)
            )
            SELECT 
                date::VARCHAR,
                COALESCE(total_memory_gb, 0) as total_memory_gb_used,
                COALESCE(avg_cpu_usage * concurrent_apps, 0) as total_cpu_cores_used,
                concurrent_apps as peak_concurrent_applications,
                COALESCE(total_memory_gb / NULLIF(concurrent_apps, 0), 0) as average_resource_utilization,
                LEAST(100, COALESCE(total_memory_gb / 1024 * 100, 0)) as cluster_capacity_percentage, -- Assume 1TB cluster
                NULL as projected_growth_rate -- TODO: Calculate from historical data
            FROM daily_usage
            ORDER BY date DESC
            LIMIT ?
        "#;

        let limit = params.limit.unwrap_or(30);
        let mut stmt = conn.prepare(query)?;

        let rows = stmt.query_map(
            [
                &params.start_date,
                &params.start_date,
                &params.end_date,
                &params.end_date,
                &Some(limit.to_string()),
            ],
            |row| {
                Ok(CapacityTrend {
                    date: row.get(0)?,
                    total_memory_gb_used: row.get(1)?,
                    total_cpu_cores_used: row.get(2)?,
                    peak_concurrent_applications: row.get(3)?,
                    average_resource_utilization: row.get(4)?,
                    cluster_capacity_percentage: row.get(5)?,
                    projected_growth_rate: row.get(6)?,
                })
            },
        )?;

        let mut trends = Vec::new();
        for row in rows {
            trends.push(row?);
        }

        Ok(trends)
    }

    /// Get cost optimization opportunities
    ///
    /// Cost Calculation Model:
    /// - Current Cost: peak_memory_mb * $0.001 (1GB = $1.00/month)
    /// - Optimized Cost: Varies by optimization type:
    ///   - Over-provisioned memory: 40% reduction (peak_memory_mb * $0.0006)
    ///   - Poor partitioning: 20% reduction (peak_memory_mb * $0.0008)  
    ///   - Default (spot instances): 30% reduction (peak_memory_mb * $0.0007)
    /// - Savings: current_cost - optimized_cost
    pub async fn get_cost_optimization_opportunities(
        &self,
        params: &crate::analytics_api::AnalyticsQuery,
    ) -> anyhow::Result<Vec<CostOptimization>> {
        let conn = self.connection.lock().await;

        let query = r#"
            WITH cost_analysis AS (
                SELECT 
                    app_id,
                    'app_' || app_id as app_name,
                    AVG(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Peak Execution Memory') AS BIGINT)) / 1048576.0 as avg_memory_mb,
                    MAX(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Peak Execution Memory') AS BIGINT)) / 1048576.0 as peak_memory_mb,
                    AVG(duration_ms) / 1000.0 as avg_task_duration_s,
                    COUNT(*) as task_count,
                    SUM(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Disk Bytes Spilled') AS BIGINT)) / 1048576.0 as total_spill_mb
                FROM events 
                WHERE event_type = 'SparkListenerTaskEnd'
                AND (? IS NULL OR timestamp >= ?)
                AND (? IS NULL OR timestamp <= ?)
                AND (? IS NULL OR app_id = ?)
                GROUP BY app_id
                HAVING task_count > 1
            )
            SELECT 
                app_id,
                app_name,
                CASE 
                    WHEN avg_memory_mb < peak_memory_mb * 0.8 THEN 'ReduceMemory'
                    WHEN total_spill_mb > 100 THEN 'OptimizePartitioning'
                    WHEN avg_task_duration_s > 30 THEN 'ReduceExecutors'
                    ELSE 'EnableSpotInstances'
                END as optimization_type,
                ROUND(peak_memory_mb * 0.001, 4) as current_cost, -- Rough cost estimate (rounded to 4 decimal places)
                ROUND(CASE
                    WHEN avg_memory_mb < peak_memory_mb * 0.8 THEN peak_memory_mb * 0.0006 -- 40% memory reduction
                    WHEN total_spill_mb > 100 THEN peak_memory_mb * 0.0008 -- 20% savings from better partitioning
                    ELSE peak_memory_mb * 0.0007 -- 30% savings from spot instances
                END, 4) as optimized_cost,
                CASE
                    WHEN avg_memory_mb < peak_memory_mb * 0.8 THEN 40.0
                    WHEN total_spill_mb > 100 THEN 20.0
                    ELSE 30.0
                END as savings_percentage,
                CASE
                    WHEN task_count > 50 THEN 85.0
                    WHEN task_count > 20 THEN 70.0
                    ELSE 50.0
                END as confidence_score,
                CASE
                    WHEN avg_memory_mb < peak_memory_mb * 0.8 THEN 'Easy'
                    WHEN total_spill_mb > 100 THEN 'Medium'  
                    ELSE 'Easy'
                END as difficulty,
                CASE
                    WHEN avg_memory_mb < peak_memory_mb * 0.8 THEN 'Reduce executor memory from ' || CAST(peak_memory_mb as VARCHAR) || 'MB to ' || CAST(avg_memory_mb * 1.2 as VARCHAR) || 'MB'
                    WHEN total_spill_mb > 100 THEN 'Optimize data partitioning to reduce ' || CAST(total_spill_mb as VARCHAR) || 'MB of disk spill'
                    ELSE 'Consider using spot instances for cost savings'
                END as details
            FROM cost_analysis
            WHERE 
                (avg_memory_mb < peak_memory_mb * 0.8)  -- Over-provisioned memory (relaxed from 0.5 to 0.8)
                OR (total_spill_mb > 100)               -- Poor partitioning (relaxed from 1000MB to 100MB)
                OR (avg_task_duration_s > 30)           -- Long-running tasks (relaxed from 60s to 30s)
                OR (peak_memory_mb > 1024)              -- Apps using significant memory (>1GB)
            ORDER BY savings_percentage DESC
            LIMIT ?
        "#;

        let limit = params.limit.unwrap_or(15);
        let mut stmt = conn.prepare(query)?;

        let rows = stmt.query_map(
            [
                &params.start_date,
                &params.start_date,
                &params.end_date,
                &params.end_date,
                &params.app_id,
                &params.app_id,
                &Some(limit.to_string()),
            ],
            |row| {
                let opt_type_str: String = row.get(2)?;
                let optimization_type = match opt_type_str.as_str() {
                    "ReduceMemory" => OptimizationType::ReduceMemory,
                    "OptimizePartitioning" => OptimizationType::OptimizePartitioning,
                    "ReduceExecutors" => OptimizationType::ReduceExecutors,
                    "EnableSpotInstances" => OptimizationType::EnableSpotInstances,
                    _ => OptimizationType::ScheduleOffPeak,
                };

                let difficulty_str: String = row.get(7)?;
                let difficulty = match difficulty_str.as_str() {
                    "Easy" => DifficultyLevel::Easy,
                    "Hard" => DifficultyLevel::Hard,
                    _ => DifficultyLevel::Medium,
                };

                let current_cost: f64 = row.get(3)?;
                let optimized_cost: f64 = row.get(4)?;
                let savings = (current_cost - optimized_cost).max(0.0);

                Ok(CostOptimization {
                    optimization_type,
                    app_id: row.get(0)?,
                    app_name: row.get(1)?,
                    current_cost,
                    optimized_cost,
                    savings_percentage: row.get(5)?,
                    confidence_score: row.get(6)?,
                    implementation_difficulty: difficulty,
                    optimization_details: row.get(8)?,
                    formatted_savings: format!("${:.4}", savings),
                })
            },
        )?;

        let mut opportunities = Vec::new();
        for row in rows {
            opportunities.push(row?);
        }

        Ok(opportunities)
    }
}

/// Represents a Spark event for storage
#[derive(Debug, Clone)]
pub struct SparkEvent {
    pub id: i64,
    pub app_id: String,
    pub event_type: String,
    pub timestamp: String, // ISO format
    pub raw_data: Value,
    pub job_id: Option<i64>,
    pub stage_id: Option<i64>,
    pub task_id: Option<i64>,
    pub duration_ms: Option<i64>,
}

/// Resource usage analytics result
#[derive(Debug, Clone, serde::Serialize)]
#[allow(dead_code)]
pub struct ResourceUsage {
    pub app_id: String,
    pub event_type: String,
    pub event_count: i64,
    pub avg_duration_ms: Option<f64>,
    pub event_date: String,
}

impl SparkEvent {
    /// Extract a SparkEvent from raw JSON event data
    pub fn from_json(raw_event: &Value, app_id: &str, event_id: i64) -> Result<Self> {
        let event_type = raw_event
            .get("Event")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Missing Event field"))?;

        let timestamp = raw_event
            .get("Timestamp")
            .and_then(|v| v.as_i64())
            .map(|ts| {
                use chrono::{TimeZone, Utc};
                Utc.timestamp_millis_opt(ts)
                    .single()
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_else(|| Utc::now().to_rfc3339())
            })
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());

        // Extract hot fields based on event type
        let job_id = raw_event.get("Job ID").and_then(|v| v.as_i64());
        let stage_id = raw_event.get("Stage ID").and_then(|v| v.as_i64());
        let task_id = raw_event
            .get("Task Info")
            .and_then(|ti| ti.get("Task ID"))
            .and_then(|v| v.as_i64());

        // Duration from task metrics
        let duration_ms = match event_type {
            "SparkListenerTaskEnd" => raw_event
                .get("Task Metrics")
                .and_then(|tm| tm.get("Executor Run Time"))
                .and_then(|v| v.as_i64()),
            _ => None,
        };

        Ok(Self {
            id: event_id,
            app_id: app_id.to_string(),
            event_type: event_type.to_string(),
            timestamp,
            raw_data: raw_event.clone(),
            job_id,
            stage_id,
            task_id,
            duration_ms,
        })
    }
}
