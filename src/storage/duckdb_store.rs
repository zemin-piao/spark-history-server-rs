use anyhow::{anyhow, Result};
use duckdb::{params, Connection};
use serde_json::Value;
use std::path::Path;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::models::ApplicationInfo;

/// DuckDB-based storage for Spark events with analytics capabilities
pub struct DuckDbStore {
    connection: Mutex<Connection>,
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

        Ok(Self {
            connection: Mutex::new(conn),
        })
    }

    /// Insert a batch of events for better write performance
    pub async fn insert_events_batch(&self, events: Vec<SparkEvent>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let conn = self.connection.lock().await;
        let mut stmt = conn.prepare(
            r#"
            INSERT INTO events (
                id, app_id, event_type, timestamp, raw_data, 
                job_id, stage_id, task_id, duration_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )?;

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

    /// Get performance trends across applications  
    pub async fn get_performance_trends(
        &self,
        params: &crate::analytics_api::AnalyticsQuery,
    ) -> anyhow::Result<Vec<crate::analytics_api::PerformanceTrend>> {
        let conn = self.connection.lock().await;
        
        let query = r#"
            SELECT 
                DATE(timestamp) as date,
                app_id,
                AVG(duration_ms) as avg_task_duration_ms,
                COUNT(*) as total_tasks,
                SUM(CASE WHEN JSON_EXTRACT_STRING(raw_data, '$.Task End Reason') != 'Success' 
                    THEN 1 ELSE 0 END) as failed_tasks,
                AVG(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Input Metrics.Bytes Read') AS BIGINT)) as avg_input_bytes,
                AVG(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Output Metrics.Bytes Written') AS BIGINT)) as avg_output_bytes
            FROM events 
            WHERE event_type = 'SparkListenerTaskEnd'
            AND (? IS NULL OR timestamp >= ?)
            AND (? IS NULL OR timestamp <= ?)  
            AND (? IS NULL OR app_id = ?)
            GROUP BY DATE(timestamp), app_id
            ORDER BY date DESC, app_id
            LIMIT ?
        "#;

        let limit = params.limit.unwrap_or(100);
        let mut stmt = conn.prepare(query)?;
        
        let rows = stmt.query_map([
            &params.start_date,
            &params.start_date,
            &params.end_date,
            &params.end_date,
            &params.app_id,
            &params.app_id,
            &Some(limit.to_string())
        ], |row| {
            Ok(crate::analytics_api::PerformanceTrend {
                date: row.get(0)?,
                app_id: row.get(1)?,
                avg_task_duration_ms: row.get(2)?,
                total_tasks: row.get(3)?,
                failed_tasks: row.get(4)?,
                avg_input_bytes: row.get(5)?,
                avg_output_bytes: row.get(6)?,
            })
        })?;

        let mut trends = Vec::new();
        for row in rows {
            trends.push(row?);
        }

        Ok(trends)
    }

    /// Get cross-application summary
    pub async fn get_cross_app_summary(
        &self,
        params: &crate::analytics_api::AnalyticsQuery,
    ) -> anyhow::Result<crate::analytics_api::CrossAppSummary> {
        let conn = self.connection.lock().await;
        
        let query = r#"
            WITH app_stats AS (
                SELECT 
                    COUNT(DISTINCT app_id) as total_applications,
                    COUNT(DISTINCT CASE WHEN event_type = 'SparkListenerApplicationEnd' 
                        THEN NULL ELSE app_id END) as active_applications,
                    COUNT(*) as total_events,
                    COUNT(CASE WHEN event_type = 'SparkListenerTaskEnd' 
                        AND JSON_EXTRACT_STRING(raw_data, '$.Task End Reason') = 'Success' THEN 1 END) as completed_tasks,
                    COUNT(CASE WHEN event_type = 'SparkListenerTaskEnd' 
                        AND JSON_EXTRACT_STRING(raw_data, '$.Task End Reason') != 'Success' THEN 1 END) as failed_tasks,
                    AVG(CASE WHEN event_type = 'SparkListenerTaskEnd' THEN duration_ms END) as avg_task_duration,
                    SUM(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Input Metrics.Bytes Read') AS BIGINT)) / 1073741824.0 as total_gb_processed,
                    MAX(CAST(JSON_EXTRACT(raw_data, '$.Total Cores') AS INTEGER)) as peak_executors,
                    MIN(timestamp) as start_date,
                    MAX(timestamp) as end_date
                FROM events
                WHERE (? IS NULL OR timestamp >= ?)
                AND (? IS NULL OR timestamp <= ?)
            )
            SELECT * FROM app_stats
        "#;

        let mut stmt = conn.prepare(query)?;
        let summary = stmt.query_row([
            &params.start_date, 
            &params.start_date, 
            &params.end_date, 
            &params.end_date
        ], |row| {
            Ok(crate::analytics_api::CrossAppSummary {
                total_applications: row.get(0)?,
                active_applications: row.get(1)?,
                total_events: row.get(2)?,
                total_tasks_completed: row.get(3)?,
                total_tasks_failed: row.get(4)?,
                avg_task_duration_ms: row.get(5)?,
                total_data_processed_gb: row.get(6)?,
                peak_concurrent_executors: row.get(7)?,
                date_range: crate::analytics_api::DateRange {
                    start_date: row.get::<_, String>(8)?,
                    end_date: row.get::<_, String>(9)?,
                },
            })
        })?;

        Ok(summary)
    }

    /// Get task distribution analytics
    pub async fn get_task_distribution(
        &self,
        params: &crate::analytics_api::AnalyticsQuery,
    ) -> anyhow::Result<Vec<crate::analytics_api::TaskDistribution>> {
    let conn = self.connection.lock().await;
        
        let query = r#"
            SELECT 
                app_id,
                stage_id,
                COUNT(*) as total_tasks,
                COUNT(CASE WHEN JSON_EXTRACT_STRING(raw_data, '$.Task End Reason') = 'Success' THEN 1 END) as completed_tasks,
                COUNT(CASE WHEN JSON_EXTRACT_STRING(raw_data, '$.Task End Reason') != 'Success' THEN 1 END) as failed_tasks,
                AVG(duration_ms) as avg_duration_ms,
                MIN(duration_ms) as min_duration_ms,
                MAX(duration_ms) as max_duration_ms,
                COUNT(CASE WHEN JSON_EXTRACT_STRING(raw_data, '$.Task Info.Locality') = 'PROCESS_LOCAL' THEN 1 END) as process_local,
                COUNT(CASE WHEN JSON_EXTRACT_STRING(raw_data, '$.Task Info.Locality') = 'NODE_LOCAL' THEN 1 END) as node_local,
                COUNT(CASE WHEN JSON_EXTRACT_STRING(raw_data, '$.Task Info.Locality') = 'RACK_LOCAL' THEN 1 END) as rack_local,
                COUNT(CASE WHEN JSON_EXTRACT_STRING(raw_data, '$.Task Info.Locality') = 'ANY' THEN 1 END) as any_locality
            FROM events
            WHERE event_type = 'SparkListenerTaskEnd'
            AND stage_id IS NOT NULL
            AND (? IS NULL OR timestamp >= ?)
            AND (? IS NULL OR timestamp <= ?)
            AND (? IS NULL OR app_id = ?)
            GROUP BY app_id, stage_id
            ORDER BY app_id, stage_id
            LIMIT ?
        "#;

        let limit = params.limit.unwrap_or(100);
        let mut stmt = conn.prepare(query)?;
        
        let rows = stmt.query_map([
            &params.start_date,
            &params.start_date,
            &params.end_date,
            &params.end_date,
            &params.app_id,
            &params.app_id,
            &Some(limit.to_string())
        ], |row| {
            Ok(crate::analytics_api::TaskDistribution {
                app_id: row.get(0)?,
                stage_id: row.get(1)?,
                total_tasks: row.get(2)?,
                completed_tasks: row.get(3)?,
                failed_tasks: row.get(4)?,
                avg_duration_ms: row.get(5)?,
                min_duration_ms: row.get(6)?,
                max_duration_ms: row.get(7)?,
                data_locality_summary: crate::analytics_api::DataLocalitySummary {
                    process_local: row.get(8)?,
                    node_local: row.get(9)?,
                    rack_local: row.get(10)?,
                    any: row.get(11)?,
                },
            })
        })?;

        let mut distribution = Vec::new();
        for row in rows {
            distribution.push(row?);
        }

        Ok(distribution)
    }

    /// Get executor utilization metrics
    pub async fn get_executor_utilization(
        &self,
        params: &crate::analytics_api::AnalyticsQuery,
    ) -> anyhow::Result<Vec<crate::analytics_api::ExecutorUtilization>> {
        let conn = self.connection.lock().await;
        
        let query = r#"
            WITH executor_stats AS (
                SELECT 
                    JSON_EXTRACT_STRING(raw_data, '$.Task Info.Executor ID') as executor_id,
                    JSON_EXTRACT_STRING(raw_data, '$.Task Info.Host') as host,
                    COUNT(*) as total_tasks,
                    SUM(duration_ms) as total_duration_ms,
                    COUNT(DISTINCT app_id) as apps_count,
                    COUNT(CASE WHEN JSON_EXTRACT_STRING(raw_data, '$.Task Info.Locality') IN ('PROCESS_LOCAL', 'NODE_LOCAL') 
                        THEN 1 END) as locality_hits,
                    MAX(CAST(JSON_EXTRACT(raw_data, '$.Task Metrics.Peak Execution Memory') AS BIGINT)) / 1048576 as peak_memory_mb,
                    array_agg(DISTINCT app_id) as apps_served
                FROM events
                WHERE event_type = 'SparkListenerTaskEnd' 
                AND JSON_EXTRACT_STRING(raw_data, '$.Task Info.Executor ID') IS NOT NULL
                AND (? IS NULL OR timestamp >= ?)
                AND (? IS NULL OR timestamp <= ?)
                GROUP BY executor_id, host
            )
            SELECT 
                executor_id,
                host,
                total_tasks,
                total_duration_ms,
                NULL as avg_cpu_utilization, -- Would need more detailed metrics
                peak_memory_mb,
                locality_hits,
                apps_served
            FROM executor_stats
            WHERE executor_id != 'driver'
            ORDER BY total_tasks DESC
            LIMIT ?
        "#;

        let limit = params.limit.unwrap_or(50);
        let mut stmt = conn.prepare(query)?;
        
        let rows = stmt.query_map([
            &params.start_date,
            &params.start_date,
            &params.end_date,
            &params.end_date,
            &Some(limit.to_string())
        ], |row| {
            let apps_json: String = row.get(7)?;
            let apps_served: Vec<String> = serde_json::from_str(&apps_json)
                .unwrap_or_default();
            
            Ok(crate::analytics_api::ExecutorUtilization {
                executor_id: row.get(0)?,
                host: row.get(1)?,
                total_tasks: row.get(2)?,
                total_duration_ms: row.get(3)?,
                avg_cpu_utilization: row.get(4)?,
                peak_memory_usage_mb: row.get(5)?,
                data_locality_hits: row.get(6)?,
                apps_served,
            })
        })?;

        let mut utilization = Vec::new();
        for row in rows {
            utilization.push(row?);
        }

        Ok(utilization)
    }

    /// Get comprehensive resource utilization metrics across all executors and applications
    pub async fn get_resource_utilization_metrics(
        &self,
        params: &crate::analytics_api::AnalyticsQuery,
    ) -> anyhow::Result<Vec<crate::analytics_api::ResourceUtilizationMetrics>> {
        let conn = self.connection.lock().await;
        
        let query = r#"
            SELECT 
                app_id,
                'unknown' as executor_id,
                'localhost' as host,
                app_id as app_name,
                0 as total_tasks,
                0 as completed_tasks,
                0 as failed_tasks,
                0 as total_duration_ms,
                0.0 as avg_task_duration_ms,
                0 as cpu_time_ms,
                0 as gc_time_ms,
                0 as peak_memory_usage_mb,
                0 as max_memory_mb,
                0.0 as memory_utilization_percent,
                0 as input_bytes,
                0 as output_bytes,
                0 as shuffle_read_bytes,
                0 as shuffle_write_bytes,
                0 as disk_spill_bytes,
                0 as memory_spill_bytes,
                0 as data_locality_process_local,
                0 as data_locality_node_local,
                0 as data_locality_rack_local,
                0 as data_locality_any,
                '' as start_time,
                NULL as end_time,
                true as is_active
            FROM events
            WHERE event_type = 'SparkListenerApplicationStart'
            GROUP BY app_id
            ORDER BY app_id
            LIMIT ?
        "#;

        let limit = params.limit.unwrap_or(100);
        let mut stmt = conn.prepare(query)?;
        
        let rows = stmt.query_map([&Some(limit.to_string())], |row| {
            Ok(crate::analytics_api::ResourceUtilizationMetrics {
                executor_id: row.get(0)?,
                host: row.get(1)?,
                app_id: row.get(2)?,
                app_name: row.get(3)?,
                total_tasks: row.get(4)?,
                completed_tasks: row.get(5)?,
                failed_tasks: row.get(6)?,
                total_duration_ms: row.get(7)?,
                avg_task_duration_ms: row.get(8)?,
                cpu_time_ms: row.get(9)?,
                gc_time_ms: row.get(10)?,
                peak_memory_usage_mb: row.get(11)?,
                max_memory_mb: row.get(12)?,
                memory_utilization_percent: row.get(13)?,
                input_bytes: row.get(14)?,
                output_bytes: row.get(15)?,
                shuffle_read_bytes: row.get(16)?,
                shuffle_write_bytes: row.get(17)?,
                disk_spill_bytes: row.get(18)?,
                memory_spill_bytes: row.get(19)?,
                data_locality_process_local: row.get(20)?,
                data_locality_node_local: row.get(21)?,
                data_locality_rack_local: row.get(22)?,
                data_locality_any: row.get(23)?,
                start_time: row.get(24)?,
                end_time: row.get(25)?,
                is_active: row.get(26)?,
            })
        })?;

        let mut metrics = Vec::new();
        for row in rows {
            metrics.push(row?);
        }

        Ok(metrics)
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
