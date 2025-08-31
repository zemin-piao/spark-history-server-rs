use anyhow::{anyhow, Result};
use duckdb::{Connection, params};
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
        
        // Create the events table with schema
        conn.execute_batch(
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
        )?;

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
        let param_refs: Vec<&dyn duckdb::ToSql> = param_values.iter().map(|p| p as &dyn duckdb::ToSql).collect();
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
    pub async fn get_app_events(&self, app_id: &str) -> Result<Vec<Value>> {
        let conn = self.connection.lock().await;
        
        let mut stmt = conn.prepare(
            "SELECT raw_data FROM events WHERE app_id = ? ORDER BY timestamp"
        )?;
        
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
        self.get_applications(None, None, None).await
    }

    /// Get a specific application (compatibility method)
    pub async fn get(&self, app_id: &str) -> Result<Option<ApplicationInfo>> {
        let apps = self.get_applications(Some(1), None, None).await?;
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
#[derive(Debug, Clone)]
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
                use chrono::{DateTime, Utc, TimeZone};
                Utc.timestamp_millis_opt(ts)
                    .single()
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_else(|| Utc::now().to_rfc3339())
            })
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());

        // Extract hot fields based on event type
        let job_id = raw_event.get("Job ID").and_then(|v| v.as_i64());
        let stage_id = raw_event.get("Stage ID").and_then(|v| v.as_i64());
        let task_id = raw_event.get("Task Info")
            .and_then(|ti| ti.get("Task ID"))
            .and_then(|v| v.as_i64());
        
        // Duration from task metrics
        let duration_ms = match event_type {
            "SparkListenerTaskEnd" => {
                raw_event
                    .get("Task Metrics")
                    .and_then(|tm| tm.get("Executor Run Time"))
                    .and_then(|v| v.as_i64())
            }
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