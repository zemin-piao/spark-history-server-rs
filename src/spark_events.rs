use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Common Spark event types we handle
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SparkEventType {
    // Application lifecycle
    #[serde(rename = "SparkListenerApplicationStart")]
    ApplicationStart,
    #[serde(rename = "SparkListenerApplicationEnd")]
    ApplicationEnd,

    // Job lifecycle
    #[serde(rename = "SparkListenerJobStart")]
    JobStart,
    #[serde(rename = "SparkListenerJobEnd")]
    JobEnd,

    // Stage lifecycle
    #[serde(rename = "SparkListenerStageSubmitted")]
    StageSubmitted,
    #[serde(rename = "SparkListenerStageCompleted")]
    StageCompleted,

    // Task lifecycle
    #[serde(rename = "SparkListenerTaskStart")]
    TaskStart,
    #[serde(rename = "SparkListenerTaskEnd")]
    TaskEnd,

    // Executor events
    #[serde(rename = "SparkListenerExecutorAdded")]
    ExecutorAdded,
    #[serde(rename = "SparkListenerExecutorRemoved")]
    ExecutorRemoved,

    // Block manager events
    #[serde(rename = "SparkListenerBlockManagerAdded")]
    BlockManagerAdded,
    #[serde(rename = "SparkListenerBlockManagerRemoved")]
    BlockManagerRemoved,

    // Environment updates
    #[serde(rename = "SparkListenerEnvironmentUpdate")]
    EnvironmentUpdate,

    // SQL events
    #[serde(rename = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart")]
    SqlExecutionStart,
    #[serde(rename = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd")]
    SqlExecutionEnd,

    // Other/Unknown events
    Other(String),
}

impl From<&str> for SparkEventType {
    fn from(s: &str) -> Self {
        match s {
            "SparkListenerApplicationStart" => Self::ApplicationStart,
            "SparkListenerApplicationEnd" => Self::ApplicationEnd,
            "SparkListenerJobStart" => Self::JobStart,
            "SparkListenerJobEnd" => Self::JobEnd,
            "SparkListenerStageSubmitted" => Self::StageSubmitted,
            "SparkListenerStageCompleted" => Self::StageCompleted,
            "SparkListenerTaskStart" => Self::TaskStart,
            "SparkListenerTaskEnd" => Self::TaskEnd,
            "SparkListenerExecutorAdded" => Self::ExecutorAdded,
            "SparkListenerExecutorRemoved" => Self::ExecutorRemoved,
            "SparkListenerBlockManagerAdded" => Self::BlockManagerAdded,
            "SparkListenerBlockManagerRemoved" => Self::BlockManagerRemoved,
            "SparkListenerEnvironmentUpdate" => Self::EnvironmentUpdate,
            "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart" => {
                Self::SqlExecutionStart
            }
            "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd" => {
                Self::SqlExecutionEnd
            }
            other => Self::Other(other.to_string()),
        }
    }
}

/// Parsed Spark event with extracted fields
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparkEvent {
    pub event_type: SparkEventType,
    pub timestamp: DateTime<Utc>,
    pub app_id: String,
    pub raw_data: Value,

    // Extracted hot fields for fast queries
    pub job_id: Option<i64>,
    pub stage_id: Option<i64>,
    pub task_id: Option<i64>,
    pub duration_ms: Option<i64>,
    pub executor_id: Option<String>,
    pub host: Option<String>,
    pub memory_bytes: Option<i64>,
    pub cores: Option<i32>,
}

impl SparkEvent {
    /// Parse a Spark event from raw JSON
    pub fn from_json(raw_event: &Value, app_id: &str) -> Result<Self> {
        let event_type_str = raw_event
            .get("Event")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Missing Event field"))?;

        let event_type = SparkEventType::from(event_type_str);

        let timestamp = raw_event
            .get("Timestamp")
            .and_then(|v| v.as_i64())
            .and_then(|ts| {
                use chrono::TimeZone;
                Utc.timestamp_millis_opt(ts).single()
            })
            .unwrap_or_else(Utc::now);

        // Extract fields based on event type
        let (job_id, stage_id, task_id, duration_ms, executor_id, host, memory_bytes, cores) =
            Self::extract_fields(&event_type, raw_event);

        Ok(Self {
            event_type,
            timestamp,
            app_id: app_id.to_string(),
            raw_data: raw_event.clone(),
            job_id,
            stage_id,
            task_id,
            duration_ms,
            executor_id,
            host,
            memory_bytes,
            cores,
        })
    }

    /// Extract relevant fields based on event type
    fn extract_fields(
        event_type: &SparkEventType,
        raw_event: &Value,
    ) -> (
        Option<i64>,
        Option<i64>,
        Option<i64>,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<i32>,
    ) {
        match event_type {
            SparkEventType::JobStart | SparkEventType::JobEnd => {
                let job_id = raw_event.get("Job ID").and_then(|v| v.as_i64());
                (job_id, None, None, None, None, None, None, None)
            }

            SparkEventType::StageSubmitted | SparkEventType::StageCompleted => {
                let stage_id = raw_event
                    .get("Stage Info")
                    .and_then(|si| si.get("Stage ID"))
                    .and_then(|v| v.as_i64());
                (None, stage_id, None, None, None, None, None, None)
            }

            SparkEventType::TaskStart => {
                let task_info = raw_event.get("Task Info");
                let task_id = task_info
                    .and_then(|ti| ti.get("Task ID"))
                    .and_then(|v| v.as_i64());
                let stage_id = task_info
                    .and_then(|ti| ti.get("Stage ID"))
                    .and_then(|v| v.as_i64());
                let executor_id = task_info
                    .and_then(|ti| ti.get("Executor ID"))
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let host = task_info
                    .and_then(|ti| ti.get("Host"))
                    .and_then(|v| v.as_str())
                    .map(String::from);
                (None, stage_id, task_id, None, executor_id, host, None, None)
            }

            SparkEventType::TaskEnd => {
                let task_info = raw_event.get("Task Info");
                let task_metrics = raw_event.get("Task Metrics");

                let task_id = task_info
                    .and_then(|ti| ti.get("Task ID"))
                    .and_then(|v| v.as_i64());
                let stage_id = task_info
                    .and_then(|ti| ti.get("Stage ID"))
                    .and_then(|v| v.as_i64());
                let executor_id = task_info
                    .and_then(|ti| ti.get("Executor ID"))
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let host = task_info
                    .and_then(|ti| ti.get("Host"))
                    .and_then(|v| v.as_str())
                    .map(String::from);

                let duration_ms = task_metrics
                    .and_then(|tm| tm.get("Executor Run Time"))
                    .and_then(|v| v.as_i64());

                (
                    None,
                    stage_id,
                    task_id,
                    duration_ms,
                    executor_id,
                    host,
                    None,
                    None,
                )
            }

            SparkEventType::ExecutorAdded => {
                let executor_info = raw_event.get("Executor Info");
                let executor_id = raw_event
                    .get("Executor ID")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let host = executor_info
                    .and_then(|ei| ei.get("Host"))
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let cores = executor_info
                    .and_then(|ei| ei.get("Total Cores"))
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32);
                (None, None, None, None, executor_id, host, None, cores)
            }

            SparkEventType::ExecutorRemoved => {
                let executor_id = raw_event
                    .get("Executor ID")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                (None, None, None, None, executor_id, None, None, None)
            }

            _ => (None, None, None, None, None, None, None, None),
        }
    }

    /// Get unique identifier for database
    pub fn get_id(&self) -> String {
        format!(
            "{}_{}_{}_{}",
            self.app_id,
            self.timestamp.timestamp_millis(),
            self.event_type_str(),
            self.task_id.unwrap_or(0)
        )
    }

    /// Get event type as string
    pub fn event_type_str(&self) -> String {
        match &self.event_type {
            SparkEventType::Other(s) => s.clone(),
            _ => serde_json::to_string(&self.event_type)
                .unwrap_or_default()
                .trim_matches('"')
                .to_string(),
        }
    }
}

/// Application attempt information extracted from events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplicationAttempt {
    pub attempt_id: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub duration: Option<i64>,
    pub spark_user: String,
    pub completed: bool,
    pub app_spark_version: String,
}

/// Job information extracted from events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobInfo {
    pub job_id: i64,
    pub name: String,
    pub submission_time: DateTime<Utc>,
    pub completion_time: Option<DateTime<Utc>>,
    pub status: JobStatus,
    pub num_tasks: i64,
    pub num_active_tasks: i64,
    pub num_completed_tasks: i64,
    pub num_skipped_tasks: i64,
    pub num_failed_tasks: i64,
    pub num_active_stages: i64,
    pub num_completed_stages: i64,
    pub num_skipped_stages: i64,
    pub num_failed_stages: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JobStatus {
    Running,
    Succeeded,
    Failed,
    Unknown,
}

/// Stage information extracted from events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageInfo {
    pub stage_id: i64,
    pub attempt_id: i64,
    pub name: String,
    pub num_tasks: i64,
    pub status: StageStatus,
    pub submission_time: Option<DateTime<Utc>>,
    pub first_task_launched_time: Option<DateTime<Utc>>,
    pub completion_time: Option<DateTime<Utc>>,
    pub failure_reason: Option<String>,
    pub details: String,
    pub accumulables: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StageStatus {
    Active,
    Complete,
    Failed,
    Pending,
}

/// Task information extracted from events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    pub task_id: i64,
    pub index: i64,
    pub attempt: i64,
    pub launch_time: DateTime<Utc>,
    pub executor_id: String,
    pub host: String,
    pub task_locality: String,
    pub speculative: bool,
    pub getting_result_time: Option<DateTime<Utc>>,
    pub finish_time: Option<DateTime<Utc>>,
    pub failed: bool,
    pub killed: bool,
    pub accumulables: HashMap<String, Value>,
    pub task_metrics: Option<TaskMetrics>,
}

/// Task metrics extracted from events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMetrics {
    pub executor_deserialize_time: i64,
    pub executor_deserialize_cpu_time: i64,
    pub executor_run_time: i64,
    pub executor_cpu_time: i64,
    pub result_size: i64,
    pub jvm_gc_time: i64,
    pub result_serialization_time: i64,
    pub memory_bytes_spilled: i64,
    pub disk_bytes_spilled: i64,
    pub peak_execution_memory: i64,
    pub input_metrics: Option<InputMetrics>,
    pub output_metrics: Option<OutputMetrics>,
    pub shuffle_read_metrics: Option<ShuffleReadMetrics>,
    pub shuffle_write_metrics: Option<ShuffleWriteMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputMetrics {
    pub bytes_read: i64,
    pub records_read: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputMetrics {
    pub bytes_written: i64,
    pub records_written: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShuffleReadMetrics {
    pub remote_blocks_fetched: i64,
    pub local_blocks_fetched: i64,
    pub fetch_wait_time: i64,
    pub remote_bytes_read: i64,
    pub local_bytes_read: i64,
    pub total_records_read: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShuffleWriteMetrics {
    pub bytes_written: i64,
    pub write_time: i64,
    pub records_written: i64,
}
