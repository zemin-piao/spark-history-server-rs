#![allow(dead_code)]

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a Spark application with its metadata and attempts
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationInfo {
    pub id: String,
    pub name: String,
    pub cores_granted: Option<i32>,
    pub max_cores: Option<i32>,
    pub cores_per_executor: Option<i32>,
    pub memory_per_executor_mb: Option<i32>,
    pub attempts: Vec<ApplicationAttemptInfo>,
}

/// Represents a single attempt of a Spark application
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationAttemptInfo {
    pub attempt_id: Option<String>,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
    pub duration: i64,
    pub spark_user: String,
    pub completed: bool,
    pub app_spark_version: String,

    // Additional fields for API compatibility
    #[serde(rename = "startTimeEpoch")]
    pub start_time_epoch: i64,
    #[serde(rename = "endTimeEpoch")]
    pub end_time_epoch: i64,
    #[serde(rename = "lastUpdatedEpoch")]
    pub last_updated_epoch: i64,
}

/// Application status enum
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum ApplicationStatus {
    Running,
    Completed,
}

/// Job information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobData {
    pub job_id: i32,
    pub name: String,
    pub description: Option<String>,
    pub status: JobStatus,
    pub num_tasks: i32,
    pub num_active_tasks: i32,
    pub num_completed_tasks: i32,
    pub num_skipped_tasks: i32,
    pub num_failed_tasks: i32,
    pub num_killed_tasks: i32,
    pub num_active_stages: i32,
    pub num_completed_stages: i32,
    pub num_skipped_stages: i32,
    pub num_failed_stages: i32,
}

/// Job status enum
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum JobStatus {
    Running,
    Succeeded,
    Failed,
    Unknown,
}

/// Executor summary information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorSummary {
    pub id: String,
    pub host_port: String,
    pub is_active: bool,
    pub rdd_blocks: i32,
    pub memory_used: i64,
    pub disk_used: i64,
    pub total_cores: i32,
    pub max_tasks: i32,
    pub active_tasks: i32,
    pub failed_tasks: i32,
    pub completed_tasks: i32,
    pub total_tasks: i32,
    pub total_duration: i64,
    pub total_gc_time: i64,
    pub total_input_bytes: i64,
    pub total_shuffle_read: i64,
    pub total_shuffle_write: i64,
    pub is_excluded: bool,
    pub max_memory: i64,
    pub add_time: DateTime<Utc>,
    pub remove_time: Option<DateTime<Utc>>,
    pub remove_reason: Option<String>,
    pub executor_logs: HashMap<String, String>,
    pub memory_metrics: Option<MemoryMetrics>,
    pub attributes: HashMap<String, String>,
    pub resources: HashMap<String, ResourceInformation>,
    pub resource_profile_id: i32,
    pub excluded_in_stages: Vec<i32>,
}

/// Memory metrics for executors
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryMetrics {
    pub used_on_heap_storage_memory: i64,
    pub used_off_heap_storage_memory: i64,
    pub total_on_heap_storage_memory: i64,
    pub total_off_heap_storage_memory: i64,
}

/// Resource information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceInformation {
    pub name: String,
    pub addresses: Vec<String>,
}

/// RDD storage information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RddStorageInfo {
    pub id: i32,
    pub name: String,
    pub num_partitions: i32,
    pub num_cached_partitions: i32,
    pub storage_level: String,
    pub memory_size: i64,
    pub disk_size: i64,
    pub data_distributions: Vec<RddDataDistribution>,
}

/// RDD data distribution per executor
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RddDataDistribution {
    pub address: String,
    pub memory_used: i64,
    pub memory_remaining: i64,
    pub disk_used: i64,
}

/// Application environment information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationEnvironmentInfo {
    pub runtime: RuntimeInfo,
    pub spark_properties: Vec<(String, String)>,
    pub hadoop_properties: Vec<(String, String)>,
    pub system_properties: Vec<(String, String)>,
    pub metrics_properties: Vec<(String, String)>,
    pub classpath_entries: Vec<(String, String)>,
}

/// Runtime information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeInfo {
    pub java_version: String,
    pub java_home: String,
    pub scala_version: String,
}

/// Version information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionInfo {
    pub version: String,
}

impl ApplicationAttemptInfo {
    pub fn new(
        attempt_id: Option<String>,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        last_updated: DateTime<Utc>,
        spark_user: String,
        completed: bool,
        app_spark_version: String,
    ) -> Self {
        let duration = if completed {
            (end_time - start_time).num_milliseconds()
        } else {
            (Utc::now() - start_time).num_milliseconds()
        };

        Self {
            attempt_id,
            start_time,
            end_time,
            last_updated,
            duration,
            spark_user,
            completed,
            app_spark_version,
            start_time_epoch: start_time.timestamp_millis(),
            end_time_epoch: end_time.timestamp_millis(),
            last_updated_epoch: last_updated.timestamp_millis(),
        }
    }
}
