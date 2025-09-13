/// Storage backend trait for analytical data processing
/// Provides a unified interface for different storage systems
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::HashMap;

use crate::analytics_api::{
    AnalyticsQuery, CostOptimization, CrossAppSummary, EfficiencyAnalysis, PerformanceTrend,
    ResourceHog, ResourceUsageSummary, TaskDistribution,
};

/// Storage backend types supported by the system
#[derive(Debug, Clone, PartialEq)]
pub enum StorageBackendType {
    DuckDB,
    ClickHouse,
    PostgreSQL,
}

/// Configuration for storage backends
#[derive(Debug, Clone)]
pub struct BackendConfig {
    pub backend_type: StorageBackendType,
    pub connection_params: HashMap<String, String>,
    pub performance_params: HashMap<String, Value>,
}

/// Trait defining the analytical storage backend interface
#[async_trait]
pub trait AnalyticalStorageBackend {
    /// Backend identification
    fn backend_type(&self) -> StorageBackendType;
    fn backend_name(&self) -> &'static str;

    /// Core event storage operations
    async fn insert_events_batch(&self, events: Vec<crate::storage::SparkEvent>) -> Result<()>;
    async fn get_applications(&self, limit: Option<usize>) -> Result<Vec<Value>>;
    async fn get_application_summary(&self, app_id: &str) -> Result<Option<Value>>;
    async fn get_executors(&self, app_id: &str) -> Result<Vec<Value>>;
    async fn get_active_applications(&self, limit: Option<usize>) -> Result<Vec<Value>>;

    /// Health and status operations
    async fn health_check(&self) -> Result<bool>;
    async fn get_backend_stats(&self) -> Result<BackendStats>;

    /// Advanced analytics operations
    async fn get_cross_app_summary(&self, query: &AnalyticsQuery) -> Result<CrossAppSummary>;
    async fn get_performance_trends(&self, query: &AnalyticsQuery)
        -> Result<Vec<PerformanceTrend>>;
    async fn get_resource_usage(&self, query: &AnalyticsQuery) -> Result<ResourceUsageSummary>;
    async fn get_task_distribution(&self, query: &AnalyticsQuery) -> Result<TaskDistribution>;
    async fn get_efficiency_analysis(
        &self,
        query: &AnalyticsQuery,
    ) -> Result<Vec<EfficiencyAnalysis>>;
    async fn get_resource_hogs(&self, query: &AnalyticsQuery) -> Result<Vec<ResourceHog>>;
    async fn get_cost_optimization(&self, query: &AnalyticsQuery) -> Result<CostOptimization>;

    /// Maintenance operations
    async fn optimize_storage(&self) -> Result<()>;
    async fn vacuum_storage(&self) -> Result<()>;
    async fn get_storage_size(&self) -> Result<u64>;

    /// Optional: Backend-specific operations
    async fn execute_custom_query(&self, _query: &str) -> Result<Vec<Value>> {
        // Default implementation returns empty result
        Ok(vec![])
    }
}

/// Statistics about the storage backend
#[derive(Debug, Clone)]
pub struct BackendStats {
    pub backend_type: StorageBackendType,
    pub total_events: u64,
    pub total_applications: u64,
    pub storage_size_bytes: u64,
    pub last_updated: Option<DateTime<Utc>>,
    pub throughput_events_per_sec: Option<f64>,
    pub average_query_time_ms: Option<f64>,
    pub backend_specific_metrics: HashMap<String, Value>,
}

impl BackendStats {
    pub fn new(backend_type: StorageBackendType) -> Self {
        Self {
            backend_type,
            total_events: 0,
            total_applications: 0,
            storage_size_bytes: 0,
            last_updated: None,
            throughput_events_per_sec: None,
            average_query_time_ms: None,
            backend_specific_metrics: HashMap::new(),
        }
    }
}

/// Performance characteristics of different backends
pub struct BackendPerformanceProfile {
    pub max_write_throughput_events_per_sec: u64,
    pub max_concurrent_writers: usize,
    pub typical_query_latency_ms: u64,
    pub supports_real_time_analytics: bool,
    pub supports_horizontal_scaling: bool,
}

impl BackendPerformanceProfile {
    /// Get expected performance profile for a backend type
    pub fn for_backend(backend_type: &StorageBackendType) -> Self {
        match backend_type {
            StorageBackendType::DuckDB => Self {
                max_write_throughput_events_per_sec: 10_000,
                max_concurrent_writers: 8,
                typical_query_latency_ms: 10,
                supports_real_time_analytics: true,
                supports_horizontal_scaling: false,
            },
            StorageBackendType::ClickHouse => Self {
                max_write_throughput_events_per_sec: 100_000,
                max_concurrent_writers: 100,
                typical_query_latency_ms: 50,
                supports_real_time_analytics: true,
                supports_horizontal_scaling: true,
            },
            StorageBackendType::PostgreSQL => Self {
                max_write_throughput_events_per_sec: 50_000,
                max_concurrent_writers: 50,
                typical_query_latency_ms: 25,
                supports_real_time_analytics: false,
                supports_horizontal_scaling: true,
            },
        }
    }
}
