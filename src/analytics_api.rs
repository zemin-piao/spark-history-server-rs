use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::storage::duckdb_store::ResourceUsage;
use crate::storage::HistoryProvider;

/// Analytics API router with cross-application insights
pub fn analytics_router() -> Router<HistoryProvider> {
    Router::new()
        .route("/analytics/test", get(test_analytics))
        .route("/analytics/resource-usage", get(get_resource_usage))
        .route(
            "/analytics/resource-utilization",
            get(get_resource_utilization_metrics),
        )
        .route("/analytics/performance-trends", get(get_performance_trends))
        .route("/analytics/cross-app-summary", get(get_cross_app_summary))
        .route("/analytics/task-distribution", get(get_task_distribution))
        .route(
            "/analytics/executor-utilization",
            get(get_executor_utilization),
        )
}

/// Test analytics endpoint
async fn test_analytics() -> Result<Json<serde_json::Value>, StatusCode> {
    Ok(Json(serde_json::json!({"status": "analytics working"})))
}

/// Query parameters for analytics endpoints
#[derive(Debug, Deserialize)]
pub struct AnalyticsQuery {
    #[serde(rename = "startDate")]
    pub start_date: Option<String>,
    #[serde(rename = "endDate")]
    pub end_date: Option<String>,
    pub limit: Option<usize>,
    #[serde(rename = "appId")]
    pub app_id: Option<String>,
}

/// Resource usage analytics across applications
async fn get_resource_usage(
    State(provider): State<HistoryProvider>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<Vec<ResourceUsage>>, StatusCode> {
    info!("GET /analytics/resource-usage - params: {:?}", params);

    let store = provider.get_duckdb_store();
    match store.get_resource_usage_summary().await {
        Ok(usage_data) => {
            info!("Returning {} resource usage entries", usage_data.len());
            Ok(Json(usage_data))
        }
        Err(e) => {
            tracing::error!("Failed to get resource usage: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Performance trends across applications
async fn get_performance_trends(
    State(provider): State<HistoryProvider>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<Vec<PerformanceTrend>>, StatusCode> {
    info!("GET /analytics/performance-trends - params: {:?}", params);

    let store = provider.get_duckdb_store();
    let trends = match store.get_performance_trends(&params).await {
        Ok(data) => data,
        Err(e) => {
            tracing::error!("Failed to get performance trends: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    Ok(Json(trends))
}

/// Cross-application summary analytics
async fn get_cross_app_summary(
    State(provider): State<HistoryProvider>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<CrossAppSummary>, StatusCode> {
    info!("GET /analytics/cross-app-summary - params: {:?}", params);

    let store = provider.get_duckdb_store();
    let summary = match store.get_cross_app_summary(&params).await {
        Ok(data) => data,
        Err(e) => {
            tracing::error!("Failed to get cross-app summary: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    Ok(Json(summary))
}

/// Task distribution analytics
async fn get_task_distribution(
    State(provider): State<HistoryProvider>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<Vec<TaskDistribution>>, StatusCode> {
    info!("GET /analytics/task-distribution - params: {:?}", params);

    let store = provider.get_duckdb_store();
    let distribution = match store.get_task_distribution(&params).await {
        Ok(data) => data,
        Err(e) => {
            tracing::error!("Failed to get task distribution: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    Ok(Json(distribution))
}

/// Executor utilization analytics
async fn get_executor_utilization(
    State(provider): State<HistoryProvider>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<Vec<ExecutorUtilization>>, StatusCode> {
    info!("GET /analytics/executor-utilization - params: {:?}", params);

    let store = provider.get_duckdb_store();
    let utilization = match store.get_executor_utilization(&params).await {
        Ok(data) => data,
        Err(e) => {
            tracing::error!("Failed to get executor utilization: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    Ok(Json(utilization))
}

/// Resource utilization metrics across all executors and applications
async fn get_resource_utilization_metrics(
    State(provider): State<HistoryProvider>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<Vec<ResourceUtilizationMetrics>>, StatusCode> {
    info!("GET /analytics/resource-utilization - params: {:?}", params);

    let store = provider.get_duckdb_store();
    let metrics = match store.get_resource_utilization_metrics(&params).await {
        Ok(data) => data,
        Err(e) => {
            tracing::error!("Failed to get resource utilization metrics: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    Ok(Json(metrics))
}

/// Performance trend data structure
#[derive(Debug, Clone, Serialize)]
pub struct PerformanceTrend {
    pub date: String,
    pub app_id: String,
    pub avg_task_duration_ms: Option<f64>,
    pub total_tasks: i64,
    pub failed_tasks: i64,
    pub avg_input_bytes: Option<f64>,
    pub avg_output_bytes: Option<f64>,
}

/// Cross-application summary
#[derive(Debug, Clone, Serialize)]
pub struct CrossAppSummary {
    pub total_applications: i64,
    pub active_applications: i64,
    pub total_events: i64,
    pub total_tasks_completed: i64,
    pub total_tasks_failed: i64,
    pub avg_task_duration_ms: Option<f64>,
    pub total_data_processed_gb: Option<f64>,
    pub peak_concurrent_executors: i64,
    pub date_range: DateRange,
}

/// Task distribution analytics
#[derive(Debug, Clone, Serialize)]
pub struct TaskDistribution {
    pub app_id: String,
    pub stage_id: i64,
    pub total_tasks: i64,
    pub completed_tasks: i64,
    pub failed_tasks: i64,
    pub avg_duration_ms: Option<f64>,
    pub min_duration_ms: Option<i64>,
    pub max_duration_ms: Option<i64>,
    pub data_locality_summary: DataLocalitySummary,
}

/// Executor utilization metrics
#[derive(Debug, Clone, Serialize)]
pub struct ExecutorUtilization {
    pub executor_id: String,
    pub host: String,
    pub total_tasks: i64,
    pub total_duration_ms: i64,
    pub avg_cpu_utilization: Option<f64>,
    pub peak_memory_usage_mb: Option<i64>,
    pub data_locality_hits: i64,
    pub apps_served: Vec<String>,
}

/// Enhanced resource utilization metrics across all executors and applications
#[derive(Debug, Clone, Serialize)]
pub struct ResourceUtilizationMetrics {
    pub executor_id: String,
    pub host: String,
    pub app_id: String,
    pub app_name: String,
    pub total_tasks: i64,
    pub completed_tasks: i64,
    pub failed_tasks: i64,
    pub total_duration_ms: i64,
    pub avg_task_duration_ms: Option<f64>,
    pub cpu_time_ms: i64,
    pub gc_time_ms: i64,
    pub peak_memory_usage_mb: Option<i64>,
    pub max_memory_mb: i64,
    pub memory_utilization_percent: Option<f64>,
    pub input_bytes: i64,
    pub output_bytes: i64,
    pub shuffle_read_bytes: i64,
    pub shuffle_write_bytes: i64,
    pub disk_spill_bytes: i64,
    pub memory_spill_bytes: i64,
    pub data_locality_process_local: i64,
    pub data_locality_node_local: i64,
    pub data_locality_rack_local: i64,
    pub data_locality_any: i64,
    pub start_time: String,
    pub end_time: Option<String>,
    pub is_active: bool,
}

/// Date range for analytics
#[derive(Debug, Clone, Serialize)]
pub struct DateRange {
    pub start_date: String,
    pub end_date: String,
}

/// Data locality summary
#[derive(Debug, Clone, Serialize)]
pub struct DataLocalitySummary {
    pub process_local: i64,
    pub node_local: i64,
    pub rack_local: i64,
    pub any: i64,
}
