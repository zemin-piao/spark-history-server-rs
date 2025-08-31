use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

use crate::storage::duckdb_store::{DuckDbStore, ResourceUsage};

/// Analytics API router with cross-application insights
pub fn analytics_router() -> Router<Arc<DuckDbStore>> {
    Router::new()
        .route("/analytics/resource-usage", get(get_resource_usage))
        .route("/analytics/performance-trends", get(get_performance_trends))
        .route("/analytics/cross-app-summary", get(get_cross_app_summary))
        .route("/analytics/task-distribution", get(get_task_distribution))
        .route("/analytics/executor-utilization", get(get_executor_utilization))
}

/// Query parameters for analytics endpoints
#[derive(Debug, Deserialize)]
struct AnalyticsQuery {
    #[serde(rename = "startDate")]
    start_date: Option<String>,
    #[serde(rename = "endDate")]
    end_date: Option<String>,
    limit: Option<usize>,
    #[serde(rename = "appId")]
    app_id: Option<String>,
}

/// Resource usage analytics across applications
async fn get_resource_usage(
    State(store): State<Arc<DuckDbStore>>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<Vec<ResourceUsage>>, StatusCode> {
    info!("GET /analytics/resource-usage - params: {:?}", params);

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
    State(store): State<Arc<DuckDbStore>>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<Vec<PerformanceTrend>>, StatusCode> {
    info!("GET /analytics/performance-trends - params: {:?}", params);

    // Example analytics query using DuckDB
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
    State(store): State<Arc<DuckDbStore>>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<CrossAppSummary>, StatusCode> {
    info!("GET /analytics/cross-app-summary - params: {:?}", params);

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
    State(store): State<Arc<DuckDbStore>>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<Vec<TaskDistribution>>, StatusCode> {
    info!("GET /analytics/task-distribution - params: {:?}", params);

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
    State(store): State<Arc<DuckDbStore>>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<Vec<ExecutorUtilization>>, StatusCode> {
    info!("GET /analytics/executor-utilization - params: {:?}", params);

    let utilization = match store.get_executor_utilization(&params).await {
        Ok(data) => data,
        Err(e) => {
            tracing::error!("Failed to get executor utilization: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    Ok(Json(utilization))
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

// Extensions to DuckDbStore for analytics
impl DuckDbStore {
    /// Get performance trends across applications
    pub async fn get_performance_trends(
        &self,
        params: &AnalyticsQuery,
    ) -> anyhow::Result<Vec<PerformanceTrend>> {
        let conn = self.connection.lock().await;
        
        let query = r#"
            SELECT 
                DATE(timestamp) as date,
                app_id,
                AVG(duration_ms) as avg_task_duration_ms,
                COUNT(*) as total_tasks,
                SUM(CASE WHEN raw_data::JSON ->> 'Task End Reason' != 'Success' 
                    THEN 1 ELSE 0 END) as failed_tasks,
                AVG(CAST(raw_data::JSON -> 'Task Metrics' ->> 'Input Metrics' ->> 'Bytes Read' AS BIGINT)) as avg_input_bytes,
                AVG(CAST(raw_data::JSON -> 'Task Metrics' ->> 'Output Metrics' ->> 'Bytes Written' AS BIGINT)) as avg_output_bytes
            FROM events 
            WHERE event_type = 'SparkListenerTaskEnd'
            AND ($1::TEXT IS NULL OR timestamp >= $1::TIMESTAMP)
            AND ($2::TEXT IS NULL OR timestamp <= $2::TIMESTAMP)  
            AND ($3::TEXT IS NULL OR app_id = $3)
            GROUP BY DATE(timestamp), app_id
            ORDER BY date DESC, app_id
            LIMIT $4
        "#;

        let limit = params.limit.unwrap_or(100);
        let mut stmt = conn.prepare(query)?;
        
        let rows = stmt.query_map([
            &params.start_date,
            &params.end_date, 
            &params.app_id,
            &Some(limit.to_string())
        ], |row| {
            Ok(PerformanceTrend {
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
        params: &AnalyticsQuery,
    ) -> anyhow::Result<CrossAppSummary> {
        let conn = self.connection.lock().await;
        
        let query = r#"
            WITH app_stats AS (
                SELECT 
                    COUNT(DISTINCT app_id) as total_applications,
                    COUNT(DISTINCT CASE WHEN event_type = 'SparkListenerApplicationEnd' 
                        THEN NULL ELSE app_id END) as active_applications,
                    COUNT(*) as total_events,
                    COUNT(CASE WHEN event_type = 'SparkListenerTaskEnd' 
                        AND raw_data::JSON ->> 'Task End Reason' = 'Success' THEN 1 END) as completed_tasks,
                    COUNT(CASE WHEN event_type = 'SparkListenerTaskEnd' 
                        AND raw_data::JSON ->> 'Task End Reason' != 'Success' THEN 1 END) as failed_tasks,
                    AVG(CASE WHEN event_type = 'SparkListenerTaskEnd' THEN duration_ms END) as avg_task_duration,
                    SUM(CAST(raw_data::JSON -> 'Task Metrics' ->> 'Input Metrics' ->> 'Bytes Read' AS BIGINT)) / 1073741824.0 as total_gb_processed,
                    MAX(CAST(raw_data::JSON ->> 'Total Cores' AS INTEGER)) as peak_executors,
                    MIN(timestamp) as start_date,
                    MAX(timestamp) as end_date
                FROM events
                WHERE ($1::TEXT IS NULL OR timestamp >= $1::TIMESTAMP)
                AND ($2::TEXT IS NULL OR timestamp <= $2::TIMESTAMP)
            )
            SELECT * FROM app_stats
        "#;

        let mut stmt = conn.prepare(query)?;
        let summary = stmt.query_row([&params.start_date, &params.end_date], |row| {
            Ok(CrossAppSummary {
                total_applications: row.get(0)?,
                active_applications: row.get(1)?,
                total_events: row.get(2)?,
                total_tasks_completed: row.get(3)?,
                total_tasks_failed: row.get(4)?,
                avg_task_duration_ms: row.get(5)?,
                total_data_processed_gb: row.get(6)?,
                peak_concurrent_executors: row.get(7)?,
                date_range: DateRange {
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
        params: &AnalyticsQuery,
    ) -> anyhow::Result<Vec<TaskDistribution>> {
        let conn = self.connection.lock().await;
        
        let query = r#"
            SELECT 
                app_id,
                stage_id,
                COUNT(*) as total_tasks,
                COUNT(CASE WHEN raw_data::JSON ->> 'Task End Reason' = 'Success' THEN 1 END) as completed_tasks,
                COUNT(CASE WHEN raw_data::JSON ->> 'Task End Reason' != 'Success' THEN 1 END) as failed_tasks,
                AVG(duration_ms) as avg_duration_ms,
                MIN(duration_ms) as min_duration_ms,
                MAX(duration_ms) as max_duration_ms,
                COUNT(CASE WHEN raw_data::JSON -> 'Task Info' ->> 'Locality' = 'PROCESS_LOCAL' THEN 1 END) as process_local,
                COUNT(CASE WHEN raw_data::JSON -> 'Task Info' ->> 'Locality' = 'NODE_LOCAL' THEN 1 END) as node_local,
                COUNT(CASE WHEN raw_data::JSON -> 'Task Info' ->> 'Locality' = 'RACK_LOCAL' THEN 1 END) as rack_local,
                COUNT(CASE WHEN raw_data::JSON -> 'Task Info' ->> 'Locality' = 'ANY' THEN 1 END) as any_locality
            FROM events
            WHERE event_type = 'SparkListenerTaskEnd'
            AND stage_id IS NOT NULL
            AND ($1::TEXT IS NULL OR timestamp >= $1::TIMESTAMP)
            AND ($2::TEXT IS NULL OR timestamp <= $2::TIMESTAMP)
            AND ($3::TEXT IS NULL OR app_id = $3)
            GROUP BY app_id, stage_id
            ORDER BY app_id, stage_id
            LIMIT $4
        "#;

        let limit = params.limit.unwrap_or(100);
        let mut stmt = conn.prepare(query)?;
        
        let rows = stmt.query_map([
            &params.start_date,
            &params.end_date,
            &params.app_id,
            &Some(limit.to_string())
        ], |row| {
            Ok(TaskDistribution {
                app_id: row.get(0)?,
                stage_id: row.get(1)?,
                total_tasks: row.get(2)?,
                completed_tasks: row.get(3)?,
                failed_tasks: row.get(4)?,
                avg_duration_ms: row.get(5)?,
                min_duration_ms: row.get(6)?,
                max_duration_ms: row.get(7)?,
                data_locality_summary: DataLocalitySummary {
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
        params: &AnalyticsQuery,
    ) -> anyhow::Result<Vec<ExecutorUtilization>> {
        let conn = self.connection.lock().await;
        
        let query = r#"
            WITH executor_stats AS (
                SELECT 
                    raw_data::JSON -> 'Task Info' ->> 'Executor ID' as executor_id,
                    raw_data::JSON -> 'Task Info' ->> 'Host' as host,
                    COUNT(*) as total_tasks,
                    SUM(duration_ms) as total_duration_ms,
                    COUNT(DISTINCT app_id) as apps_count,
                    COUNT(CASE WHEN raw_data::JSON -> 'Task Info' ->> 'Locality' IN ('PROCESS_LOCAL', 'NODE_LOCAL') 
                        THEN 1 END) as locality_hits,
                    MAX(CAST(raw_data::JSON -> 'Task Metrics' ->> 'Peak Execution Memory' AS BIGINT)) / 1048576 as peak_memory_mb,
                    array_agg(DISTINCT app_id) as apps_served
                FROM events
                WHERE event_type = 'SparkListenerTaskEnd' 
                AND raw_data::JSON -> 'Task Info' ->> 'Executor ID' IS NOT NULL
                AND ($1::TEXT IS NULL OR timestamp >= $1::TIMESTAMP)
                AND ($2::TEXT IS NULL OR timestamp <= $2::TIMESTAMP)
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
            LIMIT $3
        "#;

        let limit = params.limit.unwrap_or(50);
        let mut stmt = conn.prepare(query)?;
        
        let rows = stmt.query_map([
            &params.start_date,
            &params.end_date,
            &Some(limit.to_string())
        ], |row| {
            let apps_json: String = row.get(7)?;
            let apps_served: Vec<String> = serde_json::from_str(&apps_json)
                .unwrap_or_default();
            
            Ok(ExecutorUtilization {
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
}