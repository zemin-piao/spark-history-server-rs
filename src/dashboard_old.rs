use askama::Template;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Html,
    routing::get,
    Router,
};
use serde::Deserialize;

use crate::storage::HistoryProvider;
use crate::analytics_api::{ResourceHog, EfficiencyAnalysis, CapacityTrend, CostOptimization};

#[derive(Template)]
#[template(path = "enhanced_simple.html")]
struct ClusterTemplate {
    #[allow(dead_code)]
    active_tab: String,
    cross_app_summary: DisplayCrossAppSummary,
    active_applications: Vec<ApplicationSummary>,
    #[allow(dead_code)]
    recent_applications: Vec<ApplicationSummary>,
}


#[derive(Clone)]
pub struct DisplayCrossAppSummary {
    pub total_applications: i64,
    pub active_applications: i64,
    pub total_events: i64,
    pub total_tasks_completed: i64,
    pub total_tasks_failed: i64,
    pub avg_task_duration_ms: String,
    pub total_data_processed_gb: String,
    pub peak_concurrent_executors: i64,
}

impl Default for DisplayCrossAppSummary {
    fn default() -> Self {
        Self {
            total_applications: 0,
            active_applications: 0,
            total_events: 0,
            total_tasks_completed: 0,
            total_tasks_failed: 0,
            avg_task_duration_ms: "-".to_string(),
            total_data_processed_gb: "-".to_string(),
            peak_concurrent_executors: 0,
        }
    }
}

#[derive(Template)]
#[template(path = "platform_engineering_optimize.html")]
struct OptimizeTemplate {
    active_tab: String,
    resource_hogs: Vec<ResourceHog>,
    efficiency_analysis: Vec<EfficiencyAnalysis>, 
    capacity_trends: Vec<CapacityTrend>,
    cost_optimizations: Vec<CostOptimization>,
    summary_stats: OptimizationSummary,
}

#[derive(Clone)]
pub struct OptimizationSummary {
    pub total_resource_hogs: usize,
    pub over_provisioned_apps: usize,
    pub under_provisioned_apps: usize,
    pub potential_monthly_savings: String,
    pub apps_needing_optimization: usize,
    pub high_confidence_optimizations: usize,
}

// ResourcesTemplate removed - resources view simplified

#[derive(Clone)]
pub struct DisplayPerformanceTrend {
    pub date: String,
    pub app_id: String,
    pub avg_task_duration_ms: String,
    pub total_tasks: i64,
    pub failed_tasks: i64,
    pub avg_input_bytes: String,
    #[allow(dead_code)]
    pub avg_output_bytes: String,
}

impl From<PerformanceTrend> for DisplayPerformanceTrend {
    fn from(trend: PerformanceTrend) -> Self {
        Self {
            date: trend.date,
            app_id: trend.app_id,
            avg_task_duration_ms: trend
                .avg_task_duration_ms
                .map(|v| format!("{:.1}", v))
                .unwrap_or_else(|| "-".to_string()),
            total_tasks: trend.total_tasks,
            failed_tasks: trend.failed_tasks,
            avg_input_bytes: trend
                .avg_input_bytes
                .map(|v| format!("{:.1}", v / 1048576.0))
                .unwrap_or_else(|| "-".to_string()),
            avg_output_bytes: trend
                .avg_output_bytes
                .map(|v| format!("{:.1}", v / 1048576.0))
                .unwrap_or_else(|| "-".to_string()),
        }
    }
}

#[derive(Clone)]
pub struct ResourceSummary {
    pub total_executors: i64,
    pub unique_hosts: i64,
    pub total_memory_gb: i64,
    pub memory_utilization_percent: f64,
    pub total_cpu_cores: i64,
    pub cpu_utilization_percent: f64,
    pub total_spill_gb: f64,
    pub spill_applications: i64,
}

#[derive(Clone)]
pub struct DisplayResourceUtilizationMetrics {
    pub executor_id: String,
    pub host: String,
    #[allow(dead_code)]
    pub app_id: String,
    pub app_name: String,
    pub total_tasks: i64,
    pub completed_tasks: i64,
    pub failed_tasks: i64,
    pub total_duration_ms: i64,
    pub avg_task_duration_ms: String,
    pub cpu_time_ms: i64,
    pub peak_memory_usage_mb: String,
    pub max_memory_mb: i64,
    pub memory_utilization_percent: String,
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
    pub is_active: bool,
}

impl From<ResourceUtilizationMetrics> for DisplayResourceUtilizationMetrics {
    fn from(metrics: ResourceUtilizationMetrics) -> Self {
        let memory_utilization = if metrics.max_memory_mb > 0 {
            metrics.peak_memory_usage_mb.unwrap_or(0) as f64 / metrics.max_memory_mb as f64 * 100.0
        } else {
            0.0
        };

        Self {
            executor_id: metrics.executor_id,
            host: metrics.host,
            app_id: metrics.app_id,
            app_name: metrics.app_name,
            total_tasks: metrics.total_tasks,
            completed_tasks: metrics.completed_tasks,
            failed_tasks: metrics.failed_tasks,
            total_duration_ms: metrics.total_duration_ms,
            avg_task_duration_ms: metrics
                .avg_task_duration_ms
                .map(|v| format!("{:.1}", v))
                .unwrap_or_else(|| "-".to_string()),
            cpu_time_ms: metrics.cpu_time_ms,
            peak_memory_usage_mb: metrics
                .peak_memory_usage_mb
                .map(|v| v.to_string())
                .unwrap_or_else(|| "0".to_string()),
            max_memory_mb: metrics.max_memory_mb,
            memory_utilization_percent: format!("{:.1}", memory_utilization),
            input_bytes: metrics.input_bytes / 1048576, // Convert to MB
            output_bytes: metrics.output_bytes / 1048576, // Convert to MB
            shuffle_read_bytes: metrics.shuffle_read_bytes / 1048576, // Convert to MB
            shuffle_write_bytes: metrics.shuffle_write_bytes / 1048576, // Convert to MB
            disk_spill_bytes: metrics.disk_spill_bytes / 1048576, // Convert to MB
            memory_spill_bytes: metrics.memory_spill_bytes / 1048576, // Convert to MB
            data_locality_process_local: metrics.data_locality_process_local,
            data_locality_node_local: metrics.data_locality_node_local,
            data_locality_rack_local: metrics.data_locality_rack_local,
            data_locality_any: metrics.data_locality_any,
            is_active: metrics.is_active,
        }
    }
}

#[derive(Clone)]
pub struct DisplayTaskDistribution {
    pub app_id: String,
    pub stage_id: i64,
    pub total_tasks: i64,
    pub completed_tasks: i64,
    pub failed_tasks: i64,
    pub avg_duration_ms: String,
    pub data_locality_summary: crate::analytics_api::DataLocalitySummary,
}

impl From<TaskDistribution> for DisplayTaskDistribution {
    fn from(dist: TaskDistribution) -> Self {
        Self {
            app_id: dist.app_id,
            stage_id: dist.stage_id,
            total_tasks: dist.total_tasks,
            completed_tasks: dist.completed_tasks,
            failed_tasks: dist.failed_tasks,
            avg_duration_ms: dist
                .avg_duration_ms
                .map(|v| format!("{:.1}", v))
                .unwrap_or_else(|| "-".to_string()),
            data_locality_summary: dist.data_locality_summary,
        }
    }
}

#[derive(Deserialize)]
pub struct DashboardQuery {
    #[allow(dead_code)]
    range: Option<String>,
    #[allow(dead_code)]
    min_runtime: Option<u32>,
}


#[derive(Clone)]
pub struct ApplicationSummary {
    pub id: String,
    pub user: String,
    #[allow(dead_code)]
    pub name: String,
    pub duration: String,
    pub cores: u32,
    pub memory: u32,
    pub status: String,
    #[allow(dead_code)]
    pub end_time: String,
}

pub fn dashboard_router() -> Router<HistoryProvider> {
    Router::new()
        .route("/", get(cluster_overview))
        .route("/cluster", get(cluster_overview))
        .route("/resources", get(resources_view))
        .route("/optimize", get(optimize_view))
        .route("/teams", get(teams_view))
}

pub async fn cluster_overview(
    Query(params): Query<DashboardQuery>,
    State(provider): State<HistoryProvider>,
) -> Result<Html<String>, StatusCode> {
    let cross_app_summary = get_cross_app_summary(&provider, &params).await?;
    let active_applications = get_active_applications(&provider).await?;
    let recent_applications = get_recent_applications(&provider, &params).await?;

    let template = ClusterTemplate {
        active_tab: "cluster".to_string(),
        cross_app_summary,
        active_applications,
        recent_applications,
    };

    match template.render() {
        Ok(html) => Ok(Html(html)),
        Err(e) => {
            tracing::error!("Template render error: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn optimize_view(
    Query(_params): Query<DashboardQuery>,
    State(provider): State<HistoryProvider>,
) -> Result<Html<String>, StatusCode> {
    let analytics_query = crate::analytics_api::AnalyticsQuery {
        start_date: None,
        end_date: None,
        limit: Some(10), // TOP 10 for each category
        app_id: None,
    };

    // Fetch data from new platform-engineering endpoints
    let store = provider.get_duckdb_store();
    
    let resource_hogs = match store.get_top_resource_consumers(&analytics_query).await {
        Ok(hogs) => hogs,
        Err(e) => {
            tracing::error!("Failed to get resource hogs: {}", e);
            vec![]
        }
    };

    let efficiency_analysis = match store.get_efficiency_analysis(&analytics_query).await {
        Ok(analysis) => analysis,
        Err(e) => {
            tracing::error!("Failed to get efficiency analysis: {}", e);
            vec![]
        }
    };

    let capacity_query = crate::analytics_api::AnalyticsQuery {
        start_date: None,
        end_date: None,
        limit: Some(30), // Last 30 days for trends
        app_id: None,
    };

    let capacity_trends = match store.get_capacity_usage_trends(&capacity_query).await {
        Ok(trends) => trends,
        Err(e) => {
            tracing::error!("Failed to get capacity trends: {}", e);
            vec![]
        }
    };

    let cost_optimizations = match store.get_cost_optimization_opportunities(&analytics_query).await {
        Ok(optimizations) => optimizations,
        Err(e) => {
            tracing::error!("Failed to get cost optimizations: {}", e);
            vec![]
        }
    };

    // Calculate summary statistics
    let over_provisioned_count = efficiency_analysis
        .iter()
        .filter(|e| matches!(e.efficiency_category, crate::analytics_api::EfficiencyCategory::OverProvisioned))
        .count();
        
    let under_provisioned_count = efficiency_analysis
        .iter()
        .filter(|e| matches!(e.efficiency_category, crate::analytics_api::EfficiencyCategory::UnderProvisioned))
        .count();

    let total_potential_savings: f64 = cost_optimizations
        .iter()
        .map(|c| c.current_cost - c.optimized_cost)
        .sum();

    let high_confidence_optimizations = cost_optimizations
        .iter()
        .filter(|c| c.confidence_score >= 80.0)
        .count();

    let summary_stats = OptimizationSummary {
        total_resource_hogs: resource_hogs.len(),
        over_provisioned_apps: over_provisioned_count,
        under_provisioned_apps: under_provisioned_count,
        potential_monthly_savings: format!("${:.0}", total_potential_savings * 30.0 * 24.0), // Rough monthly estimate
        apps_needing_optimization: efficiency_analysis.len(),
        high_confidence_optimizations,
    };

    let template = OptimizeTemplate {
        active_tab: "optimize".to_string(),
        resource_hogs,
        efficiency_analysis,
        capacity_trends,
        cost_optimizations,
        summary_stats,
    };

    match template.render() {
        Ok(html) => Ok(Html(html)),
        Err(e) => {
            tracing::error!("Template render error: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}


pub async fn resources_view(
    Query(_params): Query<DashboardQuery>,
    State(_provider): State<HistoryProvider>,
) -> Result<Html<String>, StatusCode> {
    // Analytics removed - redirect to optimization view
    let html = r#"
    <!DOCTYPE html>
    <html>
    <head>
        <title>Resources - Spark Platform</title>
        <meta http-equiv="refresh" content="0; url=/optimize">
        <style>
            body { font-family: system-ui; margin: 40px; text-align: center; }
            .message { background: #f0f9ff; border: 1px solid #0ea5e9; padding: 20px; border-radius: 8px; }
        </style>
    </head>
    <body>
        <div class="message">
            <h2>Resources View Moved</h2>
            <p>The resources view has been integrated into the <a href="/optimize">Optimization Dashboard</a>.</p>
            <p>Redirecting automatically...</p>
        </div>
    </body>
    </html>
    "#;
    Ok(Html(html.to_string()))
}

pub async fn teams_view(
    Query(_params): Query<DashboardQuery>,
    State(_provider): State<HistoryProvider>,
) -> Result<Html<String>, StatusCode> {
    // TODO: Implement teams view
    let html = r#"
    <html>
    <body>
        <h1>Teams View</h1>
        <p>Coming soon - team/user resource attribution</p>
        <a href="/">← Back to Cluster Overview</a>
    </body>
    </html>
    "#;
    Ok(Html(html.to_string()))
}

// Real API call implementations
async fn get_cross_app_summary(
    _provider: &HistoryProvider,
    _params: &DashboardQuery,
) -> Result<DisplayCrossAppSummary, StatusCode> {
    // Analytics removed - return default/stub data
    Ok(DisplayCrossAppSummary::default())
}

// Analytics removed - stub functions for compatibility

async fn get_resource_utilization(
    provider: &HistoryProvider,
    analytics_query: &crate::analytics_api::AnalyticsQuery,
) -> Result<Vec<ResourceUtilizationMetrics>, StatusCode> {
    let store = provider.get_duckdb_store();
    match store
        .get_resource_utilization_metrics(analytics_query)
        .await
    {
        Ok(metrics) => Ok(metrics),
        Err(e) => {
            tracing::error!("Failed to get resource utilization: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn get_task_distribution(
    provider: &HistoryProvider,
    analytics_query: &crate::analytics_api::AnalyticsQuery,
) -> Result<Vec<TaskDistribution>, StatusCode> {
    let store = provider.get_duckdb_store();
    match store.get_task_distribution(analytics_query).await {
        Ok(distribution) => Ok(distribution),
        Err(e) => {
            tracing::error!("Failed to get task distribution: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn get_executor_utilization(
    provider: &HistoryProvider,
    analytics_query: &crate::analytics_api::AnalyticsQuery,
) -> Result<Vec<ExecutorUtilization>, StatusCode> {
    let store = provider.get_duckdb_store();
    match store.get_executor_utilization(analytics_query).await {
        Ok(utilization) => Ok(utilization),
        Err(e) => {
            tracing::error!("Failed to get executor utilization: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn get_active_applications(
    provider: &HistoryProvider,
) -> Result<Vec<ApplicationSummary>, StatusCode> {
    match provider
        .get_applications(
            Some(10),
            Some(vec![crate::models::ApplicationStatus::Running]),
            None,
            None,
            None,
            None,
        )
        .await
    {
        Ok(apps) => {
            let summaries = apps
                .into_iter()
                .map(|app| {
                    let attempt = app.attempts.first();
                    ApplicationSummary {
                        id: app.id,
                        user: attempt
                            .map(|a| a.spark_user.clone())
                            .unwrap_or_else(|| "unknown".to_string()),
                        name: app.name,
                        duration: attempt
                            .map(|a| format_duration(a.duration))
                            .unwrap_or_else(|| "unknown".to_string()),
                        cores: app.max_cores.unwrap_or(0) as u32,
                        memory: (app.memory_per_executor_mb.unwrap_or(0) / 1024) as u32,
                        status: "RUNNING".to_string(),
                        end_time: "".to_string(),
                    }
                })
                .collect();
            Ok(summaries)
        }
        Err(e) => {
            tracing::error!("Failed to get active applications: {}", e);
            Ok(vec![]) // Return empty list instead of error to keep UI working
        }
    }
}

async fn get_recent_applications(
    provider: &HistoryProvider,
    _params: &DashboardQuery,
) -> Result<Vec<ApplicationSummary>, StatusCode> {
    match provider
        .get_applications(
            Some(20),
            Some(vec![crate::models::ApplicationStatus::Completed]),
            None,
            None,
            None,
            None,
        )
        .await
    {
        Ok(apps) => {
            let summaries = apps
                .into_iter()
                .map(|app| {
                    let attempt = app.attempts.first();
                    ApplicationSummary {
                        id: app.id,
                        user: attempt
                            .map(|a| a.spark_user.clone())
                            .unwrap_or_else(|| "unknown".to_string()),
                        name: app.name,
                        duration: attempt
                            .map(|a| format_duration(a.duration))
                            .unwrap_or_else(|| "unknown".to_string()),
                        cores: app.max_cores.unwrap_or(0) as u32,
                        memory: (app.memory_per_executor_mb.unwrap_or(0) / 1024) as u32,
                        status: if attempt.map(|a| a.completed).unwrap_or(false) {
                            "SUCCEEDED".to_string()
                        } else {
                            "FAILED".to_string()
                        },
                        end_time: attempt
                            .map(|a| format_time_ago(a.end_time))
                            .unwrap_or_else(|| "unknown".to_string()),
                    }
                })
                .collect();
            Ok(summaries)
        }
        Err(e) => {
            tracing::error!("Failed to get recent applications: {}", e);
            Ok(vec![]) // Return empty list instead of error to keep UI working
        }
    }
}

// Helper function to create resource summary
fn create_resource_summary(resource_utilization: &[ResourceUtilizationMetrics]) -> ResourceSummary {
    let total_executors = resource_utilization.len() as i64;
    let unique_hosts = resource_utilization
        .iter()
        .map(|r| &r.host)
        .collect::<std::collections::HashSet<_>>()
        .len() as i64;

    let total_memory_mb: i64 = resource_utilization.iter().map(|r| r.max_memory_mb).sum();
    let total_memory_gb = total_memory_mb / 1024;

    let peak_memory_mb: i64 = resource_utilization
        .iter()
        .map(|r| r.peak_memory_usage_mb.unwrap_or(0))
        .sum();

    let memory_utilization_percent = if total_memory_mb > 0 {
        (peak_memory_mb as f64 / total_memory_mb as f64 * 100.0).min(100.0)
    } else {
        0.0
    };

    // Estimate CPU cores (assuming 1 core per executor)
    let total_cpu_cores = total_executors;

    // Estimate CPU utilization based on task runtime vs wall clock time
    let total_cpu_time: i64 = resource_utilization.iter().map(|r| r.cpu_time_ms).sum();
    let total_runtime: i64 = resource_utilization
        .iter()
        .map(|r| r.total_duration_ms)
        .sum();

    let cpu_utilization_percent = if total_runtime > 0 {
        (total_cpu_time as f64 / total_runtime as f64 * 100.0).min(100.0)
    } else {
        0.0
    };

    let total_disk_spill: i64 = resource_utilization
        .iter()
        .map(|r| r.disk_spill_bytes)
        .sum();
    let total_memory_spill: i64 = resource_utilization
        .iter()
        .map(|r| r.memory_spill_bytes)
        .sum();
    let total_spill_gb =
        (total_disk_spill + total_memory_spill) as f64 / (1024.0 * 1024.0 * 1024.0);

    let spill_applications = resource_utilization
        .iter()
        .filter(|r| r.disk_spill_bytes > 0 || r.memory_spill_bytes > 0)
        .map(|r| &r.app_id)
        .collect::<std::collections::HashSet<_>>()
        .len() as i64;

    ResourceSummary {
        total_executors,
        unique_hosts,
        total_memory_gb,
        memory_utilization_percent,
        total_cpu_cores,
        cpu_utilization_percent,
        total_spill_gb,
        spill_applications,
    }
}

// Utility functions
fn format_duration(duration_ms: i64) -> String {
    let minutes = duration_ms / 60000;
    if minutes < 60 {
        format!("{}min", minutes)
    } else {
        let hours = minutes / 60;
        let remaining_min = minutes % 60;
        if remaining_min == 0 {
            format!("{}h", hours)
        } else {
            format!("{}h {}min", hours, remaining_min)
        }
    }
}

fn format_time_ago(end_time: chrono::DateTime<chrono::Utc>) -> String {
    let now = chrono::Utc::now();
    let duration = now.signed_duration_since(end_time);

    if duration.num_hours() < 1 {
        format!("{}min ago", duration.num_minutes())
    } else if duration.num_days() < 1 {
        format!("{}h ago", duration.num_hours())
    } else {
        format!("{}d ago", duration.num_days())
    }
}
