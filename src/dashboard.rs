use askama::Template;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Html,
    routing::get,
    Router,
};
use serde::Deserialize;

use crate::analytics_api::{CostOptimization, EfficiencyAnalysis, ResourceHog};
use crate::storage::HistoryProvider;

#[derive(Clone)]
pub struct SimpleCrossAppSummary {
    pub total_applications: i64,
    pub active_applications: i64,
    pub total_events: i64,
    pub total_tasks_completed: i64,
    pub total_tasks_failed: i64,
    pub avg_task_duration_ms: String,
    pub total_data_processed_gb: String,
    pub peak_concurrent_executors: i64,
}

impl Default for SimpleCrossAppSummary {
    fn default() -> Self {
        Self {
            total_applications: 0,
            active_applications: 0,
            total_events: 0,
            total_tasks_completed: 0,
            total_tasks_failed: 0,
            avg_task_duration_ms: "0".to_string(),
            total_data_processed_gb: "0".to_string(),
            peak_concurrent_executors: 0,
        }
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct CapacityTrendForTemplate {
    pub date: String,
    pub total_memory_gb_used: f64,
    pub total_cpu_cores_used: f64,
    pub peak_concurrent_applications: u32,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SimpleApplicationSummary {
    pub id: String,
    pub user: String,
    pub duration: String,
    pub cores: u32,
    pub memory: u32,
    pub status: String,
}

#[derive(Clone)]
pub struct SummaryStats {
    pub total_resource_hogs: usize,
    pub over_provisioned_apps: usize,
    pub under_provisioned_apps: usize,
    pub potential_monthly_savings: String,
    pub apps_needing_optimization: usize,
    pub high_confidence_optimizations: usize,
}

#[derive(Template)]
#[template(path = "enhanced_simple.html")]
struct ClusterTemplate {
    cross_app_summary: SimpleCrossAppSummary,
    active_applications: Vec<SimpleApplicationSummary>,
}

#[derive(Template)]
#[template(path = "platform_engineering_optimize.html")]
struct OptimizeTemplate {
    summary_stats: SummaryStats,
    resource_hogs: Vec<ResourceHog>,
    efficiency_analysis: Vec<EfficiencyAnalysis>,
    capacity_trends: Vec<CapacityTrendForTemplate>,
    cost_optimizations: Vec<CostOptimization>,
}

#[derive(Debug, Deserialize)]
pub struct DashboardQuery {
    // Query parameters for future use - currently unused
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
    Query(_params): Query<DashboardQuery>,
    State(provider): State<HistoryProvider>,
) -> Result<Html<String>, StatusCode> {
    // Fetch real cross-app summary data
    let cross_app_summary = provider
        .get_cross_app_summary(&crate::analytics_api::AnalyticsQuery {
            start_date: None,
            end_date: None,
            limit: None,
            app_id: None,
        })
        .await
        .map_err(|e| {
            tracing::error!("Failed to get cross app summary: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Fetch real active applications data
    let active_applications_values =
        provider
            .get_active_applications(Some(10))
            .await
            .map_err(|e| {
                tracing::error!("Failed to get active applications: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

    let active_applications: Result<Vec<SimpleApplicationSummary>, _> = active_applications_values
        .into_iter()
        .map(serde_json::from_value)
        .collect();

    let active_applications = active_applications.map_err(|e| {
        tracing::error!("Failed to deserialize active applications: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Convert CrossAppSummary to SimpleCrossAppSummary
    let simple_cross_app_summary = SimpleCrossAppSummary {
        total_applications: cross_app_summary.total_applications as i64,
        active_applications: cross_app_summary.active_applications as i64,
        total_events: cross_app_summary.total_events as i64,
        total_tasks_completed: 0,
        total_tasks_failed: 0,
        avg_task_duration_ms: format!("{:.2}ms", cross_app_summary.average_duration_ms),
        total_data_processed_gb: "0".to_string(),
        peak_concurrent_executors: 0,
    };

    let template = ClusterTemplate {
        cross_app_summary: simple_cross_app_summary,
        active_applications,
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
        limit: Some(20),
        app_id: None,
    };

    // Fetch all optimization data
    let resource_hogs = provider
        .get_resource_hogs(&analytics_query)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get resource hogs: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let efficiency_analysis = provider
        .get_efficiency_analysis(&analytics_query)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get efficiency analysis: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let capacity_trends = provider
        .get_performance_trends(&analytics_query)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get capacity trends: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let cost_optimizations = provider
        .get_cost_optimization(&analytics_query)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get cost optimizations: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Calculate summary stats from the data
    let summary_stats = SummaryStats {
        total_resource_hogs: resource_hogs.len(),
        over_provisioned_apps: efficiency_analysis
            .iter()
            .filter(|e| {
                matches!(
                    e.efficiency_category,
                    crate::analytics_api::EfficiencyCategory::OverProvisioned
                )
            })
            .count(),
        under_provisioned_apps: efficiency_analysis
            .iter()
            .filter(|e| {
                matches!(
                    e.efficiency_category,
                    crate::analytics_api::EfficiencyCategory::UnderProvisioned
                )
            })
            .count(),
        potential_monthly_savings: {
            let total_savings =
                (cost_optimizations.current_cost - cost_optimizations.optimized_cost).max(0.0);
            format!("${:.2}", total_savings)
        },
        apps_needing_optimization: 1,
        high_confidence_optimizations: if cost_optimizations.confidence_score > 80.0 {
            1
        } else {
            0
        },
    };

    // Convert PerformanceTrend to CapacityTrendForTemplate
    let capacity_trends_for_template: Vec<CapacityTrendForTemplate> = capacity_trends
        .into_iter()
        .map(|trend| CapacityTrendForTemplate {
            date: format!("timestamp_{}", trend.timestamp),
            total_memory_gb_used: trend.metric_value,
            total_cpu_cores_used: trend.metric_value * 0.8, // Mock conversion
            peak_concurrent_applications: trend.application_count,
        })
        .collect();

    let template = OptimizeTemplate {
        summary_stats,
        resource_hogs,
        efficiency_analysis,
        capacity_trends: capacity_trends_for_template,
        cost_optimizations: vec![cost_optimizations],
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
    let html = r#"
    <!DOCTYPE html>
    <html>
    <head>
        <title>Teams - Spark Platform</title>
        <style>
            body { font-family: system-ui; margin: 40px; text-align: center; }
            .message { background: #f0f9ff; border: 1px solid #0ea5e9; padding: 20px; border-radius: 8px; }
        </style>
    </head>
    <body>
        <div class="message">
            <h2>Teams View</h2>
            <p>Teams functionality coming soon...</p>
            <p><a href="/">← Back to Overview</a></p>
        </div>
    </body>
    </html>
    "#;
    Ok(Html(html.to_string()))
}
