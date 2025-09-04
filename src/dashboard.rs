use askama::Template;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Html,
    routing::get,
    Router,
};
use serde::Deserialize;

use crate::analytics_api::{CapacityTrend, CostOptimization, EfficiencyAnalysis, ResourceHog};
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

#[derive(Clone)]
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
    capacity_trends: Vec<CapacityTrend>,
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
    let store = provider.get_duckdb_store();

    // Fetch real cross-app summary data
    let cross_app_summary = store.get_cross_app_summary().await.map_err(|e| {
        tracing::error!("Failed to get cross app summary: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Fetch real active applications data
    let active_applications = store.get_active_applications(Some(10)).await.map_err(|e| {
        tracing::error!("Failed to get active applications: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let template = ClusterTemplate {
        cross_app_summary,
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

    let store = provider.get_duckdb_store();

    // Fetch all optimization data
    let resource_hogs = store
        .get_top_resource_consumers(&analytics_query)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get resource hogs: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let efficiency_analysis = store
        .get_efficiency_analysis(&analytics_query)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get efficiency analysis: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let capacity_trends = store
        .get_capacity_usage_trends(&analytics_query)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get capacity trends: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let cost_optimizations = store
        .get_cost_optimization_opportunities(&analytics_query)
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
            let total_savings: f64 = cost_optimizations
                .iter()
                .map(|c| (c.current_cost - c.optimized_cost).max(0.0))
                .sum();
            format!("${:.2}", total_savings)
        },
        apps_needing_optimization: cost_optimizations.len(),
        high_confidence_optimizations: cost_optimizations
            .iter()
            .filter(|c| c.confidence_score > 80.0)
            .count(),
    };

    let template = OptimizeTemplate {
        summary_stats,
        resource_hogs,
        efficiency_analysis,
        capacity_trends,
        cost_optimizations,
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
