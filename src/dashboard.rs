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

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct CapacityTrendForTemplate {
    pub date: String,
    pub total_memory_gb_used: f64,
    pub total_cpu_cores_used: f64,
    pub peak_concurrent_applications: u32,
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
        .route("/", get(optimize_view))
        .route("/resources", get(resources_view))
        .route("/optimize", get(optimize_view))
        .route("/teams", get(teams_view))
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
