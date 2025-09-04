use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::storage::HistoryProvider;

/// Platform Engineering API router focused on resource optimization
pub fn analytics_router() -> Router<HistoryProvider> {
    Router::new()
        // Platform engineering focused endpoints
        .route(
            "/optimization/resource-hogs",
            get(get_top_resource_consumers),
        )
        .route(
            "/optimization/efficiency-analysis",
            get(get_efficiency_analysis),
        )
        .route("/capacity/usage-trends", get(get_capacity_usage_trends))
        .route(
            "/capacity/cost-optimization",
            get(get_cost_optimization_opportunities),
        )
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

// ============================================================================
// Platform Engineering Focused Data Structures
// ============================================================================

/// TOP resource consuming applications
#[derive(Debug, Clone, Serialize)]
pub struct ResourceHog {
    pub app_id: String,
    pub app_name: String,
    pub resource_type: ResourceType,
    pub consumption_value: f64,
    pub consumption_unit: String,
    pub utilization_percentage: f64,
    pub efficiency_score: f64,          // 0-100, higher = more efficient
    pub efficiency_explanation: String, // e.g. "15% (2GB spilling)"
    pub cost_impact: f64,               // Estimated cost in resource units
    pub recommendation: String,
    pub last_seen: String,
}

#[derive(Debug, Clone, Serialize)]
pub enum ResourceType {
    Memory,
    #[allow(dead_code)]
    Cpu,
    #[allow(dead_code)]
    Disk,
    #[allow(dead_code)]
    Network,
}

/// Application efficiency analysis
#[derive(Debug, Clone, Serialize)]
pub struct EfficiencyAnalysis {
    pub app_id: String,
    pub app_name: String,
    pub efficiency_category: EfficiencyCategory,
    pub memory_efficiency: f64, // % of allocated memory actually used
    pub memory_efficiency_explanation: String, // e.g. "25% (high GC overhead)"
    pub cpu_efficiency: f64,    // % of allocated CPU actually used
    pub cpu_efficiency_explanation: String, // e.g. "8% (serial processing)"
    pub recommended_memory_gb: Option<f64>,
    pub recommended_cpu_cores: Option<f64>,
    pub potential_cost_savings: f64,
    pub risk_level: RiskLevel,
    pub optimization_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub enum EfficiencyCategory {
    OverProvisioned,  // Using <50% of allocated resources
    WellTuned,        // Using 50-85% of allocated resources
    UnderProvisioned, // Using >90% of allocated resources
}

impl std::fmt::Display for EfficiencyCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EfficiencyCategory::OverProvisioned => write!(f, "Over-Provisioned"),
            EfficiencyCategory::WellTuned => write!(f, "Well-Tuned"),
            EfficiencyCategory::UnderProvisioned => write!(f, "Under-Provisioned"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum RiskLevel {
    Low,    // Safe to optimize
    Medium, // Monitor while optimizing
    High,   // Risk of performance degradation
}

/// Capacity usage trends for planning
#[derive(Debug, Clone, Serialize)]
pub struct CapacityTrend {
    pub date: String,
    pub total_memory_gb_used: f64,
    pub total_cpu_cores_used: f64,
    pub peak_concurrent_applications: i64,
    pub average_resource_utilization: f64,
    pub cluster_capacity_percentage: f64,
    pub projected_growth_rate: Option<f64>, // Monthly growth %
}

/// Cost optimization opportunities
#[derive(Debug, Clone, Serialize)]
pub struct CostOptimization {
    pub optimization_type: OptimizationType,
    pub app_id: String,
    pub app_name: String,
    pub current_cost: f64,
    pub optimized_cost: f64,
    pub savings_percentage: f64,
    pub confidence_score: f64, // 0-100, how confident we are in the recommendation
    pub implementation_difficulty: DifficultyLevel,
    pub optimization_details: String,
}

#[derive(Debug, Clone, Serialize)]
pub enum OptimizationType {
    ReduceExecutors,
    ReduceMemory,
    OptimizePartitioning,
    EnableSpotInstances,
    ScheduleOffPeak,
}

impl std::fmt::Display for OptimizationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OptimizationType::ReduceExecutors => write!(f, "Reduce Executors"),
            OptimizationType::ReduceMemory => write!(f, "Reduce Memory"),
            OptimizationType::OptimizePartitioning => write!(f, "Optimize Partitioning"),
            OptimizationType::EnableSpotInstances => write!(f, "Enable Spot Instances"),
            OptimizationType::ScheduleOffPeak => write!(f, "Schedule Off-Peak"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum DifficultyLevel {
    Easy,   // Configuration change only
    Medium, // Code changes required
    Hard,   // Architecture changes required
}

impl std::fmt::Display for DifficultyLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DifficultyLevel::Easy => write!(f, "Easy"),
            DifficultyLevel::Medium => write!(f, "Medium"),
            DifficultyLevel::Hard => write!(f, "Hard"),
        }
    }
}

// ============================================================================
// Platform Engineering Focused Endpoint Handlers
// ============================================================================

/// Get TOP resource consuming applications (Memory, CPU, Disk hogs)
async fn get_top_resource_consumers(
    State(provider): State<HistoryProvider>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<Vec<ResourceHog>>, StatusCode> {
    info!("GET /optimization/resource-hogs - params: {:?}", params);

    let store = provider.get_duckdb_store();
    match store.get_top_resource_consumers(&params).await {
        Ok(resource_hogs) => {
            info!("Returning {} resource hogs", resource_hogs.len());
            Ok(Json(resource_hogs))
        }
        Err(e) => {
            tracing::error!("Failed to get top resource consumers: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get application efficiency analysis (over/under-provisioned apps)  
async fn get_efficiency_analysis(
    State(provider): State<HistoryProvider>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<Vec<EfficiencyAnalysis>>, StatusCode> {
    info!(
        "GET /optimization/efficiency-analysis - params: {:?}",
        params
    );

    let store = provider.get_duckdb_store();
    match store.get_efficiency_analysis(&params).await {
        Ok(analysis) => {
            info!("Returning {} efficiency analysis entries", analysis.len());
            Ok(Json(analysis))
        }
        Err(e) => {
            tracing::error!("Failed to get efficiency analysis: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get capacity usage trends for planning
async fn get_capacity_usage_trends(
    State(provider): State<HistoryProvider>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<Vec<CapacityTrend>>, StatusCode> {
    info!("GET /capacity/usage-trends - params: {:?}", params);

    let store = provider.get_duckdb_store();
    match store.get_capacity_usage_trends(&params).await {
        Ok(trends) => {
            info!("Returning {} capacity trend entries", trends.len());
            Ok(Json(trends))
        }
        Err(e) => {
            tracing::error!("Failed to get capacity usage trends: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get cost optimization opportunities
async fn get_cost_optimization_opportunities(
    State(provider): State<HistoryProvider>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<Vec<CostOptimization>>, StatusCode> {
    info!("GET /capacity/cost-optimization - params: {:?}", params);

    let store = provider.get_duckdb_store();
    match store.get_cost_optimization_opportunities(&params).await {
        Ok(opportunities) => {
            info!(
                "Returning {} cost optimization opportunities",
                opportunities.len()
            );
            Ok(Json(opportunities))
        }
        Err(e) => {
            tracing::error!("Failed to get cost optimization opportunities: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
