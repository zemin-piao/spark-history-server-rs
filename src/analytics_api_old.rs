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

/// Platform Engineering API router focused on resource optimization
pub fn analytics_router() -> Router<HistoryProvider> {
    Router::new()
        // Platform engineering focused endpoints
        .route("/optimization/resource-hogs", get(get_top_resource_consumers))
        .route("/optimization/efficiency-analysis", get(get_efficiency_analysis))
        .route("/capacity/usage-trends", get(get_capacity_usage_trends))
        .route("/capacity/cost-optimization", get(get_cost_optimization_opportunities))
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
// Platform Engineering Focused Endpoint Handlers  
// ============================================================================


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

/// GC time trends analytics
async fn get_gc_time_trends(
    State(provider): State<HistoryProvider>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<Vec<GcTimeTrend>>, StatusCode> {
    info!("GET /analytics/gc-time-trends - params: {:?}", params);

    let store = provider.get_duckdb_store();
    let trends = match store.get_gc_time_trends(&params).await {
        Ok(data) => data,
        Err(e) => {
            tracing::error!("Failed to get GC time trends: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    Ok(Json(trends))
}

/// CPU utilization and idle cores analysis
async fn get_cpu_utilization_analysis(
    State(provider): State<HistoryProvider>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<Vec<CpuUtilizationAnalysis>>, StatusCode> {
    info!(
        "GET /analytics/cpu-utilization-analysis - params: {:?}",
        params
    );

    let store = provider.get_duckdb_store();
    let analysis = match store.get_cpu_utilization_analysis(&params).await {
        Ok(data) => data,
        Err(e) => {
            tracing::error!("Failed to get CPU utilization analysis: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    Ok(Json(analysis))
}

/// Memory usage analysis and segregation
async fn get_memory_usage_analysis(
    State(provider): State<HistoryProvider>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<Json<Vec<MemoryUsageAnalysis>>, StatusCode> {
    info!(
        "GET /analytics/memory-usage-analysis - params: {:?}",
        params
    );

    let store = provider.get_duckdb_store();
    let analysis = match store.get_memory_usage_analysis(&params).await {
        Ok(data) => data,
        Err(e) => {
            tracing::error!("Failed to get memory usage analysis: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    Ok(Json(analysis))
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

/// GC time trend data structure
#[derive(Debug, Clone, Serialize)]
pub struct GcTimeTrend {
    pub date: String,
    pub app_id: String,
    pub total_gc_time_ms: i64,
    pub avg_gc_time_ms: Option<f64>,
    pub total_tasks: i64,
    pub gc_time_per_task_ms: Option<f64>,
}

/// CPU utilization and idle cores analysis
#[derive(Debug, Clone, Serialize)]
pub struct CpuUtilizationAnalysis {
    pub date: String,
    pub app_id: String,
    pub executor_id: String,
    pub total_tasks: i64,
    pub total_duration_ms: i64,
    pub actual_cpu_time_ms: i64,
    pub theoretical_cpu_time_ms: i64,
    pub idle_cpu_time_ms: i64,
    pub cpu_utilization_percent: Option<f64>,
    pub efficiency_rating: String, // "High", "Medium", "Low"
}

/// Memory usage analysis and segregation
#[derive(Debug, Clone, Serialize)]
pub struct MemoryUsageAnalysis {
    pub date: String,
    pub app_id: String,
    pub executor_id: String,
    pub max_memory_mb: i64,
    pub peak_memory_usage_mb: i64,
    pub avg_memory_usage_mb: Option<f64>,
    pub memory_utilization_percent: Option<f64>,
    pub memory_spill_mb: i64,
    pub disk_spill_mb: i64,
    pub total_tasks: i64,
    pub memory_efficiency_rating: String, // "Excellent", "Good", "Poor", "Critical"
    pub spill_ratio: Option<f64>,         // memory_spill / peak_memory_usage
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

// ============================================================================
// NEW: Platform Engineering Focused Data Structures
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
    pub efficiency_score: f64, // 0-100, higher = more efficient
    pub cost_impact: f64, // Estimated cost in resource units
    pub recommendation: String,
    pub last_seen: String,
}

#[derive(Debug, Clone, Serialize)]
pub enum ResourceType {
    Memory,
    CPU,
    Disk,
    Network,
}

/// Application efficiency analysis
#[derive(Debug, Clone, Serialize)]
pub struct EfficiencyAnalysis {
    pub app_id: String,
    pub app_name: String,
    pub efficiency_category: EfficiencyCategory,
    pub memory_efficiency: f64, // % of allocated memory actually used
    pub cpu_efficiency: f64, // % of allocated CPU actually used
    pub recommended_memory_gb: Option<f64>,
    pub recommended_cpu_cores: Option<f64>,
    pub potential_cost_savings: f64,
    pub risk_level: RiskLevel,
    pub optimization_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub enum EfficiencyCategory {
    OverProvisioned, // Using <50% of allocated resources
    WellTuned,       // Using 50-85% of allocated resources  
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
// NEW: Platform Engineering Focused Endpoint Handlers
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
    info!("GET /optimization/efficiency-analysis - params: {:?}", params);

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
            info!("Returning {} cost optimization opportunities", opportunities.len());
            Ok(Json(opportunities))
        }
        Err(e) => {
            tracing::error!("Failed to get cost optimization opportunities: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
