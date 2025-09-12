use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde::Deserialize;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{error, info};

use crate::analytics_api;
use crate::dashboard;
use crate::models::{ApplicationInfo, ApplicationStatus, VersionInfo};
use crate::storage::HistoryProvider;

/// Create the main application router
pub async fn create_app(history_provider: HistoryProvider) -> anyhow::Result<Router> {
    info!("Setting up API routes");

    let app = Router::new()
        // API v1 routes
        .route("/api/v1/applications", get(list_applications))
        .route("/api/v1/applications/:app_id", get(get_application))
        .route(
            "/api/v1/applications/:app_id/jobs",
            get(get_application_jobs),
        )
        .route(
            "/api/v1/applications/:app_id/executors",
            get(get_application_executors),
        )
        .route(
            "/api/v1/applications/:app_id/stages",
            get(get_application_stages),
        )
        .route(
            "/api/v1/applications/:app_id/storage/rdd",
            get(get_application_storage),
        )
        .route(
            "/api/v1/applications/:app_id/environment",
            get(get_application_environment),
        )
        .route("/api/v1/version", get(get_version))
        // Health check endpoint
        .route("/health", get(health_check))
        // Add analytics routes
        .nest("/api/v1", analytics_api::analytics_router())
        // Add dashboard routes (web UI)
        .nest("/", dashboard::dashboard_router())
        // Add middleware
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(history_provider);

    Ok(app)
}

/// Query parameters for application list endpoint
#[derive(Debug, Deserialize)]
struct ApplicationListQuery {
    status: Option<String>,
    #[serde(rename = "minDate")]
    min_date: Option<String>,
    #[serde(rename = "maxDate")]
    max_date: Option<String>,
    #[serde(rename = "minEndDate")]
    min_end_date: Option<String>,
    #[serde(rename = "maxEndDate")]
    max_end_date: Option<String>,
    limit: Option<usize>,
}

/// List all applications with optional filtering
async fn list_applications(
    State(provider): State<HistoryProvider>,
    Query(params): Query<ApplicationListQuery>,
) -> Result<Json<Vec<ApplicationInfo>>, StatusCode> {
    info!("GET /api/v1/applications - params: {:?}", params);

    // Parse status filter
    let _status_filter = params.status.as_ref().and_then(|s| {
        s.split(',')
            .filter_map(|status| match status.trim().to_uppercase().as_str() {
                "RUNNING" => Some(ApplicationStatus::Running),
                "COMPLETED" => Some(ApplicationStatus::Completed),
                _ => None,
            })
            .collect::<Vec<_>>()
            .into()
    });

    // Parse date filters
    let _min_date = parse_date_param(params.min_date.as_deref());
    let _max_date = parse_date_param(params.max_date.as_deref());
    let _min_end_date = parse_date_param(params.min_end_date.as_deref());
    let _max_end_date = parse_date_param(params.max_end_date.as_deref());

    match provider
        .get_applications(params.limit)
        .await
    {
        Ok(applications) => {
            info!("Returning {} applications", applications.len());
            let apps: Result<Vec<ApplicationInfo>, _> = applications
                .into_iter()
                .map(|v| serde_json::from_value(v))
                .collect();
            match apps {
                Ok(apps) => Ok(Json(apps)),
                Err(e) => {
                    error!("Failed to deserialize applications: {}", e);
                    Err(StatusCode::INTERNAL_SERVER_ERROR)
                }
            }
        }
        Err(e) => {
            tracing::error!("Failed to get applications: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get specific application by ID
async fn get_application(
    State(provider): State<HistoryProvider>,
    Path(app_id): Path<String>,
) -> Result<Json<ApplicationInfo>, StatusCode> {
    info!("GET /api/v1/applications/{}", app_id);

    match provider.get_application_summary(&app_id).await {
        Ok(Some(app_value)) => {
            match serde_json::from_value::<ApplicationInfo>(app_value) {
                Ok(app) => {
                    info!("Found application: {}", app_id);
                    Ok(Json(app))
                }
                Err(e) => {
                    error!("Failed to deserialize application {}: {}", app_id, e);
                    Err(StatusCode::INTERNAL_SERVER_ERROR)
                }
            }
        }
        Ok(None) => {
            info!("Application not found: {}", app_id);
            Err(StatusCode::NOT_FOUND)
        }
        Err(e) => {
            tracing::error!("Failed to get application {}: {}", app_id, e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get jobs for a specific application (placeholder)
async fn get_application_jobs(
    Path(app_id): Path<String>,
) -> Result<Json<Vec<serde_json::Value>>, StatusCode> {
    info!("GET /api/v1/applications/{}/jobs", app_id);
    // TODO: Implement job parsing from event logs
    Ok(Json(vec![]))
}

/// Get executors for a specific application
async fn get_application_executors(
    State(provider): State<HistoryProvider>,
    Path(app_id): Path<String>,
) -> Result<Json<Vec<crate::models::ExecutorSummary>>, StatusCode> {
    info!("GET /api/v1/applications/{}/executors", app_id);

    match provider.get_executors(&app_id).await {
        Ok(executor_values) => {
            let executors: Result<Vec<crate::models::ExecutorSummary>, _> = executor_values
                .into_iter()
                .map(|v| serde_json::from_value(v))
                .collect();
            match executors {
                Ok(executors) => {
                    info!(
                        "Found {} executors for application: {}",
                        executors.len(),
                        app_id
                    );
                    Ok(Json(executors))
                }
                Err(e) => {
                    error!("Failed to deserialize executors for app {}: {}", app_id, e);
                    Err(StatusCode::INTERNAL_SERVER_ERROR)
                }
            }
        }
        Err(e) => {
            tracing::error!("Failed to get executors for {}: {}", app_id, e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get stages for a specific application (placeholder)
async fn get_application_stages(
    Path(app_id): Path<String>,
) -> Result<Json<Vec<serde_json::Value>>, StatusCode> {
    info!("GET /api/v1/applications/{}/stages", app_id);
    // TODO: Implement stage parsing from event logs
    Ok(Json(vec![]))
}

/// Get storage info for a specific application (placeholder)
async fn get_application_storage(
    Path(app_id): Path<String>,
) -> Result<Json<Vec<serde_json::Value>>, StatusCode> {
    info!("GET /api/v1/applications/{}/storage/rdd", app_id);
    // TODO: Implement RDD storage parsing from event logs
    Ok(Json(vec![]))
}

/// Get environment for a specific application (placeholder)
async fn get_application_environment(
    Path(app_id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    info!("GET /api/v1/applications/{}/environment", app_id);
    // TODO: Implement environment parsing from event logs
    Ok(Json(serde_json::json!({})))
}

/// Get version information
async fn get_version() -> Json<VersionInfo> {
    Json(VersionInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Health check endpoint
async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "healthy",
        "timestamp": Utc::now().to_rfc3339()
    }))
}

/// Parse date parameter in various formats
fn parse_date_param(date_str: Option<&str>) -> Option<DateTime<Utc>> {
    date_str.and_then(|s| {
        // Try parsing as Unix timestamp first
        if let Ok(timestamp) = s.parse::<i64>() {
            return Utc
                .timestamp_opt(timestamp / 1000, ((timestamp % 1000) * 1_000_000) as u32)
                .single();
        }

        // Try parsing as RFC3339/ISO 8601
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Some(dt.with_timezone(&Utc));
        }

        // Try parsing as simple date format (YYYY-MM-DD)
        if let Ok(naive_date) =
            NaiveDateTime::parse_from_str(&format!("{}T00:00:00", s), "%Y-%m-%dT%H:%M:%S")
        {
            return Some(Utc.from_utc_datetime(&naive_date));
        }

        None
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_date_param() {
        // Test Unix timestamp
        let dt = parse_date_param(Some("1700486400000")).unwrap();
        assert_eq!(dt.timestamp_millis(), 1700486400000);

        // Test ISO 8601
        let dt = parse_date_param(Some("2023-11-20T12:00:00Z")).unwrap();
        assert_eq!(dt.timestamp_millis(), 1700481600000); // 2023-11-20T12:00:00Z

        // Test simple date (should be midnight UTC)
        let dt = parse_date_param(Some("2023-11-20")).unwrap();
        assert_eq!(dt.timestamp_millis(), 1700438400000); // 2023-11-20T00:00:00Z
    }
}
