//! Health check routes

use axum::Json;
use serde_json::json;

/// Liveness probe - always returns 200 if the server is running
pub async fn liveness() -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok",
        "service": "analyticsdb-gateway",
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}

/// Readiness probe - checks if dependencies are available
pub async fn readiness(
    State(_state): State<std::sync::Arc<crate::GatewayState>>,
) -> Json<serde_json::Value> {
    // For now, always return ready
    // In production, check dependencies like control plane, etc.
    Json(json!({
        "status": "ready",
        "service": "analyticsdb-gateway",
        "checks": {
            "gateway": "ok"
        },
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}
