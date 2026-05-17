//! Session routes

use axum::{extract::State, Extension, Json};
use serde_json::json;

use crate::error::GatewayResult;
use crate::session::SessionClaims;
use crate::GatewayState;

/// Get current session info
pub async fn get_session(
    Extension(claims): Extension<SessionClaims>,
    State(state): State<std::sync::Arc<crate::GatewayState>>,
) -> GatewayResult<Json<serde_json::Value>> {
    Ok(Json(json!({
        "username": claims.sub,
        "role": claims.role,
        "database": claims.database,
        "schema": claims.schema,
        "session_id": claims.session_id,
    })))
}

/// Update session (database, schema)
pub async fn update_session(
    Extension(claims): Extension<SessionClaims>,
    State(state): State<std::sync::Arc<crate::GatewayState>>,
    Json(req): Json<serde_json::Value>,
) -> GatewayResult<Json<serde_json::Value>> {
    // In production, this would update the session in the store
    // For now, just acknowledge the request
    Ok(Json(json!({
        "message": "Session update acknowledged",
        "requested": req,
    })))
}
