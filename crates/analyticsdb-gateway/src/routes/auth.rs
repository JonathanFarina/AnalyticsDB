//! Authentication module with OIDC support (simplified)

use std::sync::Arc;

use axum::{
    extract::{Extension, Query, State},
    response::{Json, Redirect},
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::GatewayResult;
use crate::session::SessionClaims;
use crate::GatewayState;

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub token: String,
    pub session: SessionClaims,
}

#[derive(Debug, Deserialize)]
pub struct OidcAuthRequest {
    pub provider: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct OidcCallbackQuery {
    pub code: String,
    pub state: String,
}

/// Local login with username/password (placeholder - accepts admin/admin)
pub async fn login(
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<LoginRequest>,
) -> GatewayResult<Json<LoginResponse>> {
    // Placeholder implementation - accepts admin/admin
    if req.username != "admin" || req.password != "admin" {
        return Err(anyhow::anyhow!("Invalid credentials").into());
    }

    // Create session
    let token = state
        .session_store
        .create_session("admin", "admin", "default", "public")?;

    let claims = state.session_store.validate_token(&token)?;

    Ok(Json(LoginResponse {
        token,
        session: claims,
    }))
}

/// Logout (client-side token disposal, could also add token blacklisting)
pub async fn logout() -> Json<serde_json::Value> {
    Json(json!({ "message": "Logged out successfully" }))
}

/// Refresh session token
pub async fn refresh(
    Extension(claims): Extension<SessionClaims>,
    State(state): State<Arc<GatewayState>>,
) -> GatewayResult<Json<LoginResponse>> {
    let token = state.session_store.create_session(
        &claims.sub,
        &claims.role,
        &claims.database,
        &claims.schema,
    )?;

    let new_claims = state.session_store.validate_token(&token)?;

    Ok(Json(LoginResponse {
        token,
        session: new_claims,
    }))
}

/// OIDC authorization redirect (simplified - returns error for now)
pub async fn oidc_authorize(
    State(_state): State<Arc<GatewayState>>,
    Query(_query): Query<OidcAuthRequest>,
) -> GatewayResult<Redirect> {
    // OIDC implementation simplified for now
    Err(anyhow::anyhow!("OIDC not fully implemented yet").into())
}

/// OIDC callback handler (simplified - returns error for now)
pub async fn oidc_callback(
    State(_state): State<Arc<GatewayState>>,
    Query(_query): Query<OidcCallbackQuery>,
) -> GatewayResult<Json<LoginResponse>> {
    // OIDC callback simplified for now
    Err(anyhow::anyhow!("OIDC callback not fully implemented yet").into())
}

/// Extract claims from request extension (set by auth middleware)
pub fn extract_claims(claims: Option<Extension<SessionClaims>>) -> GatewayResult<SessionClaims> {
    claims
        .map(|ext| ext.0)
        .ok_or_else(|| anyhow::anyhow!("No session claims found").into())
}
