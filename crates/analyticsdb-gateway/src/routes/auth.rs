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

const DEFAULT_DATABASE: &str = "postgres";
const DEFAULT_SCHEMA: &str = "public";

/// Local login with username/password.
///
/// Credentials are validated by opening an authenticated pg-wire connection to
/// the running server (the single source of truth). On success the password is
/// cached in the session store so subsequent SQL is executed as this user, and
/// the session role is set to `admin` when the account is an effective
/// administrator (a member of the `Administrators` group).
pub async fn login(
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<LoginRequest>,
) -> GatewayResult<Json<LoginResponse>> {
    use crate::error::GatewayError;

    // Authenticate against the server over pg-wire.
    crate::proxy::execute_sql(
        &state.pg_endpoint(),
        &req.username,
        &req.password,
        DEFAULT_DATABASE,
        "SELECT 1",
    )
    .await
    .map_err(|_| GatewayError::Unauthorized)?;

    // Determine admin status from the shared catalog (group-derived).
    let role = match state.open_catalog().await {
        Ok(catalog) if catalog.is_admin(&req.username).await => "admin",
        _ => "user",
    };

    let token = state.session_store.create_session_with_password(
        &req.username,
        role,
        DEFAULT_DATABASE,
        DEFAULT_SCHEMA,
        &req.password,
    )?;

    let claims = state.session_store.validate_token(&token)?;

    Ok(Json(LoginResponse {
        token,
        session: claims,
    }))
}

/// Logout — forget the cached pg-wire password for this session.
pub async fn logout(
    Extension(claims): Extension<SessionClaims>,
    State(state): State<Arc<GatewayState>>,
) -> Json<serde_json::Value> {
    state.session_store.forget(&claims.session_id);
    Json(json!({ "message": "Logged out successfully" }))
}

/// Refresh session token, carrying the cached pg-wire password forward.
pub async fn refresh(
    Extension(claims): Extension<SessionClaims>,
    State(state): State<Arc<GatewayState>>,
) -> GatewayResult<Json<LoginResponse>> {
    let password = state
        .session_store
        .password_for(&claims.session_id)
        .unwrap_or_default();

    let token = state.session_store.create_session_with_password(
        &claims.sub,
        &claims.role,
        &claims.database,
        &claims.schema,
        &password,
    )?;
    state.session_store.forget(&claims.session_id);

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
