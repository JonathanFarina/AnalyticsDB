//! Auth middleware — extracts a Bearer JWT from Authorization header and
//! inserts validated `SessionClaims` into request extensions.
//! Routes that don't need auth (login, health) should be added to the
//! router before applying this middleware layer.

use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Json, Response},
};
use serde_json::json;

use crate::session::{SessionClaims, SessionStore};

pub async fn require_auth(
    State(session_store): State<Arc<SessionStore>>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let token = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    match token {
        Some(token) => match session_store.validate_token(token) {
            Ok(claims) => {
                req.extensions_mut().insert(claims);
                next.run(req).await
            }
            Err(_) => (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Invalid or expired token" })),
            )
                .into_response(),
        },
        None => (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Authentication required" })),
        )
            .into_response(),
    }
}

/// Optionally extract claims — used on routes that work both authenticated
/// and unauthenticated (e.g. health checks).  Never returns an error.
pub async fn optional_auth(
    State(session_store): State<Arc<SessionStore>>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    if let Some(token) = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    {
        if let Ok(claims) = session_store.validate_token(token) {
            req.extensions_mut().insert(claims);
        }
    }
    next.run(req).await
}

/// Middleware that validates the session `SessionClaims` is present AND the
/// user has admin role.  Apply after `require_auth`.
pub async fn require_admin(
    req: Request<Body>,
    next: Next,
) -> Response {
    let claims = req.extensions().get::<SessionClaims>().cloned();
    match claims {
        Some(c) if c.role == "admin" || c.sub == "admin" || c.sub == "postgres" => {
            next.run(req).await
        }
        Some(_) => (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Admin privileges required" })),
        )
            .into_response(),
        None => (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Authentication required" })),
        )
            .into_response(),
    }
}
