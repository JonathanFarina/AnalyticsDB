//! Gateway error types

use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("Authentication required")]
    Unauthorized,

    #[error("Insufficient permissions")]
    Forbidden,

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Internal server error: {0}")]
    Internal(#[from] anyhow::Error),

    #[error("JWT error: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),

    #[error("OIDC error: {0}")]
    Oidc(String),
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> axum::response::Response {
        let (status, error_message) = match &self {
            GatewayError::Unauthorized => (StatusCode::UNAUTHORIZED, self.to_string()),
            GatewayError::Forbidden => (StatusCode::FORBIDDEN, self.to_string()),
            GatewayError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            GatewayError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            GatewayError::Internal(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".to_string(),
            ),
            GatewayError::Jwt(_) => (
                StatusCode::UNAUTHORIZED,
                "Invalid or expired token".to_string(),
            ),
            GatewayError::Oidc(msg) => (StatusCode::BAD_GATEWAY, msg.clone()),
        };

        let body = Json(json!({
            "error": error_message,
            "code": status.as_u16()
        }));

        (status, body).into_response()
    }
}

pub type GatewayResult<T> = Result<T, GatewayError>;
