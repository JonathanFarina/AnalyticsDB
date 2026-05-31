//! Session management for the gateway

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::GatewayConfig;
use crate::error::GatewayResult;

/// Session information stored in JWT claims
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionClaims {
    pub sub: String,         // username
    pub role: String,         // user role
    pub database: String,     // current database
    pub schema: String,       // current schema
    pub exp: usize,           // expiration timestamp
    pub iat: usize,          // issued at timestamp
    pub session_id: String,   // unique session ID
}

/// Session store for managing active sessions.
///
/// In addition to issuing/validating JWTs, it caches each session's password in
/// memory so the gateway can open authenticated pg-wire connections to the
/// server on behalf of the user. Passwords are never written to the JWT or to
/// disk; if the gateway restarts, the cache is empty and the user must sign in
/// again (their token is rejected with 401, prompting re-login).
pub struct SessionStore {
    config: Arc<GatewayConfig>,
    credentials: Mutex<HashMap<String, String>>,
}

impl SessionStore {
    pub fn new(_session_timeout_seconds: u64) -> Self {
        Self {
            config: Arc::new(crate::config::GatewayConfig::default()),
            credentials: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_config(config: Arc<GatewayConfig>) -> Self {
        Self {
            config,
            credentials: Mutex::new(HashMap::new()),
        }
    }

    /// Create a new session, cache the password for pg-wire proxying, and return
    /// a JWT token.
    pub fn create_session_with_password(
        &self,
        username: &str,
        role: &str,
        database: &str,
        schema: &str,
        password: &str,
    ) -> GatewayResult<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize;

        let session_id = Uuid::new_v4().to_string();
        let exp = now + self.config.session_timeout_seconds as usize;

        let claims = SessionClaims {
            sub: username.to_string(),
            role: role.to_string(),
            database: database.to_string(),
            schema: schema.to_string(),
            exp,
            iat: now,
            session_id: session_id.clone(),
        };

        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.config.jwt_secret.as_bytes()),
        )?;

        if let Ok(mut creds) = self.credentials.lock() {
            creds.insert(session_id, password.to_string());
        }

        Ok(token)
    }

    /// Returns the cached password for a session, if present.
    pub fn password_for(&self, session_id: &str) -> Option<String> {
        self.credentials
            .lock()
            .ok()
            .and_then(|creds| creds.get(session_id).cloned())
    }

    /// Forgets a session's cached password (called on logout).
    pub fn forget(&self, session_id: &str) {
        if let Ok(mut creds) = self.credentials.lock() {
            creds.remove(session_id);
        }
    }

    /// Create a new session and return a JWT token (no cached password).
    pub fn create_session(
        &self,
        username: &str,
        role: &str,
        database: &str,
        schema: &str,
    ) -> GatewayResult<String> {
        self.create_session_with_password(username, role, database, schema, "")
    }

    /// Validate and decode a JWT token
    pub fn validate_token(&self, token: &str) -> GatewayResult<SessionClaims> {
        let validation = Validation::default();
        let token_data = decode::<SessionClaims>(
            token,
            &DecodingKey::from_secret(self.config.jwt_secret.as_bytes()),
            &validation,
        )?;

        Ok(token_data.claims)
    }

    /// Refresh a session (create new token with extended expiry)
    pub fn refresh_session(&self, token: &str) -> GatewayResult<String> {
        let claims = self.validate_token(token)?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize;

        let exp = now + self.config.session_timeout_seconds as usize;

        let new_claims = SessionClaims {
            exp,
            iat: now,
            ..claims
        };

        let new_token = encode(
            &Header::default(),
            &new_claims,
            &EncodingKey::from_secret(self.config.jwt_secret.as_bytes()),
        )?;

        Ok(new_token)
    }
}

/// Session information returned to the client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub username: String,
    pub role: String,
    pub database: String,
    pub schema: String,
    pub expires_at: DateTime<Utc>,
}

impl From<SessionClaims> for SessionInfo {
    fn from(claims: SessionClaims) -> Self {
        let exp_datetime = UNIX_EPOCH + Duration::from_secs(claims.exp as u64);
        Self {
            username: claims.sub,
            role: claims.role,
            database: claims.database,
            schema: claims.schema,
            expires_at: DateTime::<Utc>::from(exp_datetime),
        }
    }
}

/// Create a dummy session context for API calls
pub fn create_session_context(user: &str, database: &str, schema: &str) -> SessionContext {
    // This is a placeholder - in production, this would create a proper session context
    // For now, return a default session context
    SessionContext {
        user: user.to_string(),
        database: database.to_string(),
        schema: schema.to_string(),
    }
}

/// Simple session context for API calls
#[derive(Debug, Clone)]
pub struct SessionContext {
    pub user: String,
    pub database: String,
    pub schema: String,
}
