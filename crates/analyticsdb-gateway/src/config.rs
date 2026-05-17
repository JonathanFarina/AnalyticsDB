//! Gateway configuration

use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    /// Bind address for the gateway server
    pub bind_addr: String,

    /// Control plane config path
    pub control_plane_config_path: String,

    /// Session timeout in seconds (default: 3600 = 1 hour)
    pub session_timeout_seconds: u64,

    /// JWT secret for session tokens
    pub jwt_secret: String,

    /// OIDC configuration
    pub oidc: OidcConfig,

    /// AnalyticsDB server endpoint (for proxying queries)
    pub analyticsdb_pg_endpoint: Option<String>,
    pub analyticsdb_flight_endpoint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OidcConfig {
    /// Enable OIDC authentication
    pub enabled: bool,

    /// OIDC provider issuer URL
    pub issuer_url: Option<String>,

    /// OAuth2 client ID
    pub client_id: Option<String>,

    /// OAuth2 client secret
    pub client_secret: Option<String>,

    /// OAuth2 redirect URL
    pub redirect_url: Option<String>,

    /// Allowed OIDC scopes
    pub scopes: Vec<String>,

    /// Supported providers
    pub providers: Vec<OidcProvider>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OidcProvider {
    pub name: String,
    pub display_name: String,
    pub issuer_url: String,
    pub client_id: String,
    pub client_secret: String,
    pub scopes: Vec<String>,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:8080".to_string(),
            control_plane_config_path: "cluster-config.json".to_string(),
            session_timeout_seconds: 3600,
            jwt_secret: "change-me-in-production".to_string(),
            oidc: OidcConfig::default(),
            analyticsdb_pg_endpoint: Some("127.0.0.1:5432".to_string()),
            analyticsdb_flight_endpoint: Some("127.0.0.1:8081".to_string()),
        }
    }
}

impl Default for OidcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            issuer_url: None,
            client_id: None,
            client_secret: None,
            redirect_url: None,
            scopes: vec![
                "openid".to_string(),
                "profile".to_string(),
                "email".to_string(),
            ],
            providers: vec![],
        }
    }
}

impl OidcConfig {
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl GatewayConfig {
    pub fn from_env() -> Result<Self, anyhow::Error> {
        let mut config = Self::default();

        if let Ok(addr) = env::var("ANALYTICSDB_GATEWAY_BIND_ADDR") {
            config.bind_addr = addr;
        }

        if let Ok(path) = env::var("ANALYTICSDB_CONTROL_PLANE_CONFIG") {
            config.control_plane_config_path = path;
        }

        if let Ok(timeout) = env::var("ANALYTICSDB_SESSION_TIMEOUT_SECONDS") {
            config.session_timeout_seconds = timeout.parse().unwrap_or(3600);
        }

        if let Ok(secret) = env::var("ANALYTICSDB_JWT_SECRET") {
            config.jwt_secret = secret;
        }

        // OIDC configuration
        config.oidc.enabled = env::var("ANALYTICSDB_OIDC_ENABLED").is_ok();

        if let Ok(url) = env::var("ANALYTICSDB_OIDC_ISSUER_URL") {
            config.oidc.issuer_url = Some(url);
        }

        if let Ok(id) = env::var("ANALYTICSDB_OIDC_CLIENT_ID") {
            config.oidc.client_id = Some(id);
        }

        if let Ok(secret) = env::var("ANALYTICSDB_OIDC_CLIENT_SECRET") {
            config.oidc.client_secret = Some(secret);
        }

        if let Ok(url) = env::var("ANALYTICSDB_OIDC_REDIRECT_URL") {
            config.oidc.redirect_url = Some(url);
        }

        if let Ok(endpoint) = env::var("ANALYTICSDB_PG_ENDPOINT") {
            config.analyticsdb_pg_endpoint = Some(endpoint);
        }

        if let Ok(endpoint) = env::var("ANALYTICSDB_FLIGHT_ENDPOINT") {
            config.analyticsdb_flight_endpoint = Some(endpoint);
        }

        Ok(config)
    }
}
