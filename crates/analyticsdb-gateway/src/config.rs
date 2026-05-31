//! Gateway configuration

use std::env;
use serde::{Deserialize, Serialize};

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

    /// Path to the catalog store (SQLite). Used by the admin API to read and
    /// mutate users/groups directly. When `None`, it is resolved from the
    /// control-plane config file (`catalog_path`) with sensible fallbacks.
    pub catalog_path: Option<String>,
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
            control_plane_config_path: discover_config_path(),
            session_timeout_seconds: 3600,
            jwt_secret: "change-me-in-production".to_string(),
            oidc: OidcConfig::default(),
            analyticsdb_pg_endpoint: None,
            analyticsdb_flight_endpoint: Some("127.0.0.1:8081".to_string()),
            catalog_path: None,
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
            scopes: vec!["openid".to_string(), "profile".to_string(), "email".to_string()],
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

        if let Ok(path) = env::var("ANALYTICSDB_CATALOG_PATH") {
            config.catalog_path = Some(path);
        }

        Ok(config)
    }

    /// The server's default catalog file (its `--catalog-path` default).
    pub const DEFAULT_CATALOG_PATH: &'static str = "analyticsdb-catalog.db";
    /// The server's default pg-wire endpoint.
    pub const DEFAULT_PG_ENDPOINT: &'static str = "127.0.0.1:5432";

    /// Reads the server's config file (e.g. `config/cluster-config.json`) so the
    /// gateway can pick up the catalog path and pg-wire endpoint the server is
    /// using. Returns `None` if the file is missing or unparseable.
    fn server_config(&self) -> Option<serde_json::Value> {
        let raw = std::fs::read_to_string(&self.control_plane_config_path).ok()?;
        serde_json::from_str(&raw).ok()
    }

    /// Resolves the catalog store path the gateway reads metadata from.
    ///
    /// Order: `ANALYTICSDB_CATALOG_PATH` env → `catalog_path` from the server's
    /// config file → the server's default catalog file.
    pub fn resolve_catalog_path(&self) -> String {
        if let Some(path) = &self.catalog_path {
            return path.clone();
        }
        if let Some(path) = self
            .server_config()
            .as_ref()
            .and_then(|c| c.get("catalog_path"))
            .and_then(|v| v.as_str())
        {
            return path.to_string();
        }
        Self::DEFAULT_CATALOG_PATH.to_string()
    }

    /// Resolves the server pg-wire endpoint SQL is proxied to.
    ///
    /// Order: `ANALYTICSDB_PG_ENDPOINT` env → `postgres_addr` from the server's
    /// config file → `host:base_postgres_port` derived from the config file →
    /// the server's default endpoint.
    pub fn resolve_pg_endpoint(&self) -> String {
        if let Some(endpoint) = &self.analyticsdb_pg_endpoint {
            return endpoint.clone();
        }
        if let Some(config) = self.server_config() {
            if let Some(addr) = config.get("postgres_addr").and_then(|v| v.as_str()) {
                return addr.to_string();
            }
            if let Some(port) = config.get("base_postgres_port").and_then(|v| v.as_u64()) {
                let host = config
                    .get("advertise_host")
                    .and_then(|v| v.as_str())
                    .unwrap_or("127.0.0.1");
                return format!("{host}:{port}");
            }
        }
        Self::DEFAULT_PG_ENDPOINT.to_string()
    }
}

/// Default locations to look for the server config, in priority order. Config
/// lives in a `config/` directory by convention.
pub const DEFAULT_CONFIG_PATHS: [&str; 2] = ["config/cluster-config.json", "cluster-config.json"];

/// Returns the first existing default config path, or the preferred default
/// (`config/cluster-config.json`) when none exist yet.
pub fn discover_config_path() -> String {
    DEFAULT_CONFIG_PATHS
        .iter()
        .find(|path| std::path::Path::new(path).exists())
        .unwrap_or(&DEFAULT_CONFIG_PATHS[0])
        .to_string()
}
