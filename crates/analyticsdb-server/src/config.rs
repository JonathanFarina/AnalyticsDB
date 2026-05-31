use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;

/// Centralized configuration for AnalyticsDB server.
/// This is the single source of truth for all configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Node role: control, compute, storage, gateway
    pub role: String,
    
    /// Node ID (auto-assigned if None)
    pub node_id: Option<String>,
    
    /// Address to bind PostgreSQL wire protocol
    pub postgres_addr: Option<String>,
    
    /// Address to bind Flight SQL protocol
    pub flight_sql_addr: Option<String>,
    
    /// Address to bind node-to-node communication
    pub node_addr: Option<String>,
    
    /// Address to bind admin HTTP server (health checks)
    pub admin_addr: Option<String>,
    
    /// Hostname/IP that peer nodes use to reach this node
    pub advertise_host: String,
    
    /// Path to catalog database
    pub catalog_path: String,
    
    /// Path to cluster config file
    pub cluster_config: Option<String>,
    
    /// Whether to initialize a new cluster
    pub init_cluster: bool,
    
    /// Coordinator endpoint to join
    pub join: Option<String>,
    
    /// Storage root URI (s3://, gs://, azure://, file://)
    pub storage_root: Option<String>,
    
    /// TLS certificate path
    #[serde(alias = "tls_cert_path")]
    pub tls_cert: Option<PathBuf>,
    
    /// TLS key path
    #[serde(alias = "tls_key_path")]
    pub tls_key: Option<PathBuf>,
    
    /// TLS CA certificate path
    #[serde(alias = "tls_ca_cert_path")]
    pub tls_ca_cert: Option<PathBuf>,
    
    /// TLS domain for verification
    pub tls_domain: Option<String>,
    
    /// Disable TLS verification (insecure)
    pub tls_insecure: bool,
    
    /// HS256 secret used to sign Flight SQL JWT bearer tokens
    pub jwt_secret: Option<String>,

    /// Base PostgreSQL port
    #[serde(alias = "base_postgres_port")]
    pub base_postgres_port: Option<u16>,

    /// Base Flight SQL port
    #[serde(alias = "base_flight_sql_port")]
    pub base_flight_sql_port: Option<u16>,

    /// Base Node port
    #[serde(alias = "base_node_port")]
    pub base_node_port: Option<u16>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            role: "control".to_string(),
            node_id: None,
            postgres_addr: Some("127.0.0.1:5432".to_string()),
            flight_sql_addr: Some("127.0.0.1:8815".to_string()),
            node_addr: Some("127.0.0.1:8816".to_string()),
            admin_addr: Some("127.0.0.1:9090".to_string()),
            advertise_host: "127.0.0.1".to_string(),
            catalog_path: "analyticsdb-catalog.db".to_string(),
            cluster_config: None,
            init_cluster: false,
            join: None,
            storage_root: None,
            tls_cert: None,
            tls_key: None,
            tls_ca_cert: None,
            tls_domain: None,
            tls_insecure: false,
            jwt_secret: None,
            base_postgres_port: None,
            base_flight_sql_port: None,
            base_node_port: None,
        }
    }
}

/// Default locations to look for the cluster config when `--cluster-config` is
/// not given, in priority order. Config lives in a `config/` directory by
/// convention; the repo-root path is kept as a fallback for older layouts.
pub const DEFAULT_CONFIG_PATHS: [&str; 2] = ["config/cluster-config.json", "cluster-config.json"];

/// Returns the first existing default config path, if any.
pub fn discover_config_path() -> Option<String> {
    DEFAULT_CONFIG_PATHS
        .iter()
        .find(|path| std::path::Path::new(path).exists())
        .map(|path| path.to_string())
}

impl Config {
    /// Load configuration from a file.
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;
        serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path))
    }
    
    /// Validate configuration.
    pub fn validate(&self) -> Result<()> {
        // Validate TLS config
        match (&self.tls_cert, &self.tls_key) {
            (Some(_), Some(_)) => anyhow::Ok(()),
            (None, None) => anyhow::Ok(()),
            _ => anyhow::bail!("Both --tls-cert and --tls-key must be provided to enable TLS"),
        }?;

        // Validate addresses
        if let Some(addr) = &self.postgres_addr {
            addr.parse::<SocketAddr>()
                .map_err(|e| anyhow::anyhow!("Invalid postgres address '{}': {}", addr, e))?;
        }
        if let Some(addr) = &self.flight_sql_addr {
            addr.parse::<SocketAddr>()
                .map_err(|e| anyhow::anyhow!("Invalid flight sql address '{}': {}", addr, e))?;
        }
        if let Some(addr) = &self.node_addr {
            addr.parse::<SocketAddr>()
                .map_err(|e| anyhow::anyhow!("Invalid node address '{}': {}", addr, e))?;
        }
        if let Some(addr) = &self.admin_addr {
            addr.parse::<SocketAddr>()
                .map_err(|e| anyhow::anyhow!("Invalid admin address '{}': {}", addr, e))?;
        }

        Ok(())
    }
}
