//! AnalyticsDB Gateway Server
//!
//! Web admin console gateway that terminates sessions, handles authentication (including OIDC),
//! and proxies queries to the AnalyticsDB engine over PostgreSQL or Flight SQL protocols.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    routing::{delete, get, post},
    Router,
};
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

pub mod config;
pub mod error;
pub mod routes;
pub mod session;
pub mod proxy;

#[derive(Clone)]
pub struct GatewayState {
    pub config: Arc<config::GatewayConfig>,
    pub session_store: Arc<session::SessionStore>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Load configuration
    let config = config::GatewayConfig::from_env().unwrap_or_default();
    let config = Arc::new(config);

    tracing::info!("Starting AnalyticsDB Gateway on {}", config.bind_addr);
    tracing::info!("OIDC enabled: {}", config.oidc.is_enabled());

    // Initialize session store
    let session_store = Arc::new(session::SessionStore::new(config.session_timeout_seconds));

    // Create shared state
    let state = Arc::new(GatewayState {
        config: config.clone(),
        session_store,
    });

    // Configure CORS
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Build router
    let app = Router::new()
        // Health check
        .route("/healthz", get(routes::health::liveness))
        .route("/readyz", get(routes::health::readiness))
        // Auth routes
        .route("/api/auth/login", post(routes::auth::login))
        .route("/api/auth/logout", post(routes::auth::logout))
        .route("/api/auth/refresh", post(routes::auth::refresh))
        .route("/api/auth/oidc/authorize", get(routes::auth::oidc_authorize))
        .route("/api/auth/oidc/callback", get(routes::auth::oidc_callback))
        // Session routes
        .route("/api/session", get(routes::session::get_session))
        .route("/api/session", post(routes::session::update_session))
        // Explorer routes (live metadata)
        .route("/api/explorer", get(routes::explorer::get_explorer_snapshot))
        .route("/api/explorer/databases", get(routes::explorer::list_databases))
        .route("/api/explorer/schemas", get(routes::explorer::list_schemas))
        .route("/api/explorer/tables", get(routes::explorer::list_tables))
        .route("/api/explorer/views", get(routes::explorer::list_views))
        .route("/api/explorer/columns", get(routes::explorer::list_columns))
        // Query execution
        .route("/api/query", post(routes::query::execute_query))
        // Admin routes (placeholder)
        .route("/api/admin/databases", get(routes::admin::list_databases).post(routes::admin::create_database))
        .route("/api/admin/databases/:name", delete(routes::admin::drop_database))
        .route("/api/admin/users", get(routes::admin::list_users).post(routes::admin::create_user))
        .route("/api/admin/users/:name", delete(routes::admin::drop_user))
        // System metrics
        .route("/api/system/metrics", get(routes::system::get_metrics))
        .route("/api/system/query-log", get(routes::system::get_query_log))
        .route("/api/system/audit-log", get(routes::system::get_audit_log))
        // Add middleware
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state);

    // Start server
    let addr: SocketAddr = config.bind_addr.parse()?;
    tracing::info!("Gateway listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
