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
pub mod middleware;
pub mod routes;
pub mod session;

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

    // Protected routes — all require a valid Bearer JWT.
    let protected = Router::new()
        // Session
        .route("/api/session", get(routes::session::get_session))
        .route("/api/session", post(routes::session::update_session))
        // Auth refresh (needs valid token)
        .route("/api/auth/refresh", post(routes::auth::refresh))
        // Explorer (live metadata)
        .route("/api/explorer", get(routes::explorer::get_explorer_snapshot))
        .route("/api/explorer/databases", get(routes::explorer::list_databases))
        .route("/api/explorer/schemas", get(routes::explorer::list_schemas))
        .route("/api/explorer/tables", get(routes::explorer::list_tables))
        .route("/api/explorer/views", get(routes::explorer::list_views))
        .route("/api/explorer/columns", get(routes::explorer::list_columns))
        // Query execution
        .route("/api/query", post(routes::query::execute_query))
        // Admin
        .route("/api/admin/databases", get(routes::admin::list_databases).post(routes::admin::create_database))
        .route("/api/admin/databases/:name", delete(routes::admin::drop_database))
        .route("/api/admin/users", get(routes::admin::list_users).post(routes::admin::create_user))
        .route("/api/admin/users/:name", delete(routes::admin::drop_user))
        // System
        .route("/api/system/metrics", get(routes::system::get_metrics))
        .route("/api/system/query-log", get(routes::system::get_query_log))
        .route("/api/system/audit-log", get(routes::system::get_audit_log))
        .route_layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state.session_store),
            middleware::require_auth,
        ))
        .with_state(Arc::clone(&state));

    // Public routes — no auth required.
    let public = Router::new()
        .route("/healthz", get(routes::health::liveness))
        .route("/readyz", get(routes::health::readiness))
        .route("/api/auth/login", post(routes::auth::login))
        .route("/api/auth/logout", post(routes::auth::logout))
        .route("/api/auth/oidc/authorize", get(routes::auth::oidc_authorize))
        .route("/api/auth/oidc/callback", get(routes::auth::oidc_callback))
        .with_state(Arc::clone(&state));

    let app = public
        .merge(protected)
        .layer(TraceLayer::new_for_http())
        .layer(cors);

    // Start server
    let addr: SocketAddr = config.bind_addr.parse()?;
    tracing::info!("Gateway listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
