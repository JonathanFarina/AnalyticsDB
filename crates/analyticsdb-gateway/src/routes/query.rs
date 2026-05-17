//! Query execution routes

use axum::{
    extract::{Extension, State},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::error::GatewayResult;
use crate::session::SessionClaims;

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
    pub protocol: Option<String>, // "pg" or "flight"
}

#[derive(Debug, Serialize)]
pub struct QueryResult {
    pub query_id: String,
    pub statement_type: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub affected_rows: Option<usize>,
    pub timings: QueryTimings,
    pub messages: Vec<QueryMessage>,
}

#[derive(Debug, Serialize)]
pub struct QueryTimings {
    pub queue_ms: u64,
    pub plan_ms: u64,
    pub execute_ms: u64,
    pub fetch_ms: u64,
    pub total_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct QueryMessage {
    pub level: String, // "info", "warning", "error"
    pub text: String,
}

/// Execute a SQL query through the gateway
pub async fn execute_query(
    Extension(_claims): Extension<SessionClaims>,
    State(_state): State<std::sync::Arc<crate::GatewayState>>,
    Json(_req): Json<QueryRequest>,
) -> GatewayResult<Json<QueryResult>> {
    let query_id = format!("gw-{}", uuid::Uuid::new_v4());

    // In production, this would:
    // 1. Connect to AnalyticsDB via PG or Flight SQL protocol
    // 2. Execute the query with the user's session context
    // 3. Return the results

    // For now, return a placeholder response
    Ok(Json(QueryResult {
        query_id,
        statement_type: "select".to_string(),
        columns: vec!["result".to_string()],
        rows: vec![vec![serde_json::json!(1)]],
        affected_rows: None,
        timings: QueryTimings {
            queue_ms: 1,
            plan_ms: 5,
            execute_ms: 10,
            fetch_ms: 2,
            total_ms: 18,
        },
        messages: vec![QueryMessage {
            level: "info".to_string(),
            text: "Query executed via gateway (prototype)".to_string(),
        }],
    }))
}
