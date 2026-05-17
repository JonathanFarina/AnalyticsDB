//! System routes - metrics, query log, audit log

use axum::{
    extract::{Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::GatewayResult;
use crate::GatewayState;

#[derive(Debug, Deserialize)]
pub struct LogQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub query_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SystemMetrics {
    pub query_throughput_per_second: f64,
    pub avg_latency_ms: f64,
    pub error_rate_percent: f64,
    pub active_connections: usize,
    pub active_queries: usize,
}

/// Get system metrics
pub async fn get_metrics(
    State(state): State<std::sync::Arc<crate::GatewayState>>,
) -> GatewayResult<Json<SystemMetrics>> {
    // In production, this would read from the query log and system tables
    // For now, return placeholder metrics
    Ok(Json(SystemMetrics {
        query_throughput_per_second: 42.5,
        avg_latency_ms: 18.0,
        error_rate_percent: 0.5,
        active_connections: 12,
        active_queries: 3,
    }))
}

/// Get query log
pub async fn get_query_log(
    Query(query): Query<LogQuery>,
    State(state): State<std::sync::Arc<crate::GatewayState>>,
) -> GatewayResult<Json<Vec<serde_json::Value>>> {
    let limit = query.limit.unwrap_or(100);
    let offset = query.offset.unwrap_or(0);

    // In production, query system.query_log table
    // For now, return placeholder data
    let mut logs = Vec::new();
    for i in 0..limit.min(10) {
        logs.push(json!({
            "query_id": format!("qry-{}", i),
            "username": "admin",
            "database": "default",
            "schema": "public",
            "query_text": "SELECT 1",
            "start_time": "2026-01-01T00:00:00Z",
            "end_time": "2026-01-01T00:00:01Z",
            "total_ms": 1,
            "row_count": 1,
            "error_message": null,
        }));
    }

    Ok(Json(logs))
}

/// Get audit log
pub async fn get_audit_log(
    Query(query): Query<LogQuery>,
    State(state): State<std::sync::Arc<crate::GatewayState>>,
) -> GatewayResult<Json<Vec<serde_json::Value>>> {
    let limit = query.limit.unwrap_or(100);

    // In production, query system.audit_log table
    // For now, return placeholder data
    let mut logs = Vec::new();
    for i in 0..limit.min(10) {
        logs.push(json!({
            "event_id": format!("evt-{}", i),
            "event_type": "CREATE_TABLE",
            "username": "admin",
            "database": "default",
            "schema": "public",
            "object_name": format!("table_{}", i),
            "event_time": "2026-01-01T00:00:00Z",
            "details": null,
        }));
    }

    Ok(Json(logs))
}
