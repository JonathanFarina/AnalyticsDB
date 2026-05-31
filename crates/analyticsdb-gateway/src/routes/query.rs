//! Query execution route — proxies SQL to the server's pg-wire endpoint so the
//! server's engine is the single source of truth.

use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Extension, State},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::error::{GatewayError, GatewayResult};
use crate::session::SessionClaims;
use crate::GatewayState;

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
    pub protocol: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryTimings {
    pub queue_ms: u64,
    pub plan_ms: u64,
    pub execute_ms: u64,
    pub fetch_ms: u64,
    pub total_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct QueryMessage {
    pub level: String,
    pub text: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResult {
    pub query_id: String,
    pub statement_type: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub affected_rows: Option<u64>,
    pub timings: QueryTimings,
    pub messages: Vec<QueryMessage>,
}

/// Execute a SQL statement on behalf of the authenticated session by proxying it
/// to the server over pg-wire, authenticated as the session's user.
pub async fn execute_query(
    Extension(claims): Extension<SessionClaims>,
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<QueryRequest>,
) -> GatewayResult<Json<QueryResult>> {
    let sql = req.sql.trim();
    if sql.is_empty() {
        return Err(GatewayError::BadRequest("SQL statement is empty".to_string()));
    }

    let password = state
        .session_store
        .password_for(&claims.session_id)
        // No cached credential (e.g. gateway restarted) — force re-login.
        .ok_or(GatewayError::Unauthorized)?;

    let database = req.database.unwrap_or_else(|| claims.database.clone());
    let query_id = format!("gw-{}", uuid::Uuid::new_v4());
    let started = Instant::now();

    let outcome = crate::proxy::execute_sql(
        &state.pg_endpoint(),
        &claims.sub,
        &password,
        &database,
        &req.sql,
    )
    .await;
    let total_ms = started.elapsed().as_millis() as u64;

    match outcome {
        Ok(result) => {
            let has_columns = !result.columns.is_empty();
            let row_count = result.rows.len();
            let raw_affected = result.affected_rows;
            let rows = result
                .rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|cell| match cell {
                            Some(text) => serde_json::Value::String(text),
                            None => serde_json::Value::Null,
                        })
                        .collect()
                })
                .collect();
            let statement_type = classify_sql(&req.sql, has_columns);
            // Only surface an affected-row count for non-SELECT statements.
            let affected_rows = if has_columns { None } else { raw_affected };
            let messages = vec![QueryMessage {
                level: "info".to_string(),
                text: result_message(&statement_type, raw_affected, row_count),
            }];

            Ok(Json(QueryResult {
                query_id,
                statement_type,
                columns: result.columns,
                rows,
                affected_rows,
                timings: timings(total_ms),
                messages,
            }))
        }
        // Surface execution errors inline so the console renders them in the
        // results panel rather than as an HTTP failure.
        Err(error) => Ok(Json(QueryResult {
            query_id,
            statement_type: "unknown".to_string(),
            columns: vec![],
            rows: vec![],
            affected_rows: None,
            timings: timings(total_ms),
            messages: vec![QueryMessage {
                level: "error".to_string(),
                text: clean_error(&error.to_string()),
            }],
        })),
    }
}

fn timings(total_ms: u64) -> QueryTimings {
    QueryTimings {
        queue_ms: 0,
        plan_ms: 0,
        execute_ms: total_ms,
        fetch_ms: 0,
        total_ms,
    }
}

/// Classify a statement from its leading keyword (and whether it returned rows).
fn classify_sql(sql: &str, has_columns: bool) -> String {
    let verb = sql
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()
        .unwrap_or("")
        .to_uppercase();
    match verb.as_str() {
        "SELECT" | "WITH" | "SHOW" | "TABLE" | "VALUES" => "select".to_string(),
        "INSERT" | "UPDATE" | "DELETE" | "COPY" | "MERGE" => "dml".to_string(),
        "CREATE" | "DROP" | "ALTER" | "TRUNCATE" | "REINDEX" | "GRANT" | "REVOKE" => {
            "ddl".to_string()
        }
        "EXPLAIN" => "explain".to_string(),
        _ if has_columns => "select".to_string(),
        _ => "metadata".to_string(),
    }
}

fn result_message(statement_type: &str, affected: Option<u64>, row_count: usize) -> String {
    match statement_type {
        "select" | "explain" => format!("{row_count} row(s) returned."),
        "dml" => format!("{} row(s) affected.", affected.unwrap_or(0)),
        _ => "Statement executed successfully.".to_string(),
    }
}

/// Strips the tokio-postgres `db error: ERROR:` prefix for a cleaner message.
fn clean_error(message: &str) -> String {
    message
        .trim_start_matches("db error: ")
        .trim_start_matches("ERROR: ")
        .to_string()
}
