use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Protocol {
    Embedded,
    PostgreSql,
    ArrowFlightSql,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransactionStatus {
    Idle,
    InTransaction,
    Aborted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionContext {
    pub user: String,
    pub role: String,
    pub database: String,
    pub schema: String,
    pub auth_method: String,
    pub protocol: Protocol,
    #[serde(default = "default_transaction_status")]
    pub transaction_status: TransactionStatus,
}

fn default_transaction_status() -> TransactionStatus {
    TransactionStatus::Idle
}

impl Default for SessionContext {
    fn default() -> Self {
        Self {
            user: "postgres".to_string(),
            role: "postgres".to_string(),
            database: "postgres".to_string(),
            schema: "public".to_string(),
            auth_method: "embedded-prototype".to_string(),
            protocol: Protocol::Embedded,
            transaction_status: TransactionStatus::Idle,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryRequest {
    pub sql: String,
    pub session: SessionContext,
    #[serde(default)]
    pub query_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryResponse {
    pub query_id: String,
    pub coordinator_node_id: String,
    pub session: SessionContext,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub message: String,
    pub execution_time_ms: u128,
}

impl QueryResponse {
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StatementOutcome {
    Rows,
    Command { tag: String, rows_affected: u64 },
}
