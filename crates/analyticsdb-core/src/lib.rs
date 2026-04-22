use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Protocol {
    Embedded,
    PostgreSql,
    ArrowFlightSql,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionContext {
    pub user: String,
    pub database: String,
    pub schema: String,
    pub protocol: Protocol,
}

impl Default for SessionContext {
    fn default() -> Self {
        Self {
            user: "postgres".to_string(),
            database: "postgres".to_string(),
            schema: "public".to_string(),
            protocol: Protocol::Embedded,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryRequest {
    pub sql: String,
    pub session: SessionContext,
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
