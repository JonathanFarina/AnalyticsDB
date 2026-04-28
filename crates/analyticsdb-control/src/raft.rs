use crate::{CatalogColumn, CatalogRelation, CatalogRelationKind, ClusterNode, NodeStatus};
use serde::{Deserialize, Serialize};

pub type NodeId = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogEntry {
    RegisterNode(ClusterNode),
    UpdateNodeStatus {
        node_id: String,
        status: NodeStatus,
        last_heartbeat_at_epoch_ms: u128,
    },
    CreateDatabase {
        name: String,
    },
    CreateSchema {
        database: Option<String>,
        name: String,
    },
    RegisterRelation(CatalogRelation),
    DropRelation {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        kind: CatalogRelationKind,
    },
    AddColumn {
        database: Option<String>,
        schema: Option<String>,
        table_name: String,
        column: CatalogColumn,
    },
    RenameRelation {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        new_name: String,
    },
    UpdateRelationStoragePath {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        new_storage_path: String,
    },
    SetUserPassword {
        name: String,
        password: String,
        version: u64,
        rotated_at: Option<u128>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Response {
    pub message: String,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = LogEntry,
        R = Response,
        NodeId = NodeId,
        Node = ClusterNode,
        Entry = openraft::entry::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime
);

pub type AnalyticsRaft = openraft::Raft<TypeConfig>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    pub node_id: Option<String>,
    pub endpoint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinResponse {
    pub node_id: String,
    pub postgres_port: u16,
    pub flight_sql_port: u16,
    pub config: crate::ClusterConfig,
}
