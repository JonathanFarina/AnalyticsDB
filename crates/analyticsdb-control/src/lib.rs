use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use analyticsdb_core::SessionContext;
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio::fs;
use tokio::sync::RwLock;
use uuid::Uuid;

pub mod raft;
pub mod raft_store;

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq, clap::ValueEnum)]
pub enum NodeRole {
    #[default]
    Control,
    Compute,
    Storage,
    Gateway,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq, clap::ValueEnum)]
pub enum NodeStatus {
    #[default]
    Ready,
    Unavailable,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterNode {
    pub id: String,
    pub role: NodeRole,
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub internal_endpoint: Option<String>,
    pub status: NodeStatus,
    #[serde(default)]
    pub last_heartbeat_at_epoch_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogDatabase {
    pub name: String,
    pub schemas: BTreeSet<String>,
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogAggregate {
    pub database: String,
    pub schema: String,
    pub name: String,
    pub owner: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogCollation {
    pub database: String,
    pub schema: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogConversion {
    pub database: String,
    pub schema: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogFunction {
    pub database: String,
    pub schema: String,
    pub name: String,
    pub owner: String,
    pub definition_sql: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogUser {
    pub name: String,
    pub is_admin: bool,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default = "default_password_version")]
    pub password_version: u64,
    #[serde(default)]
    pub password_rotated_at_epoch_ms: Option<u128>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CatalogRelationKind {
    Table,
    View,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExternalStorageFormat {
    Parquet,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    #[serde(default)]
    pub default_value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CatalogTableConstraintKind {
    PrimaryKey,
    ForeignKey,
    Unique,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogTableConstraint {
    pub name: String,
    pub kind: CatalogTableConstraintKind,
    pub columns: Vec<String>,
    pub referenced_database: Option<String>,
    pub referenced_schema: Option<String>,
    pub referenced_table: Option<String>,
    pub referenced_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogIndex {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub is_primary: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogRelation {
    pub database: String,
    pub schema: String,
    pub name: String,
    pub kind: CatalogRelationKind,
    pub definition_sql: Option<String>,
    pub storage_path: Option<String>,
    /// If set, this relation points to external storage rather than the
    /// managed columnar JSON snapshot format.
    #[serde(default)]
    pub external_format: Option<ExternalStorageFormat>,
    pub columns: Vec<CatalogColumn>,
    #[serde(default)]
    pub constraints: Vec<CatalogTableConstraint>,
    #[serde(default)]
    pub indexes: Vec<CatalogIndex>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableColumnDefinition {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableConstraintDefinition {
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        referenced_database: Option<String>,
        referenced_schema: Option<String>,
        referenced_table: String,
        referenced_columns: Vec<String>,
    },
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterSnapshot {
    pub coordinator_node_id: String,
    pub nodes: Vec<ClusterNode>,
    pub databases: Vec<CatalogDatabase>,
    pub users: Vec<CatalogUser>,
    pub relations: Vec<CatalogRelation>,
    pub functions: Vec<CatalogFunction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryAdmission {
    pub query_id: String,
    pub coordinator_node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterConfig {
    pub base_postgres_port: u16,
    pub base_flight_sql_port: u16,
    #[serde(default = "default_base_node_port")]
    pub base_node_port: u16,
    pub catalog_path: String,
    #[serde(alias = "tls_cert")]
    pub tls_cert_path: Option<String>,
    #[serde(alias = "tls_key")]
    pub tls_key_path: Option<String>,
    pub next_available_port_offset: u16,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct CatalogState {
    databases: BTreeMap<String, CatalogDatabase>,
    users: BTreeMap<String, CatalogUser>,
    nodes: BTreeMap<String, ClusterNode>,
    relations: BTreeMap<String, CatalogRelation>,
    #[serde(default)]
    aggregates: BTreeMap<String, CatalogAggregate>,
    #[serde(default)]
    collations: BTreeMap<String, CatalogCollation>,
    #[serde(default)]
    conversions: BTreeMap<String, CatalogConversion>,
    #[serde(default)]
    functions: BTreeMap<String, CatalogFunction>,
    #[serde(default)]
    config: Option<ClusterConfig>,
}

#[derive(Debug, Clone)]
pub enum AlterTableOperation {
    AddColumn {
        column: TableColumnDefinition,
    },
    RenameTable {
        new_name: String,
    },
    AddConstraint {
        constraint: TableConstraintDefinition,
    },
    DropColumn {
        column_name: String,
        if_exists: bool,
        cascade: bool,
    },
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    DropConstraint {
        name: String,
        if_exists: bool,
        cascade: bool,
    },
    AlterColumn {
        column_name: String,
        operation: AlterColumnOperation,
    },
}

#[derive(Debug, Clone)]
pub enum AlterColumnOperation {
    SetDataType { data_type: String },
    SetNotNull,
    DropNotNull,
    SetDefault { value: String },
    DropDefault,
}

#[derive(Debug, Clone)]
pub enum AlterDatabaseOperation {
    Rename { new_name: String },
    OwnerTo { new_owner: String },
    SetParam { name: String, value: String },
}

#[derive(Debug, Clone)]
pub enum AlterObjectOperation {
    Rename { new_name: String },
    OwnerTo { new_owner: String },
    SetSchema { new_schema: String },
}

#[derive(Debug, Clone)]
pub enum ReindexTarget {
    Index {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        concurrently: bool,
    },
    Table {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        concurrently: bool,
    },
}

#[derive(Debug, Clone)]
pub enum MetadataStatement {
    CreateDatabase {
        name: String,
    },
    CreateAggregate {
        database: Option<String>,
        schema: Option<String>,
        name: String,
    },
    CreateCollation {
        database: Option<String>,
        schema: Option<String>,
        name: String,
    },
    CreateConversion {
        database: Option<String>,
        schema: Option<String>,
        name: String,
    },
    CreateSchema {
        database: Option<String>,
        name: String,
    },
    CreateView {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        definition_sql: String,
    },
    CreateTableAs {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        query_sql: String,
    },
    SelectInto {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        query_sql: String,
    },
    CreateTable {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        columns: Vec<TableColumnDefinition>,
        constraints: Vec<TableConstraintDefinition>,
    },
    CreateExternalTable {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        format: ExternalStorageFormat,
        location: String,
    },
    InsertInto {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        columns: Option<Vec<String>>,
        rows: Vec<Vec<String>>,
    },
    Delete {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        selection_sql: Option<String>,
    },
    Truncate {
        database: Option<String>,
        schema: Option<String>,
        name: String,
    },
    Update {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        assignments: Vec<(String, String)>,
        selection_sql: Option<String>,
    },
    AlterTable {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        operation: AlterTableOperation,
    },
    CreateIndex {
        database: Option<String>,
        schema: Option<String>,
        table: String,
        name: String,
        columns: Vec<String>,
        unique: bool,
        concurrently: bool,
    },
    AlterIndex {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        operation: AlterObjectOperation,
    },
    DropIndex {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        if_exists: bool,
        cascade: bool,
    },
    Reindex {
        target: ReindexTarget,
    },
    AlterSchema {
        database: Option<String>,
        name: String,
        new_name: String,
    },
    AlterDatabase {
        name: String,
        operation: AlterDatabaseOperation,
    },
    AlterAggregate {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        operation: AlterObjectOperation,
    },
    AlterCollation {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        operation: AlterObjectOperation,
    },
    AlterConversion {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        operation: AlterObjectOperation,
    },
    CreateFunction {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        or_replace: bool,
        definition_sql: String,
    },
    AlterFunction {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        operation: AlterObjectOperation,
    },
    DropFunction {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        if_exists: bool,
        cascade: bool,
    },
    ShowDatabases,
    ShowSchemas {
        database: Option<String>,
    },
    ShowNodes,
    ShowTables {
        database: Option<String>,
        schema: Option<String>,
    },
    ShowViews {
        database: Option<String>,
        schema: Option<String>,
    },
    ShowColumns {
        database: Option<String>,
        schema: Option<String>,
        table: String,
    },
    InformationSchemaSchemata {
        sql: String,
    },
    InformationSchemaTables {
        sql: String,
    },
    InformationSchemaColumns {
        sql: String,
    },
    InformationSchemaViews {
        sql: String,
    },
    InformationSchemaTableConstraints {
        sql: String,
    },
    InformationSchemaKeyColumnUsage {
        sql: String,
    },
    InformationSchemaConstraintColumnUsage {
        sql: String,
    },
    InformationSchemaConstraintTableUsage {
        sql: String,
    },
    InformationSchemaReferentialConstraints {
        sql: String,
    },
    DropTable {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        if_exists: bool,
        cascade: bool,
    },
    DropView {
        database: Option<String>,
        schema: Option<String>,
        name: String,
        if_exists: bool,
        cascade: bool,
    },
    DropDatabase {
        name: String,
        if_exists: bool,
    },
    DropSchema {
        database: Option<String>,
        name: String,
        if_exists: bool,
        cascade: bool,
    },
    AlterUserPassword {
        name: String,
        password: String,
    },
    Begin,
    Commit,
    Rollback,
}

pub const DEFAULT_CATALOG_PATH: &str = "analyticsdb-catalog.json";

#[derive(Debug)]
pub struct ControlPlane {
    coordinator_node_id: String,
    catalog_path: Option<PathBuf>,
    state: RwLock<CatalogState>,
    next_round_robin_index: AtomicUsize,
}

impl ControlPlane {
    pub fn new_bootstrap() -> Self {
        Self::from_state(None, bootstrap_state())
    }

    pub async fn from_catalog_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if path.exists() {
            let raw = fs::read_to_string(path).await?;
            let state: CatalogState = serde_json::from_str(&raw)?;
            Ok(Self::from_state(Some(path.to_path_buf()), state))
        } else {
            let control_plane = Self::from_state(Some(path.to_path_buf()), bootstrap_state());
            control_plane.persist().await?;

            Ok(control_plane)
        }
    }

    fn from_state(catalog_path: Option<PathBuf>, state: CatalogState) -> Self {
        let coordinator_node_id = "control-1".to_string();

        Self {
            coordinator_node_id,
            catalog_path,
            state: RwLock::new(state),
            next_round_robin_index: AtomicUsize::new(0),
        }
    }

    pub async fn admit_query(&self, session: &SessionContext) -> Result<QueryAdmission> {
        self.validate_session(session).await?;

        let state = self.state.read().await;
        // Round-robin routing across 'Ready' nodes
        let ready_nodes: Vec<_> = state
            .nodes
            .values()
            .filter(|n| n.status == NodeStatus::Ready)
            .collect();

        let coordinator_node_id = if ready_nodes.is_empty() {
            self.coordinator_node_id.clone()
        } else {
            let index =
                self.next_round_robin_index.fetch_add(1, Ordering::SeqCst) % ready_nodes.len();
            ready_nodes[index].id.clone()
        };

        Ok(QueryAdmission {
            query_id: format!("q-{}", Uuid::now_v7()),
            coordinator_node_id,
        })
    }

    pub async fn validate_session(&self, session: &SessionContext) -> Result<()> {
        let state = self.state.read().await;
        self._validate_session(&state, session)
    }

    fn _validate_session(&self, state: &CatalogState, session: &SessionContext) -> Result<()> {
        let user = state
            .users
            .get(&session.user)
            .ok_or_else(|| anyhow::anyhow!("Unknown user '{}'", session.user))?;

        if session.role != session.user {
            let role = state
                .users
                .get(&session.role)
                .ok_or_else(|| anyhow::anyhow!("Unknown role '{}'", session.role))?;

            if !user.is_admin && role.name != user.name {
                bail!(
                    "User '{}' is not authorized to assume role '{}'",
                    session.user,
                    session.role
                );
            }
        }

        if !state.databases.contains_key(&session.database) {
            bail!("Unknown database '{}'", session.database);
        }

        let database = state.databases.get(&session.database).unwrap();
        if !database.schemas.contains(&session.schema) {
            bail!("Unknown schema '{}.{}'", session.database, session.schema);
        }

        Ok(())
    }

    pub async fn cluster_snapshot(&self) -> ClusterSnapshot {
        let state = self.state.read().await;

        ClusterSnapshot {
            coordinator_node_id: self.coordinator_node_id.clone(),
            nodes: state.nodes.values().cloned().collect(),
            databases: state.databases.values().cloned().collect(),
            users: state.users.values().cloned().collect(),
            relations: state.relations.values().cloned().collect(),
            functions: state.functions.values().cloned().collect(),
        }
    }

    pub async fn execute_metadata_statement(
        &self,
        session: &SessionContext,
        statement: &MetadataStatement,
    ) -> Result<(String, SessionContext)> {
        let mut new_session = session.clone();
        let message = match statement {
            MetadataStatement::CreateDatabase { name } => {
                self.create_database(session, name).await?
            }
            MetadataStatement::CreateAggregate {
                database,
                schema,
                name,
            } => {
                self.create_aggregate(session, database.as_deref(), schema.as_deref(), name)
                    .await?
            }
            MetadataStatement::CreateCollation {
                database,
                schema,
                name,
            } => {
                self.create_collation(session, database.as_deref(), schema.as_deref(), name)
                    .await?
            }
            MetadataStatement::CreateConversion {
                database,
                schema,
                name,
            } => {
                self.create_conversion(session, database.as_deref(), schema.as_deref(), name)
                    .await?
            }
            MetadataStatement::AlterDatabase { name, operation } => {
                self.alter_database(session, name, operation).await?
            }
            MetadataStatement::AlterAggregate {
                database,
                schema,
                name,
                operation,
            } => {
                self.alter_aggregate(
                    session,
                    database.as_deref(),
                    schema.as_deref(),
                    name,
                    operation,
                )
                .await?
            }
            MetadataStatement::AlterCollation {
                database,
                schema,
                name,
                operation,
            } => {
                self.alter_collation(
                    session,
                    database.as_deref(),
                    schema.as_deref(),
                    name,
                    operation,
                )
                .await?
            }
            MetadataStatement::AlterConversion {
                database,
                schema,
                name,
                operation,
            } => {
                self.alter_conversion(
                    session,
                    database.as_deref(),
                    schema.as_deref(),
                    name,
                    operation,
                )
                .await?
            }
            MetadataStatement::CreateFunction {
                database,
                schema,
                name,
                or_replace,
                definition_sql,
            } => {
                self.create_function(
                    session,
                    database.as_deref(),
                    schema.as_deref(),
                    name,
                    *or_replace,
                    definition_sql,
                )
                .await?
            }
            MetadataStatement::AlterFunction {
                database,
                schema,
                name,
                operation,
            } => {
                self.alter_function(
                    session,
                    database.as_deref(),
                    schema.as_deref(),
                    name,
                    operation,
                )
                .await?
            }
            MetadataStatement::DropFunction {
                database,
                schema,
                name,
                if_exists,
                cascade,
            } => {
                self.drop_function(
                    session,
                    database.as_deref(),
                    schema.as_deref(),
                    name,
                    *if_exists,
                    *cascade,
                )
                .await?
            }
            MetadataStatement::CreateSchema { database, name } => {
                self.create_schema(session, database.as_deref(), name)
                    .await?
            }
            MetadataStatement::AlterUserPassword { name, password } => {
                self.rotate_user_password(session, name, password).await?
            }
            MetadataStatement::Begin => {
                self.validate_session(session).await?;
                new_session.transaction_status = analyticsdb_core::TransactionStatus::InTransaction;
                "Command completed. 0 row(s) affected.".to_string()
            }
            MetadataStatement::Commit => {
                self.validate_session(session).await?;
                new_session.transaction_status = analyticsdb_core::TransactionStatus::Idle;
                "Command completed. 0 row(s) affected.".to_string()
            }
            MetadataStatement::Rollback => {
                self.validate_session(session).await?;
                new_session.transaction_status = analyticsdb_core::TransactionStatus::Idle;
                "Command completed. 0 row(s) affected.".to_string()
            }
            MetadataStatement::CreateIndex {
                database,
                schema,
                table,
                name,
                columns,
                unique,
                concurrently: _,
            } => {
                self.create_index(
                    session,
                    database.as_deref(),
                    schema.as_deref(),
                    table,
                    name,
                    columns.clone(),
                    *unique,
                )
                .await?
            }
            MetadataStatement::DropIndex {
                database,
                schema,
                name,
                if_exists,
                cascade: _,
            } => {
                self.drop_index(
                    session,
                    database.as_deref(),
                    schema.as_deref(),
                    name,
                    *if_exists,
                )
                .await?
            }
            MetadataStatement::CreateView { .. }
            | MetadataStatement::CreateTableAs { .. }
            | MetadataStatement::SelectInto { .. }
            | MetadataStatement::CreateTable { .. }
            | MetadataStatement::AlterIndex { .. }
            | MetadataStatement::Reindex { .. }
            | MetadataStatement::CreateExternalTable { .. }
            | MetadataStatement::InsertInto { .. }
            | MetadataStatement::Update { .. }
            | MetadataStatement::Delete { .. }
            | MetadataStatement::Truncate { .. }
            | MetadataStatement::AlterTable { .. }
            | MetadataStatement::AlterSchema { .. }
            | MetadataStatement::DropTable { .. }
            | MetadataStatement::DropView { .. }
            | MetadataStatement::DropDatabase { .. }
            | MetadataStatement::DropSchema { .. } => {
                bail!("Relation DDL and DML should be handled by the engine persistence flow")
            }
            MetadataStatement::ShowDatabases => {
                self.validate_session(session).await?;
                "Command completed.".to_string()
            }
            MetadataStatement::ShowSchemas { .. }
            | MetadataStatement::ShowNodes
            | MetadataStatement::ShowTables { .. }
            | MetadataStatement::ShowViews { .. }
            | MetadataStatement::ShowColumns { .. }
            | MetadataStatement::InformationSchemaSchemata { .. }
            | MetadataStatement::InformationSchemaTables { .. }
            | MetadataStatement::InformationSchemaColumns { .. }
            | MetadataStatement::InformationSchemaViews { .. }
            | MetadataStatement::InformationSchemaTableConstraints { .. }
            | MetadataStatement::InformationSchemaKeyColumnUsage { .. }
            | MetadataStatement::InformationSchemaConstraintColumnUsage { .. }
            | MetadataStatement::InformationSchemaConstraintTableUsage { .. }
            | MetadataStatement::InformationSchemaReferentialConstraints { .. } => {
                self.validate_session(session).await?;
                "Command completed.".to_string()
            }
        };

        Ok((message, new_session))
    }

    pub async fn register_node(&self, mut node: ClusterNode) -> Result<()> {
        node.last_heartbeat_at_epoch_ms = current_epoch_millis();
        {
            let mut state = self.state.write().await;
            state.nodes.insert(node.id.clone(), node);
        }
        self.persist().await?;
        Ok(())
    }

    pub async fn heartbeat(&self, node_id: &str) -> Result<()> {
        {
            let mut state = self.state.write().await;
            let node = state
                .nodes
                .get_mut(node_id)
                .ok_or_else(|| anyhow::anyhow!("Node '{}' not found", node_id))?;
            node.last_heartbeat_at_epoch_ms = current_epoch_millis();
            node.status = NodeStatus::Ready;
        }
        // Heartbeats don't necessarily need to be persisted every time for the prototype
        // but we'll do it for now to keep it simple and consistent.
        self.persist().await?;
        Ok(())
    }

    pub async fn prune_unhealthy_nodes(&self, threshold_ms: u128) -> Result<()> {
        let now = current_epoch_millis();
        let mut changed = false;
        {
            let mut state = self.state.write().await;
            for node in state.nodes.values_mut() {
                if node.status == NodeStatus::Ready
                    && now - node.last_heartbeat_at_epoch_ms > threshold_ms
                {
                    node.status = NodeStatus::Unavailable;
                    changed = true;
                }
            }
        }
        if changed {
            self.persist().await?;
        }
        Ok(())
    }

    pub async fn list_nodes(&self) -> Result<Vec<ClusterNode>> {
        let state = self.state.read().await;
        Ok(state.nodes.values().cloned().collect())
    }

    /// Removes all Compute nodes from the catalog.
    ///
    /// Called on coordinator startup so that stale entries from previous runs
    /// don't cause dispatch attempts to non-running workers.  Compute nodes
    /// re-register by calling `join_cluster` when they start.
    pub async fn clear_compute_nodes(&self) -> Result<()> {
        {
            let mut state = self.state.write().await;
            state.nodes.retain(|_, n| n.role != NodeRole::Compute);
        }
        self.persist().await?;
        Ok(())
    }

    pub async fn join_cluster(
        &self,
        requested_node_id: Option<&str>,
        advertise_host: Option<&str>,
    ) -> Result<raft::JoinResponse> {
        let host = advertise_host.unwrap_or("127.0.0.1");

        let (node_id, postgres_port, flight_sql_port, node_port, new_config) = {
            let mut state = self.state.write().await;

            let config = state
                .config
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Cluster config not initialized"))?;

            // 1. Determine Node ID
            let node_id = match requested_node_id {
                Some(id) => id.to_string(),
                None => {
                    let mut idx = state.nodes.len() + 1;
                    let mut candidate = format!("node-{}", idx);
                    while state.nodes.contains_key(&candidate) {
                        idx += 1;
                        candidate = format!("node-{}", idx);
                    }
                    candidate
                }
            };

            // 2. Allocate ports from the coordinator's counter.
            let offset = config.next_available_port_offset + 1;
            let postgres_port = config.base_postgres_port + offset;
            let flight_sql_port = config.base_flight_sql_port + offset;
            let node_port = config.base_node_port + offset;

            // 3. Persist the new offset so the next joining node gets different ports.
            let mut new_config = config.clone();
            new_config.next_available_port_offset = offset;
            state.config = Some(new_config.clone());

            // 4. Register both the client-facing Flight SQL endpoint and the
            //    dedicated node-to-node endpoint. Distributed execution uses
            //    the internal endpoint so client TLS policy can evolve
            //    independently from cluster transport policy.
            let scheme = if config.tls_cert_path.is_some() && config.tls_key_path.is_some() {
                "https"
            } else {
                "http"
            };
            let endpoint = format!("{}://{}:{}", scheme, host, flight_sql_port);
            let internal_endpoint = format!("http://{}:{}", host, node_port);
            let node = ClusterNode {
                id: node_id.clone(),
                role: NodeRole::Compute,
                endpoint,
                internal_endpoint: Some(internal_endpoint),
                status: NodeStatus::Ready,
                last_heartbeat_at_epoch_ms: current_epoch_millis(),
            };
            state.nodes.insert(node_id.clone(), node);
            (
                node_id,
                postgres_port,
                flight_sql_port,
                node_port,
                new_config,
            )
        };

        self.persist().await?;

        // Override catalog_path in the response so the joining node loads
        // from the same file as the coordinator (not the default path stored
        // inside the catalog's own config section).
        let mut response_config = new_config;
        if let Some(actual_path) = &self.catalog_path {
            response_config.catalog_path = actual_path.to_string_lossy().into_owned();
        }

        Ok(raft::JoinResponse {
            node_id,
            postgres_port,
            flight_sql_port,
            node_port,
            config: response_config,
        })
    }

    /// Updates only the TLS cert/key paths in the in-memory cluster config.
    ///
    /// Not persisted — the canonical source for TLS config is the
    /// cluster-config.json file, not the catalog.  Called at coordinator
    /// startup so that `join_cluster` registers compute nodes with the
    /// correct scheme (`https://` vs `http://`).
    pub async fn set_tls_paths(
        &self,
        cert_path: Option<String>,
        key_path: Option<String>,
    ) -> Result<()> {
        let mut state = self.state.write().await;
        if let Some(ref mut config) = state.config {
            config.tls_cert_path = cert_path;
            config.tls_key_path = key_path;
        }
        Ok(())
    }

    pub async fn update_cluster_config(&self, config: ClusterConfig) -> Result<()> {
        {
            let mut state = self.state.write().await;
            state.config = Some(config);
        }
        self.persist().await?;
        Ok(())
    }

    pub async fn create_index(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        table: &str,
        name: &str,
        columns: Vec<String>,
        unique: bool,
    ) -> Result<String> {
        let database_name = database.unwrap_or(&session.database).to_string();
        let schema_name = schema.unwrap_or(&session.schema).to_string();
        let key = relation_key(&database_name, &schema_name, table);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            let relation = state.relations.get_mut(&key).ok_or_else(|| {
                anyhow::anyhow!(
                    "Table '{}.{}.{}' not found",
                    database_name,
                    schema_name,
                    table
                )
            })?;

            if relation.indexes.iter().any(|i| i.name == name) {
                bail!("Index '{}' already exists on table '{}'", name, table);
            }

            relation.indexes.push(CatalogIndex {
                name: name.to_string(),
                columns,
                is_unique: unique,
                is_primary: false,
            });
        }

        self.persist().await?;
        Ok(format!("Index '{}' created successfully.", name))
    }

    pub async fn drop_index(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
        if_exists: bool,
    ) -> Result<String> {
        let database_name = database.unwrap_or(&session.database).to_string();
        let schema_name = schema.unwrap_or(&session.schema).to_string();

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            let mut found = false;
            for relation in state.relations.values_mut() {
                if relation.database == database_name && relation.schema == schema_name {
                    if let Some(pos) = relation.indexes.iter().position(|i| i.name == name) {
                        relation.indexes.remove(pos);
                        found = true;
                        break;
                    }
                }
            }

            if !found {
                if if_exists {
                    return Ok(format!(
                        "Index '{}.{}.{}' does not exist, skipping.",
                        database_name, schema_name, name
                    ));
                } else {
                    bail!("Index '{}' not found", name);
                }
            }
        }

        self.persist().await?;
        Ok(format!("Index '{}' dropped successfully.", name))
    }

    async fn create_database(&self, session: &SessionContext, name: &str) -> Result<String> {
        validate_identifier(name)?;

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            if !state.users.get(&session.user).unwrap().is_admin {
                bail!("Only administrators can create databases");
            }

            if state.databases.contains_key(name) {
                bail!("Database '{}' already exists", name);
            }

            state.databases.insert(
                name.to_string(),
                CatalogDatabase {
                    name: name.to_string(),
                    schemas: BTreeSet::from(["public".to_string()]),
                    owner: session.user.clone(),
                    parameters: BTreeMap::new(),
                },
            );
        }

        self.persist().await?;
        Ok(format!("Database '{}' created successfully.", name))
    }

    async fn alter_database(
        &self,
        session: &SessionContext,
        name: &str,
        operation: &AlterDatabaseOperation,
    ) -> Result<String> {
        validate_identifier(name)?;

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            if !state.users.get(&session.user).unwrap().is_admin {
                bail!("Only administrators can alter databases");
            }

            match operation {
                AlterDatabaseOperation::Rename { new_name } => {
                    validate_identifier(new_name)?;
                    if state.databases.contains_key(new_name) {
                        bail!("Database '{}' already exists", new_name);
                    }

                    let mut db_data = state.databases.remove(name).unwrap();
                    db_data.name = new_name.to_string();
                    state.databases.insert(new_name.to_string(), db_data);

                    // Update relations
                    let keys_to_move: Vec<String> = state
                        .relations
                        .keys()
                        .filter(|k| k.starts_with(&format!("{}.", name)))
                        .cloned()
                        .collect();
                    for key in keys_to_move {
                        let mut rel = state.relations.remove(&key).unwrap();
                        rel.database = new_name.to_string();
                        let new_key = relation_key(&rel.database, &rel.schema, &rel.name);
                        state.relations.insert(new_key, rel);
                    }

                    self.persist_state(&state).await?; // Persist inside because we changed maps
                    return Ok(format!(
                        "Database '{}' renamed to '{}' successfully.",
                        name, new_name
                    ));
                }
                AlterDatabaseOperation::OwnerTo { new_owner } => {
                    if !state.users.contains_key(new_owner) {
                        bail!("User '{}' does not exist", new_owner);
                    }
                }
                _ => {}
            }

            let database = state
                .databases
                .get_mut(name)
                .ok_or_else(|| anyhow::anyhow!("Database '{}' does not exist", name))?;

            match operation {
                AlterDatabaseOperation::OwnerTo { new_owner } => {
                    database.owner = new_owner.to_string();
                }
                AlterDatabaseOperation::SetParam {
                    name: p_name,
                    value,
                } => {
                    database
                        .parameters
                        .insert(p_name.to_string(), value.to_string());
                }
                _ => {}
            }
        }

        self.persist().await?;
        Ok(format!("ALTER DATABASE completed for '{}'.", name))
    }

    async fn create_aggregate(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
    ) -> Result<String> {
        validate_identifier(name)?;
        let database_name = database.unwrap_or(&session.database).to_string();
        let schema_name = schema.unwrap_or(&session.schema).to_string();
        let key = format!("{}.{}.{}", database_name, schema_name, name);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;
            if state.aggregates.contains_key(&key) {
                bail!("Aggregate '{}' already exists", key);
            }
            state.aggregates.insert(
                key.clone(),
                CatalogAggregate {
                    database: database_name,
                    schema: schema_name,
                    name: name.to_string(),
                    owner: session.user.clone(),
                },
            );
        }
        self.persist().await?;
        Ok(format!("Aggregate '{}' created successfully.", key))
    }

    async fn alter_aggregate(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
        operation: &AlterObjectOperation,
    ) -> Result<String> {
        let database_name = database.unwrap_or(&session.database).to_string();
        let schema_name = schema.unwrap_or(&session.schema).to_string();
        let key = format!("{}.{}.{}", database_name, schema_name, name);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;
            let mut aggregate = state
                .aggregates
                .remove(&key)
                .ok_or_else(|| anyhow::anyhow!("Aggregate '{}' not found", key))?;

            match operation {
                AlterObjectOperation::Rename { new_name } => {
                    validate_identifier(new_name)?;
                    aggregate.name = new_name.to_string();
                }
                AlterObjectOperation::OwnerTo { new_owner } => {
                    if !state.users.contains_key(new_owner) {
                        bail!("User '{}' does not exist", new_owner);
                    }
                    aggregate.owner = new_owner.to_string();
                }
                AlterObjectOperation::SetSchema { new_schema } => {
                    validate_identifier(new_schema)?;
                    let database = state.databases.get(&aggregate.database).ok_or_else(|| {
                        anyhow::anyhow!("Database '{}' does not exist", aggregate.database)
                    })?;
                    if !database.schemas.contains(new_schema) {
                        bail!(
                            "Schema '{}.{}' does not exist",
                            aggregate.database,
                            new_schema
                        );
                    }
                    aggregate.schema = new_schema.to_string();
                }
            }
            let new_key = format!(
                "{}.{}.{}",
                aggregate.database, aggregate.schema, aggregate.name
            );
            if new_key != key && state.aggregates.contains_key(&new_key) {
                state.aggregates.insert(key.clone(), aggregate);
                bail!("Aggregate '{}' already exists", new_key);
            }
            state.aggregates.insert(new_key, aggregate);
        }
        self.persist().await?;
        Ok(format!("ALTER AGGREGATE completed for '{}'.", key))
    }

    async fn create_collation(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
    ) -> Result<String> {
        validate_identifier(name)?;
        let database_name = database.unwrap_or(&session.database).to_string();
        let schema_name = schema.unwrap_or(&session.schema).to_string();
        let key = format!("{}.{}.{}", database_name, schema_name, name);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;
            if state.collations.contains_key(&key) {
                bail!("Collation '{}' already exists", key);
            }
            state.collations.insert(
                key.clone(),
                CatalogCollation {
                    database: database_name,
                    schema: schema_name,
                    name: name.to_string(),
                },
            );
        }
        self.persist().await?;
        Ok(format!("Collation '{}' created successfully.", key))
    }

    async fn alter_collation(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
        operation: &AlterObjectOperation,
    ) -> Result<String> {
        let database_name = database.unwrap_or(&session.database).to_string();
        let schema_name = schema.unwrap_or(&session.schema).to_string();
        let key = format!("{}.{}.{}", database_name, schema_name, name);
        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;
            let mut collation = state
                .collations
                .remove(&key)
                .ok_or_else(|| anyhow::anyhow!("Collation '{}' not found", key))?;
            match operation {
                AlterObjectOperation::Rename { new_name } => {
                    validate_identifier(new_name)?;
                    collation.name = new_name.to_string();
                }
                AlterObjectOperation::OwnerTo { .. } => {} // Collations don't have explicit owners in our model yet
                AlterObjectOperation::SetSchema { new_schema } => {
                    validate_identifier(new_schema)?;
                    let database = state.databases.get(&collation.database).ok_or_else(|| {
                        anyhow::anyhow!("Database '{}' does not exist", collation.database)
                    })?;
                    if !database.schemas.contains(new_schema) {
                        bail!(
                            "Schema '{}.{}' does not exist",
                            collation.database,
                            new_schema
                        );
                    }
                    collation.schema = new_schema.to_string();
                }
            }
            let new_key = format!(
                "{}.{}.{}",
                collation.database, collation.schema, collation.name
            );
            if new_key != key && state.collations.contains_key(&new_key) {
                state.collations.insert(key.clone(), collation);
                bail!("Collation '{}' already exists", new_key);
            }
            state.collations.insert(new_key, collation);
        }
        self.persist().await?;
        Ok(format!("ALTER COLLATION completed for '{}'.", key))
    }

    async fn create_conversion(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
    ) -> Result<String> {
        validate_identifier(name)?;
        let database_name = database.unwrap_or(&session.database).to_string();
        let schema_name = schema.unwrap_or(&session.schema).to_string();
        let key = format!("{}.{}.{}", database_name, schema_name, name);
        {
            let mut state = self.state.write().await;
            state.conversions.insert(
                key.clone(),
                CatalogConversion {
                    database: database_name,
                    schema: schema_name,
                    name: name.to_string(),
                },
            );
        }
        self.persist().await?;
        Ok(format!("Conversion '{}' created successfully.", key))
    }

    async fn alter_conversion(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
        operation: &AlterObjectOperation,
    ) -> Result<String> {
        let database_name = database.unwrap_or(&session.database).to_string();
        let schema_name = schema.unwrap_or(&session.schema).to_string();
        let key = format!("{}.{}.{}", database_name, schema_name, name);
        {
            let mut state = self.state.write().await;
            let mut conversion = state
                .conversions
                .remove(&key)
                .ok_or_else(|| anyhow::anyhow!("Conversion '{}' not found", key))?;
            match operation {
                AlterObjectOperation::Rename { new_name } => conversion.name = new_name.to_string(),
                AlterObjectOperation::OwnerTo { .. } => {}
                AlterObjectOperation::SetSchema { new_schema } => {
                    conversion.schema = new_schema.to_string()
                }
            }
            let new_key = format!(
                "{}.{}.{}",
                conversion.database, conversion.schema, conversion.name
            );
            state.conversions.insert(new_key, conversion);
        }
        self.persist().await?;
        Ok(format!("ALTER CONVERSION completed for '{}'.", key))
    }

    async fn create_function(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
        or_replace: bool,
        definition_sql: &str,
    ) -> Result<String> {
        validate_identifier(name)?;
        let database_name = database.unwrap_or(&session.database).to_string();
        let schema_name = schema.unwrap_or(&session.schema).to_string();
        let key = format!("{}.{}.{}", database_name, schema_name, name);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;
            if state.functions.contains_key(&key) && !or_replace {
                bail!("Function '{}' already exists", key);
            }
            state.functions.insert(
                key.clone(),
                CatalogFunction {
                    database: database_name,
                    schema: schema_name,
                    name: name.to_string(),
                    owner: session.user.clone(),
                    definition_sql: definition_sql.to_string(),
                },
            );
        }
        self.persist().await?;
        Ok(format!("Function '{}' created successfully.", key))
    }

    async fn alter_function(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
        operation: &AlterObjectOperation,
    ) -> Result<String> {
        let database_name = database.unwrap_or(&session.database).to_string();
        let schema_name = schema.unwrap_or(&session.schema).to_string();
        let key = format!("{}.{}.{}", database_name, schema_name, name);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;
            let mut function = state
                .functions
                .remove(&key)
                .ok_or_else(|| anyhow::anyhow!("Function '{}' not found", key))?;

            match operation {
                AlterObjectOperation::Rename { new_name } => {
                    validate_identifier(new_name)?;
                    function.name = new_name.to_string();
                }
                AlterObjectOperation::OwnerTo { new_owner } => {
                    if !state.users.contains_key(new_owner) {
                        bail!("User '{}' does not exist", new_owner);
                    }
                    function.owner = new_owner.to_string();
                }
                AlterObjectOperation::SetSchema { new_schema } => {
                    validate_identifier(new_schema)?;
                    let database = state.databases.get(&function.database).ok_or_else(|| {
                        anyhow::anyhow!("Database '{}' does not exist", function.database)
                    })?;
                    if !database.schemas.contains(new_schema) {
                        bail!(
                            "Schema '{}.{}' does not exist",
                            function.database,
                            new_schema
                        );
                    }
                    function.schema = new_schema.to_string();
                }
            }
            let new_key = format!(
                "{}.{}.{}",
                function.database, function.schema, function.name
            );
            if new_key != key && state.functions.contains_key(&new_key) {
                state.functions.insert(key.clone(), function);
                bail!("Function '{}' already exists", new_key);
            }
            state.functions.insert(new_key, function);
        }
        self.persist().await?;
        Ok(format!("ALTER FUNCTION completed for '{}'.", key))
    }

    async fn drop_function(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
        if_exists: bool,
        _cascade: bool,
    ) -> Result<String> {
        let database_name = database.unwrap_or(&session.database).to_string();
        let schema_name = schema.unwrap_or(&session.schema).to_string();
        let key = format!("{}.{}.{}", database_name, schema_name, name);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;
            if state.functions.remove(&key).is_none() {
                if if_exists {
                    return Ok(format!("Function '{}' does not exist, skipping.", key));
                } else {
                    bail!("Function '{}' does not exist", key);
                }
            }
        }
        self.persist().await?;
        Ok(format!("Function '{}' dropped successfully.", key))
    }

    async fn create_schema(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema_name: &str,
    ) -> Result<String> {
        validate_identifier(schema_name)?;
        let database_name = database.unwrap_or(&session.database);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            let database = state
                .databases
                .get_mut(database_name)
                .ok_or_else(|| anyhow::anyhow!("Unknown database '{}'", database_name))?;

            if database.schemas.contains(schema_name) {
                bail!("Schema '{}.{}' already exists", database_name, schema_name);
            }

            database.schemas.insert(schema_name.to_string());
        }

        self.persist().await?;
        Ok(format!(
            "Schema '{}.{}' created successfully.",
            database_name, schema_name
        ))
    }

    pub async fn list_databases(&self, session: &SessionContext) -> Result<Vec<String>> {
        let state = self.state.read().await;
        self._validate_session(&state, session)?;

        Ok(state.databases.keys().cloned().collect())
    }

    pub async fn list_schemas(
        &self,
        session: &SessionContext,
        database: Option<&str>,
    ) -> Result<Vec<String>> {
        let state = self.state.read().await;
        self._validate_session(&state, session)?;

        let database_name = database.unwrap_or(&session.database);
        let database = state
            .databases
            .get(database_name)
            .ok_or_else(|| anyhow::anyhow!("Unknown database '{}'", database_name))?;

        Ok(database.schemas.iter().cloned().collect())
    }

    pub async fn list_all_relations(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<CatalogRelation>> {
        let state = self.state.read().await;
        self._validate_session(&state, session)?;
        Ok(state
            .relations
            .values()
            .filter(|rel| rel.database == session.database)
            .cloned()
            .collect())
    }

    pub async fn list_relations(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        kind: CatalogRelationKind,
    ) -> Result<Vec<CatalogRelation>> {
        let state = self.state.read().await;
        self._validate_session(&state, session)?;

        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);

        Ok(state
            .relations
            .values()
            .filter(|rel| {
                rel.kind == kind && rel.database == database_name && rel.schema == schema_name
            })
            .cloned()
            .collect())
    }

    pub async fn find_relation(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
    ) -> Result<CatalogRelation> {
        let state = self.state.read().await;
        self._validate_session(&state, session)?;

        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);

        let key = relation_key(database_name, schema_name, name);
        state
            .relations
            .get(&key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Relation '{}' not found", key))
    }

    pub async fn relation_columns(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
    ) -> Result<Vec<CatalogColumn>> {
        let relation = self.find_relation(session, database, schema, name).await?;
        Ok(relation.columns)
    }

    pub async fn table_relation(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
    ) -> Result<CatalogRelation> {
        let relation = self.find_relation(session, database, schema, name).await?;
        if relation.kind != CatalogRelationKind::Table {
            bail!("Relation '{}' is not a table", name);
        }
        Ok(relation)
    }

    pub async fn list_tables_for_session(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<CatalogRelation>> {
        self.list_relations(
            session,
            Some(&session.database),
            Some(&session.schema),
            CatalogRelationKind::Table,
        )
        .await
    }

    pub async fn list_relations_for_database(
        &self,
        session: &SessionContext,
        database: &str,
        kind: CatalogRelationKind,
    ) -> Result<Vec<CatalogRelation>> {
        let state = self.state.read().await;
        self._validate_session(&state, session)?;

        let db = state
            .databases
            .get(database)
            .ok_or_else(|| anyhow::anyhow!("Unknown database '{}'", database))?;

        Ok(state
            .relations
            .values()
            .filter(|relation| {
                relation.kind == kind
                    && relation.database == db.name
                    && db.schemas.contains(&relation.schema)
            })
            .cloned()
            .collect())
    }

    pub async fn list_views_for_session(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<CatalogRelation>> {
        self.list_relations(
            session,
            Some(&session.database),
            Some(&session.schema),
            CatalogRelationKind::View,
        )
        .await
    }

    pub async fn register_view(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        view_name: &str,
        definition_sql: &str,
        columns: Vec<CatalogColumn>,
    ) -> Result<String> {
        validate_identifier(view_name)?;
        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            let database = state
                .databases
                .get(database_name)
                .ok_or_else(|| anyhow::anyhow!("Unknown database '{}'", database_name))?;

            if !database.schemas.contains(schema_name) {
                bail!("Unknown schema '{}.{}'", database_name, schema_name);
            }

            let relation_key = relation_key(database_name, schema_name, view_name);
            if state.relations.contains_key(&relation_key) {
                bail!(
                    "Relation '{}.{}.{}' already exists",
                    database_name,
                    schema_name,
                    view_name
                );
            }

            state.relations.insert(
                relation_key,
                CatalogRelation {
                    database: database_name.to_string(),
                    schema: schema_name.to_string(),
                    name: view_name.to_string(),
                    kind: CatalogRelationKind::View,
                    definition_sql: Some(definition_sql.trim().to_string()),
                    storage_path: None,
                    external_format: None,
                    columns,
                    constraints: Vec::new(),
                    indexes: Vec::new(),
                },
            );
        }

        self.persist().await?;
        Ok(format!(
            "View '{}.{}.{}' created successfully.",
            database_name, schema_name, view_name
        ))
    }

    pub async fn managed_table_storage_location(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
    ) -> Result<PathBuf> {
        validate_identifier(table_name)?;
        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);

        {
            let state = self.state.read().await;
            self._validate_session(&state, session)?;
        }

        let catalog_path_buf = self
            .catalog_path
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_CATALOG_PATH));
        let catalog_path = catalog_path_buf.as_path();

        let base_name = catalog_path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("analyticsdb-catalog");
        let parent_dir = catalog_path.parent().unwrap_or_else(|| Path::new("."));
        let data_dir = parent_dir.join(format!("{base_name}.managed"));

        Ok(data_dir.join(format!(
            "{database_name}__{schema_name}__{table_name}.table.parquet"
        )))
    }

    pub async fn register_managed_table(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
        storage_path: &Path,
        columns: Vec<CatalogColumn>,
        constraints: Vec<CatalogTableConstraint>,
    ) -> Result<String> {
        validate_identifier(table_name)?;
        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            let database = state
                .databases
                .get(database_name)
                .ok_or_else(|| anyhow::anyhow!("Unknown database '{}'", database_name))?;

            if !database.schemas.contains(schema_name) {
                bail!("Unknown schema '{}.{}'", database_name, schema_name);
            }

            let relation_key = relation_key(database_name, schema_name, table_name);
            if state.relations.contains_key(&relation_key) {
                bail!(
                    "Relation '{}.{}.{}' already exists",
                    database_name,
                    schema_name,
                    table_name
                );
            }

            let indexes = indexes_from_constraints(table_name, &constraints);

            state.relations.insert(
                relation_key,
                CatalogRelation {
                    database: database_name.to_string(),
                    schema: schema_name.to_string(),
                    name: table_name.to_string(),
                    kind: CatalogRelationKind::Table,
                    definition_sql: None,
                    storage_path: Some(storage_path.to_string_lossy().to_string()),
                    external_format: None,
                    columns,
                    constraints,
                    indexes,
                },
            );
        }

        self.persist().await?;
        Ok(format!(
            "Table '{}.{}.{}' created successfully.",
            database_name, schema_name, table_name
        ))
    }

    pub async fn add_column(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
        column: CatalogColumn,
    ) -> Result<String> {
        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            let relation_key = relation_key(database_name, schema_name, table_name);
            let relation = state.relations.get_mut(&relation_key).ok_or_else(|| {
                anyhow::anyhow!(
                    "Table '{}.{}.{}' not found",
                    database_name,
                    schema_name,
                    table_name
                )
            })?;

            if relation.kind != CatalogRelationKind::Table {
                bail!(
                    "Relation '{}.{}.{}' is not a table",
                    database_name,
                    schema_name,
                    table_name
                );
            }

            if relation.columns.iter().any(|c| c.name == column.name) {
                bail!(
                    "Column '{}' already exists in table '{}.{}.{}'",
                    column.name,
                    database_name,
                    schema_name,
                    table_name
                );
            }

            relation.columns.push(column);
        }

        self.persist().await?;
        Ok(format!(
            "Column added successfully to '{}.{}.{}'.",
            database_name, schema_name, table_name
        ))
    }

    pub async fn preview_add_constraint(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
        constraint: &CatalogTableConstraint,
    ) -> Result<CatalogRelation> {
        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);
        let state = self.state.read().await;
        self._validate_session(&state, session)?;

        build_relation_with_catalog_constraint(
            &state,
            database_name,
            schema_name,
            table_name,
            constraint.clone(),
        )
    }

    pub async fn add_constraint(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
        constraint: CatalogTableConstraint,
    ) -> Result<String> {
        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);
        let relation_key = relation_key(database_name, schema_name, table_name);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;
            let preview = build_relation_with_catalog_constraint(
                &state,
                database_name,
                schema_name,
                table_name,
                constraint.clone(),
            )?;
            state.relations.insert(relation_key.clone(), preview);
        }

        self.persist().await?;
        Ok(format!(
            "Constraint '{}' added to '{}.{}.{}'.",
            constraint.name, database_name, schema_name, table_name
        ))
    }

    pub async fn drop_column(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> Result<String> {
        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            let relation_key = relation_key(database_name, schema_name, table_name);
            let relation = state.relations.get_mut(&relation_key).ok_or_else(|| {
                anyhow::anyhow!(
                    "Table '{}.{}.{}' not found",
                    database_name,
                    schema_name,
                    table_name
                )
            })?;

            if let Some(pos) = relation.columns.iter().position(|c| c.name == column_name) {
                relation.columns.remove(pos);
            } else if !if_exists {
                bail!(
                    "Column '{}' not found in table '{}.{}.{}'",
                    column_name,
                    database_name,
                    schema_name,
                    table_name
                );
            }
        }

        self.persist().await?;
        Ok(format!(
            "Column '{}' dropped from '{}.{}.{}'.",
            column_name, database_name, schema_name, table_name
        ))
    }

    pub async fn index_relation(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        index_name: &str,
    ) -> Result<CatalogRelation> {
        let state = self.state.read().await;
        self._validate_session(&state, session)?;

        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);

        state
            .relations
            .values()
            .find(|rel| {
                rel.database == database_name
                    && rel.schema == schema_name
                    && rel.indexes.iter().any(|idx| idx.name == index_name)
            })
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Index '{}' not found", index_name))
    }

    pub async fn preview_create_index(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
        index_name: &str,
        columns: &[String],
        unique: bool,
    ) -> Result<CatalogRelation> {
        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);
        let state = self.state.read().await;
        self._validate_session(&state, session)?;

        let relation_key = relation_key(database_name, schema_name, table_name);
        let relation = state.relations.get(&relation_key).ok_or_else(|| {
            anyhow::anyhow!(
                "Table '{}.{}.{}' not found",
                database_name,
                schema_name,
                table_name
            )
        })?;

        if relation.kind != CatalogRelationKind::Table {
            bail!(
                "Relation '{}.{}.{}' is not a table",
                database_name,
                schema_name,
                table_name
            );
        }

        if schema_contains_index_name(&state, database_name, schema_name, index_name, None) {
            bail!(
                "Index '{}' already exists in schema '{}.{}'",
                index_name,
                database_name,
                schema_name
            );
        }

        let mut preview = relation.clone();
        preview.indexes.push(CatalogIndex {
            name: index_name.to_string(),
            columns: columns.to_vec(),
            is_unique: unique,
            is_primary: false,
        });

        Ok(preview)
    }

    pub async fn rename_column(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<String> {
        validate_identifier(new_name)?;
        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            let relation_key = relation_key(database_name, schema_name, table_name);
            let relation = state.relations.get_mut(&relation_key).ok_or_else(|| {
                anyhow::anyhow!(
                    "Table '{}.{}.{}' not found",
                    database_name,
                    schema_name,
                    table_name
                )
            })?;

            if relation.columns.iter().any(|c| c.name == new_name) {
                bail!(
                    "Column '{}' already exists in table '{}.{}.{}'",
                    new_name,
                    database_name,
                    schema_name,
                    table_name
                );
            }

            if let Some(col) = relation.columns.iter_mut().find(|c| c.name == old_name) {
                col.name = new_name.to_string();
            } else {
                bail!(
                    "Column '{}' not found in table '{}.{}.{}'",
                    old_name,
                    database_name,
                    schema_name,
                    table_name
                );
            }
        }

        self.persist().await?;
        Ok(format!(
            "Column '{}' renamed to '{}' in '{}.{}.{}'.",
            old_name, new_name, database_name, schema_name, table_name
        ))
    }

    pub async fn preview_drop_constraint(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
        constraint_name: &str,
        cascade: bool,
    ) -> Result<CatalogRelation> {
        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);
        let state = self.state.read().await;
        self._validate_session(&state, session)?;

        build_relation_with_dropped_constraint(
            &state,
            database_name,
            schema_name,
            table_name,
            constraint_name,
            cascade,
        )
    }

    pub async fn drop_constraint(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
        constraint_name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> Result<String> {
        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);
        let relation_key = relation_key(database_name, schema_name, table_name);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            match build_relation_with_dropped_constraint(
                &state,
                database_name,
                schema_name,
                table_name,
                constraint_name,
                cascade,
            ) {
                Ok(preview) => {
                    state.relations.insert(relation_key.clone(), preview);

                    if cascade {
                        // Find and drop dependent foreign keys in other tables
                        let mut dependent_updates = Vec::new();
                        for (other_key, other_rel) in &state.relations {
                            if other_key == &relation_key {
                                continue;
                            }

                            let mut other_preview = other_rel.clone();
                            let initial_len = other_preview.constraints.len();
                            other_preview.constraints.retain(|c| {
                                if let CatalogTableConstraintKind::ForeignKey = c.kind {
                                    let ref_db =
                                        c.referenced_database.as_deref().unwrap_or(database_name);
                                    let ref_sch =
                                        c.referenced_schema.as_deref().unwrap_or(schema_name);
                                    let ref_tab = c.referenced_table.as_deref().unwrap_or("");

                                    !(ref_db == database_name
                                        && ref_sch == schema_name
                                        && ref_tab == table_name)
                                } else {
                                    true
                                }
                            });

                            if other_preview.constraints.len() < initial_len {
                                dependent_updates.push((other_key.clone(), other_preview));
                            }
                        }

                        for (key, rel) in dependent_updates {
                            state.relations.insert(key, rel);
                        }
                    }
                }
                Err(e) => {
                    if if_exists && e.to_string().contains("not found") {
                        return Ok(format!(
                            "Constraint '{}' does not exist, skipping.",
                            constraint_name
                        ));
                    }
                    return Err(e);
                }
            }
        }

        self.persist().await?;
        Ok(format!(
            "Constraint '{}' dropped from '{}.{}.{}'.",
            constraint_name, database_name, schema_name, table_name
        ))
    }

    pub async fn alter_column(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
        column_name: &str,
        operation: AlterColumnOperation,
    ) -> Result<String> {
        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            let relation_key = relation_key(database_name, schema_name, table_name);
            let relation = state.relations.get_mut(&relation_key).ok_or_else(|| {
                anyhow::anyhow!(
                    "Table '{}.{}.{}' not found",
                    database_name,
                    schema_name,
                    table_name
                )
            })?;

            let column = relation
                .columns
                .iter_mut()
                .find(|c| c.name == column_name)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Column '{}' not found in table '{}.{}.{}'",
                        column_name,
                        database_name,
                        schema_name,
                        table_name
                    )
                })?;

            match operation {
                AlterColumnOperation::SetDataType { data_type } => {
                    column.data_type = data_type;
                }
                AlterColumnOperation::SetNotNull => {
                    column.nullable = false;
                }
                AlterColumnOperation::DropNotNull => {
                    column.nullable = true;
                }
                AlterColumnOperation::SetDefault { value } => {
                    column.default_value = Some(value);
                }
                AlterColumnOperation::DropDefault => {
                    column.default_value = None;
                }
            }
        }

        self.persist().await?;
        Ok(format!(
            "Column '{}' altered in '{}.{}.{}'.",
            column_name, database_name, schema_name, table_name
        ))
    }

    pub async fn rename_relation(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
        new_name: &str,
    ) -> Result<String> {
        validate_identifier(new_name)?;
        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            let old_key = relation_key(database_name, schema_name, name);
            let new_key = relation_key(database_name, schema_name, new_name);

            if !state.relations.contains_key(&old_key) {
                bail!(
                    "Relation '{}.{}.{}' not found",
                    database_name,
                    schema_name,
                    name
                );
            }

            if state.relations.contains_key(&new_key) {
                bail!(
                    "Relation '{}.{}.{}' already exists",
                    database_name,
                    schema_name,
                    new_name
                );
            }

            let mut relation = state.relations.remove(&old_key).unwrap();
            relation.name = new_name.to_string();

            // If it's a managed table, update the storage path as well to match the new name?
            // Actually, managed tables use a directory name derived from the table name often.
            // Let's see how register_managed_table does it.
            // It seems it takes a storage_path as argument.
            // If we rename physically in the engine, we should update the storage_path in metadata.

            state.relations.insert(new_key, relation);
        }

        self.persist().await?;
        Ok(format!(
            "Relation '{}.{}.{}' renamed to '{}' successfully.",
            database_name, schema_name, name, new_name
        ))
    }

    pub async fn rename_schema(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        name: &str,
        new_name: &str,
    ) -> Result<String> {
        validate_identifier(new_name)?;
        let database_name = database.unwrap_or(&session.database);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            let db = state
                .databases
                .get_mut(database_name)
                .ok_or_else(|| anyhow::anyhow!("Unknown database '{}'", database_name))?;

            if !db.schemas.contains(name) {
                bail!("Schema '{}.{}' not found", database_name, name);
            }

            if db.schemas.contains(new_name) {
                bail!("Schema '{}.{}' already exists", database_name, new_name);
            }

            // Update database schemas set
            db.schemas.remove(name);
            db.schemas.insert(new_name.to_string());

            // Update all relations in this schema
            let old_prefix = format!("{}.{}.", database_name, name);
            let new_prefix = format!("{}.{}.", database_name, new_name);

            let keys_to_update: Vec<String> = state
                .relations
                .keys()
                .filter(|k| k.starts_with(&old_prefix))
                .cloned()
                .collect();

            for old_key in keys_to_update {
                let mut relation = state.relations.remove(&old_key).unwrap();
                relation.schema = new_name.to_string();
                let new_key = format!("{}{}", new_prefix, relation.name);

                // If it's a managed table, we might need to update the storage path too.
                // But let's handle that in the engine for now or keep it simple.

                state.relations.insert(new_key, relation);
            }
        }

        self.persist().await?;
        Ok(format!(
            "Schema '{}.{}' renamed to '{}' successfully.",
            database_name, name, new_name
        ))
    }

    pub async fn update_relation_storage_path(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
        new_storage_path: &str,
    ) -> Result<String> {
        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            let key = relation_key(database_name, schema_name, name);
            let relation = state.relations.get_mut(&key).ok_or_else(|| {
                anyhow::anyhow!(
                    "Relation '{}.{}.{}' not found",
                    database_name,
                    schema_name,
                    name
                )
            })?;

            relation.storage_path = Some(new_storage_path.to_string());
        }

        self.persist().await?;
        Ok(format!(
            "Storage path for '{}.{}.{}' updated successfully.",
            database_name, schema_name, name
        ))
    }

    pub async fn register_external_table(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
        location: &str,
        format: ExternalStorageFormat,
        columns: Vec<CatalogColumn>,
    ) -> Result<String> {
        validate_identifier(table_name)?;
        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            let database = state
                .databases
                .get(database_name)
                .ok_or_else(|| anyhow::anyhow!("Unknown database '{}'", database_name))?;

            if !database.schemas.contains(schema_name) {
                bail!("Unknown schema '{}.{}'", database_name, schema_name);
            }

            let relation_key = relation_key(database_name, schema_name, table_name);
            if state.relations.contains_key(&relation_key) {
                bail!(
                    "Relation '{}.{}.{}' already exists",
                    database_name,
                    schema_name,
                    table_name
                );
            }

            state.relations.insert(
                relation_key,
                CatalogRelation {
                    database: database_name.to_string(),
                    schema: schema_name.to_string(),
                    name: table_name.to_string(),
                    kind: CatalogRelationKind::Table,
                    definition_sql: None,
                    storage_path: Some(location.to_string()),
                    external_format: Some(format),
                    columns,
                    constraints: Vec::new(),
                    indexes: Vec::new(),
                },
            );
        }

        self.persist().await?;
        Ok(format!(
            "External table '{}.{}.{}' registered successfully.",
            database_name, schema_name, table_name
        ))
    }

    pub async fn drop_relation(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
        kind: CatalogRelationKind,
        if_exists: bool,
    ) -> Result<String> {
        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            let key = relation_key(database_name, schema_name, name);
            if let Some(rel) = state.relations.get(&key) {
                if rel.kind != kind {
                    bail!(
                        "Relation '{}' is a {}, not a {:?}",
                        key,
                        if rel.kind == CatalogRelationKind::Table {
                            "table"
                        } else {
                            "view"
                        },
                        kind
                    );
                }
                state.relations.remove(&key);
            } else {
                if if_exists {
                    return Ok(format!(
                        "{} '{}' does not exist, skipping.",
                        if kind == CatalogRelationKind::Table {
                            "Table"
                        } else {
                            "View"
                        },
                        key
                    ));
                } else {
                    bail!(
                        "{} '{}' not found",
                        if kind == CatalogRelationKind::Table {
                            "Table"
                        } else {
                            "View"
                        },
                        key
                    );
                }
            }
        }

        self.persist().await?;
        Ok(format!(
            "{} '{}.{}.{}' dropped successfully.",
            if kind == CatalogRelationKind::Table {
                "Table"
            } else {
                "View"
            },
            database_name,
            schema_name,
            name
        ))
    }

    pub async fn drop_database(
        &self,
        session: &SessionContext,
        name: &str,
        if_exists: bool,
    ) -> Result<String> {
        validate_identifier(name)?;

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            if !state.users.get(&session.user).unwrap().is_admin {
                bail!("Only administrators can drop databases");
            }

            if name == "postgres" {
                bail!("Cannot drop the default 'postgres' database");
            }

            if let Some(_db) = state.databases.remove(name) {
                // Cascade: Remove all relations belonging to this database
                state
                    .relations
                    .retain(|key, _| !key.starts_with(&format!("{name}.")));
            } else {
                if if_exists {
                    return Ok(format!("Database '{}' does not exist, skipping.", name));
                } else {
                    bail!("Database '{}' not found", name);
                }
            }
        }

        self.persist().await?;
        Ok(format!("Database '{}' dropped successfully.", name))
    }

    pub async fn drop_schema(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> Result<String> {
        validate_identifier(name)?;
        let database_name = database.unwrap_or(&session.database);

        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            if name == "public" || name == "pg_catalog" || name == "information_schema" {
                bail!("Cannot drop system schema '{}'", name);
            }

            let (schema_exists, has_relations) = {
                let db = state
                    .databases
                    .get(database_name)
                    .ok_or_else(|| anyhow::anyhow!("Unknown database '{}'", database_name))?;

                let exists = db.schemas.contains(name);
                let has_rels = if exists {
                    let schema_prefix = format!("{database_name}.{name}.");
                    state
                        .relations
                        .keys()
                        .any(|k| k.starts_with(&schema_prefix))
                } else {
                    false
                };
                (exists, has_rels)
            };

            if schema_exists {
                if has_relations && !cascade {
                    bail!(
                        "Schema '{}.{}' is not empty and CASCADE was not specified",
                        database_name,
                        name
                    );
                }

                // Remove schema and its relations
                let db = state.databases.get_mut(database_name).unwrap();
                db.schemas.remove(name);
                let schema_prefix = format!("{database_name}.{name}.");
                state
                    .relations
                    .retain(|key, _| !key.starts_with(&schema_prefix));
            } else {
                if if_exists {
                    return Ok(format!(
                        "Schema '{}.{}' does not exist, skipping.",
                        database_name, name
                    ));
                } else {
                    bail!("Schema '{}.{}' not found", database_name, name);
                }
            }
        }

        self.persist().await?;
        Ok(format!(
            "Schema '{}.{}' dropped successfully.",
            database_name, name
        ))
    }

    async fn persist(&self) -> Result<()> {
        let state = self.state.read().await;
        self.persist_state(&state).await
    }

    async fn persist_state(&self, state: &CatalogState) -> Result<()> {
        let Some(path) = &self.catalog_path else {
            return Ok(());
        };

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let raw = serde_json::to_string_pretty(state)?;
        fs::write(path, raw).await?;
        Ok(())
    }

    pub async fn catalog_user(&self, user: &str) -> Result<CatalogUser> {
        let state = self.state.read().await;
        state
            .users
            .get(user)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Unknown user '{}'", user))
    }

    pub async fn authorize_role_assumption(&self, user: &str, role: &str) -> Result<()> {
        let state = self.state.read().await;
        let catalog_user = state
            .users
            .get(user)
            .ok_or_else(|| anyhow::anyhow!("Unknown user '{}'", user))?;

        if user == role {
            return Ok(());
        }

        let catalog_role = state
            .users
            .get(role)
            .ok_or_else(|| anyhow::anyhow!("Unknown role '{}'", role))?;

        if !catalog_user.is_admin && catalog_user.name != catalog_role.name {
            bail!(
                "User '{}' is not authorized to assume role '{}'",
                user,
                role
            );
        }

        Ok(())
    }

    pub async fn validate_credentials(
        &self,
        user: &str,
        password: Option<&str>,
    ) -> Result<CatalogUser> {
        let state = self.state.read().await;
        let catalog_user = state
            .users
            .get(user)
            .ok_or_else(|| anyhow::anyhow!("Unknown user '{}'", user))?;

        if let Some(expected) = &catalog_user.password {
            let provided =
                password.ok_or_else(|| anyhow::anyhow!("Password required for user '{}'", user))?;
            if provided != expected {
                bail!("Invalid credentials for user '{}'", user);
            }
        }

        Ok(catalog_user.clone())
    }

    async fn rotate_user_password(
        &self,
        session: &SessionContext,
        user_name: &str,
        password: &str,
    ) -> Result<String> {
        if password.is_empty() {
            bail!("Password must not be empty");
        }
        {
            let mut state = self.state.write().await;
            self._validate_session(&state, session)?;

            if !state.users.get(&session.user).unwrap().is_admin {
                bail!(
                    "User '{}' is not allowed to rotate credentials",
                    session.user
                );
            }

            let user = state
                .users
                .get_mut(user_name)
                .ok_or_else(|| anyhow::anyhow!("Unknown user '{}'", user_name))?;

            user.password = Some(password.to_string());
            user.password_version += 1;
            user.password_rotated_at_epoch_ms = Some(current_epoch_millis());
        }

        self.persist().await?;
        Ok(format!(
            "Password for user '{}' rotated successfully.",
            user_name
        ))
    }
}

fn bootstrap_state() -> CatalogState {
    let mut databases = BTreeMap::new();
    databases.insert(
        "postgres".to_string(),
        CatalogDatabase {
            name: "postgres".to_string(),
            schemas: BTreeSet::from(["public".to_string()]),
            owner: "postgres".to_string(),
            parameters: BTreeMap::new(),
        },
    );

    let mut users = BTreeMap::new();
    users.insert(
        "postgres".to_string(),
        CatalogUser {
            name: "postgres".to_string(),
            is_admin: true,
            password: Some("postgres".to_string()),
            password_version: 1,
            password_rotated_at_epoch_ms: Some(current_epoch_millis()),
        },
    );
    users.insert(
        "analytics_reader".to_string(),
        CatalogUser {
            name: "analytics_reader".to_string(),
            is_admin: false,
            password: Some("analytics_reader".to_string()),
            password_version: 1,
            password_rotated_at_epoch_ms: Some(current_epoch_millis()),
        },
    );
    users.insert(
        "analyticsdb_admin".to_string(),
        CatalogUser {
            name: "analyticsdb_admin".to_string(),
            is_admin: false,
            password: Some("analyticsdb_admin".to_string()),
            password_version: 1,
            password_rotated_at_epoch_ms: Some(current_epoch_millis()),
        },
    );

    let nodes = BTreeMap::new();
    let relations = BTreeMap::new();

    let config = Some(ClusterConfig {
        base_postgres_port: 5432,
        base_flight_sql_port: 50051,
        base_node_port: default_base_node_port(),
        catalog_path: DEFAULT_CATALOG_PATH.to_string(),
        tls_cert_path: None,
        tls_key_path: None,
        next_available_port_offset: 0,
    });

    CatalogState {
        databases,
        users,
        nodes,
        relations,
        aggregates: BTreeMap::new(),
        collations: BTreeMap::new(),
        conversions: BTreeMap::new(),
        functions: BTreeMap::new(),
        config,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_async_test<F>(future: F)
    where
        F: std::future::Future<Output = ()>,
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should build")
            .block_on(future);
    }

    #[test]
    fn join_cluster_advertises_plaintext_endpoint_without_tls_config() {
        run_async_test(async {
            let control_plane = ControlPlane::new_bootstrap();

            let response = control_plane
                .join_cluster(Some("worker-1"), Some("10.0.0.2"))
                .await
                .expect("join should succeed");

            let nodes = control_plane.list_nodes().await.expect("nodes should list");
            let worker = nodes
                .iter()
                .find(|node| node.id == response.node_id)
                .expect("joined node should be registered");

            assert_eq!(worker.endpoint, "http://10.0.0.2:50052");
            assert_eq!(
                worker.internal_endpoint.as_deref(),
                Some("http://10.0.0.2:60052")
            );
        });
    }

    #[test]
    fn join_cluster_advertises_tls_endpoint_when_tls_config_is_available() {
        run_async_test(async {
            let control_plane = ControlPlane::new_bootstrap();
            control_plane
                .set_tls_paths(
                    Some("certs/server.crt".to_string()),
                    Some("certs/server.key".to_string()),
                )
                .await
                .expect("tls paths should update");

            let response = control_plane
                .join_cluster(Some("worker-1"), Some("10.0.0.2"))
                .await
                .expect("join should succeed");

            let nodes = control_plane.list_nodes().await.expect("nodes should list");
            let worker = nodes
                .iter()
                .find(|node| node.id == response.node_id)
                .expect("joined node should be registered");

            assert_eq!(worker.endpoint, "https://10.0.0.2:50052");
            assert_eq!(
                worker.internal_endpoint.as_deref(),
                Some("http://10.0.0.2:60052")
            );
        });
    }
}

fn validate_identifier(value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("Identifiers must not be empty");
    }

    if value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Ok(());
    }

    bail!(
        "Identifier '{}' is invalid in the current prototype. Use ASCII letters, digits, or underscores.",
        value
    );
}

fn parse_qualified_name(
    raw: &str,
    default_database: Option<String>,
    default_schema: Option<String>,
) -> Result<(Option<String>, Option<String>, String)> {
    let parts = raw.split('.').collect::<Vec<_>>();

    match parts.as_slice() {
        [name] => Ok((default_database, default_schema, (*name).to_string())),
        [schema, name] => Ok((
            default_database,
            Some((*schema).to_string()),
            (*name).to_string(),
        )),
        [database, schema, name] => Ok((
            Some((*database).to_string()),
            Some((*schema).to_string()),
            (*name).to_string(),
        )),
        _ => bail!("Unsupported qualified name '{}'", raw),
    }
}

fn relation_key(database: &str, schema: &str, name: &str) -> String {
    format!("{database}.{schema}.{name}")
}

fn default_password_version() -> u64 {
    0
}

fn default_base_node_port() -> u16 {
    60051
}

fn current_epoch_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_millis()
}

fn parse_column_def(
    element: &sqlparser::ast::ColumnDef,
) -> (TableColumnDefinition, Vec<TableConstraintDefinition>) {
    let mut nullable = true;
    let mut default_value = None;
    let mut constraints = Vec::new();

    for option_def in &element.options {
        match &option_def.option {
            sqlparser::ast::ColumnOption::NotNull => nullable = false,
            sqlparser::ast::ColumnOption::Default(expr) => {
                default_value = Some(expr.to_string());
            }
            sqlparser::ast::ColumnOption::PrimaryKey(_) => {
                constraints.push(TableConstraintDefinition::PrimaryKey {
                    name: None,
                    columns: vec![element.name.to_string()],
                });
            }
            _ => {}
        }
    }

    (
        TableColumnDefinition {
            name: element.name.to_string(),
            data_type: element.data_type.to_string(),
            nullable,
            default_value,
        },
        constraints,
    )
}

pub fn parse_metadata_statement(sql: &str) -> Option<MetadataStatement> {
    let trimmed = sql.trim();
    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("SELECT ") && upper.contains(" FROM INFORMATION_SCHEMA.") {
        return parse_metadata_statement_fallback(sql);
    }

    let dialect = PostgreSqlDialect {};
    let statements = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return parse_metadata_statement_fallback(sql),
    };

    if statements.len() != 1 {
        return parse_metadata_statement_fallback(sql);
    }

    match &statements[0] {
        sqlparser::ast::Statement::CreateDatabase { db_name, .. } => {
            Some(MetadataStatement::CreateDatabase {
                name: db_name.to_string(),
            })
        }
        sqlparser::ast::Statement::CreateSchema {
            schema_name,
            if_not_exists: _,
            ..
        } => {
            let (db, name) = match schema_name {
                sqlparser::ast::SchemaName::Simple(n) => {
                    let idents: Vec<String> = n.0.iter().map(|i| i.to_string()).collect();
                    match idents.as_slice() {
                        [name] => (None, name.clone()),
                        [db, name] => (Some(db.clone()), name.clone()),
                        _ => return None,
                    }
                }
                _ => return None,
            };
            Some(MetadataStatement::CreateSchema { database: db, name })
        }
        sqlparser::ast::Statement::CreateTable(create_table) => {
            let name = &create_table.name;
            let idents: Vec<String> = name.0.iter().map(|i| i.to_string()).collect();
            let (database, schema, table_name) = match idents.as_slice() {
                [n] => (None, None, n.clone()),
                [s, n] => (None, Some(s.clone()), n.clone()),
                [d, s, n] => (Some(d.clone()), Some(s.clone()), n.clone()),
                _ => return None,
            };

            if create_table.external {
                return Some(MetadataStatement::CreateExternalTable {
                    database,
                    schema,
                    name: table_name,
                    format: ExternalStorageFormat::Parquet,
                    location: create_table.location.clone().unwrap_or_default(),
                });
            }

            if let Some(query) = &create_table.query {
                return Some(MetadataStatement::CreateTableAs {
                    database,
                    schema,
                    name: table_name,
                    query_sql: query.to_string(),
                });
            }

            let mut columns = Vec::new();
            let mut constraints = Vec::new();

            for element in &create_table.columns {
                let (col_def, col_constraints) = parse_column_def(element);
                columns.push(col_def);
                constraints.extend(col_constraints);
            }

            for constraint in &create_table.constraints {
                match constraint {
                    sqlparser::ast::TableConstraint::PrimaryKey(p) => {
                        constraints.push(TableConstraintDefinition::PrimaryKey {
                            name: p.name.as_ref().map(|i| i.to_string()),
                            columns: p.columns.iter().map(|c| c.to_string()).collect(),
                        });
                    }
                    sqlparser::ast::TableConstraint::ForeignKey(f) => {
                        let ft_idents: Vec<String> =
                            f.foreign_table.0.iter().map(|i| i.to_string()).collect();
                        let (f_db, f_sch, f_name) = match ft_idents.as_slice() {
                            [n] => (None, None, n.clone()),
                            [s, n] => (None, Some(s.clone()), n.clone()),
                            [d, s, n] => (Some(d.clone()), Some(s.clone()), n.clone()),
                            _ => return None,
                        };

                        constraints.push(TableConstraintDefinition::ForeignKey {
                            name: f.name.as_ref().map(|i| i.to_string()),
                            columns: f.columns.iter().map(|i| i.to_string()).collect(),
                            referenced_database: f_db,
                            referenced_schema: f_sch,
                            referenced_table: f_name,
                            referenced_columns: f
                                .referred_columns
                                .iter()
                                .map(|i| i.to_string())
                                .collect(),
                        });
                    }
                    sqlparser::ast::TableConstraint::Unique(u) => {
                        constraints.push(TableConstraintDefinition::Unique {
                            name: u.name.as_ref().map(|i| i.to_string()),
                            columns: u.columns.iter().map(|c| c.to_string()).collect(),
                        });
                    }
                    _ => {}
                }
            }

            Some(MetadataStatement::CreateTable {
                database,
                schema,
                name: table_name,
                columns,
                constraints,
            })
        }
        sqlparser::ast::Statement::Query(query) => {
            let mut query = query.clone();
            let select = match query.body.as_mut() {
                sqlparser::ast::SetExpr::Select(select) => select,
                _ => return None,
            };
            let select_into = select.into.take()?;
            let idents: Vec<String> = select_into.name.0.iter().map(|i| i.to_string()).collect();
            let (database, schema, table_name) = match idents.as_slice() {
                [n] => (None, None, n.clone()),
                [s, n] => (None, Some(s.clone()), n.clone()),
                [d, s, n] => (Some(d.clone()), Some(s.clone()), n.clone()),
                _ => return None,
            };
            Some(MetadataStatement::SelectInto {
                database,
                schema,
                name: table_name,
                query_sql: query.to_string(),
            })
        }
        sqlparser::ast::Statement::CreateFunction(create_func) => {
            let idents: Vec<String> = create_func.name.0.iter().map(|i| i.to_string()).collect();
            let (database, schema, func_name) = match idents.as_slice() {
                [n] => (None, None, n.clone()),
                [s, n] => (None, Some(s.clone()), n.clone()),
                [d, s, n] => (Some(d.clone()), Some(s.clone()), n.clone()),
                _ => return None,
            };

            Some(MetadataStatement::CreateFunction {
                database,
                schema,
                name: func_name,
                or_replace: create_func.or_replace,
                definition_sql: sql.to_string(),
            })
        }
        sqlparser::ast::Statement::Insert(insert) => {
            let table_name_obj = match &insert.table {
                sqlparser::ast::TableObject::TableName(n) => n,
                _ => return None,
            };
            let idents: Vec<String> = table_name_obj.0.iter().map(|i| i.to_string()).collect();
            let (database, schema, name) = match idents.as_slice() {
                [n] => (None, None, n.clone()),
                [s, n] => (None, Some(s.clone()), n.clone()),
                [d, s, n] => (Some(d.clone()), Some(s.clone()), n.clone()),
                _ => return None,
            };

            let columns = if insert.columns.is_empty() {
                None
            } else {
                Some(insert.columns.iter().map(|i| i.to_string()).collect())
            };

            let rows = match insert.source.as_deref() {
                Some(query) => match &*query.body {
                    sqlparser::ast::SetExpr::Values(values) => {
                        let mut result_rows = Vec::new();
                        for row in &values.rows {
                            let mut result_row = Vec::new();
                            for expr in row {
                                result_row.push(expr.to_string());
                            }
                            result_rows.push(result_row);
                        }
                        result_rows
                    }
                    _ => return None,
                },
                None => return None,
            };

            Some(MetadataStatement::InsertInto {
                database,
                schema,
                name,
                columns,
                rows,
            })
        }
        sqlparser::ast::Statement::CreateIndex(create_index) => {
            let idents: Vec<String> = create_index
                .table_name
                .0
                .iter()
                .map(|i| i.to_string())
                .collect();
            let (database, schema, table) = match idents.as_slice() {
                [n] => (None, None, n.clone()),
                [s, n] => (None, Some(s.clone()), n.clone()),
                [d, s, n] => (Some(d.clone()), Some(s.clone()), n.clone()),
                _ => return None,
            };

            let index_name = if let Some(name) = &create_index.name {
                name.0
                    .iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>()
                    .join(".")
            } else {
                return None;
            };

            Some(MetadataStatement::CreateIndex {
                database,
                schema,
                table,
                name: index_name,
                columns: create_index
                    .columns
                    .iter()
                    .map(|c| c.column.expr.to_string())
                    .collect(),
                unique: create_index.unique,
                concurrently: create_index.concurrently,
            })
        }
        sqlparser::ast::Statement::Delete(delete) => {
            let table_name_obj = match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(v) => match &v[0].relation {
                    sqlparser::ast::TableFactor::Table { name, .. } => name,
                    _ => return None,
                },
                _ => return None,
            };
            let idents: Vec<String> = table_name_obj.0.iter().map(|i| i.to_string()).collect();
            let (database, schema, name) = match idents.as_slice() {
                [n] => (None, None, n.clone()),
                [s, n] => (None, Some(s.clone()), n.clone()),
                [d, s, n] => (Some(d.clone()), Some(s.clone()), n.clone()),
                _ => return None,
            };

            Some(MetadataStatement::Delete {
                database,
                schema,
                name,
                selection_sql: delete.selection.as_ref().map(|e| e.to_string()),
            })
        }
        sqlparser::ast::Statement::Truncate(truncate) => {
            let name = &truncate.table_names[0].name;
            let idents: Vec<String> = name.0.iter().map(|i| i.to_string()).collect();
            let (database, schema, table_name) = match idents.as_slice() {
                [n] => (None, None, n.clone()),
                [s, n] => (None, Some(s.clone()), n.clone()),
                [d, s, n] => (Some(d.clone()), Some(s.clone()), n.clone()),
                _ => return None,
            };

            Some(MetadataStatement::Truncate {
                database,
                schema,
                name: table_name,
            })
        }
        sqlparser::ast::Statement::Update(update) => {
            let idents: Vec<String> = match &update.table.relation {
                sqlparser::ast::TableFactor::Table { name, .. } => {
                    name.0.iter().map(|i| i.to_string()).collect()
                }
                _ => return None,
            };
            let (database, schema, name) = match idents.as_slice() {
                [n] => (None, None, n.clone()),
                [s, n] => (None, Some(s.clone()), n.clone()),
                [d, s, n] => (Some(d.clone()), Some(s.clone()), n.clone()),
                _ => return None,
            };

            let mut result_assignments = Vec::new();
            for assignment in &update.assignments {
                let col = match &assignment.target {
                    sqlparser::ast::AssignmentTarget::ColumnName(name) => name
                        .0
                        .iter()
                        .map(|i| i.to_string())
                        .collect::<Vec<_>>()
                        .join("."),
                    _ => return None, // Unsupported assignment target (e.g. Tuple)
                };
                result_assignments.push((col, assignment.value.to_string()));
            }

            Some(MetadataStatement::Update {
                database,
                schema,
                name,
                assignments: result_assignments,
                selection_sql: update.selection.as_ref().map(|e| e.to_string()),
            })
        }
        sqlparser::ast::Statement::AlterTable(alter) => {
            let idents: Vec<String> = alter.name.0.iter().map(|i| i.to_string()).collect();
            let (database, schema, table_name) = match idents.as_slice() {
                [n] => (None, None, n.clone()),
                [s, n] => (None, Some(s.clone()), n.clone()),
                [d, s, n] => (Some(d.clone()), Some(s.clone()), n.clone()),
                _ => return None,
            };

            if alter.operations.is_empty() {
                return None;
            }

            match &alter.operations[0] {
                sqlparser::ast::AlterTableOperation::AddColumn { column_def, .. } => {
                    let (col_def, _) = parse_column_def(column_def);
                    Some(MetadataStatement::AlterTable {
                        database,
                        schema,
                        name: table_name,
                        operation: AlterTableOperation::AddColumn { column: col_def },
                    })
                }
                sqlparser::ast::AlterTableOperation::RenameTable {
                    table_name: new_table_name,
                } => {
                    let mut new_name = new_table_name.to_string();
                    if let Some(stripped) = new_name.strip_prefix("TO ") {
                        new_name = stripped.to_string();
                    }
                    Some(MetadataStatement::AlterTable {
                        database,
                        schema,
                        name: table_name,
                        operation: AlterTableOperation::RenameTable { new_name },
                    })
                }
                sqlparser::ast::AlterTableOperation::AddConstraint { constraint, .. } => {
                    let constraint_def = match constraint {
                        sqlparser::ast::TableConstraint::PrimaryKey(p) => {
                            TableConstraintDefinition::PrimaryKey {
                                name: p.name.as_ref().map(|i| i.to_string()),
                                columns: p.columns.iter().map(|i| i.to_string()).collect(),
                            }
                        }
                        sqlparser::ast::TableConstraint::Unique(u) => {
                            TableConstraintDefinition::Unique {
                                name: u.name.as_ref().map(|i| i.to_string()),
                                columns: u.columns.iter().map(|c| c.to_string()).collect(),
                            }
                        }
                        sqlparser::ast::TableConstraint::ForeignKey(f) => {
                            let ft_idents: Vec<String> =
                                f.foreign_table.0.iter().map(|i| i.to_string()).collect();
                            let (f_db, f_sch, f_name) = match ft_idents.as_slice() {
                                [n] => (None, None, n.clone()),
                                [s, n] => (None, Some(s.clone()), n.clone()),
                                [d, s, n] => (Some(d.clone()), Some(s.clone()), n.clone()),
                                _ => return None,
                            };

                            TableConstraintDefinition::ForeignKey {
                                name: f.name.as_ref().map(|i| i.to_string()),
                                columns: f.columns.iter().map(|i| i.to_string()).collect(),
                                referenced_database: f_db,
                                referenced_schema: f_sch,
                                referenced_table: f_name,
                                referenced_columns: f
                                    .referred_columns
                                    .iter()
                                    .map(|i| i.to_string())
                                    .collect(),
                            }
                        }
                        _ => return None,
                    };
                    Some(MetadataStatement::AlterTable {
                        database,
                        schema,
                        name: table_name,
                        operation: AlterTableOperation::AddConstraint {
                            constraint: constraint_def,
                        },
                    })
                }
                sqlparser::ast::AlterTableOperation::DropColumn {
                    column_names,
                    if_exists,
                    ..
                } => Some(MetadataStatement::AlterTable {
                    database,
                    schema,
                    name: table_name,
                    operation: AlterTableOperation::DropColumn {
                        column_name: column_names[0].to_string(),
                        if_exists: *if_exists,
                        cascade: false,
                    },
                }),
                sqlparser::ast::AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => Some(MetadataStatement::AlterTable {
                    database,
                    schema,
                    name: table_name,
                    operation: AlterTableOperation::RenameColumn {
                        old_name: old_column_name.to_string(),
                        new_name: new_column_name.to_string(),
                    },
                }),
                sqlparser::ast::AlterTableOperation::DropConstraint {
                    name,
                    if_exists,
                    drop_behavior,
                } => Some(MetadataStatement::AlterTable {
                    database,
                    schema,
                    name: table_name,
                    operation: AlterTableOperation::DropConstraint {
                        name: name.to_string(),
                        if_exists: *if_exists,
                        cascade: matches!(
                            drop_behavior,
                            Some(sqlparser::ast::DropBehavior::Cascade)
                        ),
                    },
                }),
                sqlparser::ast::AlterTableOperation::AlterColumn { column_name, op } => {
                    let operation = match op {
                        sqlparser::ast::AlterColumnOperation::SetDataType { data_type, .. } => {
                            AlterColumnOperation::SetDataType {
                                data_type: data_type.to_string(),
                            }
                        }
                        sqlparser::ast::AlterColumnOperation::SetNotNull => {
                            AlterColumnOperation::SetNotNull
                        }
                        sqlparser::ast::AlterColumnOperation::DropNotNull => {
                            AlterColumnOperation::DropNotNull
                        }
                        sqlparser::ast::AlterColumnOperation::SetDefault { value } => {
                            AlterColumnOperation::SetDefault {
                                value: value.to_string(),
                            }
                        }
                        sqlparser::ast::AlterColumnOperation::DropDefault => {
                            AlterColumnOperation::DropDefault
                        }
                        _ => return None,
                    };
                    Some(MetadataStatement::AlterTable {
                        database,
                        schema,
                        name: table_name,
                        operation: AlterTableOperation::AlterColumn {
                            column_name: column_name.to_string(),
                            operation,
                        },
                    })
                }
                _ => None,
            }
        }
        sqlparser::ast::Statement::AlterSchema(alter) => {
            let idents: Vec<String> = alter.name.0.iter().map(|i| i.to_string()).collect();
            let (database, schema_name) = match idents.as_slice() {
                [n] => (None, n.clone()),
                [d, n] => (Some(d.clone()), n.clone()),
                _ => return None,
            };

            if alter.operations.is_empty() {
                return None;
            }

            match &alter.operations[0] {
                sqlparser::ast::AlterSchemaOperation::Rename { name: new_name } => {
                    Some(MetadataStatement::AlterSchema {
                        database,
                        name: schema_name,
                        new_name: new_name.to_string(),
                    })
                }
                _ => None,
            }
        }
        sqlparser::ast::Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            ..
        } => {
            let name = &names[0];
            let idents: Vec<String> = name.0.iter().map(|i| i.to_string()).collect();

            match object_type {
                sqlparser::ast::ObjectType::Table => {
                    let (database, schema, obj_name) = match idents.as_slice() {
                        [n] => (None, None, n.clone()),
                        [s, n] => (None, Some(s.clone()), n.clone()),
                        [d, s, n] => (Some(d.clone()), Some(s.clone()), n.clone()),
                        _ => return None,
                    };
                    Some(MetadataStatement::DropTable {
                        database,
                        schema,
                        name: obj_name,
                        if_exists: *if_exists,
                        cascade: *cascade,
                    })
                }
                sqlparser::ast::ObjectType::View => {
                    let (database, schema, obj_name) = match idents.as_slice() {
                        [n] => (None, None, n.clone()),
                        [s, n] => (None, Some(s.clone()), n.clone()),
                        [d, s, n] => (Some(d.clone()), Some(s.clone()), n.clone()),
                        _ => return None,
                    };
                    Some(MetadataStatement::DropView {
                        database,
                        schema,
                        name: obj_name,
                        if_exists: *if_exists,
                        cascade: *cascade,
                    })
                }
                sqlparser::ast::ObjectType::Database => {
                    let (database, _schema, obj_name) = match idents.as_slice() {
                        [n] => (None::<String>, None::<String>, n.clone()),
                        [d, n] => (Some(d.clone()), None::<String>, n.clone()),
                        _ => return None,
                    };
                    // For DROP DATABASE, the name is the identifier itself
                    let db_name = database.unwrap_or(obj_name);
                    Some(MetadataStatement::DropDatabase {
                        name: db_name,
                        if_exists: *if_exists,
                    })
                }
                sqlparser::ast::ObjectType::Schema => {
                    let (database, schema_name) = match idents.as_slice() {
                        [n] => (None, n.clone()),
                        [d, n] => (Some(d.clone()), n.clone()),
                        _ => return None,
                    };
                    Some(MetadataStatement::DropSchema {
                        database,
                        name: schema_name,
                        if_exists: *if_exists,
                        cascade: *cascade,
                    })
                }
                _ => None,
            }
        }
        sqlparser::ast::Statement::DropFunction(drop_func) => {
            // Note: We only support dropping one function at a time for now
            let desc = drop_func.func_desc.first()?;
            let idents: Vec<String> = desc.name.0.iter().map(|i| i.to_string()).collect();
            let (database, schema, func_name) = match idents.as_slice() {
                [n] => (None, None, n.clone()),
                [s, n] => (None, Some(s.clone()), n.clone()),
                [d, s, n] => (Some(d.clone()), Some(s.clone()), n.clone()),
                _ => return None,
            };

            Some(MetadataStatement::DropFunction {
                database,
                schema,
                name: func_name,
                if_exists: drop_func.if_exists,
                cascade: matches!(
                    drop_func.drop_behavior,
                    Some(sqlparser::ast::DropBehavior::Cascade)
                ),
            })
        }
        _ => parse_metadata_statement_fallback(sql),
    }
}

fn parse_alter_database_remainder(remainder: &str) -> Option<(String, AlterDatabaseOperation)> {
    let tokens: Vec<&str> = remainder.split_whitespace().collect();
    if tokens.len() < 4 {
        return None;
    }

    let name = tokens[0].to_string();
    let op_upper = tokens[1].to_ascii_uppercase();

    if op_upper == "RENAME" && tokens[2].eq_ignore_ascii_case("TO") {
        return Some((
            name,
            AlterDatabaseOperation::Rename {
                new_name: tokens[3].to_string(),
            },
        ));
    }
    if op_upper == "OWNER" && tokens[2].eq_ignore_ascii_case("TO") {
        return Some((
            name,
            AlterDatabaseOperation::OwnerTo {
                new_owner: tokens[3].to_string(),
            },
        ));
    }
    if op_upper == "SET" {
        // SET name = value OR SET name TO value
        let param_name = tokens[2].to_string();
        let value =
            if tokens.len() >= 5 && (tokens[3] == "=" || tokens[3].eq_ignore_ascii_case("TO")) {
                tokens[4..].join(" ")
            } else {
                tokens[3..].join(" ")
            };
        return Some((
            name,
            AlterDatabaseOperation::SetParam {
                name: param_name,
                value,
            },
        ));
    }

    None
}

fn parse_alter_object_remainder(
    remainder: &str,
) -> Option<(Option<String>, Option<String>, String, AlterObjectOperation)> {
    let upper = remainder.to_ascii_uppercase();

    let (name_part, op_part) = if let Some(idx) = upper.find(" RENAME TO ") {
        (&remainder[..idx], &remainder[idx..])
    } else if let Some(idx) = upper.find(" OWNER TO ") {
        (&remainder[..idx], &remainder[idx..])
    } else if let Some(idx) = upper.find(" SET SCHEMA ") {
        (&remainder[..idx], &remainder[idx..])
    } else {
        return None;
    };

    let (database, schema, name) = parse_qualified_name(name_part.trim(), None, None).ok()?;
    let op_tokens: Vec<&str> = op_part.split_whitespace().collect();

    let operation = match op_tokens[0].to_ascii_uppercase().as_str() {
        "RENAME" => AlterObjectOperation::Rename {
            new_name: op_tokens[2].to_string(),
        },
        "OWNER" => AlterObjectOperation::OwnerTo {
            new_owner: op_tokens[2].to_string(),
        },
        "SET" => AlterObjectOperation::SetSchema {
            new_schema: op_tokens[2].to_string(),
        },
        _ => return None,
    };

    Some((database, schema, name, operation))
}

fn parse_metadata_statement_fallback(sql: &str) -> Option<MetadataStatement> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();

    if tokens.is_empty() {
        return None;
    }

    if upper == "BEGIN" || upper == "BEGIN TRANSACTION" || upper == "START TRANSACTION" {
        return Some(MetadataStatement::Begin);
    }
    if upper == "COMMIT" || upper == "END" || upper == "END TRANSACTION" {
        return Some(MetadataStatement::Commit);
    }
    if upper == "ROLLBACK"
        || upper == "ABORT"
        || upper == "ABORT WORK"
        || upper == "ABORT TRANSACTION"
    {
        return Some(MetadataStatement::Rollback);
    }

    if upper.starts_with("CREATE AGGREGATE ") {
        let (database, schema, name) =
            parse_qualified_name(trimmed["CREATE AGGREGATE ".len()..].trim(), None, None).ok()?;
        return Some(MetadataStatement::CreateAggregate {
            database,
            schema,
            name,
        });
    }
    if upper.starts_with("CREATE COLLATION ") {
        let (database, schema, name) =
            parse_qualified_name(trimmed["CREATE COLLATION ".len()..].trim(), None, None).ok()?;
        return Some(MetadataStatement::CreateCollation {
            database,
            schema,
            name,
        });
    }
    if upper.starts_with("CREATE CONVERSION ") {
        let (database, schema, name) =
            parse_qualified_name(trimmed["CREATE CONVERSION ".len()..].trim(), None, None).ok()?;
        return Some(MetadataStatement::CreateConversion {
            database,
            schema,
            name,
        });
    }

    if upper.starts_with("ALTER AGGREGATE ") {
        let remainder = trimmed["ALTER AGGREGATE ".len()..].trim();
        let (database, schema, name, op) = parse_alter_object_remainder(remainder)?;
        return Some(MetadataStatement::AlterAggregate {
            database,
            schema,
            name,
            operation: op,
        });
    }
    if upper.starts_with("ALTER COLLATION ") {
        let remainder = trimmed["ALTER COLLATION ".len()..].trim();
        let (database, schema, name, op) = parse_alter_object_remainder(remainder)?;
        return Some(MetadataStatement::AlterCollation {
            database,
            schema,
            name,
            operation: op,
        });
    }
    if upper.starts_with("ALTER CONVERSION ") {
        let remainder = trimmed["ALTER CONVERSION ".len()..].trim();
        let (database, schema, name, op) = parse_alter_object_remainder(remainder)?;
        return Some(MetadataStatement::AlterConversion {
            database,
            schema,
            name,
            operation: op,
        });
    }
    if upper.starts_with("ALTER FUNCTION ") {
        let remainder = trimmed["ALTER FUNCTION ".len()..].trim();
        let (database, schema, name, op) = parse_alter_object_remainder(remainder)?;
        return Some(MetadataStatement::AlterFunction {
            database,
            schema,
            name,
            operation: op,
        });
    }

    if upper.starts_with("ALTER DATABASE ") {
        let remainder = trimmed["ALTER DATABASE ".len()..].trim();
        let (name, operation) = parse_alter_database_remainder(remainder)?;
        return Some(MetadataStatement::AlterDatabase { name, operation });
    }

    if upper.starts_with("DROP FUNCTION ") {
        let mut cascade = false;
        let mut target_sql = trimmed;
        if upper.ends_with(" CASCADE") {
            cascade = true;
            target_sql = &trimmed[..trimmed.len() - " CASCADE".len()].trim();
        } else if upper.ends_with(" RESTRICT") {
            target_sql = &trimmed[..trimmed.len() - " RESTRICT".len()].trim();
        }

        let upper_target = target_sql.to_ascii_uppercase();
        let (database, schema, name, if_exists) = if upper_target.contains(" IF EXISTS ") {
            let name_remainder = if upper_target.starts_with("DROP FUNCTION IF EXISTS ") {
                &target_sql["DROP FUNCTION IF EXISTS ".len()..]
            } else {
                &target_sql["DROP FUNCTION ".len()..].replace("IF EXISTS ", "")
            };
            let (db, sch, n) = parse_qualified_name(name_remainder.trim(), None, None).ok()?;
            (db, sch, n, true)
        } else {
            let name_remainder = &target_sql["DROP FUNCTION ".len()..];
            let (db, sch, n) = parse_qualified_name(name_remainder.trim(), None, None).ok()?;
            (db, sch, n, false)
        };
        return Some(MetadataStatement::DropFunction {
            database,
            schema,
            name,
            if_exists,
            cascade,
        });
    }

    if upper.starts_with("SELECT ") && upper.contains(" FROM INFORMATION_SCHEMA.SCHEMATA") {
        return Some(MetadataStatement::InformationSchemaSchemata {
            sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SELECT ") && upper.contains(" FROM INFORMATION_SCHEMA.TABLES") {
        return Some(MetadataStatement::InformationSchemaTables {
            sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SELECT ") && upper.contains(" FROM INFORMATION_SCHEMA.COLUMNS") {
        return Some(MetadataStatement::InformationSchemaColumns {
            sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SELECT ") && upper.contains(" FROM INFORMATION_SCHEMA.VIEWS") {
        return Some(MetadataStatement::InformationSchemaViews {
            sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SELECT ") && upper.contains(" FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS")
    {
        return Some(MetadataStatement::InformationSchemaTableConstraints {
            sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SELECT ") && upper.contains(" FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE") {
        return Some(MetadataStatement::InformationSchemaKeyColumnUsage {
            sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SELECT ")
        && upper.contains(" FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE")
    {
        return Some(MetadataStatement::InformationSchemaConstraintColumnUsage {
            sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SELECT ")
        && upper.contains(" FROM INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE")
    {
        return Some(MetadataStatement::InformationSchemaConstraintTableUsage {
            sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SELECT ")
        && upper.contains(" FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS")
    {
        return Some(MetadataStatement::InformationSchemaReferentialConstraints {
            sql: trimmed.to_string(),
        });
    }

    if tokens.len() == 3
        && tokens[0].eq_ignore_ascii_case("CREATE")
        && tokens[1].eq_ignore_ascii_case("DATABASE")
    {
        return Some(MetadataStatement::CreateDatabase {
            name: tokens[2].to_string(),
        });
    }

    if tokens.len() == 3
        && tokens[0].eq_ignore_ascii_case("CREATE")
        && tokens[1].eq_ignore_ascii_case("SCHEMA")
    {
        if let Some((database, schema)) = tokens[2].split_once('.') {
            return Some(MetadataStatement::CreateSchema {
                database: Some(database.to_string()),
                name: schema.to_string(),
            });
        }

        return Some(MetadataStatement::CreateSchema {
            database: None,
            name: tokens[2].to_string(),
        });
    }

    if upper.starts_with("CREATE VIEW ") {
        let remainder = trimmed["CREATE VIEW ".len()..].trim();
        let upper_remainder = remainder.to_ascii_uppercase();
        let as_index = upper_remainder.find(" AS ")?;

        let raw_name = remainder[..as_index].trim();
        let definition_sql = remainder[as_index + 4..].trim();
        let Ok((database, schema, name)) = parse_qualified_name(raw_name, None, None) else {
            return None;
        };

        return Some(MetadataStatement::CreateView {
            database,
            schema,
            name,
            definition_sql: definition_sql.to_string(),
        });
    }

    if upper.starts_with("CREATE EXTERNAL TABLE ") {
        let remainder = trimmed["CREATE EXTERNAL TABLE ".len()..].trim();
        let upper_remainder = remainder.to_ascii_uppercase();

        // Expect: <name> STORED AS PARQUET LOCATION '<path>'
        let stored_as_parquet = " STORED AS PARQUET ";
        if let Some(stored_index) = upper_remainder.find(stored_as_parquet) {
            let raw_name = remainder[..stored_index].trim();
            let after_stored = &remainder[stored_index + stored_as_parquet.len()..];
            let upper_after = after_stored.to_ascii_uppercase();

            if let Some(loc_index) = upper_after.find("LOCATION ") {
                let raw_location = after_stored[loc_index + "LOCATION ".len()..].trim();
                let Ok(location) = parse_sql_single_quoted_literal(raw_location) else {
                    return None;
                };
                let Ok((database, schema, name)) = parse_qualified_name(raw_name, None, None)
                else {
                    return None;
                };

                return Some(MetadataStatement::CreateExternalTable {
                    database,
                    schema,
                    name,
                    format: ExternalStorageFormat::Parquet,
                    location,
                });
            }
        }

        return None;
    }

    if upper.starts_with("CREATE TABLE ") {
        let remainder = trimmed["CREATE TABLE ".len()..].trim();
        let upper_remainder = remainder.to_ascii_uppercase();

        if let Some(as_index) = upper_remainder.find(" AS ") {
            let raw_name = remainder[..as_index].trim();
            let query_sql = remainder[as_index + 4..].trim();
            let Ok((database, schema, name)) = parse_qualified_name(raw_name, None, None) else {
                return None;
            };

            return Some(MetadataStatement::CreateTableAs {
                database,
                schema,
                name,
                query_sql: query_sql.to_string(),
            });
        }

        let open_paren = remainder.find('(')?;
        if !remainder.ends_with(')') {
            return None;
        }

        let raw_name = remainder[..open_paren].trim();
        let raw_columns = &remainder[open_paren + 1..remainder.len() - 1];
        let Ok((database, schema, name)) = parse_qualified_name(raw_name, None, None) else {
            return None;
        };
        let Ok((columns, constraints)) = parse_table_columns(raw_columns) else {
            return None;
        };

        return Some(MetadataStatement::CreateTable {
            database,
            schema,
            name,
            columns,
            constraints,
        });
    }

    if upper.starts_with("INSERT INTO ") {
        let remainder = trimmed["INSERT INTO ".len()..].trim();
        let upper_remainder = remainder.to_ascii_uppercase();
        let values_index = upper_remainder.find(" VALUES ")?;

        let raw_target = remainder[..values_index].trim();
        let raw_rows = remainder[values_index + " VALUES ".len()..].trim();
        let Ok((raw_name, columns)) = parse_insert_target(raw_target) else {
            return None;
        };
        let Ok((database, schema, name)) = parse_qualified_name(&raw_name, None, None) else {
            return None;
        };
        let Ok(rows) = parse_insert_rows(raw_rows) else {
            return None;
        };

        return Some(MetadataStatement::InsertInto {
            database,
            schema,
            name,
            columns,
            rows,
        });
    }

    if tokens.len() == 2
        && tokens[0].eq_ignore_ascii_case("SHOW")
        && tokens[1].eq_ignore_ascii_case("DATABASES")
    {
        return Some(MetadataStatement::ShowDatabases);
    }

    if tokens.len() == 2
        && tokens[0].eq_ignore_ascii_case("SHOW")
        && tokens[1].eq_ignore_ascii_case("NODES")
    {
        return Some(MetadataStatement::ShowNodes);
    }

    if tokens.len() == 2
        && tokens[0].eq_ignore_ascii_case("SHOW")
        && tokens[1].eq_ignore_ascii_case("SCHEMAS")
    {
        return Some(MetadataStatement::ShowSchemas { database: None });
    }

    if tokens.len() == 4
        && tokens[0].eq_ignore_ascii_case("SHOW")
        && tokens[1].eq_ignore_ascii_case("SCHEMAS")
        && tokens[2].eq_ignore_ascii_case("FROM")
    {
        return Some(MetadataStatement::ShowSchemas {
            database: Some(tokens[3].to_string()),
        });
    }

    if tokens.len() == 2
        && tokens[0].eq_ignore_ascii_case("SHOW")
        && tokens[1].eq_ignore_ascii_case("TABLES")
    {
        return Some(MetadataStatement::ShowTables {
            database: None,
            schema: None,
        });
    }

    if tokens.len() == 4
        && tokens[0].eq_ignore_ascii_case("SHOW")
        && tokens[1].eq_ignore_ascii_case("TABLES")
        && tokens[2].eq_ignore_ascii_case("FROM")
    {
        let Ok((database, schema)) = parse_schema_scope(tokens[3]) else {
            return None;
        };
        return Some(MetadataStatement::ShowTables { database, schema });
    }

    if tokens.len() == 2
        && tokens[0].eq_ignore_ascii_case("SHOW")
        && tokens[1].eq_ignore_ascii_case("VIEWS")
    {
        return Some(MetadataStatement::ShowViews {
            database: None,
            schema: None,
        });
    }

    if tokens.len() == 4
        && tokens[0].eq_ignore_ascii_case("SHOW")
        && tokens[1].eq_ignore_ascii_case("VIEWS")
        && tokens[2].eq_ignore_ascii_case("FROM")
    {
        let Ok((database, schema)) = parse_schema_scope(tokens[3]) else {
            return None;
        };
        return Some(MetadataStatement::ShowViews { database, schema });
    }

    if tokens.len() == 4
        && tokens[0].eq_ignore_ascii_case("SHOW")
        && tokens[1].eq_ignore_ascii_case("COLUMNS")
        && tokens[2].eq_ignore_ascii_case("FROM")
    {
        let Ok((database, schema, name)) = parse_qualified_name(tokens[3], None, None) else {
            return None;
        };
        return Some(MetadataStatement::ShowColumns {
            database,
            schema,
            table: name,
        });
    }

    if tokens.len() == 2 && tokens[0].eq_ignore_ascii_case("DESCRIBE") {
        let Ok((database, schema, name)) = parse_qualified_name(tokens[1], None, None) else {
            return None;
        };
        return Some(MetadataStatement::ShowColumns {
            database,
            schema,
            table: name,
        });
    }

    if upper.starts_with("ALTER USER ") {
        let remainder = trimmed["ALTER USER ".len()..].trim();
        let upper_remainder = remainder.to_ascii_uppercase();
        let pass_idx = upper_remainder.find(" PASSWORD ")?;

        let user_name = remainder[..pass_idx].trim();
        let pass_val = remainder[pass_idx + " PASSWORD ".len()..].trim();

        let Ok(password) = parse_sql_single_quoted_literal(pass_val) else {
            return None;
        };

        return Some(MetadataStatement::AlterUserPassword {
            name: user_name.to_string(),
            password,
        });
    }

    if upper.starts_with("REINDEX ") {
        return parse_reindex_statement(trimmed);
    }

    None
}

fn parse_reindex_statement(sql: &str) -> Option<MetadataStatement> {
    let remainder = sql["REINDEX".len()..].trim();
    if remainder.is_empty() {
        return None;
    }

    let first = remainder.split_whitespace().next()?;
    let (target_kind, after_kind) = if first.eq_ignore_ascii_case("INDEX") {
        ("INDEX", remainder["INDEX".len()..].trim())
    } else if first.eq_ignore_ascii_case("TABLE") {
        ("TABLE", remainder["TABLE".len()..].trim())
    } else {
        ("TABLE", remainder)
    };

    if after_kind.is_empty() {
        return None;
    }

    let (concurrently, raw_name) = if after_kind.to_ascii_uppercase().starts_with("CONCURRENTLY ") {
        (true, after_kind["CONCURRENTLY ".len()..].trim())
    } else {
        (false, after_kind)
    };

    if raw_name.is_empty() || raw_name.split_whitespace().count() != 1 {
        return None;
    }

    let (database, schema, name) = parse_qualified_name(raw_name, None, None).ok()?;
    let target = match target_kind {
        "INDEX" => ReindexTarget::Index {
            database,
            schema,
            name,
            concurrently,
        },
        "TABLE" => ReindexTarget::Table {
            database,
            schema,
            name,
            concurrently,
        },
        _ => return None,
    };

    Some(MetadataStatement::Reindex { target })
}

fn parse_table_columns(
    raw: &str,
) -> Result<(Vec<TableColumnDefinition>, Vec<TableConstraintDefinition>)> {
    let mut columns = Vec::new();
    let mut constraints = Vec::new();

    for element in split_sql_top_level(raw, ',')? {
        if let Some(constraint) = parse_table_constraint_definition(&element)? {
            constraints.push(constraint);
            continue;
        }
        columns.push(parse_table_column_definition(&element)?);
    }

    Ok((columns, constraints))
}

fn parse_table_column_definition(column: &str) -> Result<TableColumnDefinition> {
    let tokens = column.split_whitespace().collect::<Vec<_>>();
    if tokens.len() < 2 {
        bail!(
            "Unsupported column definition '{}' in the current prototype",
            column
        );
    }

    let nullable = !(tokens.len() >= 4
        && tokens[tokens.len() - 2].eq_ignore_ascii_case("NOT")
        && tokens[tokens.len() - 1].eq_ignore_ascii_case("NULL"));

    let data_type_end = if nullable {
        tokens.len()
    } else {
        tokens.len() - 2
    };
    let data_type = tokens[1..data_type_end].join(" ");

    if data_type.is_empty() {
        bail!(
            "Unsupported column definition '{}' in the current prototype",
            column
        );
    }

    Ok(TableColumnDefinition {
        name: tokens[0].to_string(),
        data_type,
        nullable,
        default_value: None,
    })
}

fn parse_table_constraint_definition(raw: &str) -> Result<Option<TableConstraintDefinition>> {
    let trimmed = raw.trim();
    let upper = trimmed.to_ascii_uppercase();

    let (constraint_name, definition) = if upper.starts_with("CONSTRAINT ") {
        let rest = trimmed["CONSTRAINT ".len()..].trim();
        let (name, remainder) = rest.split_once(' ').ok_or_else(|| {
            anyhow::anyhow!(
                "Unsupported table constraint syntax '{}' in the current prototype",
                raw
            )
        })?;
        (Some(name.to_string()), remainder.trim())
    } else {
        (None, trimmed)
    };

    let definition_upper = definition.to_ascii_uppercase();
    if definition_upper.starts_with("PRIMARY KEY") {
        let open = definition.find('(').ok_or_else(|| {
            anyhow::anyhow!("PRIMARY KEY constraint requires column list in '{}'.", raw)
        })?;
        let close = definition.rfind(')').ok_or_else(|| {
            anyhow::anyhow!("PRIMARY KEY constraint requires closing ')' in '{}'.", raw)
        })?;
        let columns = split_sql_top_level(&definition[open + 1..close], ',')?;
        if columns.is_empty() {
            bail!(
                "PRIMARY KEY constraint requires at least one column in '{}'.",
                raw
            );
        }
        return Ok(Some(TableConstraintDefinition::PrimaryKey {
            name: constraint_name,
            columns,
        }));
    }

    if definition_upper.starts_with("FOREIGN KEY") {
        let open = definition.find('(').ok_or_else(|| {
            anyhow::anyhow!("FOREIGN KEY constraint requires column list in '{}'.", raw)
        })?;
        let close = definition.rfind(')').ok_or_else(|| {
            anyhow::anyhow!("FOREIGN KEY constraint requires closing ')' in '{}'.", raw)
        })?;
        let columns = split_sql_top_level(&definition[open + 1..close], ',')?;
        if columns.is_empty() {
            bail!(
                "FOREIGN KEY constraint requires at least one column in '{}'.",
                raw
            );
        }

        let after_columns = definition[close + 1..].trim();
        let upper_after = after_columns.to_ascii_uppercase();
        if !upper_after.starts_with("REFERENCES ") {
            bail!(
                "FOREIGN KEY constraint requires REFERENCES clause in '{}'.",
                raw
            );
        }

        let ref_remainder = after_columns["REFERENCES ".len()..].trim();
        let open_ref = ref_remainder.find('(').ok_or_else(|| {
            anyhow::anyhow!("FOREIGN KEY REFERENCES requires column list in '{}'.", raw)
        })?;
        let close_ref = ref_remainder.rfind(')').ok_or_else(|| {
            anyhow::anyhow!("FOREIGN KEY REFERENCES requires closing ')' in '{}'.", raw)
        })?;

        let raw_table = ref_remainder[..open_ref].trim();
        let (referenced_database, referenced_schema, referenced_table) =
            parse_qualified_name(raw_table, None, None)?;
        let referenced_columns = split_sql_top_level(&ref_remainder[open_ref + 1..close_ref], ',')?;

        return Ok(Some(TableConstraintDefinition::ForeignKey {
            name: constraint_name,
            columns,
            referenced_database,
            referenced_schema,
            referenced_table,
            referenced_columns,
        }));
    }

    Ok(None)
}

fn parse_insert_target(raw: &str) -> Result<(String, Option<Vec<String>>)> {
    let trimmed = raw.trim();
    if let Some(open) = trimmed.find('(') {
        if !trimmed.ends_with(')') {
            bail!(
                "Unsupported INSERT target '{}' in the current prototype",
                trimmed
            );
        }
        let name = trimmed[..open].trim();
        let columns = split_sql_top_level(&trimmed[open + 1..trimmed.len() - 1], ',')?
            .iter()
            .map(|column| column.trim().to_string())
            .collect::<Vec<_>>();

        if columns.is_empty() || columns.iter().any(|column| column.is_empty()) {
            bail!(
                "Unsupported INSERT target '{}' in the current prototype",
                trimmed
            );
        }

        return Ok((name.to_string(), Some(columns)));
    }

    Ok((trimmed.to_string(), None))
}

fn parse_insert_rows(raw: &str) -> Result<Vec<Vec<String>>> {
    let mut rows = Vec::new();
    for row_fragment in split_sql_top_level(raw, ',')? {
        let trimmed = row_fragment.trim();
        if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
            bail!(
                "Unsupported INSERT row fragment '{}' in the current prototype",
                trimmed
            );
        }
        let values = split_sql_top_level(&trimmed[1..trimmed.len() - 1], ',')?;
        rows.push(values);
    }
    Ok(rows)
}

fn parse_schema_scope(raw: &str) -> Result<(Option<String>, Option<String>)> {
    let parts = raw.split('.').collect::<Vec<_>>();

    match parts.as_slice() {
        [schema] => Ok((None, Some((*schema).to_string()))),
        [database, schema] => Ok((Some((*database).to_string()), Some((*schema).to_string()))),
        _ => bail!("Unsupported schema scope '{}'", raw),
    }
}

fn parse_sql_single_quoted_literal(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if !trimmed.starts_with('\'') || !trimmed.ends_with('\'') || trimmed.len() < 2 {
        bail!("Expected single-quoted SQL string literal, got '{}'.", raw);
    }

    let mut result = String::new();
    let mut chars = trimmed[1..trimmed.len() - 1].chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            if matches!(chars.peek(), Some('\'')) {
                let _ = chars.next();
                result.push('\'');
            } else {
                bail!("Unescaped quote in SQL string literal '{}'.", raw);
            }
        } else {
            result.push(ch);
        }
    }

    Ok(result)
}

fn split_sql_top_level(input: &str, delimiter: char) -> Result<Vec<String>> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut in_single_quote = false;
    let mut chars = input.char_indices().peekable();

    while let Some((index, character)) = chars.next() {
        if character == '\'' {
            if in_single_quote {
                if matches!(chars.peek(), Some((_, '\''))) {
                    let _ = chars.next();
                } else {
                    in_single_quote = false;
                }
            } else {
                in_single_quote = true;
            }
            continue;
        }

        if in_single_quote {
            continue;
        }

        match character {
            '(' => depth += 1,
            ')' => {
                if depth == 0 {
                    bail!("Unbalanced ')' in SQL fragment '{}'", input);
                }
                depth -= 1;
            }
            _ if character == delimiter && depth == 0 => {
                parts.push(input[start..index].trim().to_string());
                start = index + character.len_utf8();
            }
            _ => {}
        }
    }

    if in_single_quote || depth != 0 {
        bail!("Unbalanced SQL fragment '{}'", input);
    }

    let tail = input[start..].trim();
    if !tail.is_empty() {
        parts.push(tail.to_string());
    }

    Ok(parts)
}

fn schema_contains_index_name(
    state: &CatalogState,
    database_name: &str,
    schema_name: &str,
    index_name: &str,
    skip_relation_key: Option<&str>,
) -> bool {
    for (rel_key, relation) in &state.relations {
        if let Some(skip) = skip_relation_key {
            if rel_key == skip {
                continue;
            }
        }
        if relation.database == database_name && relation.schema == schema_name {
            if relation.indexes.iter().any(|i| i.name == index_name) {
                return true;
            }
        }
    }
    false
}

fn build_relation_with_catalog_constraint(
    state: &CatalogState,
    database_name: &str,
    schema_name: &str,
    table_name: &str,
    constraint: CatalogTableConstraint,
) -> Result<CatalogRelation> {
    let relation_key = relation_key(database_name, schema_name, table_name);
    let relation = state.relations.get(&relation_key).ok_or_else(|| {
        anyhow::anyhow!(
            "Table '{}.{}.{}' not found",
            database_name,
            schema_name,
            table_name
        )
    })?;

    if relation.kind != CatalogRelationKind::Table {
        bail!(
            "Relation '{}.{}.{}' is not a table",
            database_name,
            schema_name,
            table_name
        );
    }

    if relation
        .constraints
        .iter()
        .any(|c| c.name == constraint.name)
    {
        bail!(
            "Constraint '{}' already exists on table '{}.{}.{}'",
            constraint.name,
            database_name,
            schema_name,
            table_name
        );
    }

    // Check if index name already exists in schema (if this constraint has a backing index)
    if matches!(
        constraint.kind,
        CatalogTableConstraintKind::PrimaryKey | CatalogTableConstraintKind::Unique
    ) {
        if schema_contains_index_name(
            state,
            database_name,
            schema_name,
            &constraint.name,
            Some(&relation_key),
        ) {
            bail!(
                "Index '{}' already exists in schema '{}.{}'",
                constraint.name,
                database_name,
                schema_name
            );
        }
    }

    let mut preview = relation.clone();
    preview.constraints.push(constraint.clone());

    // Update indexes if it's PK or Unique
    if matches!(
        constraint.kind,
        CatalogTableConstraintKind::PrimaryKey | CatalogTableConstraintKind::Unique
    ) {
        preview.indexes.push(CatalogIndex {
            name: constraint.name.clone(),
            columns: constraint.columns.clone(),
            is_unique: true,
            is_primary: matches!(constraint.kind, CatalogTableConstraintKind::PrimaryKey),
        });
    }

    Ok(preview)
}

#[allow(dead_code)]
fn build_relation_with_added_constraint(
    state: &CatalogState,
    database_name: &str,
    schema_name: &str,
    table_name: &str,
    constraint_def: TableConstraintDefinition,
) -> Result<CatalogRelation> {
    let relation_key = relation_key(database_name, schema_name, table_name);
    let relation = state.relations.get(&relation_key).ok_or_else(|| {
        anyhow::anyhow!(
            "Table '{}.{}.{}' not found",
            database_name,
            schema_name,
            table_name
        )
    })?;

    if relation.kind != CatalogRelationKind::Table {
        bail!(
            "Relation '{}.{}.{}' is not a table",
            database_name,
            schema_name,
            table_name
        );
    }

    let (kind, name, columns, f_db, f_sch, f_table, f_cols) = match constraint_def {
        TableConstraintDefinition::PrimaryKey { name, columns } => (
            CatalogTableConstraintKind::PrimaryKey,
            name.unwrap_or_else(|| "auto_constraint".to_string()),
            columns,
            None,
            None,
            None,
            Vec::new(),
        ),
        TableConstraintDefinition::Unique { name, columns } => (
            CatalogTableConstraintKind::Unique,
            name.unwrap_or_else(|| "auto_constraint".to_string()),
            columns,
            None,
            None,
            None,
            Vec::new(),
        ),
        TableConstraintDefinition::ForeignKey {
            name,
            columns,
            referenced_database,
            referenced_schema,
            referenced_table,
            referenced_columns,
        } => (
            CatalogTableConstraintKind::ForeignKey,
            name.unwrap_or_else(|| "auto_constraint".to_string()),
            columns,
            referenced_database,
            referenced_schema,
            Some(referenced_table),
            referenced_columns,
        ),
    };

    let constraint = CatalogTableConstraint {
        name,
        kind,
        columns,
        referenced_database: f_db,
        referenced_schema: f_sch,
        referenced_table: f_table,
        referenced_columns: f_cols,
    };

    if relation
        .constraints
        .iter()
        .any(|existing| existing.name == constraint.name)
    {
        bail!(
            "Constraint '{}' already exists on table '{}.{}.{}'",
            constraint.name,
            database_name,
            schema_name,
            table_name
        );
    }

    let mut preview = relation.clone();
    preview.constraints.push(constraint);

    let new_indexes = indexes_from_constraints(table_name, &preview.constraints);
    for new_index in new_indexes {
        if preview
            .indexes
            .iter()
            .any(|existing| existing.name == new_index.name)
        {
            continue;
        }

        if schema_contains_index_name(
            state,
            database_name,
            schema_name,
            &new_index.name,
            Some(&relation_key),
        ) {
            bail!(
                "Index '{}' already exists in schema '{}.{}'",
                new_index.name,
                database_name,
                schema_name
            );
        }
        preview.indexes.push(new_index);
    }

    Ok(preview)
}

fn build_relation_with_dropped_constraint(
    state: &CatalogState,
    database_name: &str,
    schema_name: &str,
    table_name: &str,
    constraint_name: &str,
    cascade: bool,
) -> Result<CatalogRelation> {
    let relation_key = relation_key(database_name, schema_name, table_name);
    let relation = state.relations.get(&relation_key).ok_or_else(|| {
        anyhow::anyhow!(
            "Table '{}.{}.{}' not found",
            database_name,
            schema_name,
            table_name
        )
    })?;

    if relation.kind != CatalogRelationKind::Table {
        bail!(
            "Relation '{}.{}.{}' is not a table",
            database_name,
            schema_name,
            table_name
        );
    }

    // Check for dependencies if it's a PK or Unique constraint
    let mut is_referenced = false;
    if let Some(target_con) = relation
        .constraints
        .iter()
        .find(|c| c.name == constraint_name)
    {
        if matches!(
            target_con.kind,
            CatalogTableConstraintKind::PrimaryKey | CatalogTableConstraintKind::Unique
        ) {
            for (other_key, other_rel) in &state.relations {
                if other_key == &relation_key {
                    continue;
                }
                for con in &other_rel.constraints {
                    if let CatalogTableConstraintKind::ForeignKey = con.kind {
                        let ref_db = con
                            .referenced_database
                            .as_deref()
                            .unwrap_or(&other_rel.database);
                        let ref_sch = con
                            .referenced_schema
                            .as_deref()
                            .unwrap_or(&other_rel.schema);
                        let ref_tab = con.referenced_table.as_deref().unwrap_or("");

                        if ref_db == database_name
                            && ref_sch == schema_name
                            && ref_tab == table_name
                        {
                            is_referenced = true;
                            break;
                        }
                    }
                }
                if is_referenced {
                    break;
                }
            }
        }
    }

    if is_referenced && !cascade {
        bail!("Cannot drop constraint '{}' on table '{}.{}.{}' because other objects depend on it. Use CASCADE to drop dependent objects.", constraint_name, database_name, schema_name, table_name);
    }

    let mut preview = relation.clone();
    let original_len = preview.constraints.len();
    preview.constraints.retain(|c| c.name != constraint_name);

    if preview.constraints.len() == original_len {
        bail!(
            "Constraint '{}' not found on table '{}.{}.{}'",
            constraint_name,
            database_name,
            schema_name,
            table_name
        );
    }

    // Also remove any backing index with the same name
    preview.indexes.retain(|i| i.name != constraint_name);

    Ok(preview)
}

#[allow(dead_code)]
fn indexes_from_constraints(
    table_name: &str,
    constraints: &[CatalogTableConstraint],
) -> Vec<CatalogIndex> {
    constraints
        .iter()
        .filter_map(|constraint| match constraint.kind {
            CatalogTableConstraintKind::PrimaryKey => Some(CatalogIndex {
                name: constraint.name.clone(),
                columns: constraint.columns.clone(),
                is_unique: true,
                is_primary: true,
            }),
            CatalogTableConstraintKind::Unique => Some(CatalogIndex {
                name: constraint.name.clone(),
                columns: constraint.columns.clone(),
                is_unique: true,
                is_primary: false,
            }),
            CatalogTableConstraintKind::ForeignKey => None,
        })
        .map(|mut index| {
            if index.name == "auto_constraint" {
                index.name = format!("{}_{}_idx", table_name, index.columns.join("_"));
            }
            index
        })
        .collect()
}
