use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use analyticsdb_core::SessionContext;
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeRole {
    Control,
    Compute,
    Storage,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeStatus {
    Ready,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterNode {
    pub id: String,
    pub role: NodeRole,
    pub endpoint: String,
    pub status: NodeStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogDatabase {
    pub name: String,
    pub schemas: BTreeSet<String>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CatalogTableConstraintKind {
    PrimaryKey,
    ForeignKey,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogTableConstraint {
    pub name: String,
    pub kind: CatalogTableConstraintKind,
    pub columns: Vec<String>,
    #[serde(default)]
    pub referenced_database: Option<String>,
    #[serde(default)]
    pub referenced_schema: Option<String>,
    #[serde(default)]
    pub referenced_table: Option<String>,
    #[serde(default)]
    pub referenced_columns: Vec<String>,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableColumnDefinition {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterSnapshot {
    pub coordinator_node_id: String,
    pub nodes: Vec<ClusterNode>,
    pub databases: Vec<CatalogDatabase>,
    pub users: Vec<CatalogUser>,
    pub relations: Vec<CatalogRelation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryAdmission {
    pub query_id: String,
    pub coordinator_node_id: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct CatalogState {
    databases: BTreeMap<String, CatalogDatabase>,
    users: BTreeMap<String, CatalogUser>,
    nodes: BTreeMap<String, ClusterNode>,
    relations: BTreeMap<String, CatalogRelation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataStatement {
    CreateDatabase {
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
    ShowDatabases,
    ShowSchemas {
        database: Option<String>,
    },
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
        name: String,
    },
    DescribeRelation {
        database: Option<String>,
        schema: Option<String>,
        name: String,
    },
    PgCatalogTables {
        sql: String,
    },
    PgCatalogViews {
        sql: String,
    },
    PgCatalogNamespace {
        sql: String,
    },
    PgCatalogDatabase {
        sql: String,
    },
    PgCatalogRoles {
        sql: String,
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
    AlterUserPassword {
        name: String,
        password: String,
    },
}

pub struct ControlPlane {
    coordinator_node_id: String,
    catalog_path: Option<PathBuf>,
    state: RwLock<CatalogState>,
}

impl ControlPlane {
    pub fn new_bootstrap() -> Self {
        Self::from_state(None, bootstrap_state())
    }

    pub fn from_catalog_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let state = if path.exists() {
            let raw = fs::read_to_string(&path)?;
            if raw.trim().is_empty() {
                bootstrap_state()
            } else {
                serde_json::from_str::<CatalogState>(&raw)?
            }
        } else {
            bootstrap_state()
        };

        let control_plane = Self::from_state(Some(path), state);
        control_plane.persist()?;

        Ok(control_plane)
    }

    fn from_state(catalog_path: Option<PathBuf>, state: CatalogState) -> Self {
        let coordinator_node_id = "control-1".to_string();

        Self {
            coordinator_node_id,
            catalog_path,
            state: RwLock::new(state),
        }
    }

    pub fn admit_query(&self, session: &SessionContext) -> Result<QueryAdmission> {
        self.validate_session(session)?;

        Ok(QueryAdmission {
            query_id: format!("q-{}", Uuid::now_v7()),
            coordinator_node_id: self.coordinator_node_id.clone(),
        })
    }

    pub fn validate_session(&self, session: &SessionContext) -> Result<()> {
        let _ = self.catalog_user(&session.user)?;
        self.authorize_role_assumption(&session.user, &session.role)?;

        let state = self
            .state
            .read()
            .expect("control plane lock should not poison");

        let database = state
            .databases
            .get(&session.database)
            .ok_or_else(|| anyhow::anyhow!("Unknown database '{}'", session.database))?;

        if !database.schemas.contains(&session.schema) {
            bail!("Unknown schema '{}.{}'", session.database, session.schema);
        }

        Ok(())
    }

    pub fn validate_credentials(&self, user: &str, password: Option<&str>) -> Result<CatalogUser> {
        let catalog_user = self.catalog_user(user)?;

        if let Some(expected_password) = catalog_user.password.as_deref() {
            let provided_password = password
                .ok_or_else(|| anyhow::anyhow!("Missing credentials for user '{}'", user))?;
            if provided_password != expected_password {
                bail!("Invalid credentials for user '{}'", user);
            }
        }

        Ok(catalog_user)
    }

    pub fn catalog_user(&self, user: &str) -> Result<CatalogUser> {
        let state = self
            .state
            .read()
            .expect("control plane lock should not poison");

        state
            .users
            .get(user)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Unknown user '{}'", user))
    }

    pub fn authorize_role_assumption(&self, user: &str, role: &str) -> Result<()> {
        let state = self
            .state
            .read()
            .expect("control plane lock should not poison");

        let user_entry = state
            .users
            .get(user)
            .ok_or_else(|| anyhow::anyhow!("Unknown user '{}'", user))?;
        if !state.users.contains_key(role) {
            bail!("Unknown role '{}'", role);
        }

        if role != user && !user_entry.is_admin {
            bail!(
                "User '{}' is not allowed to assume role '{}' in the current prototype",
                user,
                role
            );
        }

        Ok(())
    }

    pub fn rotate_user_password(
        &self,
        session: &SessionContext,
        user: &str,
        new_password: &str,
    ) -> Result<String> {
        self.validate_session(session)?;
        if new_password.trim().is_empty() {
            bail!("Password must not be empty in the current prototype");
        }

        let acting_user = self.catalog_user(&session.user)?;
        if !acting_user.is_admin {
            bail!(
                "User '{}' is not allowed to rotate credentials in the current prototype",
                session.user
            );
        }

        {
            let mut state = self
                .state
                .write()
                .expect("control plane lock should not poison");
            let catalog_user = state
                .users
                .get_mut(user)
                .ok_or_else(|| anyhow::anyhow!("Unknown user '{}'", user))?;

            catalog_user.password = Some(new_password.to_string());
            catalog_user.password_version = catalog_user.password_version.saturating_add(1);
            catalog_user.password_rotated_at_epoch_ms = Some(current_epoch_millis());
        }

        self.persist()?;
        Ok(format!("Credentials rotated for user '{}'.", user))
    }

    pub fn execute_metadata_statement(
        &self,
        session: &SessionContext,
        statement: &MetadataStatement,
    ) -> Result<String> {
        self.validate_session(session)?;

        match statement {
            MetadataStatement::CreateDatabase { name } => self.create_database(name),
            MetadataStatement::CreateSchema { database, name } => {
                let database_name = database
                    .as_ref()
                    .map_or(session.database.as_str(), String::as_str);
                self.create_schema(database_name, name)
            }
            MetadataStatement::CreateView {
                database,
                schema,
                name,
                definition_sql,
            } => {
                let database_name = database
                    .as_ref()
                    .map_or(session.database.as_str(), String::as_str);
                let schema_name = schema
                    .as_ref()
                    .map_or(session.schema.as_str(), String::as_str);
                self.create_view(database_name, schema_name, name, definition_sql)
            }
            MetadataStatement::AlterUserPassword { name, password } => {
                self.rotate_user_password(session, name, password)
            }
            MetadataStatement::CreateTableAs { .. }
            | MetadataStatement::CreateTable { .. }
            | MetadataStatement::CreateExternalTable { .. }
            | MetadataStatement::InsertInto { .. } => {
                bail!("Managed table DDL and DML should be handled by the engine persistence flow")
            }
            MetadataStatement::ShowDatabases
            | MetadataStatement::ShowSchemas { .. }
            | MetadataStatement::ShowTables { .. }
            | MetadataStatement::ShowViews { .. }
            | MetadataStatement::ShowColumns { .. }
            | MetadataStatement::DescribeRelation { .. }
            | MetadataStatement::PgCatalogTables { .. }
            | MetadataStatement::PgCatalogViews { .. }
            | MetadataStatement::PgCatalogNamespace { .. }
            | MetadataStatement::PgCatalogDatabase { .. }
            | MetadataStatement::PgCatalogRoles { .. }
            | MetadataStatement::InformationSchemaSchemata { .. }
            | MetadataStatement::InformationSchemaTables { .. }
            | MetadataStatement::InformationSchemaColumns { .. }
            | MetadataStatement::InformationSchemaViews { .. }
            | MetadataStatement::InformationSchemaTableConstraints { .. }
            | MetadataStatement::InformationSchemaKeyColumnUsage { .. }
            | MetadataStatement::InformationSchemaConstraintColumnUsage { .. }
            | MetadataStatement::InformationSchemaConstraintTableUsage { .. }
            | MetadataStatement::InformationSchemaReferentialConstraints { .. } => {
                bail!("Listing statements should be handled through list metadata helpers")
            }
        }
    }

    pub fn list_databases(&self, session: &SessionContext) -> Result<Vec<String>> {
        self.validate_session(session)?;

        let state = self
            .state
            .read()
            .expect("control plane lock should not poison");

        Ok(state.databases.keys().cloned().collect())
    }

    pub fn list_schemas(
        &self,
        session: &SessionContext,
        database: Option<&str>,
    ) -> Result<Vec<String>> {
        self.validate_session(session)?;

        let database_name = database.unwrap_or(&session.database);
        let state = self
            .state
            .read()
            .expect("control plane lock should not poison");
        let database = state
            .databases
            .get(database_name)
            .ok_or_else(|| anyhow::anyhow!("Unknown database '{}'", database_name))?;

        Ok(database.schemas.iter().cloned().collect())
    }

    pub fn list_relations(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        kind: CatalogRelationKind,
    ) -> Result<Vec<CatalogRelation>> {
        self.validate_session(session)?;

        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);

        let state = self
            .state
            .read()
            .expect("control plane lock should not poison");
        let database = state
            .databases
            .get(database_name)
            .ok_or_else(|| anyhow::anyhow!("Unknown database '{}'", database_name))?;

        if !database.schemas.contains(schema_name) {
            bail!("Unknown schema '{}.{}'", database_name, schema_name);
        }

        Ok(state
            .relations
            .values()
            .filter(|relation| {
                relation.kind == kind
                    && relation.database == database_name
                    && relation.schema == schema_name
            })
            .cloned()
            .collect())
    }

    pub fn list_views_for_session(&self, session: &SessionContext) -> Result<Vec<CatalogRelation>> {
        self.list_relations(
            session,
            Some(session.database.as_str()),
            Some(session.schema.as_str()),
            CatalogRelationKind::View,
        )
    }

    pub fn list_tables_for_session(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<CatalogRelation>> {
        self.list_relations(
            session,
            Some(session.database.as_str()),
            Some(session.schema.as_str()),
            CatalogRelationKind::Table,
        )
    }

    pub fn relation_columns(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
    ) -> Result<Vec<CatalogColumn>> {
        let relation = self.find_relation(session, database, schema, name)?;
        Ok(relation.columns)
    }

    pub fn table_relation(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
    ) -> Result<CatalogRelation> {
        let relation = self.find_relation(session, database, schema, name)?;

        if relation.kind != CatalogRelationKind::Table {
            bail!(
                "Relation '{}.{}.{}' is not a managed table",
                relation.database,
                relation.schema,
                relation.name
            );
        }

        Ok(relation)
    }

    pub fn cluster_snapshot(&self) -> ClusterSnapshot {
        let state = self
            .state
            .read()
            .expect("control plane lock should not poison");

        ClusterSnapshot {
            coordinator_node_id: self.coordinator_node_id.clone(),
            nodes: state.nodes.values().cloned().collect(),
            databases: state.databases.values().cloned().collect(),
            users: state.users.values().cloned().collect(),
            relations: state.relations.values().cloned().collect(),
        }
    }

    fn create_database(&self, name: &str) -> Result<String> {
        validate_identifier(name)?;

        {
            let mut state = self
                .state
                .write()
                .expect("control plane lock should not poison");

            if state.databases.contains_key(name) {
                bail!("Database '{}' already exists", name);
            }

            state.databases.insert(
                name.to_string(),
                CatalogDatabase {
                    name: name.to_string(),
                    schemas: ["public".to_string()].into_iter().collect(),
                },
            );
        }

        self.persist()?;
        Ok(format!("Database '{name}' created successfully."))
    }

    fn create_schema(&self, database_name: &str, schema_name: &str) -> Result<String> {
        validate_identifier(schema_name)?;

        {
            let mut state = self
                .state
                .write()
                .expect("control plane lock should not poison");
            let database = state
                .databases
                .get_mut(database_name)
                .ok_or_else(|| anyhow::anyhow!("Unknown database '{}'", database_name))?;

            if database.schemas.contains(schema_name) {
                bail!("Schema '{}.{}' already exists", database_name, schema_name);
            }

            database.schemas.insert(schema_name.to_string());
        }

        self.persist()?;
        Ok(format!(
            "Schema '{}.{}' created successfully.",
            database_name, schema_name
        ))
    }

    fn create_view(
        &self,
        database_name: &str,
        schema_name: &str,
        view_name: &str,
        definition_sql: &str,
    ) -> Result<String> {
        validate_identifier(view_name)?;

        {
            let mut state = self
                .state
                .write()
                .expect("control plane lock should not poison");
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
                    columns: Vec::new(),
                    constraints: Vec::new(),
                },
            );
        }

        self.persist()?;
        Ok(format!(
            "View '{}.{}.{}' created successfully.",
            database_name, schema_name, view_name
        ))
    }

    pub fn managed_table_storage_path(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
    ) -> Result<PathBuf> {
        self.validate_session(session)?;
        validate_identifier(table_name)?;

        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);
        let catalog_path = self.catalog_path.as_ref().ok_or_else(|| {
            anyhow::anyhow!("Managed tables require --catalog-path in the current prototype")
        })?;

        let base_name = catalog_path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("analyticsdb-catalog");
        let parent_dir = catalog_path.parent().unwrap_or_else(|| Path::new("."));
        let data_dir = parent_dir.join(format!("{base_name}.managed"));

        Ok(data_dir.join(format!(
            "{database_name}__{schema_name}__{table_name}.table.json"
        )))
    }

    pub fn register_managed_table(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
        storage_path: &Path,
        columns: Vec<CatalogColumn>,
        constraints: Vec<CatalogTableConstraint>,
    ) -> Result<String> {
        self.validate_session(session)?;
        validate_identifier(table_name)?;

        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);

        {
            let mut state = self
                .state
                .write()
                .expect("control plane lock should not poison");
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
                    storage_path: Some(storage_path.to_string_lossy().into_owned()),
                    external_format: None,
                    columns,
                    constraints,
                },
            );
        }

        self.persist()?;
        Ok(format!(
            "Table '{}.{}.{}' created successfully.",
            database_name, schema_name, table_name
        ))
    }

    pub fn register_external_table(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
        location: &str,
        format: ExternalStorageFormat,
    ) -> Result<String> {
        self.validate_session(session)?;
        validate_identifier(table_name)?;

        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);

        {
            let mut state = self
                .state
                .write()
                .expect("control plane lock should not poison");
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
                    columns: Vec::new(),
                    constraints: Vec::new(),
                },
            );
        }

        self.persist()?;
        Ok(format!(
            "External table '{}.{}.{}' registered successfully.",
            database_name, schema_name, table_name
        ))
    }

    fn persist(&self) -> Result<()> {
        let Some(path) = &self.catalog_path else {
            return Ok(());
        };

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let state = self
            .state
            .read()
            .expect("control plane lock should not poison")
            .clone();
        let raw = serde_json::to_string_pretty(&state)?;
        fs::write(path, raw)?;

        Ok(())
    }

    fn find_relation(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
    ) -> Result<CatalogRelation> {
        self.validate_session(session)?;

        let database_name = database.unwrap_or(&session.database);
        let schema_name = schema.unwrap_or(&session.schema);
        let state = self
            .state
            .read()
            .expect("control plane lock should not poison");
        let relation_key = relation_key(database_name, schema_name, name);

        state.relations.get(&relation_key).cloned().ok_or_else(|| {
            anyhow::anyhow!(
                "Unknown relation '{}.{}.{}'",
                database_name,
                schema_name,
                name
            )
        })
    }
}

fn bootstrap_state() -> CatalogState {
    let mut databases = BTreeMap::new();
    databases.insert(
        "postgres".to_string(),
        CatalogDatabase {
            name: "postgres".to_string(),
            schemas: ["public".to_string(), "information_schema".to_string()]
                .into_iter()
                .collect(),
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
        "analyticsdb_admin".to_string(),
        CatalogUser {
            name: "analyticsdb_admin".to_string(),
            is_admin: true,
            password: Some("analyticsdb_admin".to_string()),
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

    let mut nodes = BTreeMap::new();
    nodes.insert(
        "control-1".to_string(),
        ClusterNode {
            id: "control-1".to_string(),
            role: NodeRole::Control,
            endpoint: "embedded://control-1".to_string(),
            status: NodeStatus::Ready,
        },
    );
    nodes.insert(
        "compute-1".to_string(),
        ClusterNode {
            id: "compute-1".to_string(),
            role: NodeRole::Compute,
            endpoint: "embedded://compute-1".to_string(),
            status: NodeStatus::Ready,
        },
    );

    CatalogState {
        databases,
        users,
        nodes,
        relations: BTreeMap::new(),
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

fn current_epoch_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_millis()
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
        let close = definition.find(')').ok_or_else(|| {
            anyhow::anyhow!("FOREIGN KEY constraint requires closing ')' in '{}'.", raw)
        })?;
        let columns = split_sql_top_level(&definition[open + 1..close], ',')?;
        let after_columns = definition[close + 1..].trim();
        let after_upper = after_columns.to_ascii_uppercase();
        let references_prefix = "REFERENCES ";
        if !after_upper.starts_with(references_prefix) {
            bail!(
                "FOREIGN KEY constraint requires REFERENCES clause in '{}'.",
                raw
            );
        }
        let ref_target_and_columns = after_columns[references_prefix.len()..].trim();
        let ref_open = ref_target_and_columns.find('(').ok_or_else(|| {
            anyhow::anyhow!(
                "REFERENCES clause requires referenced column list in '{}'.",
                raw
            )
        })?;
        let ref_close = ref_target_and_columns.rfind(')').ok_or_else(|| {
            anyhow::anyhow!("REFERENCES clause requires closing ')' in '{}'.", raw)
        })?;
        let ref_target = ref_target_and_columns[..ref_open].trim();
        let referenced_columns =
            split_sql_top_level(&ref_target_and_columns[ref_open + 1..ref_close], ',')?;
        let (referenced_database, referenced_schema, referenced_table) =
            parse_qualified_name(ref_target, None, None)?;

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

fn parse_insert_rows(raw: &str) -> Result<Vec<Vec<String>>> {
    let mut rows = Vec::new();
    let mut in_single_quote = false;
    let mut depth = 0usize;
    let mut row_start = None;
    let chars = raw.char_indices().collect::<Vec<_>>();
    let mut index = 0usize;

    while index < chars.len() {
        let (offset, character) = chars[index];

        if character == '\'' {
            if in_single_quote {
                if index + 1 < chars.len() && chars[index + 1].1 == '\'' {
                    index += 2;
                    continue;
                }
                in_single_quote = false;
            } else {
                in_single_quote = true;
            }
            index += 1;
            continue;
        }

        if in_single_quote {
            index += 1;
            continue;
        }

        match character {
            '(' => {
                if depth == 0 {
                    row_start = Some(offset + character.len_utf8());
                }
                depth += 1;
            }
            ')' => {
                if depth == 0 {
                    bail!("Unbalanced ')' in VALUES clause '{}'", raw);
                }

                depth -= 1;
                if depth == 0 {
                    let start = row_start.ok_or_else(|| {
                        anyhow::anyhow!("Malformed VALUES clause '{}': missing row start", raw)
                    })?;
                    rows.push(split_sql_top_level(&raw[start..offset], ',')?);
                    row_start = None;
                }
            }
            ',' if depth == 0 => {}
            whitespace if whitespace.is_whitespace() && depth == 0 => {}
            _ if depth == 0 => {
                bail!(
                    "Unsupported VALUES syntax '{}' in the current prototype",
                    raw
                )
            }
            _ => {}
        }

        index += 1;
    }

    if in_single_quote || depth != 0 {
        bail!("Unbalanced VALUES clause '{}'", raw);
    }

    if rows.is_empty() {
        bail!("VALUES clause must contain at least one row");
    }

    Ok(rows)
}

fn parse_insert_target(raw: &str) -> Result<(String, Option<Vec<String>>)> {
    let trimmed = raw.trim();

    if let Some(open_paren) = trimmed.find('(') {
        if !trimmed.ends_with(')') {
            bail!(
                "Unsupported INSERT target '{}' in the current prototype",
                trimmed
            );
        }

        let name = trimmed[..open_paren].trim();
        let columns_raw = &trimmed[open_paren + 1..trimmed.len() - 1];
        let columns = split_sql_top_level(columns_raw, ',')?
            .into_iter()
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

pub fn parse_metadata_statement(sql: &str) -> Option<MetadataStatement> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();

    if tokens.is_empty() {
        return None;
    }

    if upper.starts_with("SELECT ") && upper.contains(" FROM PG_CATALOG.PG_TABLES") {
        return Some(MetadataStatement::PgCatalogTables {
            sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SELECT ") && upper.contains(" FROM PG_CATALOG.PG_VIEWS") {
        return Some(MetadataStatement::PgCatalogViews {
            sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SELECT ") && upper.contains(" FROM PG_CATALOG.PG_NAMESPACE") {
        return Some(MetadataStatement::PgCatalogNamespace {
            sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SELECT ") && upper.contains(" FROM PG_CATALOG.PG_DATABASE") {
        return Some(MetadataStatement::PgCatalogDatabase {
            sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SELECT ") && upper.contains(" FROM PG_CATALOG.PG_ROLES") {
        return Some(MetadataStatement::PgCatalogRoles {
            sql: trimmed.to_string(),
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
            name,
        });
    }

    if tokens.len() == 2 && tokens[0].eq_ignore_ascii_case("DESCRIBE") {
        let Ok((database, schema, name)) = parse_qualified_name(tokens[1], None, None) else {
            return None;
        };

        return Some(MetadataStatement::DescribeRelation {
            database,
            schema,
            name,
        });
    }

    if upper.starts_with("ALTER USER ") {
        let remainder = trimmed["ALTER USER ".len()..].trim();
        let upper_remainder = remainder.to_ascii_uppercase();
        let password_index = upper_remainder.find(" PASSWORD ")?;
        let user_name = remainder[..password_index].trim();
        if user_name.is_empty() {
            return None;
        }
        let raw_password = remainder[password_index + " PASSWORD ".len()..].trim();
        let Ok(password) = parse_sql_single_quoted_literal(raw_password) else {
            return None;
        };

        return Some(MetadataStatement::AlterUserPassword {
            name: user_name.to_string(),
            password,
        });
    }

    None
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use analyticsdb_core::{Protocol, SessionContext};
    use uuid::Uuid;

    use super::{
        parse_metadata_statement, CatalogRelationKind, ControlPlane, MetadataStatement, NodeRole,
        QueryAdmission, TableColumnDefinition, TableConstraintDefinition,
    };

    fn default_session() -> SessionContext {
        SessionContext {
            protocol: Protocol::Embedded,
            ..SessionContext::default()
        }
    }

    fn temp_catalog_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("analyticsdb-{name}-{}.json", Uuid::now_v7()));
        path
    }

    #[test]
    fn admits_bootstrap_session_and_generates_query_id() {
        let control_plane = ControlPlane::new_bootstrap();
        let admission = control_plane
            .admit_query(&default_session())
            .expect("bootstrap session should be admitted");

        assert!(matches!(
            admission,
            QueryAdmission {
                coordinator_node_id,
                ..
            } if coordinator_node_id == "control-1"
        ));
        assert!(admission.query_id.starts_with("q-"));
    }

    #[test]
    fn rejects_unknown_database() {
        let control_plane = ControlPlane::new_bootstrap();
        let mut session = default_session();
        session.database = "missing".to_string();

        let error = control_plane
            .validate_session(&session)
            .expect_err("unknown database should fail");

        assert!(error.to_string().contains("Unknown database"));
    }

    #[test]
    fn rejects_unknown_role_in_session_validation() {
        let control_plane = ControlPlane::new_bootstrap();
        let mut session = default_session();
        session.role = "missing_role".to_string();

        let error = control_plane
            .validate_session(&session)
            .expect_err("unknown role should fail validation");

        assert!(error.to_string().contains("Unknown role"));
    }

    #[test]
    fn rejects_non_admin_role_assumption() {
        let control_plane = ControlPlane::new_bootstrap();
        let mut session = default_session();
        session.user = "analytics_reader".to_string();
        session.role = "postgres".to_string();

        let error = control_plane
            .validate_session(&session)
            .expect_err("non-admin user should not assume postgres role");

        assert!(error.to_string().contains("is not allowed to assume role"));
    }

    #[test]
    fn allows_admin_role_assumption() {
        let control_plane = ControlPlane::new_bootstrap();
        let mut session = default_session();
        session.user = "analyticsdb_admin".to_string();
        session.role = "postgres".to_string();

        control_plane
            .validate_session(&session)
            .expect("admin role assumption should be allowed");
    }

    #[test]
    fn validates_credentials_with_unknown_user() {
        let control_plane = ControlPlane::new_bootstrap();
        let error = control_plane
            .validate_credentials("missing", Some("secret"))
            .expect_err("unknown user must fail credentials validation");

        assert!(error.to_string().contains("Unknown user"));
    }

    #[test]
    fn validates_credentials_with_expected_bootstrap_password() {
        let control_plane = ControlPlane::new_bootstrap();
        let user = control_plane
            .validate_credentials("postgres", Some("postgres"))
            .expect("postgres bootstrap password should be accepted");

        assert_eq!(user.name, "postgres");
    }

    #[test]
    fn rejects_invalid_bootstrap_password() {
        let control_plane = ControlPlane::new_bootstrap();
        let error = control_plane
            .validate_credentials("postgres", Some("wrong-password"))
            .expect_err("wrong password should be rejected");

        assert!(error.to_string().contains("Invalid credentials"));
    }

    #[test]
    fn rejects_missing_password_for_passworded_bootstrap_user() {
        let control_plane = ControlPlane::new_bootstrap();
        let error = control_plane
            .validate_credentials("postgres", None)
            .expect_err("missing password should be rejected");

        assert!(error.to_string().contains("Missing credentials"));
    }

    #[test]
    fn rotates_password_and_invalidates_previous_credentials() {
        let path = temp_catalog_path("password-rotation");
        let control_plane = ControlPlane::from_catalog_path(&path).expect("catalog should load");

        let before = control_plane
            .catalog_user("analytics_reader")
            .expect("reader user should exist before rotation");
        assert_eq!(before.password_version, 1);

        let message = control_plane
            .rotate_user_password(&default_session(), "analytics_reader", "reader-next")
            .expect("password rotation should succeed");
        assert!(message.contains("Credentials rotated"));

        let stale = control_plane
            .validate_credentials("analytics_reader", Some("analytics_reader"))
            .expect_err("old password should be rejected after rotation");
        assert!(stale.to_string().contains("Invalid credentials"));

        let updated = control_plane
            .validate_credentials("analytics_reader", Some("reader-next"))
            .expect("new password should be accepted");
        assert_eq!(updated.password_version, 2);
        assert!(updated.password_rotated_at_epoch_ms.is_some());
    }

    #[test]
    fn rejects_password_rotation_for_non_admin_user() {
        let control_plane = ControlPlane::new_bootstrap();
        let mut session = default_session();
        session.user = "analytics_reader".to_string();
        session.role = "analytics_reader".to_string();

        let error = control_plane
            .rotate_user_password(&session, "postgres", "next")
            .expect_err("non-admin should not rotate passwords");
        assert!(error
            .to_string()
            .contains("not allowed to rotate credentials"));
    }

    #[test]
    fn metadata_statement_alter_user_password_rotates_credentials() {
        let control_plane = ControlPlane::new_bootstrap();
        let message = control_plane
            .execute_metadata_statement(
                &default_session(),
                &MetadataStatement::AlterUserPassword {
                    name: "analytics_reader".to_string(),
                    password: "reader-sql-rotated".to_string(),
                },
            )
            .expect("admin ALTER USER PASSWORD should succeed");
        assert!(message.contains("Credentials rotated"));

        let stale = control_plane
            .validate_credentials("analytics_reader", Some("analytics_reader"))
            .expect_err("old password should be invalidated");
        assert!(stale.to_string().contains("Invalid credentials"));

        control_plane
            .validate_credentials("analytics_reader", Some("reader-sql-rotated"))
            .expect("new rotated password should be accepted");
    }

    #[test]
    fn metadata_statement_alter_user_password_rejects_non_admin_actor() {
        let control_plane = ControlPlane::new_bootstrap();
        let mut reader_session = default_session();
        reader_session.user = "analytics_reader".to_string();
        reader_session.role = "analytics_reader".to_string();

        let error = control_plane
            .execute_metadata_statement(
                &reader_session,
                &MetadataStatement::AlterUserPassword {
                    name: "postgres".to_string(),
                    password: "pwned".to_string(),
                },
            )
            .expect_err("non-admin ALTER USER PASSWORD should fail");
        assert!(error
            .to_string()
            .contains("not allowed to rotate credentials"));
    }

    #[test]
    fn exposes_bootstrap_cluster_snapshot() {
        let control_plane = ControlPlane::new_bootstrap();
        let snapshot = control_plane.cluster_snapshot();

        assert_eq!(snapshot.coordinator_node_id, "control-1");
        assert!(snapshot
            .nodes
            .iter()
            .any(|node| node.role == NodeRole::Control && node.id == "control-1"));
        assert!(snapshot
            .databases
            .iter()
            .any(|database| database.name == "postgres"));
        assert!(snapshot.relations.is_empty());
    }

    #[test]
    fn persists_created_database_and_schema() {
        let path = temp_catalog_path("catalog");
        let control_plane = ControlPlane::from_catalog_path(&path).expect("catalog should load");

        control_plane
            .execute_metadata_statement(
                &default_session(),
                &MetadataStatement::CreateDatabase {
                    name: "analytics".to_string(),
                },
            )
            .expect("database creation should succeed");

        control_plane
            .execute_metadata_statement(
                &default_session(),
                &MetadataStatement::CreateSchema {
                    database: Some("analytics".to_string()),
                    name: "reporting".to_string(),
                },
            )
            .expect("schema creation should succeed");

        let reloaded = ControlPlane::from_catalog_path(&path).expect("catalog should reload");
        let databases = reloaded
            .list_databases(&default_session())
            .expect("databases should list");
        let schemas = reloaded
            .list_schemas(&default_session(), Some("analytics"))
            .expect("schemas should list");

        assert!(databases.iter().any(|database| database == "analytics"));
        assert!(schemas.iter().any(|schema| schema == "reporting"));
    }

    #[test]
    fn persists_created_view() {
        let path = temp_catalog_path("view");
        let control_plane = ControlPlane::from_catalog_path(&path).expect("catalog should load");

        control_plane
            .execute_metadata_statement(
                &default_session(),
                &MetadataStatement::CreateView {
                    database: None,
                    schema: None,
                    name: "daily_metrics".to_string(),
                    definition_sql: "SELECT 7 AS metric".to_string(),
                },
            )
            .expect("view creation should succeed");

        let reloaded = ControlPlane::from_catalog_path(&path).expect("catalog should reload");
        let views = reloaded
            .list_relations(
                &default_session(),
                Some("postgres"),
                Some("public"),
                CatalogRelationKind::View,
            )
            .expect("views should list");

        assert!(views.iter().any(|view| {
            view.name == "daily_metrics"
                && view.definition_sql.as_deref() == Some("SELECT 7 AS metric")
        }));
    }

    #[test]
    fn parses_metadata_sql_subset() {
        assert_eq!(
            parse_metadata_statement("CREATE DATABASE analytics;"),
            Some(MetadataStatement::CreateDatabase {
                name: "analytics".to_string()
            })
        );
        assert_eq!(
            parse_metadata_statement("SHOW SCHEMAS FROM analytics"),
            Some(MetadataStatement::ShowSchemas {
                database: Some("analytics".to_string())
            })
        );
        assert_eq!(
            parse_metadata_statement("CREATE VIEW reporting.daily_metrics AS SELECT 7 AS metric"),
            Some(MetadataStatement::CreateView {
                database: None,
                schema: Some("reporting".to_string()),
                name: "daily_metrics".to_string(),
                definition_sql: "SELECT 7 AS metric".to_string()
            })
        );
        assert_eq!(
            parse_metadata_statement("SHOW VIEWS"),
            Some(MetadataStatement::ShowViews {
                database: None,
                schema: None
            })
        );
        assert_eq!(
            parse_metadata_statement("CREATE TABLE reporting.fact_metrics AS SELECT 1 AS metric"),
            Some(MetadataStatement::CreateTableAs {
                database: None,
                schema: Some("reporting".to_string()),
                name: "fact_metrics".to_string(),
                query_sql: "SELECT 1 AS metric".to_string()
            })
        );
        assert_eq!(
            parse_metadata_statement(
                "CREATE TABLE reporting.fact_metrics (metric BIGINT NOT NULL, status TEXT)"
            ),
            Some(MetadataStatement::CreateTable {
                database: None,
                schema: Some("reporting".to_string()),
                name: "fact_metrics".to_string(),
                columns: vec![
                    TableColumnDefinition {
                        name: "metric".to_string(),
                        data_type: "BIGINT".to_string(),
                        nullable: false,
                    },
                    TableColumnDefinition {
                        name: "status".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: true,
                    }
                ],
                constraints: Vec::new(),
            })
        );
        assert_eq!(
            parse_metadata_statement(
                "CREATE TABLE reporting.fact_events (metric_id BIGINT NOT NULL, CONSTRAINT fact_events_pkey PRIMARY KEY (metric_id), CONSTRAINT fact_events_metric_fk FOREIGN KEY (metric_id) REFERENCES reporting.fact_metrics(metric))"
            ),
            Some(MetadataStatement::CreateTable {
                database: None,
                schema: Some("reporting".to_string()),
                name: "fact_events".to_string(),
                columns: vec![TableColumnDefinition {
                    name: "metric_id".to_string(),
                    data_type: "BIGINT".to_string(),
                    nullable: false,
                }],
                constraints: vec![
                    TableConstraintDefinition::PrimaryKey {
                        name: Some("fact_events_pkey".to_string()),
                        columns: vec!["metric_id".to_string()],
                    },
                    TableConstraintDefinition::ForeignKey {
                        name: Some("fact_events_metric_fk".to_string()),
                        columns: vec!["metric_id".to_string()],
                        referenced_database: None,
                        referenced_schema: Some("reporting".to_string()),
                        referenced_table: "fact_metrics".to_string(),
                        referenced_columns: vec!["metric".to_string()],
                    },
                ],
            })
        );
        assert_eq!(
            parse_metadata_statement(
                "INSERT INTO reporting.fact_metrics VALUES (11, 'ok'), (12, 'warn')"
            ),
            Some(MetadataStatement::InsertInto {
                database: None,
                schema: Some("reporting".to_string()),
                name: "fact_metrics".to_string(),
                columns: None,
                rows: vec![
                    vec!["11".to_string(), "'ok'".to_string()],
                    vec!["12".to_string(), "'warn'".to_string()]
                ]
            })
        );
        assert_eq!(
            parse_metadata_statement(
                "INSERT INTO reporting.fact_metrics (status, metric) VALUES ('ok', 11)"
            ),
            Some(MetadataStatement::InsertInto {
                database: None,
                schema: Some("reporting".to_string()),
                name: "fact_metrics".to_string(),
                columns: Some(vec!["status".to_string(), "metric".to_string()]),
                rows: vec![vec!["'ok'".to_string(), "11".to_string()]]
            })
        );
        assert_eq!(
            parse_metadata_statement("SHOW TABLES FROM analytics.reporting"),
            Some(MetadataStatement::ShowTables {
                database: Some("analytics".to_string()),
                schema: Some("reporting".to_string())
            })
        );
        assert_eq!(
            parse_metadata_statement("SHOW VIEWS FROM reporting"),
            Some(MetadataStatement::ShowViews {
                database: None,
                schema: Some("reporting".to_string())
            })
        );
        assert_eq!(
            parse_metadata_statement("ALTER USER analytics_reader PASSWORD 'reader-next'"),
            Some(MetadataStatement::AlterUserPassword {
                name: "analytics_reader".to_string(),
                password: "reader-next".to_string(),
            })
        );
        assert_eq!(
            parse_metadata_statement("ALTER USER analytics_reader PASSWORD 'reader''s next'"),
            Some(MetadataStatement::AlterUserPassword {
                name: "analytics_reader".to_string(),
                password: "reader's next".to_string(),
            })
        );
        assert_eq!(
            parse_metadata_statement("ALTER USER analytics_reader PASSWORD reader-next"),
            None
        );
        assert_eq!(
            parse_metadata_statement("SELECT * FROM pg_catalog.pg_tables"),
            Some(MetadataStatement::PgCatalogTables {
                sql: "SELECT * FROM pg_catalog.pg_tables".to_string(),
            })
        );
        assert_eq!(
            parse_metadata_statement("SELECT * FROM pg_catalog.pg_views"),
            Some(MetadataStatement::PgCatalogViews {
                sql: "SELECT * FROM pg_catalog.pg_views".to_string(),
            })
        );
        assert_eq!(
            parse_metadata_statement("SELECT * FROM pg_catalog.pg_namespace"),
            Some(MetadataStatement::PgCatalogNamespace {
                sql: "SELECT * FROM pg_catalog.pg_namespace".to_string(),
            })
        );
        assert_eq!(
            parse_metadata_statement("SELECT * FROM pg_catalog.pg_database"),
            Some(MetadataStatement::PgCatalogDatabase {
                sql: "SELECT * FROM pg_catalog.pg_database".to_string(),
            })
        );
        assert_eq!(
            parse_metadata_statement("SELECT * FROM pg_catalog.pg_roles"),
            Some(MetadataStatement::PgCatalogRoles {
                sql: "SELECT * FROM pg_catalog.pg_roles".to_string(),
            })
        );
        assert_eq!(
            parse_metadata_statement("SELECT * FROM information_schema.schemata"),
            Some(MetadataStatement::InformationSchemaSchemata {
                sql: "SELECT * FROM information_schema.schemata".to_string(),
            })
        );
        assert_eq!(
            parse_metadata_statement("SELECT * FROM information_schema.tables"),
            Some(MetadataStatement::InformationSchemaTables {
                sql: "SELECT * FROM information_schema.tables".to_string(),
            })
        );
        assert_eq!(
            parse_metadata_statement("SELECT * FROM information_schema.columns"),
            Some(MetadataStatement::InformationSchemaColumns {
                sql: "SELECT * FROM information_schema.columns".to_string(),
            })
        );
        assert_eq!(
            parse_metadata_statement("SELECT * FROM information_schema.views"),
            Some(MetadataStatement::InformationSchemaViews {
                sql: "SELECT * FROM information_schema.views".to_string(),
            })
        );
        assert_eq!(
            parse_metadata_statement("SELECT * FROM information_schema.table_constraints"),
            Some(MetadataStatement::InformationSchemaTableConstraints {
                sql: "SELECT * FROM information_schema.table_constraints".to_string(),
            })
        );
        assert_eq!(
            parse_metadata_statement("SELECT * FROM information_schema.key_column_usage"),
            Some(MetadataStatement::InformationSchemaKeyColumnUsage {
                sql: "SELECT * FROM information_schema.key_column_usage".to_string(),
            })
        );
        assert_eq!(
            parse_metadata_statement("SELECT * FROM information_schema.constraint_column_usage"),
            Some(MetadataStatement::InformationSchemaConstraintColumnUsage {
                sql: "SELECT * FROM information_schema.constraint_column_usage".to_string(),
            })
        );
        assert_eq!(
            parse_metadata_statement("SELECT * FROM information_schema.constraint_table_usage"),
            Some(MetadataStatement::InformationSchemaConstraintTableUsage {
                sql: "SELECT * FROM information_schema.constraint_table_usage".to_string(),
            })
        );
        assert_eq!(
            parse_metadata_statement("SELECT * FROM information_schema.referential_constraints"),
            Some(MetadataStatement::InformationSchemaReferentialConstraints {
                sql: "SELECT * FROM information_schema.referential_constraints".to_string(),
            })
        );
    }

    #[test]
    fn bootstraps_when_catalog_file_exists_but_is_empty() {
        let path = temp_catalog_path("empty");
        fs::write(&path, "").expect("empty catalog file should be written");

        let control_plane =
            ControlPlane::from_catalog_path(&path).expect("empty file should bootstrap");
        let databases = control_plane
            .list_databases(&default_session())
            .expect("databases should list after bootstrap");

        assert!(databases.iter().any(|database| database == "postgres"));

        let _ = fs::remove_file(path);
    }
}
