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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CatalogRelationKind {
    Table,
    View,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogRelation {
    pub database: String,
    pub schema: String,
    pub name: String,
    pub kind: CatalogRelationKind,
    pub definition_sql: Option<String>,
    pub storage_path: Option<String>,
    pub columns: Vec<CatalogColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableColumnDefinition {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
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
        let state = self
            .state
            .read()
            .expect("control plane lock should not poison");

        if !state.users.contains_key(&session.user) {
            bail!("Unknown user '{}'", session.user);
        }

        let database = state
            .databases
            .get(&session.database)
            .ok_or_else(|| anyhow::anyhow!("Unknown database '{}'", session.database))?;

        if !database.schemas.contains(&session.schema) {
            bail!("Unknown schema '{}.{}'", session.database, session.schema);
        }

        Ok(())
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
            MetadataStatement::CreateTableAs { .. }
            | MetadataStatement::CreateTable { .. }
            | MetadataStatement::InsertInto { .. } => {
                bail!("Managed table DDL and DML should be handled by the engine persistence flow")
            }
            MetadataStatement::ShowDatabases
            | MetadataStatement::ShowSchemas { .. }
            | MetadataStatement::ShowTables { .. }
            | MetadataStatement::ShowViews { .. }
            | MetadataStatement::ShowColumns { .. }
            | MetadataStatement::DescribeRelation { .. } => {
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
                    columns: Vec::new(),
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
                    columns,
                },
            );
        }

        self.persist()?;
        Ok(format!(
            "Table '{}.{}.{}' created successfully.",
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
        },
    );
    users.insert(
        "analyticsdb_admin".to_string(),
        CatalogUser {
            name: "analyticsdb_admin".to_string(),
            is_admin: true,
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

fn parse_table_columns(raw: &str) -> Result<Vec<TableColumnDefinition>> {
    split_sql_top_level(raw, ',')?
        .into_iter()
        .map(|column| {
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
        })
        .collect()
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

pub fn parse_metadata_statement(sql: &str) -> Option<MetadataStatement> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();

    if tokens.is_empty() {
        return None;
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
        let Ok(columns) = parse_table_columns(raw_columns) else {
            return None;
        };

        return Some(MetadataStatement::CreateTable {
            database,
            schema,
            name,
            columns,
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

    None
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use analyticsdb_core::{Protocol, SessionContext};
    use uuid::Uuid;

    use super::{
        parse_metadata_statement, CatalogRelationKind, ControlPlane, MetadataStatement, NodeRole,
        QueryAdmission, TableColumnDefinition,
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

        let _ = fs::remove_file(path);
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

        let _ = fs::remove_file(path);
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
                ]
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
