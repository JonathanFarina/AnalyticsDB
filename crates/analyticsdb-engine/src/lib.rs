use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use analyticsdb_control::{
    parse_metadata_statement, CatalogColumn, CatalogRelation, CatalogRelationKind,
    CatalogTableConstraint, CatalogTableConstraintKind, ControlPlane, ExternalStorageFormat,
    MetadataStatement, TableColumnDefinition, TableConstraintDefinition,
};
use analyticsdb_core::{QueryRequest, QueryResponse};
use anyhow::Result;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    LargeStringArray, NullArray, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedTableFile {
    storage_layout: PersistedStorageLayout,
    schema: Vec<PersistedField>,
    columns: Vec<PersistedColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedField {
    name: String,
    data_type: PersistedDataType,
    nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum PersistedStorageLayout {
    Columnar,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedColumn {
    values: Vec<PersistedScalarValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum PersistedDataType {
    Boolean,
    Float32,
    Float64,
    Int32,
    Int64,
    Null,
    UInt32,
    UInt64,
    Utf8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum PersistedScalarValue {
    Boolean(bool),
    Float32(f32),
    Float64(f64),
    Int32(i32),
    Int64(i64),
    Null,
    UInt32(u32),
    UInt64(u64),
    Utf8(String),
}

pub struct PrototypeEngine {
    control_plane: Arc<ControlPlane>,
    runtime: Runtime,
}

pub struct QueryExecutionResult {
    pub query_id: String,
    pub coordinator_node_id: String,
    pub session: analyticsdb_core::SessionContext,
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub message: String,
    pub execution_time_ms: u128,
}

impl QueryExecutionResult {
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    pub fn columns(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect()
    }
}

impl PrototypeEngine {
    pub fn new() -> Result<Self> {
        Self::with_control_plane(Arc::new(ControlPlane::new_bootstrap()))
    }

    pub fn from_catalog_path(path: &str) -> Result<Self> {
        Self::with_control_plane(Arc::new(ControlPlane::from_catalog_path(path)?))
    }

    pub fn with_control_plane(control_plane: Arc<ControlPlane>) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        Ok(Self {
            control_plane,
            runtime,
        })
    }

    pub fn control_plane(&self) -> Arc<ControlPlane> {
        Arc::clone(&self.control_plane)
    }

    pub fn execute_query(&self, request: &QueryRequest) -> Result<QueryResponse> {
        let execution = self.execute_query_batches(request)?;
        let columns = execution.columns();
        let rows = batches_to_rows(&execution.batches)?;

        Ok(QueryResponse {
            query_id: execution.query_id,
            coordinator_node_id: execution.coordinator_node_id,
            session: execution.session,
            columns,
            rows,
            message: execution.message,
            execution_time_ms: execution.execution_time_ms,
        })
    }

    pub fn execute_query_batches(&self, request: &QueryRequest) -> Result<QueryExecutionResult> {
        let started = Instant::now();
        let admission = self.control_plane.admit_query(&request.session)?;
        if let Some(statement) = parse_metadata_statement(&request.sql) {
            return self.execute_metadata_query(request, statement, admission, started);
        }

        let control_plane = Arc::clone(&self.control_plane);
        let sql = request.sql.clone();
        let session = request.session.clone();

        self.runtime.block_on(async move {
            let context = SessionContext::new();
            register_persisted_tables(&context, &control_plane, &session).await?;
            register_persisted_views(&context, &control_plane, &session).await?;
            let dataframe = context.sql(&sql).await?;
            let schema = Arc::new(dataframe.schema().as_arrow().as_ref().clone());
            let batches = dataframe.collect().await?;
            let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();

            Ok(QueryExecutionResult {
                query_id: admission.query_id,
                coordinator_node_id: admission.coordinator_node_id,
                session,
                schema,
                batches,
                message: format!("Query executed successfully. {row_count} row(s) returned."),
                execution_time_ms: started.elapsed().as_millis(),
            })
        })
    }

    fn execute_metadata_query(
        &self,
        request: &QueryRequest,
        statement: MetadataStatement,
        admission: analyticsdb_control::QueryAdmission,
        started: Instant,
    ) -> Result<QueryExecutionResult> {
        let session = request.session.clone();

        let (schema, batches, message) = match statement {
            MetadataStatement::CreateDatabase { .. }
            | MetadataStatement::CreateSchema { .. }
            | MetadataStatement::CreateView { .. }
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
            | MetadataStatement::InformationSchemaReferentialConstraints { .. }
            | MetadataStatement::AlterUserPassword { .. } => match statement {
                MetadataStatement::PgCatalogTables { sql } => {
                    let columns = [
                        "schemaname",
                        "tablename",
                        "tableowner",
                        "tablespace",
                        "hasindexes",
                        "hasrules",
                        "hastriggers",
                        "rowsecurity",
                    ];
                    let rows = self
                        .list_relations_for_current_database(
                            &request.session,
                            CatalogRelationKind::Table,
                        )?
                        .into_iter()
                        .map(|relation| {
                            vec![
                                relation.schema,
                                relation.name,
                                "postgres".to_string(),
                                String::new(),
                                "false".to_string(),
                                "false".to_string(),
                                "false".to_string(),
                                "false".to_string(),
                            ]
                        })
                        .collect::<Vec<_>>();
                    let (batch, row_count) =
                        execute_pg_catalog_select(&sql, "pg_catalog.pg_tables", &columns, &rows)?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!("{row_count} pg_catalog.pg_tables row(s) listed successfully."),
                    )
                }
                MetadataStatement::PgCatalogViews { sql } => {
                    let columns = ["schemaname", "viewname", "viewowner", "definition"];
                    let rows = self
                        .list_relations_for_current_database(
                            &request.session,
                            CatalogRelationKind::View,
                        )?
                        .into_iter()
                        .map(|relation| {
                            vec![
                                relation.schema,
                                relation.name,
                                "postgres".to_string(),
                                relation.definition_sql.unwrap_or_default(),
                            ]
                        })
                        .collect::<Vec<_>>();
                    let (batch, row_count) =
                        execute_pg_catalog_select(&sql, "pg_catalog.pg_views", &columns, &rows)?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!("{row_count} pg_catalog.pg_views row(s) listed successfully."),
                    )
                }
                MetadataStatement::PgCatalogNamespace { sql } => {
                    let columns = ["oid", "nspname", "nspowner", "nspacl"];
                    let rows = self
                        .control_plane
                        .list_schemas(&request.session, Some(&request.session.database))?
                        .into_iter()
                        .map(|schema| {
                            vec![
                                synthetic_namespace_oid(&request.session.database, &schema)
                                    .to_string(),
                                schema,
                                "postgres".to_string(),
                                String::new(),
                            ]
                        })
                        .collect::<Vec<_>>();
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "pg_catalog.pg_namespace",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!("{row_count} pg_catalog.pg_namespace row(s) listed successfully."),
                    )
                }
                MetadataStatement::PgCatalogDatabase { sql } => {
                    let columns = [
                        "oid",
                        "datname",
                        "datdba",
                        "encoding",
                        "datcollate",
                        "datctype",
                        "datistemplate",
                        "datallowconn",
                        "datconnlimit",
                        "datlastsysoid",
                        "datfrozenxid",
                        "datminmxid",
                        "dattablespace",
                        "datacl",
                    ];
                    let rows = self
                        .control_plane
                        .list_databases(&request.session)?
                        .into_iter()
                        .map(|database| {
                            vec![
                                synthetic_database_oid(&database).to_string(),
                                database,
                                "10".to_string(),
                                "6".to_string(),
                                "C".to_string(),
                                "C".to_string(),
                                "false".to_string(),
                                "true".to_string(),
                                "-1".to_string(),
                                "0".to_string(),
                                "0".to_string(),
                                "1".to_string(),
                                "1663".to_string(),
                                String::new(),
                            ]
                        })
                        .collect::<Vec<_>>();
                    let (batch, row_count) =
                        execute_pg_catalog_select(&sql, "pg_catalog.pg_database", &columns, &rows)?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!("{row_count} pg_catalog.pg_database row(s) listed successfully."),
                    )
                }
                MetadataStatement::PgCatalogRoles { sql } => {
                    let columns = [
                        "oid",
                        "rolname",
                        "rolsuper",
                        "rolinherit",
                        "rolcreaterole",
                        "rolcreatedb",
                        "rolcanlogin",
                        "rolreplication",
                        "rolbypassrls",
                        "rolconnlimit",
                        "rolpassword",
                        "rolvaliduntil",
                    ];
                    let mut users = self.control_plane.cluster_snapshot().users;
                    users.sort_by(|left, right| left.name.cmp(&right.name));
                    let rows = users
                        .into_iter()
                        .map(|user| {
                            vec![
                                synthetic_role_oid(&user.name).to_string(),
                                user.name,
                                user.is_admin.to_string(),
                                "true".to_string(),
                                user.is_admin.to_string(),
                                user.is_admin.to_string(),
                                "true".to_string(),
                                "false".to_string(),
                                "false".to_string(),
                                "-1".to_string(),
                                String::new(),
                                String::new(),
                            ]
                        })
                        .collect::<Vec<_>>();
                    let (batch, row_count) =
                        execute_pg_catalog_select(&sql, "pg_catalog.pg_roles", &columns, &rows)?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!("{row_count} pg_catalog.pg_roles row(s) listed successfully."),
                    )
                }
                MetadataStatement::InformationSchemaSchemata { sql } => {
                    let columns = [
                        "catalog_name",
                        "schema_name",
                        "schema_owner",
                        "default_character_set_catalog",
                        "default_character_set_schema",
                        "default_character_set_name",
                        "sql_path",
                    ];
                    let rows = self.information_schema_schemata_rows(&request.session)?;
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "information_schema.schemata",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!(
                            "{row_count} information_schema.schemata row(s) listed successfully."
                        ),
                    )
                }
                MetadataStatement::InformationSchemaTables { sql } => {
                    let columns = [
                        "table_catalog",
                        "table_schema",
                        "table_name",
                        "table_type",
                        "self_referencing_column_name",
                        "reference_generation",
                        "user_defined_type_catalog",
                        "user_defined_type_schema",
                        "user_defined_type_name",
                        "is_insertable_into",
                        "is_typed",
                        "commit_action",
                    ];
                    let rows = self.information_schema_tables_rows(&request.session)?;
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "information_schema.tables",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!(
                            "{row_count} information_schema.tables row(s) listed successfully."
                        ),
                    )
                }
                MetadataStatement::InformationSchemaColumns { sql } => {
                    let columns = [
                        "table_catalog",
                        "table_schema",
                        "table_name",
                        "column_name",
                        "ordinal_position",
                        "column_default",
                        "is_nullable",
                        "data_type",
                        "character_maximum_length",
                        "character_octet_length",
                        "numeric_precision",
                        "numeric_precision_radix",
                        "numeric_scale",
                        "datetime_precision",
                    ];
                    let rows = self.information_schema_columns_rows(&request.session)?;
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "information_schema.columns",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!(
                            "{row_count} information_schema.columns row(s) listed successfully."
                        ),
                    )
                }
                MetadataStatement::InformationSchemaViews { sql } => {
                    let columns = [
                        "table_catalog",
                        "table_schema",
                        "table_name",
                        "view_definition",
                        "check_option",
                        "is_updatable",
                        "is_insertable_into",
                        "is_trigger_updatable",
                        "is_trigger_deletable",
                        "is_trigger_insertable_into",
                    ];
                    let rows = self.information_schema_views_rows(&request.session)?;
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "information_schema.views",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!("{row_count} information_schema.views row(s) listed successfully."),
                    )
                }
                MetadataStatement::InformationSchemaTableConstraints { sql } => {
                    let columns = [
                        "constraint_catalog",
                        "constraint_schema",
                        "constraint_name",
                        "table_catalog",
                        "table_schema",
                        "table_name",
                        "constraint_type",
                        "is_deferrable",
                        "initially_deferred",
                        "enforced",
                        "nulls_distinct",
                    ];
                    let rows = self.information_schema_table_constraints_rows(&request.session)?;
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "information_schema.table_constraints",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!(
                            "{row_count} information_schema.table_constraints row(s) listed successfully."
                        ),
                    )
                }
                MetadataStatement::InformationSchemaKeyColumnUsage { sql } => {
                    let columns = [
                        "constraint_catalog",
                        "constraint_schema",
                        "constraint_name",
                        "table_catalog",
                        "table_schema",
                        "table_name",
                        "column_name",
                        "ordinal_position",
                        "position_in_unique_constraint",
                    ];
                    let rows = self.information_schema_key_column_usage_rows(&request.session)?;
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "information_schema.key_column_usage",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!(
                            "{row_count} information_schema.key_column_usage row(s) listed successfully."
                        ),
                    )
                }
                MetadataStatement::InformationSchemaConstraintColumnUsage { sql } => {
                    let columns = [
                        "table_catalog",
                        "table_schema",
                        "table_name",
                        "column_name",
                        "constraint_catalog",
                        "constraint_schema",
                        "constraint_name",
                    ];
                    let rows =
                        self.information_schema_constraint_column_usage_rows(&request.session)?;
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "information_schema.constraint_column_usage",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!(
                            "{row_count} information_schema.constraint_column_usage row(s) listed successfully."
                        ),
                    )
                }
                MetadataStatement::InformationSchemaConstraintTableUsage { sql } => {
                    let columns = [
                        "table_catalog",
                        "table_schema",
                        "table_name",
                        "constraint_catalog",
                        "constraint_schema",
                        "constraint_name",
                    ];
                    let rows =
                        self.information_schema_constraint_table_usage_rows(&request.session)?;
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "information_schema.constraint_table_usage",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!(
                            "{row_count} information_schema.constraint_table_usage row(s) listed successfully."
                        ),
                    )
                }
                MetadataStatement::InformationSchemaReferentialConstraints { sql } => {
                    let columns = [
                        "constraint_catalog",
                        "constraint_schema",
                        "constraint_name",
                        "unique_constraint_catalog",
                        "unique_constraint_schema",
                        "unique_constraint_name",
                        "match_option",
                        "update_rule",
                        "delete_rule",
                    ];
                    let rows =
                        self.information_schema_referential_constraints_rows(&request.session)?;
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "information_schema.referential_constraints",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!(
                            "{row_count} information_schema.referential_constraints row(s) listed successfully."
                        ),
                    )
                }
                _ => {
                    let message = self
                        .control_plane
                        .execute_metadata_statement(&request.session, &statement)?;
                    (Arc::new(Schema::empty()), Vec::new(), message)
                }
            },
            MetadataStatement::CreateExternalTable {
                database,
                schema,
                name,
                format,
                location,
            } => {
                let message = self.control_plane.register_external_table(
                    &request.session,
                    database.as_deref(),
                    schema.as_deref(),
                    &name,
                    &location,
                    format,
                )?;

                (Arc::new(Schema::empty()), Vec::new(), message)
            }
            MetadataStatement::CreateTableAs {
                database,
                schema,
                name,
                query_sql,
            } => {
                let storage_path = self.control_plane.managed_table_storage_path(
                    &request.session,
                    database.as_deref(),
                    schema.as_deref(),
                    &name,
                )?;

                let control_plane = Arc::clone(&self.control_plane);
                let session = request.session.clone();
                let storage_path_for_write = storage_path.clone();
                let (row_count, columns_metadata) = self.runtime.block_on(async move {
                    let context = SessionContext::new();
                    register_persisted_tables(&context, &control_plane, &session).await?;
                    register_persisted_views(&context, &control_plane, &session).await?;
                    let dataframe = context.sql(&query_sql).await?;
                    let arrow_schema = dataframe.schema().as_arrow().clone();
                    let batches = dataframe.collect().await?;
                    let columns_metadata = catalog_columns_from_schema(&arrow_schema);

                    let row_count =
                        persist_table_snapshot(&storage_path_for_write, &arrow_schema, &batches)?;

                    Ok::<_, anyhow::Error>((row_count, columns_metadata))
                })?;

                let created_message = self.control_plane.register_managed_table(
                    &request.session,
                    database.as_deref(),
                    schema.as_deref(),
                    &name,
                    &storage_path,
                    columns_metadata,
                    Vec::new(),
                )?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!("{created_message} {row_count} row(s) materialized."),
                )
            }
            MetadataStatement::CreateTable {
                database,
                schema,
                name,
                columns,
                constraints,
            } => {
                let storage_path = self.control_plane.managed_table_storage_path(
                    &request.session,
                    database.as_deref(),
                    schema.as_deref(),
                    &name,
                )?;
                let arrow_schema = build_arrow_schema_from_definitions(&columns)?;

                persist_empty_table_snapshot(&storage_path, &arrow_schema)?;

                let created_message = self.control_plane.register_managed_table(
                    &request.session,
                    database.as_deref(),
                    schema.as_deref(),
                    &name,
                    &storage_path,
                    catalog_columns_from_schema(&arrow_schema),
                    catalog_constraints_from_definitions(
                        &name,
                        database.as_deref(),
                        schema.as_deref(),
                        &request.session,
                        &constraints,
                    )?,
                )?;

                (Arc::new(Schema::empty()), Vec::new(), created_message)
            }
            MetadataStatement::InsertInto {
                database,
                schema,
                name,
                columns,
                rows,
            } => {
                let relation = self.control_plane.table_relation(
                    &request.session,
                    database.as_deref(),
                    schema.as_deref(),
                    &name,
                )?;
                let storage_path = relation.storage_path.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Managed table '{}.{}.{}' is missing a storage path",
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })?;
                let inserted_row_count = append_rows_to_table_snapshot(
                    Path::new(storage_path),
                    columns.as_deref(),
                    &rows,
                )?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!(
                        "Inserted {inserted_row_count} row(s) into '{}.{}.{}'.",
                        relation.database, relation.schema, relation.name
                    ),
                )
            }
            MetadataStatement::ShowDatabases => {
                let rows = self
                    .control_plane
                    .list_databases(&request.session)?
                    .into_iter()
                    .map(|database| vec![database])
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(&["database_name"], &rows)?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} database(s) listed successfully."),
                )
            }
            MetadataStatement::ShowSchemas { database } => {
                let rows = self
                    .control_plane
                    .list_schemas(&request.session, database.as_deref())?
                    .into_iter()
                    .map(|schema| vec![schema])
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(&["schema_name"], &rows)?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} schema(s) listed successfully."),
                )
            }
            MetadataStatement::ShowTables { database, schema } => {
                let rows = self
                    .control_plane
                    .list_relations(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        CatalogRelationKind::Table,
                    )?
                    .into_iter()
                    .map(|relation| vec![relation.name])
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(&["table_name"], &rows)?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} table(s) listed successfully."),
                )
            }
            MetadataStatement::ShowViews { database, schema } => {
                let rows = self
                    .control_plane
                    .list_relations(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        CatalogRelationKind::View,
                    )?
                    .into_iter()
                    .map(|relation| vec![relation.name])
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(&["view_name"], &rows)?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} view(s) listed successfully."),
                )
            }
            MetadataStatement::ShowColumns {
                database,
                schema,
                name,
            }
            | MetadataStatement::DescribeRelation {
                database,
                schema,
                name,
            } => {
                let rows = self
                    .control_plane
                    .relation_columns(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )?
                    .into_iter()
                    .map(|column| {
                        vec![
                            column.name,
                            column.data_type,
                            if column.nullable {
                                "YES".to_string()
                            } else {
                                "NO".to_string()
                            },
                        ]
                    })
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(&["column_name", "data_type", "is_nullable"], &rows)?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} column(s) described successfully."),
                )
            }
        };

        Ok(QueryExecutionResult {
            query_id: admission.query_id,
            coordinator_node_id: admission.coordinator_node_id,
            session,
            schema,
            batches,
            message,
            execution_time_ms: started.elapsed().as_millis(),
        })
    }

    pub fn list_databases(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<String>> {
        self.control_plane.list_databases(session)
    }

    pub fn list_schemas(
        &self,
        session: &analyticsdb_core::SessionContext,
        database: Option<&str>,
    ) -> Result<Vec<String>> {
        self.control_plane.list_schemas(session, database)
    }

    pub fn list_relations(
        &self,
        session: &analyticsdb_core::SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        kind: CatalogRelationKind,
    ) -> Result<Vec<analyticsdb_control::CatalogRelation>> {
        self.control_plane
            .list_relations(session, database, schema, kind)
    }
}

impl PrototypeEngine {
    fn list_relations_for_current_database(
        &self,
        session: &analyticsdb_core::SessionContext,
        kind: CatalogRelationKind,
    ) -> Result<Vec<CatalogRelation>> {
        let mut rows = Vec::new();
        let mut schemas = self
            .control_plane
            .list_schemas(session, Some(&session.database))?;
        schemas.sort();

        for schema in schemas {
            rows.extend(self.control_plane.list_relations(
                session,
                Some(&session.database),
                Some(&schema),
                kind.clone(),
            )?);
        }

        rows.sort_by(|left, right| {
            (left.schema.as_str(), left.name.as_str())
                .cmp(&(right.schema.as_str(), right.name.as_str()))
        });

        Ok(rows)
    }

    fn list_all_relations_for_current_database(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<CatalogRelation>> {
        let mut rows =
            self.list_relations_for_current_database(session, CatalogRelationKind::Table)?;
        rows.extend(self.list_relations_for_current_database(session, CatalogRelationKind::View)?);
        rows.sort_by(|left, right| {
            (left.schema.as_str(), left.name.as_str())
                .cmp(&(right.schema.as_str(), right.name.as_str()))
        });
        Ok(rows)
    }

    fn information_schema_schemata_rows(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let mut schemas = self
            .control_plane
            .list_schemas(session, Some(&session.database))?;
        schemas.sort();

        Ok(schemas
            .into_iter()
            .map(|schema| {
                vec![
                    session.database.clone(),
                    schema,
                    "postgres".to_string(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                ]
            })
            .collect())
    }

    fn information_schema_tables_rows(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        Ok(self
            .list_all_relations_for_current_database(session)?
            .into_iter()
            .map(|relation| {
                let (table_type, is_insertable_into) = match relation.kind {
                    CatalogRelationKind::Table => ("BASE TABLE", "YES"),
                    CatalogRelationKind::View => ("VIEW", "NO"),
                };
                vec![
                    relation.database,
                    relation.schema,
                    relation.name,
                    table_type.to_string(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    is_insertable_into.to_string(),
                    "NO".to_string(),
                    String::new(),
                ]
            })
            .collect())
    }

    fn information_schema_columns_rows(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let mut rows = Vec::new();
        for relation in self.list_all_relations_for_current_database(session)? {
            rows.extend(relation.columns.iter().enumerate().map(|(index, column)| {
                vec![
                    relation.database.clone(),
                    relation.schema.clone(),
                    relation.name.clone(),
                    column.name.clone(),
                    (index + 1).to_string(),
                    String::new(),
                    if column.nullable {
                        "YES".to_string()
                    } else {
                        "NO".to_string()
                    },
                    normalize_information_schema_data_type(column),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                ]
            }));
        }
        Ok(rows)
    }

    fn information_schema_views_rows(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let mut views =
            self.list_relations_for_current_database(session, CatalogRelationKind::View)?;
        views.sort_by(|left, right| {
            (left.schema.as_str(), left.name.as_str())
                .cmp(&(right.schema.as_str(), right.name.as_str()))
        });

        Ok(views
            .into_iter()
            .map(|view| {
                vec![
                    view.database,
                    view.schema,
                    view.name,
                    view.definition_sql.unwrap_or_default(),
                    "NONE".to_string(),
                    "NO".to_string(),
                    "NO".to_string(),
                    "NO".to_string(),
                    "NO".to_string(),
                    "NO".to_string(),
                ]
            })
            .collect())
    }

    fn information_schema_table_constraints_rows(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        Ok(self
            .information_schema_constraints(session)?
            .into_iter()
            .map(|constraint| {
                vec![
                    constraint.table_catalog.clone(),
                    constraint.table_schema.clone(),
                    constraint.constraint_name.clone(),
                    constraint.table_catalog,
                    constraint.table_schema,
                    constraint.table_name,
                    constraint.constraint_type,
                    "NO".to_string(),
                    "NO".to_string(),
                    "YES".to_string(),
                    String::new(),
                ]
            })
            .collect())
    }

    fn information_schema_key_column_usage_rows(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let mut rows = Vec::new();
        for constraint in self.information_schema_constraints(session)? {
            if constraint.constraint_type != "PRIMARY KEY"
                && constraint.constraint_type != "FOREIGN KEY"
            {
                continue;
            }
            for (index, column_name) in constraint.columns.iter().enumerate() {
                rows.push(vec![
                    constraint.table_catalog.clone(),
                    constraint.table_schema.clone(),
                    constraint.constraint_name.clone(),
                    constraint.table_catalog.clone(),
                    constraint.table_schema.clone(),
                    constraint.table_name.clone(),
                    column_name.clone(),
                    (index + 1).to_string(),
                    if constraint.constraint_type == "FOREIGN KEY" {
                        (index + 1).to_string()
                    } else {
                        String::new()
                    },
                ]);
            }
        }
        Ok(rows)
    }

    fn information_schema_constraint_column_usage_rows(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let mut rows = Vec::new();
        for constraint in self.information_schema_constraints(session)? {
            for column_name in &constraint.columns {
                rows.push(vec![
                    constraint.table_catalog.clone(),
                    constraint.table_schema.clone(),
                    constraint.table_name.clone(),
                    column_name.clone(),
                    constraint.table_catalog.clone(),
                    constraint.table_schema.clone(),
                    constraint.constraint_name.clone(),
                ]);
            }
        }
        Ok(rows)
    }

    fn information_schema_constraint_table_usage_rows(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let mut rows = Vec::new();
        for constraint in self.information_schema_constraints(session)? {
            let (table_catalog, table_schema, table_name) =
                if constraint.constraint_type == "FOREIGN KEY" {
                    (
                        constraint
                            .referenced_catalog
                            .clone()
                            .unwrap_or_else(|| constraint.table_catalog.clone()),
                        constraint
                            .referenced_schema
                            .clone()
                            .unwrap_or_else(|| constraint.table_schema.clone()),
                        constraint
                            .referenced_table
                            .clone()
                            .unwrap_or_else(|| constraint.table_name.clone()),
                    )
                } else {
                    (
                        constraint.table_catalog.clone(),
                        constraint.table_schema.clone(),
                        constraint.table_name.clone(),
                    )
                };
            rows.push(vec![
                table_catalog,
                table_schema,
                table_name,
                constraint.table_catalog.clone(),
                constraint.table_schema.clone(),
                constraint.constraint_name.clone(),
            ]);
        }
        Ok(rows)
    }

    fn information_schema_referential_constraints_rows(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        Ok(self
            .information_schema_constraints(session)?
            .into_iter()
            .filter(|constraint| constraint.constraint_type == "FOREIGN KEY")
            .map(|constraint| {
                let unique_constraint_name = constraint
                    .referenced_unique_constraint_name
                    .unwrap_or_else(|| {
                        format!(
                            "{}_pkey",
                            constraint
                                .referenced_table
                                .clone()
                                .unwrap_or_else(|| constraint.table_name.clone())
                        )
                    });
                vec![
                    constraint.table_catalog.clone(),
                    constraint.table_schema.clone(),
                    constraint.constraint_name,
                    constraint
                        .referenced_catalog
                        .unwrap_or_else(|| constraint.table_catalog.clone()),
                    constraint
                        .referenced_schema
                        .unwrap_or_else(|| constraint.table_schema.clone()),
                    unique_constraint_name,
                    "NONE".to_string(),
                    "NO ACTION".to_string(),
                    "NO ACTION".to_string(),
                ]
            })
            .collect())
    }

    fn information_schema_constraints(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<InformationSchemaConstraintRow>> {
        let mut constraints = Vec::new();
        for relation in
            self.list_relations_for_current_database(session, CatalogRelationKind::Table)?
        {
            for column in &relation.columns {
                if !column.nullable {
                    constraints.push(InformationSchemaConstraintRow {
                        table_catalog: relation.database.clone(),
                        table_schema: relation.schema.clone(),
                        table_name: relation.name.clone(),
                        columns: vec![column.name.clone()],
                        constraint_name: format!("{}_{}_not_null", relation.name, column.name),
                        constraint_type: "CHECK".to_string(),
                        referenced_catalog: None,
                        referenced_schema: None,
                        referenced_table: None,
                        referenced_unique_constraint_name: None,
                    });
                }
            }

            for relation_constraint in relation.constraints {
                match relation_constraint.kind {
                    CatalogTableConstraintKind::PrimaryKey => {
                        constraints.push(InformationSchemaConstraintRow {
                            table_catalog: relation.database.clone(),
                            table_schema: relation.schema.clone(),
                            table_name: relation.name.clone(),
                            columns: relation_constraint.columns,
                            constraint_name: relation_constraint.name,
                            constraint_type: "PRIMARY KEY".to_string(),
                            referenced_catalog: None,
                            referenced_schema: None,
                            referenced_table: None,
                            referenced_unique_constraint_name: None,
                        })
                    }
                    CatalogTableConstraintKind::ForeignKey => {
                        let referenced_table = relation_constraint.referenced_table.clone();
                        let referenced_unique_constraint_name = relation_constraint
                            .referenced_table
                            .as_ref()
                            .map(|name| format!("{name}_pkey"));
                        constraints.push(InformationSchemaConstraintRow {
                            table_catalog: relation.database.clone(),
                            table_schema: relation.schema.clone(),
                            table_name: relation.name.clone(),
                            columns: relation_constraint.columns,
                            constraint_name: relation_constraint.name,
                            constraint_type: "FOREIGN KEY".to_string(),
                            referenced_catalog: relation_constraint.referenced_database,
                            referenced_schema: relation_constraint.referenced_schema,
                            referenced_table,
                            referenced_unique_constraint_name,
                        });
                    }
                }
            }
        }
        constraints.sort_by(|left, right| {
            (
                left.table_schema.as_str(),
                left.table_name.as_str(),
                left.constraint_name.as_str(),
            )
                .cmp(&(
                    right.table_schema.as_str(),
                    right.table_name.as_str(),
                    right.constraint_name.as_str(),
                ))
        });
        Ok(constraints)
    }
}

#[derive(Clone)]
struct InformationSchemaConstraintRow {
    table_catalog: String,
    table_schema: String,
    table_name: String,
    columns: Vec<String>,
    constraint_name: String,
    constraint_type: String,
    referenced_catalog: Option<String>,
    referenced_schema: Option<String>,
    referenced_table: Option<String>,
    referenced_unique_constraint_name: Option<String>,
}

fn normalize_information_schema_data_type(column: &CatalogColumn) -> String {
    let normalized = column.data_type.trim().to_ascii_lowercase();
    if normalized.starts_with("character varying") {
        "character varying".to_string()
    } else if normalized.starts_with("timestamp") {
        "timestamp without time zone".to_string()
    } else {
        normalized
    }
}

fn synthetic_namespace_oid(database: &str, schema: &str) -> u32 {
    // Deterministic FNV-like hash for stable synthetic namespace IDs in the prototype.
    let mut hash = 2166136261_u32;
    for byte in database.bytes().chain([b'.']).chain(schema.bytes()) {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }

    if hash < 16384 {
        hash + 16384
    } else {
        hash
    }
}

fn synthetic_database_oid(database: &str) -> u32 {
    let mut hash = 2166136261_u32;
    for byte in database.bytes() {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }

    if hash < 16384 {
        hash + 16384
    } else {
        hash
    }
}

fn synthetic_role_oid(role: &str) -> u32 {
    let mut hash = 2166136261_u32;
    for byte in role.bytes() {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }

    if hash < 16384 {
        hash + 16384
    } else {
        hash
    }
}

fn execute_pg_catalog_select(
    sql: &str,
    relation_name: &str,
    source_columns: &[&str],
    source_rows: &[Vec<String>],
) -> Result<(RecordBatch, usize)> {
    let spec = parse_pg_catalog_select_spec(sql, relation_name, source_columns)?;
    let mut working_rows = source_rows.to_vec();

    if let Some(filter) = spec.filter {
        match filter {
            PgCatalogFilter::Equals(column_index, expected_value) => {
                working_rows.retain(|row| row.get(column_index) == Some(&expected_value));
            }
            PgCatalogFilter::In(column_index, expected_values) => {
                working_rows.retain(|row| {
                    row.get(column_index)
                        .map(|value| expected_values.iter().any(|expected| expected == value))
                        .unwrap_or(false)
                });
            }
        }
    }

    if !spec.order_by.is_empty() {
        working_rows.sort_by(|left, right| {
            for key in &spec.order_by {
                let mut ordering = left.get(key.column_index).cmp(&right.get(key.column_index));
                if key.descending {
                    ordering = ordering.reverse();
                }
                if !ordering.is_eq() {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    let projected_rows = working_rows
        .into_iter()
        .map(|row| {
            spec.projection_indices
                .iter()
                .map(|index| row[*index].clone())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let projected_columns = spec
        .projection_indices
        .iter()
        .map(|index| source_columns[*index])
        .collect::<Vec<_>>();

    let batch = utf8_record_batch(&projected_columns, &projected_rows)?;
    Ok((batch, projected_rows.len()))
}

struct PgCatalogSelectSpec {
    projection_indices: Vec<usize>,
    filter: Option<PgCatalogFilter>,
    order_by: Vec<PgCatalogOrderByKey>,
}

enum PgCatalogFilter {
    Equals(usize, String),
    In(usize, Vec<String>),
}

struct PgCatalogOrderByKey {
    column_index: usize,
    descending: bool,
}

fn parse_pg_catalog_select_spec(
    sql: &str,
    relation_name: &str,
    source_columns: &[&str],
) -> Result<PgCatalogSelectSpec> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    let from_clause = format!(" FROM {}", relation_name.to_ascii_uppercase());
    let from_index = upper.find(&from_clause).ok_or_else(|| {
        anyhow::anyhow!(
            "Unsupported pg_catalog query: expected FROM {} in '{}'",
            relation_name,
            sql
        )
    })?;

    let select_prefix = "SELECT ";
    if !upper.starts_with(select_prefix) {
        anyhow::bail!("Unsupported pg_catalog query: expected SELECT in '{}'", sql);
    }

    let projection_sql = trimmed[select_prefix.len()..from_index].trim();
    let after_from_sql = trimmed[from_index + from_clause.len()..].trim();
    let after_from_upper = after_from_sql.to_ascii_uppercase();

    let where_index = after_from_upper.find("WHERE ");
    let order_index = after_from_upper.find("ORDER BY ");

    let filter_sql = where_index.map(|where_idx| {
        let start = where_idx + "WHERE ".len();
        let end = order_index.unwrap_or(after_from_sql.len());
        after_from_sql[start..end].trim().to_string()
    });
    let order_sql = order_index.map(|order_idx| {
        after_from_sql[order_idx + "ORDER BY ".len()..]
            .trim()
            .to_string()
    });

    let projection_indices = if projection_sql == "*" {
        (0..source_columns.len()).collect()
    } else {
        projection_sql
            .split(',')
            .map(str::trim)
            .map(|column| pg_catalog_column_index(source_columns, column))
            .collect::<Result<Vec<_>>>()?
    };

    let filter = if let Some(filter_sql) = filter_sql {
        parse_pg_catalog_filter(&filter_sql, source_columns, sql)?
    } else {
        None
    };

    let order_by = if let Some(order_sql) = order_sql {
        order_sql
            .split(',')
            .map(str::trim)
            .map(|column| parse_order_by_column(column, source_columns, sql))
            .collect::<Result<Vec<_>>>()?
    } else {
        Vec::new()
    };

    Ok(PgCatalogSelectSpec {
        projection_indices,
        filter,
        order_by,
    })
}

fn parse_pg_catalog_filter(
    filter_sql: &str,
    source_columns: &[&str],
    original_sql: &str,
) -> Result<Option<PgCatalogFilter>> {
    let upper = filter_sql.to_ascii_uppercase();

    if let Some(in_index) = upper.find(" IN ") {
        let column = filter_sql[..in_index].trim();
        let values_sql = filter_sql[in_index + " IN ".len()..].trim();
        let column_index = pg_catalog_column_index(source_columns, column)?;
        let values = parse_sql_in_string_list(values_sql)?;
        return Ok(Some(PgCatalogFilter::In(column_index, values)));
    }

    if let Some((column, value)) = filter_sql.split_once('=') {
        let column_index = pg_catalog_column_index(source_columns, column.trim())?;
        let parsed_value = parse_single_quoted_sql_literal(value.trim())?;
        return Ok(Some(PgCatalogFilter::Equals(column_index, parsed_value)));
    }

    anyhow::bail!("Unsupported WHERE clause in '{}'", original_sql)
}

fn parse_order_by_column(
    token: &str,
    columns: &[&str],
    original_sql: &str,
) -> Result<PgCatalogOrderByKey> {
    let parts = token.split_whitespace().collect::<Vec<_>>();
    if parts.is_empty() {
        anyhow::bail!("Unsupported ORDER BY clause in '{}'", original_sql);
    }

    let descending = if parts.len() > 1 {
        match parts[1].to_ascii_uppercase().as_str() {
            "ASC" => false,
            "DESC" => true,
            _ => {
                anyhow::bail!(
                    "Unsupported ORDER BY direction in '{}' (only ASC/DESC/default supported)",
                    original_sql
                )
            }
        }
    } else {
        false
    };

    if parts.len() > 1 {
        if parts.len() != 2 {
            anyhow::bail!(
                "Unsupported ORDER BY clause in '{}' (expected '<column>' or '<column> ASC|DESC')",
                original_sql
            );
        }
    }

    Ok(PgCatalogOrderByKey {
        column_index: pg_catalog_column_index(columns, parts[0])?,
        descending,
    })
}

fn parse_sql_in_string_list(raw: &str) -> Result<Vec<String>> {
    let trimmed = raw.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        anyhow::bail!("Expected IN (...) list, got '{}'", raw);
    }

    let inner = trimmed[1..trimmed.len() - 1].trim();
    if inner.is_empty() {
        return Ok(Vec::new());
    }

    let mut values = Vec::new();
    let mut start = 0usize;
    let mut in_quotes = false;
    let chars = inner.char_indices().collect::<Vec<_>>();
    let mut index = 0usize;

    while index < chars.len() {
        let (offset, ch) = chars[index];
        if ch == '\'' {
            if in_quotes && index + 1 < chars.len() && chars[index + 1].1 == '\'' {
                index += 1;
            } else {
                in_quotes = !in_quotes;
            }
        } else if ch == ',' && !in_quotes {
            let token = inner[start..offset].trim();
            values.push(parse_single_quoted_sql_literal(token)?);
            start = offset + 1;
        }
        index += 1;
    }

    let last = inner[start..].trim();
    if !last.is_empty() {
        values.push(parse_single_quoted_sql_literal(last)?);
    }

    Ok(values)
}

fn pg_catalog_column_index(columns: &[&str], wanted: &str) -> Result<usize> {
    let wanted_lower = wanted.to_ascii_lowercase();
    columns
        .iter()
        .position(|column| column.eq_ignore_ascii_case(&wanted_lower))
        .ok_or_else(|| anyhow::anyhow!("Unknown pg_catalog column '{}'", wanted))
}

fn parse_single_quoted_sql_literal(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if !trimmed.starts_with('\'') || !trimmed.ends_with('\'') || trimmed.len() < 2 {
        anyhow::bail!("Expected single-quoted SQL string literal, got '{}'", raw);
    }

    let mut out = String::new();
    let mut chars = trimmed[1..trimmed.len() - 1].chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            if matches!(chars.peek(), Some('\'')) {
                let _ = chars.next();
                out.push('\'');
            } else {
                anyhow::bail!("Unescaped quote in SQL string literal '{}'", raw);
            }
        } else {
            out.push(ch);
        }
    }

    Ok(out)
}

impl PersistedField {
    fn from_arrow(field: &Field) -> Result<Self> {
        Ok(Self {
            name: field.name().to_string(),
            data_type: PersistedDataType::from_arrow(field.data_type())?,
            nullable: field.is_nullable(),
        })
    }

    fn to_arrow_field(&self) -> Field {
        Field::new(
            self.name.clone(),
            self.data_type.to_arrow_data_type(),
            self.nullable,
        )
    }
}

fn catalog_columns_from_schema(schema: &Schema) -> Vec<CatalogColumn> {
    schema
        .fields()
        .iter()
        .map(|field| CatalogColumn {
            name: field.name().to_string(),
            data_type: field.data_type().to_string(),
            nullable: field.is_nullable(),
        })
        .collect()
}

fn build_arrow_schema_from_definitions(columns: &[TableColumnDefinition]) -> Result<Schema> {
    if columns.is_empty() {
        anyhow::bail!("Managed tables must define at least one column");
    }

    let mut seen = BTreeSet::new();
    let fields = columns
        .iter()
        .map(|column| {
            let normalized_name = column.name.to_ascii_lowercase();
            if !seen.insert(normalized_name) {
                anyhow::bail!(
                    "Duplicate column '{}' in managed table definition",
                    column.name
                );
            }

            Ok(Field::new(
                column.name.clone(),
                sql_type_to_arrow_data_type(&column.data_type)?,
                column.nullable,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Schema::new(fields))
}

fn catalog_constraints_from_definitions(
    table_name: &str,
    database: Option<&str>,
    schema: Option<&str>,
    session: &analyticsdb_core::SessionContext,
    constraints: &[TableConstraintDefinition],
) -> Result<Vec<CatalogTableConstraint>> {
    let default_database = database.unwrap_or(&session.database);
    let default_schema = schema.unwrap_or(&session.schema);

    constraints
        .iter()
        .map(|constraint| match constraint {
            TableConstraintDefinition::PrimaryKey { name, columns } => {
                if columns.is_empty() {
                    anyhow::bail!("PRIMARY KEY constraint requires at least one column")
                }
                Ok(CatalogTableConstraint {
                    name: name.clone().unwrap_or_else(|| format!("{table_name}_pkey")),
                    kind: CatalogTableConstraintKind::PrimaryKey,
                    columns: columns.clone(),
                    referenced_database: None,
                    referenced_schema: None,
                    referenced_table: None,
                    referenced_columns: Vec::new(),
                })
            }
            TableConstraintDefinition::ForeignKey {
                name,
                columns,
                referenced_database,
                referenced_schema,
                referenced_table,
                referenced_columns,
            } => {
                if columns.is_empty() || referenced_columns.is_empty() {
                    anyhow::bail!("FOREIGN KEY constraint requires column mappings")
                }
                Ok(CatalogTableConstraint {
                    name: name
                        .clone()
                        .unwrap_or_else(|| format!("{table_name}_{}_fkey", columns.join("_"))),
                    kind: CatalogTableConstraintKind::ForeignKey,
                    columns: columns.clone(),
                    referenced_database: Some(
                        referenced_database
                            .clone()
                            .unwrap_or_else(|| default_database.to_string()),
                    ),
                    referenced_schema: Some(
                        referenced_schema
                            .clone()
                            .unwrap_or_else(|| default_schema.to_string()),
                    ),
                    referenced_table: Some(referenced_table.clone()),
                    referenced_columns: referenced_columns.clone(),
                })
            }
        })
        .collect()
}

fn sql_type_to_arrow_data_type(raw: &str) -> Result<DataType> {
    let normalized = raw.trim().to_ascii_uppercase();

    let data_type = if normalized.starts_with("VARCHAR(")
        || normalized.starts_with("CHAR(")
        || normalized.starts_with("CHARACTER VARYING(")
    {
        DataType::Utf8
    } else {
        match normalized.as_str() {
            "BOOL" | "BOOLEAN" => DataType::Boolean,
            "FLOAT" | "FLOAT4" | "REAL" => DataType::Float32,
            "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => DataType::Float64,
            "SMALLINT" | "INT2" | "INT" | "INTEGER" | "INT4" => DataType::Int32,
            "BIGINT" | "INT8" => DataType::Int64,
            "STRING" | "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER" | "CHARACTER VARYING" => {
                DataType::Utf8
            }
            unsupported => {
                anyhow::bail!(
                    "Unsupported SQL column type '{}' in the current prototype",
                    unsupported
                )
            }
        }
    };

    Ok(data_type)
}

impl PersistedDataType {
    fn from_arrow(data_type: &DataType) -> Result<Self> {
        Ok(match data_type {
            DataType::Boolean => Self::Boolean,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::Null => Self::Null,
            DataType::UInt32 => Self::UInt32,
            DataType::UInt64 => Self::UInt64,
            DataType::Utf8 | DataType::LargeUtf8 => Self::Utf8,
            unsupported => {
                anyhow::bail!(
                    "Unsupported managed table data type in current prototype: {unsupported:?}"
                )
            }
        })
    }

    fn to_arrow_data_type(&self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::Null => DataType::Null,
            Self::UInt32 => DataType::UInt32,
            Self::UInt64 => DataType::UInt64,
            Self::Utf8 => DataType::Utf8,
        }
    }
}

impl PersistedScalarValue {
    fn from_array(array: &ArrayRef, row_index: usize) -> Result<Self> {
        if array.is_null(row_index) {
            return Ok(Self::Null);
        }

        let value = match array.data_type() {
            DataType::Boolean => Self::Boolean(
                array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("boolean array downcast should succeed")
                    .value(row_index),
            ),
            DataType::Float32 => Self::Float32(
                array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .expect("float32 array downcast should succeed")
                    .value(row_index),
            ),
            DataType::Float64 => Self::Float64(
                array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("float64 array downcast should succeed")
                    .value(row_index),
            ),
            DataType::Int32 => Self::Int32(
                array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("int32 array downcast should succeed")
                    .value(row_index),
            ),
            DataType::Int64 => Self::Int64(
                array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64 array downcast should succeed")
                    .value(row_index),
            ),
            DataType::UInt32 => Self::UInt32(
                array
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .expect("uint32 array downcast should succeed")
                    .value(row_index),
            ),
            DataType::UInt64 => Self::UInt64(
                array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("uint64 array downcast should succeed")
                    .value(row_index),
            ),
            DataType::Utf8 => Self::Utf8(
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8 array downcast should succeed")
                    .value(row_index)
                    .to_string(),
            ),
            DataType::LargeUtf8 => Self::Utf8(
                array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .expect("large utf8 array downcast should succeed")
                    .value(row_index)
                    .to_string(),
            ),
            DataType::Null => Self::Null,
            unsupported => {
                anyhow::bail!(
                    "Unsupported managed table value type in current prototype: {unsupported:?}"
                )
            }
        };

        Ok(value)
    }

    fn from_sql_literal(raw: &str, data_type: &PersistedDataType, nullable: bool) -> Result<Self> {
        let trimmed = raw.trim();

        if trimmed.eq_ignore_ascii_case("NULL") {
            if !nullable {
                anyhow::bail!("NULL is not allowed for a NOT NULL column");
            }

            return Ok(Self::Null);
        }

        let value = match data_type {
            PersistedDataType::Boolean => {
                if trimmed.eq_ignore_ascii_case("TRUE") {
                    Self::Boolean(true)
                } else if trimmed.eq_ignore_ascii_case("FALSE") {
                    Self::Boolean(false)
                } else {
                    anyhow::bail!("Expected BOOLEAN literal, found '{}'", trimmed);
                }
            }
            PersistedDataType::Float32 => Self::Float32(trimmed.parse::<f32>()?),
            PersistedDataType::Float64 => Self::Float64(trimmed.parse::<f64>()?),
            PersistedDataType::Int32 => Self::Int32(trimmed.parse::<i32>()?),
            PersistedDataType::Int64 => Self::Int64(trimmed.parse::<i64>()?),
            PersistedDataType::Null => {
                anyhow::bail!("NULL-only columns are not writable in the current prototype")
            }
            PersistedDataType::UInt32 => Self::UInt32(trimmed.parse::<u32>()?),
            PersistedDataType::UInt64 => Self::UInt64(trimmed.parse::<u64>()?),
            PersistedDataType::Utf8 => Self::Utf8(parse_sql_string_literal(trimmed)?),
        };

        Ok(value)
    }
}

impl PersistedTableFile {
    fn empty_from_schema(schema: &Schema) -> Result<Self> {
        Ok(Self {
            storage_layout: PersistedStorageLayout::Columnar,
            schema: schema
                .fields()
                .iter()
                .map(|field| PersistedField::from_arrow(field.as_ref()))
                .collect::<Result<Vec<_>>>()?,
            columns: schema
                .fields()
                .iter()
                .map(|_| PersistedColumn { values: Vec::new() })
                .collect(),
        })
    }

    fn from_batches(schema: &Schema, batches: &[RecordBatch]) -> Result<Self> {
        let persisted_schema = schema
            .fields()
            .iter()
            .map(|field| PersistedField::from_arrow(field.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        let mut columns = Vec::with_capacity(schema.fields().len());

        for column_index in 0..schema.fields().len() {
            let mut values = Vec::new();

            for batch in batches {
                for row_index in 0..batch.num_rows() {
                    let value =
                        PersistedScalarValue::from_array(batch.column(column_index), row_index)?;
                    values.push(value);
                }
            }

            columns.push(PersistedColumn { values });
        }

        Ok(Self {
            storage_layout: PersistedStorageLayout::Columnar,
            schema: persisted_schema,
            columns,
        })
    }

    fn to_record_batch(&self) -> Result<RecordBatch> {
        if self.storage_layout != PersistedStorageLayout::Columnar {
            anyhow::bail!("Unsupported managed table storage layout");
        }

        let schema = Arc::new(Schema::new(
            self.schema
                .iter()
                .map(PersistedField::to_arrow_field)
                .collect::<Vec<_>>(),
        ));
        let arrays = self
            .schema
            .iter()
            .enumerate()
            .map(|(column_index, field)| self.build_array(column_index, field))
            .collect::<Result<Vec<_>>>()?;

        Ok(RecordBatch::try_new(schema, arrays)?)
    }

    fn build_array(&self, column_index: usize, field: &PersistedField) -> Result<ArrayRef> {
        let column = self
            .columns
            .get(column_index)
            .ok_or_else(|| anyhow::anyhow!("Missing column at column index {column_index}"))?;

        let array: ArrayRef = match field.data_type {
            PersistedDataType::Boolean => Arc::new(BooleanArray::from(self.collect_optional(
                column,
                |value| match value {
                    PersistedScalarValue::Boolean(value) => Ok(Some(*value)),
                    PersistedScalarValue::Null => Ok(None),
                    other => anyhow::bail!("Expected boolean value, found {other:?}"),
                },
            )?)),
            PersistedDataType::Float32 => Arc::new(Float32Array::from(self.collect_optional(
                column,
                |value| match value {
                    PersistedScalarValue::Float32(value) => Ok(Some(*value)),
                    PersistedScalarValue::Null => Ok(None),
                    other => anyhow::bail!("Expected float32 value, found {other:?}"),
                },
            )?)),
            PersistedDataType::Float64 => Arc::new(Float64Array::from(self.collect_optional(
                column,
                |value| match value {
                    PersistedScalarValue::Float64(value) => Ok(Some(*value)),
                    PersistedScalarValue::Null => Ok(None),
                    other => anyhow::bail!("Expected float64 value, found {other:?}"),
                },
            )?)),
            PersistedDataType::Int32 => Arc::new(Int32Array::from(self.collect_optional(
                column,
                |value| match value {
                    PersistedScalarValue::Int32(value) => Ok(Some(*value)),
                    PersistedScalarValue::Null => Ok(None),
                    other => anyhow::bail!("Expected int32 value, found {other:?}"),
                },
            )?)),
            PersistedDataType::Int64 => Arc::new(Int64Array::from(self.collect_optional(
                column,
                |value| match value {
                    PersistedScalarValue::Int64(value) => Ok(Some(*value)),
                    PersistedScalarValue::Null => Ok(None),
                    other => anyhow::bail!("Expected int64 value, found {other:?}"),
                },
            )?)),
            PersistedDataType::UInt32 => Arc::new(UInt32Array::from(self.collect_optional(
                column,
                |value| match value {
                    PersistedScalarValue::UInt32(value) => Ok(Some(*value)),
                    PersistedScalarValue::Null => Ok(None),
                    other => anyhow::bail!("Expected uint32 value, found {other:?}"),
                },
            )?)),
            PersistedDataType::UInt64 => Arc::new(UInt64Array::from(self.collect_optional(
                column,
                |value| match value {
                    PersistedScalarValue::UInt64(value) => Ok(Some(*value)),
                    PersistedScalarValue::Null => Ok(None),
                    other => anyhow::bail!("Expected uint64 value, found {other:?}"),
                },
            )?)),
            PersistedDataType::Utf8 => Arc::new(StringArray::from(self.collect_optional(
                column,
                |value| match value {
                    PersistedScalarValue::Utf8(value) => Ok(Some(value.as_str())),
                    PersistedScalarValue::Null => Ok(None),
                    other => anyhow::bail!("Expected utf8 value, found {other:?}"),
                },
            )?)),
            PersistedDataType::Null => Arc::new(NullArray::new(column.values.len())),
        };

        Ok(array)
    }

    fn collect_optional<'a, T>(
        &'a self,
        column: &'a PersistedColumn,
        mapper: impl Fn(&'a PersistedScalarValue) -> Result<Option<T>>,
    ) -> Result<Vec<Option<T>>> {
        column.values.iter().map(mapper).collect()
    }

    fn row_count(&self) -> usize {
        self.columns.first().map_or(0, |column| column.values.len())
    }

    fn append_rows(
        &mut self,
        selected_columns: Option<&[String]>,
        rows: &[Vec<String>],
    ) -> Result<usize> {
        let mut column_order = Vec::new();

        match selected_columns {
            Some(selected_columns) => {
                let mut seen = BTreeSet::new();

                for selected_column in selected_columns {
                    let normalized_name = selected_column.to_ascii_lowercase();
                    if !seen.insert(normalized_name) {
                        anyhow::bail!("Duplicate column '{}' in INSERT target", selected_column);
                    }

                    let column_index = self
                        .schema
                        .iter()
                        .position(|field| field.name.eq_ignore_ascii_case(selected_column))
                        .ok_or_else(|| anyhow::anyhow!("Unknown column '{}'", selected_column))?;
                    column_order.push(column_index);
                }
            }
            None => column_order.extend(0..self.schema.len()),
        }

        for row in rows {
            if row.len() != column_order.len() {
                anyhow::bail!(
                    "Expected {} value(s) per row, found {}",
                    column_order.len(),
                    row.len()
                );
            }

            let mut row_values = vec![PersistedScalarValue::Null; self.schema.len()];
            let mut assigned_columns = vec![false; self.schema.len()];

            for (value_index, raw_value) in row.iter().enumerate() {
                let column_index = column_order[value_index];
                let field = self
                    .schema
                    .get(column_index)
                    .ok_or_else(|| anyhow::anyhow!("Missing field at index {column_index}"))?;
                let value = PersistedScalarValue::from_sql_literal(
                    raw_value,
                    &field.data_type,
                    field.nullable,
                )?;
                row_values[column_index] = value;
                assigned_columns[column_index] = true;
            }

            for (column_index, field) in self.schema.iter().enumerate() {
                if !assigned_columns[column_index] && !field.nullable {
                    anyhow::bail!(
                        "Column '{}' must be provided because it is NOT NULL",
                        field.name
                    );
                }

                self.columns
                    .get_mut(column_index)
                    .ok_or_else(|| anyhow::anyhow!("Missing column at index {column_index}"))?
                    .values
                    .push(row_values[column_index].clone());
            }
        }

        Ok(rows.len())
    }
}

fn persist_table_snapshot(path: &Path, schema: &Schema, batches: &[RecordBatch]) -> Result<usize> {
    let snapshot = PersistedTableFile::from_batches(schema, batches)?;
    write_table_snapshot(path, &snapshot)?;

    Ok(snapshot.row_count())
}

fn persist_empty_table_snapshot(path: &Path, schema: &Schema) -> Result<()> {
    let snapshot = PersistedTableFile::empty_from_schema(schema)?;
    write_table_snapshot(path, &snapshot)
}

fn write_table_snapshot(path: &Path, snapshot: &PersistedTableFile) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::write(path, serde_json::to_string_pretty(snapshot)?)?;

    Ok(())
}

fn append_rows_to_table_snapshot(
    path: &Path,
    selected_columns: Option<&[String]>,
    rows: &[Vec<String>],
) -> Result<usize> {
    let mut snapshot = load_persisted_table_file(path)?;
    let inserted_row_count = snapshot.append_rows(selected_columns, rows)?;
    write_table_snapshot(path, &snapshot)?;

    Ok(inserted_row_count)
}

fn load_persisted_table_file(path: &Path) -> Result<PersistedTableFile> {
    let raw = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&raw)?)
}

fn load_persisted_table_snapshot(path: &Path) -> Result<RecordBatch> {
    let snapshot = load_persisted_table_file(path)?;
    snapshot.to_record_batch()
}

fn parse_sql_string_literal(raw: &str) -> Result<String> {
    if !(raw.starts_with('\'') && raw.ends_with('\'')) {
        anyhow::bail!("Expected quoted string literal, found '{}'", raw);
    }

    Ok(raw[1..raw.len() - 1].replace("''", "'"))
}

async fn register_persisted_tables(
    context: &SessionContext,
    control_plane: &ControlPlane,
    session: &analyticsdb_core::SessionContext,
) -> Result<()> {
    for table in control_plane.list_tables_for_session(session)? {
        let Some(storage_path) = table.storage_path.as_deref() else {
            anyhow::bail!(
                "Table '{}.{}.{}' is missing a storage path",
                table.database,
                table.schema,
                table.name
            );
        };

        match table.external_format {
            Some(ExternalStorageFormat::Parquet) => {
                context
                    .register_parquet(&table.name, storage_path, Default::default())
                    .await?;
            }
            None => {
                let batch = load_persisted_table_snapshot(Path::new(storage_path))?;
                let provider = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
                context.register_table(table.name, Arc::new(provider))?;
            }
        }
    }

    Ok(())
}

async fn register_persisted_views(
    context: &SessionContext,
    control_plane: &ControlPlane,
    session: &analyticsdb_core::SessionContext,
) -> Result<()> {
    for view in control_plane.list_views_for_session(session)? {
        if let Some(definition_sql) = view.definition_sql.as_deref() {
            let statement = format!("CREATE VIEW {} AS {}", view.name, definition_sql);
            context.sql(&statement).await?.collect().await?;
        }
    }

    Ok(())
}

fn batches_to_rows(batches: &[RecordBatch]) -> Result<Vec<Vec<String>>> {
    let mut rows = Vec::new();

    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());

            for column_index in 0..batch.num_columns() {
                let value = array_value_to_string(batch.column(column_index).as_ref(), row_index)?;
                row.push(value);
            }

            rows.push(row);
        }
    }

    Ok(rows)
}

fn utf8_record_batch(columns: &[&str], rows: &[Vec<String>]) -> Result<RecordBatch> {
    let arrays = columns
        .iter()
        .enumerate()
        .map(|(column_index, column_name)| {
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                let value = row.get(column_index).ok_or_else(|| {
                    anyhow::anyhow!(
                        "Row had {} column(s), expected at least {}",
                        row.len(),
                        column_index + 1
                    )
                })?;
                values.push(Some(value.as_str()));
            }

            Ok((
                (*column_name).to_string(),
                Arc::new(StringArray::from(values)) as ArrayRef,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(RecordBatch::try_from_iter(arrays)?)
}

#[cfg(test)]
mod tests {
    use analyticsdb_core::{Protocol, QueryRequest, SessionContext};
    use serde_json::Value;

    use super::{PersistedStorageLayout, PrototypeEngine};

    #[test]
    fn executes_scalar_select() {
        let engine = PrototypeEngine::new().expect("engine should initialize");
        let response = engine
            .execute_query(&QueryRequest {
                sql: "SELECT 42 AS answer".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("query should execute");

        assert!(response.query_id.starts_with("q-"));
        assert_eq!(response.coordinator_node_id, "control-1");
        assert_eq!(response.session.database, "postgres");
        assert_eq!(response.columns, vec!["answer"]);
        assert_eq!(response.rows, vec![vec!["42".to_string()]]);
        assert!(response.message.contains("1 row(s)"));
    }

    #[test]
    fn rejects_unknown_schema_before_execution() {
        let engine = PrototypeEngine::new().expect("engine should initialize");
        let error = engine
            .execute_query(&QueryRequest {
                sql: "SELECT 1".to_string(),
                session: SessionContext {
                    schema: "missing".to_string(),
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect_err("unknown schema should fail");

        assert!(error.to_string().contains("Unknown schema"));
    }

    #[test]
    fn executes_metadata_show_databases() {
        let engine = PrototypeEngine::new().expect("engine should initialize");
        let response = engine
            .execute_query(&QueryRequest {
                sql: "SHOW DATABASES".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("metadata query should execute");

        assert_eq!(response.columns, vec!["database_name"]);
        assert!(response
            .rows
            .iter()
            .any(|row| row == &vec!["postgres".to_string()]));
        assert!(response.message.contains("database(s) listed"));
    }

    #[test]
    fn executes_metadata_alter_user_password() {
        let engine = PrototypeEngine::new().expect("engine should initialize");

        let response = engine
            .execute_query(&QueryRequest {
                sql: "ALTER USER analytics_reader PASSWORD 'reader-v2'".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("alter user password should execute through metadata path");
        assert!(response.message.contains("Credentials rotated"));

        let control_plane = engine.control_plane();
        let stale = control_plane
            .validate_credentials("analytics_reader", Some("analytics_reader"))
            .expect_err("old password should be invalidated after ALTER USER");
        assert!(stale.to_string().contains("Invalid credentials"));
        control_plane
            .validate_credentials("analytics_reader", Some("reader-v2"))
            .expect("new password should be valid");
    }

    #[test]
    fn executes_persisted_view_query() {
        let catalog_path = {
            let mut path = std::env::temp_dir();
            path.push(format!(
                "analyticsdb-engine-view-{}.json",
                uuid::Uuid::now_v7()
            ));
            path
        };

        let engine = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE VIEW daily_metrics AS SELECT 7 AS metric".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("view creation should succeed");

        let reloaded = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should reload");

        let response = reloaded
            .execute_query(&QueryRequest {
                sql: "SELECT * FROM daily_metrics".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("persisted view should be queryable");

        assert_eq!(response.columns, vec!["metric"]);
        assert_eq!(response.rows, vec![vec!["7".to_string()]]);

        let _ = std::fs::remove_file(catalog_path);
    }

    #[test]
    fn executes_persisted_table_query() {
        let catalog_path = {
            let mut path = std::env::temp_dir();
            path.push(format!(
                "analyticsdb-engine-table-{}.json",
                uuid::Uuid::now_v7()
            ));
            path
        };

        let engine = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics AS SELECT 11 AS metric, 'ok' AS status".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("table creation should succeed");

        let reloaded = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should reload");

        let response = reloaded
            .execute_query(&QueryRequest {
                sql: "SELECT metric, status FROM fact_metrics".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("persisted table should be queryable");

        assert_eq!(response.columns, vec!["metric", "status"]);
        assert_eq!(
            response.rows,
            vec![vec!["11".to_string(), "ok".to_string()]]
        );

        let _ = std::fs::remove_file(&catalog_path);
        let managed_dir = catalog_path.with_file_name(format!(
            "{}.managed",
            catalog_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("catalog file stem should be present")
        ));
        let _ = std::fs::remove_dir_all(managed_dir);
    }

    #[test]
    fn creates_explicit_table_inserts_rows_and_queries_it() {
        let catalog_path = {
            let mut path = std::env::temp_dir();
            path.push(format!(
                "analyticsdb-engine-insert-{}.json",
                uuid::Uuid::now_v7()
            ));
            path
        };

        let engine = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT, is_hot BOOLEAN)"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("table definition should succeed");

        engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO fact_metrics VALUES (11, 'ok', true), (12, 'warn', false)"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("insert should succeed");

        let reloaded = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should reload");

        let response = reloaded
            .execute_query(&QueryRequest {
                sql: "SELECT metric, status, is_hot FROM fact_metrics ORDER BY metric".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("persisted inserted rows should be queryable");

        assert_eq!(response.columns, vec!["metric", "status", "is_hot"]);
        assert_eq!(
            response.rows,
            vec![
                vec!["11".to_string(), "ok".to_string(), "true".to_string()],
                vec!["12".to_string(), "warn".to_string(), "false".to_string()]
            ]
        );

        let describe = reloaded
            .execute_query(&QueryRequest {
                sql: "DESCRIBE fact_metrics".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("describe should succeed");

        assert!(describe.rows.iter().any(|row| {
            row == &vec!["metric".to_string(), "Int64".to_string(), "NO".to_string()]
        }));
        assert!(describe.rows.iter().any(|row| {
            row == &vec!["status".to_string(), "Utf8".to_string(), "YES".to_string()]
        }));
        assert!(describe.rows.iter().any(|row| {
            row == &vec![
                "is_hot".to_string(),
                "Boolean".to_string(),
                "YES".to_string(),
            ]
        }));

        let _ = std::fs::remove_file(&catalog_path);
        let managed_dir = catalog_path.with_file_name(format!(
            "{}.managed",
            catalog_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("catalog file stem should be present")
        ));
        let _ = std::fs::remove_dir_all(managed_dir);
    }

    #[test]
    fn inserts_with_column_list_and_omitted_nullable_columns() {
        let catalog_path = {
            let mut path = std::env::temp_dir();
            path.push(format!(
                "analyticsdb-engine-insert-columns-{}.json",
                uuid::Uuid::now_v7()
            ));
            path
        };

        let engine = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics (metric INTEGER NOT NULL, status VARCHAR(20), score DOUBLE PRECISION, active BOOLEAN)"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("table definition should succeed");

        engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO fact_metrics (metric, active, status) VALUES (11, true, 'ok''s')"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("insert should succeed");

        let response = engine
            .execute_query(&QueryRequest {
                sql: "SELECT metric, status, score, active FROM fact_metrics".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("query should succeed");

        assert_eq!(
            response.columns,
            vec!["metric", "status", "score", "active"]
        );
        assert_eq!(
            response.rows,
            vec![vec![
                "11".to_string(),
                "ok's".to_string(),
                "".to_string(),
                "true".to_string()
            ]]
        );

        let describe = engine
            .execute_query(&QueryRequest {
                sql: "DESCRIBE fact_metrics".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("describe should succeed");

        assert!(describe.rows.iter().any(|row| {
            row == &vec!["metric".to_string(), "Int32".to_string(), "NO".to_string()]
        }));
        assert!(describe.rows.iter().any(|row| {
            row == &vec![
                "score".to_string(),
                "Float64".to_string(),
                "YES".to_string(),
            ]
        }));

        let _ = std::fs::remove_file(&catalog_path);
        let managed_dir = catalog_path.with_file_name(format!(
            "{}.managed",
            catalog_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("catalog file stem should be present")
        ));
        let _ = std::fs::remove_dir_all(managed_dir);
    }

    #[test]
    fn rejects_insert_with_wrong_column_count() {
        let catalog_path = {
            let mut path = std::env::temp_dir();
            path.push(format!(
                "analyticsdb-engine-insert-error-{}.json",
                uuid::Uuid::now_v7()
            ));
            path
        };

        let engine = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT)".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("table definition should succeed");

        let error = engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO fact_metrics VALUES (11)".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect_err("insert should fail");

        assert!(error
            .to_string()
            .contains("Expected 2 value(s) per row, found 1"));

        let _ = std::fs::remove_file(&catalog_path);
        let managed_dir = catalog_path.with_file_name(format!(
            "{}.managed",
            catalog_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("catalog file stem should be present")
        ));
        let _ = std::fs::remove_dir_all(managed_dir);
    }

    #[test]
    fn rejects_insert_when_not_null_column_is_omitted() {
        let catalog_path = {
            let mut path = std::env::temp_dir();
            path.push(format!(
                "analyticsdb-engine-insert-not-null-{}.json",
                uuid::Uuid::now_v7()
            ));
            path
        };

        let engine = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT)".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("table definition should succeed");

        let error = engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO fact_metrics (status) VALUES ('ok')".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect_err("insert should fail");

        assert!(error
            .to_string()
            .contains("Column 'metric' must be provided because it is NOT NULL"));

        let _ = std::fs::remove_file(&catalog_path);
        let managed_dir = catalog_path.with_file_name(format!(
            "{}.managed",
            catalog_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("catalog file stem should be present")
        ));
        let _ = std::fs::remove_dir_all(managed_dir);
    }

    #[test]
    fn persists_managed_table_in_columnar_layout() {
        let catalog_path = {
            let mut path = std::env::temp_dir();
            path.push(format!(
                "analyticsdb-engine-columnar-{}.json",
                uuid::Uuid::now_v7()
            ));
            path
        };

        let engine = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics AS SELECT 11 AS metric, 'ok' AS status".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("table creation should succeed");

        let managed_dir = catalog_path.with_file_name(format!(
            "{}.managed",
            catalog_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("catalog file stem should be present")
        ));
        let table_file = managed_dir.join("postgres__public__fact_metrics.table.json");
        let raw = std::fs::read_to_string(&table_file).expect("table snapshot should exist");
        let parsed: Value = serde_json::from_str(&raw).expect("table snapshot should be json");

        assert_eq!(
            parsed
                .get("storage_layout")
                .and_then(Value::as_str)
                .expect("storage layout should be present"),
            match PersistedStorageLayout::Columnar {
                PersistedStorageLayout::Columnar => "Columnar",
            }
        );
        assert!(parsed.get("columns").and_then(Value::as_array).is_some());
        assert!(parsed.get("rows").is_none());

        let _ = std::fs::remove_file(&catalog_path);
        let _ = std::fs::remove_dir_all(managed_dir);
    }

    #[test]
    fn describes_persisted_table_columns() {
        let catalog_path = {
            let mut path = std::env::temp_dir();
            path.push(format!(
                "analyticsdb-engine-describe-{}.json",
                uuid::Uuid::now_v7()
            ));
            path
        };

        let engine = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics AS SELECT 11 AS metric, 'ok' AS status".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("table creation should succeed");

        let response = engine
            .execute_query(&QueryRequest {
                sql: "DESCRIBE fact_metrics".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("describe should succeed");

        assert_eq!(
            response.columns,
            vec!["column_name", "data_type", "is_nullable"]
        );
        assert!(response.rows.iter().any(|row| {
            row == &vec!["metric".to_string(), "Int64".to_string(), "NO".to_string()]
        }));
        assert!(response.rows.iter().any(|row| {
            row == &vec!["status".to_string(), "Utf8".to_string(), "NO".to_string()]
        }));

        let _ = std::fs::remove_file(&catalog_path);
        let managed_dir = catalog_path.with_file_name(format!(
            "{}.managed",
            catalog_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("catalog file stem should be present")
        ));
        let _ = std::fs::remove_dir_all(managed_dir);
    }

    #[test]
    fn exposes_pg_catalog_tables_views_and_namespace_rows() {
        let catalog_path = {
            let mut path = std::env::temp_dir();
            path.push(format!(
                "analyticsdb-engine-pg-catalog-{}.json",
                uuid::Uuid::now_v7()
            ));
            path
        };
        let engine = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should initialize");
        let session = SessionContext {
            protocol: Protocol::Embedded,
            ..SessionContext::default()
        };

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE SCHEMA reporting".to_string(),
                session: session.clone(),
            })
            .expect("schema creation should succeed");
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE SCHEMA alpha".to_string(),
                session: session.clone(),
            })
            .expect("alpha schema creation should succeed");
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE reporting.fact_metrics (metric BIGINT NOT NULL, status TEXT)"
                    .to_string(),
                session: session.clone(),
            })
            .expect("table creation should succeed");
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE VIEW reporting.daily_metrics AS SELECT metric, status FROM reporting.fact_metrics"
                    .to_string(),
                session: session.clone(),
            })
            .expect("view creation should succeed");
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE alpha.fact_alpha (metric BIGINT NOT NULL, status TEXT)"
                    .to_string(),
                session: session.clone(),
            })
            .expect("alpha table creation should succeed");
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE VIEW alpha.daily_alpha AS SELECT metric, status FROM alpha.fact_alpha"
                    .to_string(),
                session: session.clone(),
            })
            .expect("alpha view creation should succeed");
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE reporting.dim_metrics (metric BIGINT NOT NULL, label TEXT, CONSTRAINT dim_metrics_pkey PRIMARY KEY (metric))"
                    .to_string(),
                session: session.clone(),
            })
            .expect("dim_metrics table creation should succeed");
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE reporting.fact_events (metric_id BIGINT NOT NULL, CONSTRAINT fact_events_metric_fk FOREIGN KEY (metric_id) REFERENCES reporting.dim_metrics(metric))"
                    .to_string(),
                session: session.clone(),
            })
            .expect("fact_events table creation should succeed");

        let tables = engine
            .execute_query(&QueryRequest {
                sql: "SELECT * FROM pg_catalog.pg_tables".to_string(),
                session: session.clone(),
            })
            .expect("pg_tables query should succeed");
        assert_eq!(
            tables.columns,
            vec![
                "schemaname",
                "tablename",
                "tableowner",
                "tablespace",
                "hasindexes",
                "hasrules",
                "hastriggers",
                "rowsecurity",
            ]
        );
        assert!(
            tables.rows.iter().any(|row| {
                row.first() == Some(&"reporting".to_string())
                    && row.get(1) == Some(&"fact_metrics".to_string())
            }),
            "pg_tables rows={:?}",
            tables.rows
        );

        let views = engine
            .execute_query(&QueryRequest {
                sql: "SELECT * FROM pg_catalog.pg_views".to_string(),
                session: session.clone(),
            })
            .expect("pg_views query should succeed");
        assert_eq!(
            views.columns,
            vec!["schemaname", "viewname", "viewowner", "definition"]
        );
        assert!(
            views.rows.iter().any(|row| {
                row.first() == Some(&"reporting".to_string())
                    && row.get(1) == Some(&"daily_metrics".to_string())
            }),
            "pg_views rows={:?}",
            views.rows
        );

        let namespaces = engine
            .execute_query(&QueryRequest {
                sql: "SELECT * FROM pg_catalog.pg_namespace".to_string(),
                session,
            })
            .expect("pg_namespace query should succeed");
        assert_eq!(
            namespaces.columns,
            vec!["oid", "nspname", "nspowner", "nspacl"]
        );
        assert!(
            namespaces
                .rows
                .iter()
                .any(|row| row.get(1) == Some(&"public".to_string())),
            "pg_namespace rows={:?}",
            namespaces.rows
        );
        assert!(
            namespaces
                .rows
                .iter()
                .any(|row| row.get(1) == Some(&"reporting".to_string())),
            "pg_namespace rows={:?}",
            namespaces.rows
        );

        let databases = engine
            .execute_query(&QueryRequest {
                sql: "SELECT datname FROM pg_catalog.pg_database ORDER BY datname".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("pg_database query should succeed");
        assert_eq!(databases.columns, vec!["datname"]);
        assert_eq!(databases.rows, vec![vec!["postgres".to_string()]]);

        let roles = engine
            .execute_query(&QueryRequest {
                sql: "SELECT rolname FROM pg_catalog.pg_roles WHERE rolname IN ('postgres', 'analyticsdb_admin') ORDER BY rolname"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("pg_roles query should succeed");
        assert_eq!(roles.columns, vec!["rolname"]);
        assert_eq!(
            roles.rows,
            vec![
                vec!["analyticsdb_admin".to_string()],
                vec!["postgres".to_string()]
            ]
        );

        let filtered_tables = engine
            .execute_query(&QueryRequest {
                sql: "SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname = 'reporting' ORDER BY tablename"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("filtered pg_tables query should succeed");
        assert_eq!(filtered_tables.columns, vec!["schemaname", "tablename"]);
        assert_eq!(
            filtered_tables.rows,
            vec![
                vec!["reporting".to_string(), "dim_metrics".to_string()],
                vec!["reporting".to_string(), "fact_events".to_string()],
                vec!["reporting".to_string(), "fact_metrics".to_string()],
            ]
        );

        let filtered_namespace = engine
            .execute_query(&QueryRequest {
                sql: "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = 'reporting'"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("filtered pg_namespace query should succeed");
        assert_eq!(filtered_namespace.columns, vec!["nspname"]);
        assert_eq!(filtered_namespace.rows, vec![vec!["reporting".to_string()]]);

        let in_filtered_tables = engine
            .execute_query(&QueryRequest {
                sql: "SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname IN ('reporting', 'alpha') ORDER BY schemaname, tablename"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("IN-filtered pg_tables query should succeed");
        assert_eq!(in_filtered_tables.columns, vec!["schemaname", "tablename"]);
        assert_eq!(
            in_filtered_tables.rows,
            vec![
                vec!["alpha".to_string(), "fact_alpha".to_string()],
                vec!["reporting".to_string(), "dim_metrics".to_string()],
                vec!["reporting".to_string(), "fact_events".to_string()],
                vec!["reporting".to_string(), "fact_metrics".to_string()],
            ]
        );

        let in_filtered_views = engine
            .execute_query(&QueryRequest {
                sql: "SELECT schemaname, viewname FROM pg_catalog.pg_views WHERE schemaname IN ('reporting', 'alpha') ORDER BY schemaname, viewname"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("IN-filtered pg_views query should succeed");
        assert_eq!(in_filtered_views.columns, vec!["schemaname", "viewname"]);
        assert_eq!(
            in_filtered_views.rows,
            vec![
                vec!["alpha".to_string(), "daily_alpha".to_string()],
                vec!["reporting".to_string(), "daily_metrics".to_string()],
            ]
        );

        let desc_tables = engine
            .execute_query(&QueryRequest {
                sql: "SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname IN ('reporting', 'alpha') ORDER BY schemaname DESC, tablename ASC"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("DESC pg_tables query should succeed");
        assert_eq!(
            desc_tables.rows,
            vec![
                vec!["reporting".to_string(), "dim_metrics".to_string()],
                vec!["reporting".to_string(), "fact_events".to_string()],
                vec!["reporting".to_string(), "fact_metrics".to_string()],
                vec!["alpha".to_string(), "fact_alpha".to_string()],
            ]
        );

        let desc_views = engine
            .execute_query(&QueryRequest {
                sql: "SELECT schemaname, viewname FROM pg_catalog.pg_views WHERE schemaname IN ('reporting', 'alpha') ORDER BY schemaname DESC, viewname ASC"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("DESC pg_views query should succeed");
        assert_eq!(
            desc_views.rows,
            vec![
                vec!["reporting".to_string(), "daily_metrics".to_string()],
                vec!["alpha".to_string(), "daily_alpha".to_string()],
            ]
        );

        let desc_namespace = engine
            .execute_query(&QueryRequest {
                sql: "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname IN ('reporting', 'public', 'alpha') ORDER BY nspname DESC"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("DESC pg_namespace query should succeed");
        assert_eq!(
            desc_namespace.rows,
            vec![
                vec!["reporting".to_string()],
                vec!["public".to_string()],
                vec!["alpha".to_string()],
            ]
        );

        let info_schemata = engine
            .execute_query(&QueryRequest {
                sql: "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("information_schema.schemata query should succeed");
        assert_eq!(info_schemata.columns, vec!["schema_name"]);
        assert!(info_schemata
            .rows
            .iter()
            .any(|row| row.first() == Some(&"reporting".to_string())));

        let info_tables = engine
            .execute_query(&QueryRequest {
                sql: "SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema IN ('reporting', 'alpha') ORDER BY table_schema, table_name"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("information_schema.tables query should succeed");
        assert_eq!(
            info_tables.rows,
            vec![
                vec![
                    "alpha".to_string(),
                    "daily_alpha".to_string(),
                    "VIEW".to_string()
                ],
                vec![
                    "alpha".to_string(),
                    "fact_alpha".to_string(),
                    "BASE TABLE".to_string()
                ],
                vec![
                    "reporting".to_string(),
                    "daily_metrics".to_string(),
                    "VIEW".to_string()
                ],
                vec![
                    "reporting".to_string(),
                    "dim_metrics".to_string(),
                    "BASE TABLE".to_string()
                ],
                vec![
                    "reporting".to_string(),
                    "fact_events".to_string(),
                    "BASE TABLE".to_string()
                ],
                vec![
                    "reporting".to_string(),
                    "fact_metrics".to_string(),
                    "BASE TABLE".to_string()
                ],
            ]
        );

        let info_columns = engine
            .execute_query(&QueryRequest {
                sql: "SELECT table_schema, table_name, column_name, ordinal_position, is_nullable, data_type FROM information_schema.columns WHERE table_name = 'fact_metrics' ORDER BY ordinal_position"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("information_schema.columns query should succeed");
        assert_eq!(
            info_columns.rows,
            vec![
                vec![
                    "reporting".to_string(),
                    "fact_metrics".to_string(),
                    "metric".to_string(),
                    "1".to_string(),
                    "NO".to_string(),
                    "int64".to_string()
                ],
                vec![
                    "reporting".to_string(),
                    "fact_metrics".to_string(),
                    "status".to_string(),
                    "2".to_string(),
                    "YES".to_string(),
                    "utf8".to_string()
                ],
            ]
        );

        let info_views = engine
            .execute_query(&QueryRequest {
                sql: "SELECT table_schema, table_name, is_updatable FROM information_schema.views WHERE table_schema IN ('reporting', 'alpha') ORDER BY table_schema, table_name"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("information_schema.views query should succeed");
        assert_eq!(
            info_views.rows,
            vec![
                vec![
                    "alpha".to_string(),
                    "daily_alpha".to_string(),
                    "NO".to_string()
                ],
                vec![
                    "reporting".to_string(),
                    "daily_metrics".to_string(),
                    "NO".to_string()
                ],
            ]
        );

        let info_constraints = engine
            .execute_query(&QueryRequest {
                sql: "SELECT constraint_catalog, constraint_schema, constraint_name, table_catalog, table_schema, table_name, constraint_type, is_deferrable, initially_deferred, enforced, nulls_distinct FROM information_schema.table_constraints WHERE constraint_name IN ('fact_alpha_metric_not_null', 'fact_metrics_metric_not_null') ORDER BY constraint_name"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("information_schema.table_constraints query should succeed");
        assert_eq!(
            info_constraints.rows,
            vec![
                vec![
                    "postgres".to_string(),
                    "alpha".to_string(),
                    "fact_alpha_metric_not_null".to_string(),
                    "postgres".to_string(),
                    "alpha".to_string(),
                    "fact_alpha".to_string(),
                    "CHECK".to_string(),
                    "NO".to_string(),
                    "NO".to_string(),
                    "YES".to_string(),
                    "".to_string(),
                ],
                vec![
                    "postgres".to_string(),
                    "reporting".to_string(),
                    "fact_metrics_metric_not_null".to_string(),
                    "postgres".to_string(),
                    "reporting".to_string(),
                    "fact_metrics".to_string(),
                    "CHECK".to_string(),
                    "NO".to_string(),
                    "NO".to_string(),
                    "YES".to_string(),
                    "".to_string(),
                ],
            ]
        );

        let info_key_usage = engine
            .execute_query(&QueryRequest {
                sql: "SELECT table_name, column_name, constraint_name FROM information_schema.key_column_usage WHERE table_name IN ('dim_metrics', 'fact_events') ORDER BY table_name, constraint_name"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("information_schema.key_column_usage query should succeed");
        assert_eq!(
            info_key_usage.rows,
            vec![
                vec![
                    "dim_metrics".to_string(),
                    "metric".to_string(),
                    "dim_metrics_pkey".to_string(),
                ],
                vec![
                    "fact_events".to_string(),
                    "metric_id".to_string(),
                    "fact_events_metric_fk".to_string(),
                ],
            ]
        );

        let info_constraint_column_usage = engine
            .execute_query(&QueryRequest {
                sql: "SELECT table_catalog, table_schema, table_name, column_name, constraint_catalog, constraint_schema, constraint_name FROM information_schema.constraint_column_usage WHERE constraint_name IN ('fact_alpha_metric_not_null', 'fact_metrics_metric_not_null') ORDER BY constraint_name"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("information_schema.constraint_column_usage query should succeed");
        assert_eq!(
            info_constraint_column_usage.columns,
            vec![
                "table_catalog",
                "table_schema",
                "table_name",
                "column_name",
                "constraint_catalog",
                "constraint_schema",
                "constraint_name",
            ]
        );
        assert_eq!(
            info_constraint_column_usage.rows,
            vec![
                vec![
                    "postgres".to_string(),
                    "alpha".to_string(),
                    "fact_alpha".to_string(),
                    "metric".to_string(),
                    "postgres".to_string(),
                    "alpha".to_string(),
                    "fact_alpha_metric_not_null".to_string(),
                ],
                vec![
                    "postgres".to_string(),
                    "reporting".to_string(),
                    "fact_metrics".to_string(),
                    "metric".to_string(),
                    "postgres".to_string(),
                    "reporting".to_string(),
                    "fact_metrics_metric_not_null".to_string(),
                ],
            ]
        );

        let info_constraint_table_usage = engine
            .execute_query(&QueryRequest {
                sql: "SELECT table_catalog, table_schema, table_name, constraint_catalog, constraint_schema, constraint_name FROM information_schema.constraint_table_usage WHERE constraint_name IN ('fact_alpha_metric_not_null', 'fact_metrics_metric_not_null') ORDER BY constraint_name"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("information_schema.constraint_table_usage query should succeed");
        assert_eq!(
            info_constraint_table_usage.columns,
            vec![
                "table_catalog",
                "table_schema",
                "table_name",
                "constraint_catalog",
                "constraint_schema",
                "constraint_name",
            ]
        );
        assert_eq!(
            info_constraint_table_usage.rows,
            vec![
                vec![
                    "postgres".to_string(),
                    "alpha".to_string(),
                    "fact_alpha".to_string(),
                    "postgres".to_string(),
                    "alpha".to_string(),
                    "fact_alpha_metric_not_null".to_string(),
                ],
                vec![
                    "postgres".to_string(),
                    "reporting".to_string(),
                    "fact_metrics".to_string(),
                    "postgres".to_string(),
                    "reporting".to_string(),
                    "fact_metrics_metric_not_null".to_string(),
                ],
            ]
        );

        let info_referential_constraints = engine
            .execute_query(&QueryRequest {
                sql: "SELECT constraint_name, unique_constraint_name, update_rule, delete_rule FROM information_schema.referential_constraints ORDER BY constraint_name"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("information_schema.referential_constraints query should succeed");
        assert_eq!(
            info_referential_constraints.rows,
            vec![vec![
                "fact_events_metric_fk".to_string(),
                "dim_metrics_pkey".to_string(),
                "NO ACTION".to_string(),
                "NO ACTION".to_string(),
            ],]
        );

        let _ = std::fs::remove_file(&catalog_path);
        let managed_dir = catalog_path.with_file_name(format!(
            "{}.managed",
            catalog_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("catalog file stem should be present")
        ));
        let _ = std::fs::remove_dir_all(managed_dir);
    }
}
