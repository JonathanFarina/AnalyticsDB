use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use analyticsdb_control::{
    parse_metadata_statement, AlterTableOperation, CatalogColumn, CatalogRelationKind,
    CatalogTableConstraint, CatalogTableConstraintKind, ControlPlane,
    MetadataStatement, QueryAdmission, TableColumnDefinition, TableConstraintDefinition,
};
use analyticsdb_core::{QueryRequest, QueryResponse};
use anyhow::Result;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, RecordBatchReader, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::catalog::{CatalogProvider, MemorySchemaProvider};
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::{SessionConfig, SessionContext};

pub mod functions;
pub mod postgres_compatibility;
pub mod sql_rewriter;
pub mod system_catalog;

use functions::register_postgres_functions;
use system_catalog::PgCatalogSchemaProvider;

pub struct PrototypeEngine {
    control_plane: Arc<ControlPlane>,
}

impl std::fmt::Debug for PrototypeEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrototypeEngine")
            .field("control_plane", &self.control_plane)
            .finish()
    }
}

impl Clone for PrototypeEngine {
    fn clone(&self) -> Self {
        self.clone_prototype()
    }
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

    pub async fn from_catalog_path(path: &str) -> Result<Self> {
        Self::with_control_plane(Arc::new(ControlPlane::from_catalog_path(path).await?))
    }

    pub fn with_control_plane(control_plane: Arc<ControlPlane>) -> Result<Self> {
        Ok(Self { control_plane })
    }

    pub fn clone_prototype(&self) -> Self {
        Self {
            control_plane: Arc::clone(&self.control_plane),
        }
    }

    pub fn control_plane(&self) -> Arc<ControlPlane> {
        Arc::clone(&self.control_plane)
    }

    async fn create_session_context(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<SessionContext> {
        let mut config = SessionConfig::new();
        config.options_mut().sql_parser.enable_ident_normalization = false;
        config.options_mut().sql_parser.parse_float_as_decimal = true;
        config.options_mut().optimizer.skip_failed_rules = true;
        config.options_mut().execution.parquet.schema_force_view_types = false;

        // Use the session's schema as the default for resolution.
        let config = config
            .set_str("datafusion.catalog.default_schema", &session.schema)
            .with_extension(Arc::new(session.clone()));

        let context = SessionContext::new_with_config(config);

        context.add_analyzer_rule(Arc::new(postgres_compatibility::DuplicateColumnAlerter::new()));

        register_postgres_functions(&context);

        // Register ALL databases from the ControlPlane as top-level Catalogs in DataFusion
        let snapshot = self.control_plane.cluster_snapshot().await;
        for database in snapshot.databases {
            // Register every database as a top-level catalog for 'db.schema.table' resolution
            let catalog_provider = Arc::new(datafusion::catalog::MemoryCatalogProvider::new());
            context.register_catalog(&database.name, catalog_provider.clone());

            for schema_name in database.schemas {
                if catalog_provider.schema(&schema_name).is_none() {
                    catalog_provider.register_schema(&schema_name, Arc::new(MemorySchemaProvider::new()))?;
                }
                
                // If it's the current session database, mirror schemas in the default 'datafusion' catalog
                if database.name == session.database {
                    let default_catalog = context.catalog("datafusion").unwrap();
                    if default_catalog.schema(&schema_name).is_none() {
                        default_catalog.register_schema(&schema_name, Arc::new(MemorySchemaProvider::new()))?;
                    }
                }
            }
            
            // Always register pg_catalog in every database for parity
            if catalog_provider.schema("pg_catalog").is_none() {
                catalog_provider.register_schema(
                    "pg_catalog",
                    Arc::new(PgCatalogSchemaProvider::new(Arc::clone(&self.control_plane))),
                )?;
            }
        }
        
        // Also register pg_catalog in default catalog for standard unqualified UDF resolution
        let default_catalog = context.catalog("datafusion").unwrap();
        if default_catalog.schema("pg_catalog").is_none() {
             default_catalog.register_schema(
                "pg_catalog",
                Arc::new(PgCatalogSchemaProvider::new(Arc::clone(&self.control_plane))),
            )?;
        }

        // Register ALL relations from ALL databases into their respective catalogs/schemas in DataFusion
        register_persisted_tables_comprehensive(&context, &self.control_plane, session).await?;
        register_persisted_views_comprehensive(&context, &self.control_plane, session).await?;

        Ok(context)
    }

    pub async fn execute_query(&self, request: &QueryRequest) -> Result<QueryResponse> {
        let execution = self.execute_query_batches(request).await?;
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

    pub async fn execute_query_batches(
        &self,
        request: &QueryRequest,
    ) -> Result<QueryExecutionResult> {
        let started = Instant::now();
        let admission = self.control_plane.admit_query(&request.session).await?;
        if let Some(statement) = parse_metadata_statement(&request.sql) {
            return self
                .execute_metadata_query(request, statement, admission, started)
                .await;
        }

        let control_plane = Arc::clone(&self.control_plane);
        let sql = sql_rewriter::rewrite_sql_for_postgres_compatibility(
            &request.sql,
            &control_plane,
            &request.session,
        )
        .await?;
        let session = request.session.clone();

        let context = self.create_session_context(&session).await?;
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
    }

    pub async fn plan_query_schema(&self, request: &QueryRequest) -> Result<Option<SchemaRef>> {
        if let Some(statement) = parse_metadata_statement(&request.sql) {
            match statement {
                MetadataStatement::ShowDatabases
                | MetadataStatement::ShowSchemas { .. }
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
                    // These return schemas, but for simplicity in prototype we can just 
                    // return None and let execute handle it, or return specific schemas.
                    // For now, return None to imply "not a simple table query".
                    return Ok(None);
                }
                _ => return Ok(None),
            }
        }

        let control_plane = Arc::clone(&self.control_plane);
        let sql = sql_rewriter::rewrite_sql_for_postgres_compatibility(
            &request.sql,
            &control_plane,
            &request.session,
        )
        .await?;

        let context = self.create_session_context(&request.session).await?;
        let dataframe = context.sql(&sql).await?;
        Ok(Some(Arc::new(dataframe.schema().as_arrow().as_ref().clone())))
    }

    async fn execute_metadata_query(
        &self,
        request: &QueryRequest,
        statement: MetadataStatement,
        admission: QueryAdmission,
        started: Instant,
    ) -> Result<QueryExecutionResult> {
        let session = request.session.clone();

        let (schema, batches, message) = match statement {
            MetadataStatement::CreateDatabase { .. }
            | MetadataStatement::CreateSchema { .. }
            | MetadataStatement::Begin
            | MetadataStatement::Commit
            | MetadataStatement::Rollback
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
                    let rows = self
                        .information_schema_schemata_rows(&request.session)
                        .await?;
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
                    let rows = self
                        .information_schema_tables_rows(&request.session)
                        .await?;
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
                    let rows = self
                        .information_schema_columns_rows(&request.session)
                        .await?;
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
                    let rows = self.information_schema_views_rows(&request.session).await?;
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
                    let rows = self
                        .information_schema_table_constraints_rows(&request.session)
                        .await?;
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
                    let rows = self
                        .information_schema_key_column_usage_rows(&request.session)
                        .await?;
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
                    let rows = self
                        .information_schema_constraint_column_usage_rows(&request.session)
                        .await?;
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
                    let rows = self
                        .information_schema_constraint_table_usage_rows(&request.session)
                        .await?;
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
                    let rows = self
                        .information_schema_referential_constraints_rows(&request.session)
                        .await?;
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
                        .execute_metadata_statement(&request.session, &statement)
                        .await?;
                    (Arc::new(Schema::empty()), Vec::new(), message)
                }
            },
            MetadataStatement::CreateView {
                database,
                schema,
                name,
                definition_sql,
            } => {
                // Determine schema of the view query
                let session = analyticsdb_core::SessionContext {
                    database: database.clone().unwrap_or(request.session.database.clone()),
                    schema: schema.clone().unwrap_or(request.session.schema.clone()),
                    ..request.session.clone()
                };
                let query_sql = definition_sql.clone();
                let target_schema_opt = schema.clone();
                let columns = async move {
                    // Use a context where the default schema is the target schema
                    // This ensures unqualified names in the view SQL resolve correctly.
                    let context = self.create_session_context(&session).await?;

                    let dataframe = context.sql(&query_sql).await?;
                    let arrow_schema = dataframe.schema().as_arrow().clone();
                    Ok::<_, anyhow::Error>(catalog_columns_from_schema(&arrow_schema))
                }
                .await?;

                let message = self
                    .control_plane
                    .register_view(
                        &request.session,
                        database.as_deref(),
                        target_schema_opt.as_deref(),
                        &name,
                        &definition_sql,
                        columns,
                    )
                    .await?;
                (Arc::new(Schema::empty()), Vec::new(), message)
            }

            MetadataStatement::CreateExternalTable {
                database,
                schema,
                name,
                format,
                location,
            } => {
                let message = self
                    .control_plane
                    .register_external_table(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                        &location,
                        format,
                    )
                    .await?;
                (Arc::new(Schema::empty()), Vec::new(), message)
            }

            MetadataStatement::CreateTableAs {
                database,
                schema,
                name,
                query_sql,
            } => {
                let storage_path = self
                    .control_plane
                    .managed_table_storage_path(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;

                let control_plane = Arc::clone(&self.control_plane);
                let session = request.session.clone();
                let storage_path_for_write = storage_path.clone();
                let (row_count, columns_metadata) = async move {
                    let context = SessionContext::new();
                    register_persisted_tables_comprehensive(&context, &control_plane, &session).await?;
                    register_persisted_views_comprehensive(&context, &control_plane, &session).await?;
                    let dataframe = context.sql(&query_sql).await?;
                    let arrow_schema = dataframe.schema().as_arrow().clone();
                    let batches = dataframe.collect().await?;
                    let columns_metadata = catalog_columns_from_schema(&arrow_schema);

                    let row_count =
                        persist_table_snapshot(&storage_path_for_write, &arrow_schema, &batches)?;

                    Ok::<_, anyhow::Error>((row_count, columns_metadata))
                }
                .await?;

                let created_message = self
                    .control_plane
                    .register_managed_table(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                        &storage_path,
                        columns_metadata,
                        Vec::new(),
                    )
                    .await?;

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
                let storage_path = self
                    .control_plane
                    .managed_table_storage_path(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;
                let arrow_schema = build_arrow_schema_from_definitions(&columns, false)?;

                persist_empty_table_snapshot(&storage_path, &arrow_schema)?;

                let created_message = self
                    .control_plane
                    .register_managed_table(
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
                    )
                    .await?;

                (Arc::new(Schema::empty()), Vec::new(), created_message)
            }
            MetadataStatement::InsertInto {
                database,
                schema,
                name,
                columns,
                rows,
            } => {
                let relation = self
                    .control_plane
                    .table_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;
                let storage_path = relation.storage_path.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Managed table '{}.{}.{}' is missing a storage path",
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })?;

                let column_definitions: Vec<TableColumnDefinition> = relation.columns.iter().map(|c| {
                    TableColumnDefinition {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                        nullable: c.nullable,
                        default_value: c.default_value.clone(),
                    }
                }).collect();
                let arrow_schema = build_arrow_schema_from_definitions(&column_definitions, false)?;

                let inserted_row_count = append_rows_to_table_snapshot(
                    Path::new(storage_path),
                    &arrow_schema,
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
            MetadataStatement::Delete {
                database,
                schema,
                name,
                selection_sql,
            } => {
                let relation = self
                    .control_plane
                    .table_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;
                let storage_path_str = relation.storage_path.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Managed table '{}.{}.{}' is missing a storage path",
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })?;
                let storage_path = Path::new(storage_path_str);

                // 1. Get initial count
                let initial_batch = load_persisted_table_snapshot(storage_path)?;
                let initial_count = initial_batch.num_rows();

                // 2. Filter rows to KEEP using DataFusion
                let session = request.session.clone();
                let context = self.create_session_context(&session).await?;

                let filter_clause = match selection_sql {
                    Some(sql) => format!("NOT ({})", sql),
                    None => "FALSE".to_string(), // DELETE without WHERE means keep nothing
                };

                let sql = format!(
                    "SELECT * FROM \"{}\".\"{}\".\"{}\" WHERE {}",
                    relation.database, relation.schema, relation.name, filter_clause
                );

                let dataframe = context.sql(&sql).await?;
                let remaining_batches = dataframe.collect().await?;
                let remaining_count: usize = remaining_batches.iter().map(|b| b.num_rows()).sum();
                let deleted_count = initial_count - remaining_count;

                // 3. Overwrite the table directory with the new snapshot
                // First, remove old files
                if storage_path.exists() {
                    for entry in fs::read_dir(storage_path)? {
                        let entry = entry?;
                        if entry.path().is_file() {
                            fs::remove_file(entry.path())?;
                        }
                    }
                }

                if remaining_count > 0 {
                    persist_table_snapshot(storage_path, &initial_batch.schema(), &remaining_batches)?;
                } else {
                    persist_empty_table_snapshot(storage_path, &initial_batch.schema())?;
                }

                // 4. Re-register to refresh DataFusion's view of the directory
                register_persisted_tables_comprehensive(&context, &self.control_plane, &session).await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!(
                        "DELETE completed. {deleted_count} row(s) affected on '{}.{}.{}'.",
                        relation.database, relation.schema, relation.name
                    ),
                )
            }
            MetadataStatement::Truncate {
                database,
                schema,
                name,
            } => {
                let relation = self
                    .control_plane
                    .table_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;
                let storage_path_str = relation.storage_path.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Managed table '{}.{}.{}' is missing a storage path",
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })?;
                let storage_path = Path::new(storage_path_str);

                // 1. Clear the directory
                if storage_path.exists() {
                    for entry in fs::read_dir(storage_path)? {
                        let entry = entry?;
                        if entry.path().is_file() {
                            fs::remove_file(entry.path())?;
                        }
                    }
                } else {
                    fs::create_dir_all(storage_path)?;
                }

                // 2. Write empty snapshot to maintain schema
                let column_definitions: Vec<TableColumnDefinition> = relation
                    .columns
                    .iter()
                    .map(|c| TableColumnDefinition {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                        nullable: c.nullable,
                        default_value: c.default_value.clone(),
                    })
                    .collect();
                let arrow_schema = build_arrow_schema_from_definitions(&column_definitions, false)?;
                persist_empty_table_snapshot(storage_path, &arrow_schema)?;

                // 3. Re-register
                let session = request.session.clone();
                let context = self.create_session_context(&session).await?;
                register_persisted_tables_comprehensive(&context, &self.control_plane, &session)
                    .await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!(
                        "TRUNCATE completed on '{}.{}.{}'.",
                        relation.database, relation.schema, relation.name
                    ),
                )
            }
            MetadataStatement::Update {
                database,
                schema,
                name,
                assignments,
                selection_sql,
            } => {
                let relation = self
                    .control_plane
                    .table_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;
                let storage_path_str = relation.storage_path.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Managed table '{}.{}.{}' is missing a storage path",
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })?;
                let storage_path = Path::new(storage_path_str);

                // 1. Get initial count
                let initial_batch = load_persisted_table_snapshot(storage_path)?;
                let initial_count = initial_batch.num_rows();

                // 2. Perform UPDATE using DataFusion SELECT CASE
                let session = request.session.clone();
                let context = self.create_session_context(&session).await?;

                let mut select_expressions = Vec::new();
                for col in &relation.columns {
                    if let Some((_, new_expr)) = assignments.iter().find(|(c, _)| c == &col.name) {
                        let filter = selection_sql.as_deref().unwrap_or("TRUE");
                        select_expressions.push(format!(
                            "CASE WHEN {} THEN ({}) ELSE \"{}\" END AS \"{}\"",
                            filter, new_expr, col.name, col.name
                        ));
                    } else {
                        select_expressions.push(format!("\"{}\"", col.name));
                    }
                }

                let sql = format!(
                    "SELECT {} FROM \"{}\".\"{}\".\"{}\"",
                    select_expressions.join(", "),
                    relation.database,
                    relation.schema,
                    relation.name
                );

                let dataframe = context.sql(&sql).await?;
                let updated_batches = dataframe.collect().await?;
                let updated_count: usize = updated_batches.iter().map(|b| b.num_rows()).sum();

                // 3. Overwrite the table directory with the new snapshot
                if storage_path.exists() {
                    for entry in fs::read_dir(storage_path)? {
                        let entry = entry?;
                        if entry.path().is_file() {
                            fs::remove_file(entry.path())?;
                        }
                    }
                }

                if updated_count > 0 {
                    persist_table_snapshot(storage_path, &updated_batches[0].schema(), &updated_batches)?;
                } else {
                    persist_empty_table_snapshot(storage_path, &initial_batch.schema())?;
                }

                // 4. Re-register
                register_persisted_tables_comprehensive(&context, &self.control_plane, &session).await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!(
                        "UPDATE completed. {initial_count} row(s) updated on '{}.{}.{}'.",
                        relation.database, relation.schema, relation.name
                    ),
                )
            }
            MetadataStatement::AlterTable {
                database,
                schema,
                name,
                operation,
            } => {
                let relation = self
                    .control_plane
                    .table_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;

                match operation {
                    AlterTableOperation::AddColumn { column } => {
                        let new_column_name = column.name.clone();
                        // 1. Update Catalog
                        let catalog_column = CatalogColumn {
                            name: column.name.clone(),
                            data_type: column.data_type.clone(),
                            nullable: column.nullable,
                            default_value: column.default_value.clone(),
                        };
                        self.control_plane
                            .add_column(
                                &request.session,
                                database.as_deref(),
                                schema.as_deref(),
                                &name,
                                catalog_column,
                            )
                            .await?;

                        // 2. Physically update Parquet files (if table is managed)
                        if let Some(storage_path_str) = &relation.storage_path {
                            let storage_path = Path::new(storage_path_str);
                            if storage_path.exists() {
                                let session = request.session.clone();
                                let context = self.create_session_context(&session).await?;

                                // Refresh to see the new column in metadata (though physically not there yet)
                                register_persisted_tables_comprehensive(&context, &self.control_plane, &session).await?;

                                let mut select_expressions = Vec::new();
                                for col in &relation.columns {
                                    select_expressions.push(format!("\"{}\"", col.name));
                                }
                                // Add the new column as NULL or default
                                let default_expr = column.default_value.as_deref().unwrap_or("NULL");
                                select_expressions.push(format!("CAST({} AS {}) AS \"{}\"", default_expr, column.data_type, column.name));

                                let sql = format!(
                                    "SELECT {} FROM \"{}\".\"{}\".\"{}\"",
                                    select_expressions.join(", "),
                                    relation.database,
                                    relation.schema,
                                    relation.name
                                );

                                let dataframe = context.sql(&sql).await?;
                                let new_batches = dataframe.collect().await?;

                                // Overwrite
                                for entry in fs::read_dir(storage_path)? {
                                    let entry = entry?;
                                    if entry.path().is_file() {
                                        fs::remove_file(entry.path())?;
                                    }
                                }
                                if !new_batches.is_empty() {
                                    persist_table_snapshot(storage_path, &new_batches[0].schema(), &new_batches)?;
                                } else {
                                    // Handle empty table case
                                    let column_definitions: Vec<TableColumnDefinition> = relation.columns.iter().map(|c| TableColumnDefinition {
                                        name: c.name.clone(),
                                        data_type: c.data_type.clone(),
                                        nullable: c.nullable,
                                        default_value: c.default_value.clone(),
                                    }).collect();
                                    // Add the new column definition
                                    let mut updated_defs = column_definitions;
                                    updated_defs.push(column);
                                    let arrow_schema = build_arrow_schema_from_definitions(&updated_defs, false)?;
                                    persist_empty_table_snapshot(storage_path, &arrow_schema)?;
                                }
                                
                                // Re-register again to see the physical change
                                register_persisted_tables_comprehensive(&context, &self.control_plane, &session).await?;
                            }
                        }

                        (
                            Arc::new(Schema::empty()),
                            Vec::new(),
                            format!(
                                "ALTER TABLE completed. Column '{}' added to '{}.{}.{}'.",
                                new_column_name, relation.database, relation.schema, relation.name
                            ),
                        )
                    }
                    AlterTableOperation::RenameTable { new_name } => {
                        // 1. Rename catalog metadata
                        self.control_plane
                            .rename_relation(
                                &request.session,
                                database.as_deref(),
                                schema.as_deref(),
                                &name,
                                &new_name,
                            )
                            .await?;

                        // 2. Physically rename managed directory if it exists
                        if let Some(storage_path_str) = &relation.storage_path {
                            let old_path = Path::new(storage_path_str);
                            if old_path.exists() {
                                // Calculate new path by replacing the table name part
                                // Managed tables use names like <db>__<schema>__<table>.table.parquet
                                let file_name = old_path.file_name().unwrap().to_str().unwrap();
                                let old_suffix = format!("{}.table.parquet", name);
                                let new_suffix = format!("{}.table.parquet", new_name);
                                let new_file_name = file_name.replace(&old_suffix, &new_suffix);
                                let new_path = old_path.with_file_name(new_file_name);

                                fs::rename(old_path, &new_path)?;

                                // 3. Update the storage path in catalog after physical rename
                                self.control_plane
                                    .update_relation_storage_path(
                                        &request.session,
                                        database.as_deref(),
                                        schema.as_deref(),
                                        &new_name,
                                        new_path.to_str().unwrap(),
                                    )
                                    .await?;
                            }
                        }

                        // 4. Re-register relations
                        let session = request.session.clone();
                        let context = self.create_session_context(&session).await?;
                        register_persisted_tables_comprehensive(&context, &self.control_plane, &session).await?;

                        (
                            Arc::new(Schema::empty()),
                            Vec::new(),
                            format!(
                                "ALTER TABLE completed. Relation '{}.{}.{}' renamed to '{}'.",
                                relation.database, relation.schema, relation.name, new_name
                            ),
                        )
                    }
                }
            }
            MetadataStatement::AlterSchema {
                database,
                name,
                new_name,
            } => {
                // 1. Get all relations in this schema to update their physical paths if managed
                let relations = self
                    .control_plane
                    .list_relations(
                        &request.session,
                        database.as_deref(),
                        Some(&name),
                        CatalogRelationKind::Table,
                    )
                    .await?;

                // 2. Rename schema in catalog
                self.control_plane
                    .rename_schema(&request.session, database.as_deref(), &name, &new_name)
                    .await?;

                // 3. Physically rename managed directories and update metadata
                let database_name = database.as_deref().unwrap_or(&request.session.database);
                for relation in relations {
                    if let Some(storage_path_str) = &relation.storage_path {
                        let old_path = Path::new(storage_path_str);
                        if old_path.exists() {
                            let file_name = old_path.file_name().unwrap().to_str().unwrap();
                            let old_prefix = format!("{}__{}__", database_name, name);
                            let new_prefix = format!("{}__{}__", database_name, new_name);
                            let new_file_name = file_name.replace(&old_prefix, &new_prefix);
                            let new_path = old_path.with_file_name(new_file_name);

                            fs::rename(old_path, &new_path)?;

                            self.control_plane
                                .update_relation_storage_path(
                                    &request.session,
                                    Some(database_name),
                                    Some(&new_name),
                                    &relation.name,
                                    new_path.to_str().unwrap(),
                                )
                                .await?;
                        }
                    }
                }

                // 4. Re-register relations
                let session = request.session.clone();
                let context = self.create_session_context(&session).await?;
                register_persisted_tables_comprehensive(&context, &self.control_plane, &session).await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!(
                        "ALTER SCHEMA completed. Schema '{}.{}' renamed to '{}'.",
                        database_name, name, new_name
                    ),
                )
            }
            MetadataStatement::ShowDatabases => {
                let rows = self
                    .control_plane
                    .list_databases(&request.session)
                    .await?
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
                    .list_schemas(&request.session, database.as_deref())
                    .await?
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
            MetadataStatement::ShowNodes => {
                let nodes = self.control_plane.list_nodes().await?;
                let rows = nodes
                    .into_iter()
                    .map(|node| {
                        vec![
                            node.id,
                            format!("{:?}", node.role),
                            node.endpoint,
                            format!("{:?}", node.status),
                        ]
                    })
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(&["node_id", "role", "endpoint", "status"], &rows)?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} node(s) listed successfully."),
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
                    )
                    .await?
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
                    )
                    .await?
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
            } => {
                let rows = self
                    .control_plane
                    .relation_columns(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?
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
            MetadataStatement::DropTable {
                database,
                schema,
                name,
                if_exists,
                cascade: _,
            } => {
                let relation = match self
                    .control_plane
                    .find_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await
                {
                    Ok(rel) => {
                        if rel.kind != CatalogRelationKind::Table {
                            anyhow::bail!("Relation '{}' is not a table", name);
                        }
                        Some(rel)
                    }
                    Err(_) => {
                        if if_exists {
                            None
                        } else {
                            anyhow::bail!("Table '{}' not found", name);
                        }
                    }
                };

                if let Some(rel) = relation {
                    // For managed tables, delete the storage directory
                    if rel.external_format.is_none() {
                        if let Some(path_str) = &rel.storage_path {
                            let path = Path::new(path_str);
                            if path.exists() {
                                fs::remove_dir_all(path)?;
                            }
                        }
                    }
                }

                let message = self
                    .control_plane
                    .drop_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                        CatalogRelationKind::Table,
                        if_exists,
                    )
                    .await?;

                (Arc::new(Schema::empty()), Vec::new(), message)
            }
            MetadataStatement::DropView {
                database,
                schema,
                name,
                if_exists,
                cascade: _,
            } => {
                let message = self
                    .control_plane
                    .drop_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                        CatalogRelationKind::View,
                        if_exists,
                    )
                    .await?;

                (Arc::new(Schema::empty()), Vec::new(), message)
            }
            MetadataStatement::DropDatabase { name, if_exists } => {
                let message = self
                    .control_plane
                    .drop_database(&request.session, &name, if_exists)
                    .await?;

                (Arc::new(Schema::empty()), Vec::new(), message)
            }
            MetadataStatement::DropSchema {
                database,
                name,
                if_exists,
                cascade,
            } => {
                let message = self
                    .control_plane
                    .drop_schema(
                        &request.session,
                        database.as_deref(),
                        &name,
                        if_exists,
                        cascade,
                    )
                    .await?;

                (Arc::new(Schema::empty()), Vec::new(), message)
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

    pub async fn list_databases(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<String>> {
        self.control_plane.list_databases(session).await
    }

    pub async fn list_schemas(
        &self,
        session: &analyticsdb_core::SessionContext,
        database: Option<&str>,
    ) -> Result<Vec<String>> {
        self.control_plane.list_schemas(session, database).await
    }

    pub async fn list_relations(
        &self,
        session: &analyticsdb_core::SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        kind: CatalogRelationKind,
    ) -> Result<Vec<analyticsdb_control::CatalogRelation>> {
        self.control_plane
            .list_relations(session, database, schema, kind)
            .await
    }
}

impl PrototypeEngine {
    async fn information_schema_schemata_rows(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let cluster = self.control_plane.cluster_snapshot().await;
        let mut schemas = cluster
            .databases
            .iter()
            .find(|db| db.name == session.database)
            .map(|db| db.schemas.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
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

    async fn information_schema_tables_rows(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let cluster = self.control_plane.cluster_snapshot().await;
        let mut relations = cluster
            .relations
            .into_iter()
            .filter(|relation| relation.database == session.database)
            .collect::<Vec<_>>();
        relations.sort_by(|left, right| {
            (left.schema.as_str(), left.name.as_str())
                .cmp(&(right.schema.as_str(), right.name.as_str()))
        });

        Ok(relations
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

    async fn information_schema_columns_rows(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let cluster = self.control_plane.cluster_snapshot().await;
        let mut relations = cluster
            .relations
            .into_iter()
            .filter(|relation| relation.database == session.database)
            .collect::<Vec<_>>();
        relations.sort_by(|left, right| {
            (left.schema.as_str(), left.name.as_str())
                .cmp(&(right.schema.as_str(), right.name.as_str()))
        });

        let mut rows = Vec::new();
        for relation in relations {
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

    async fn information_schema_views_rows(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let cluster = self.control_plane.cluster_snapshot().await;
        let mut views = cluster
            .relations
            .into_iter()
            .filter(|relation| {
                relation.kind == CatalogRelationKind::View && relation.database == session.database
            })
            .collect::<Vec<_>>();
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

    async fn information_schema_table_constraints_rows(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let cluster = self.control_plane.cluster_snapshot().await;
        Ok(self
            .information_schema_constraints_snapshot(session, &cluster)?
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

    async fn information_schema_key_column_usage_rows(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let cluster = self.control_plane.cluster_snapshot().await;
        let mut rows = Vec::new();
        for constraint in self.information_schema_constraints_snapshot(session, &cluster)? {
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

    async fn information_schema_constraint_column_usage_rows(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let cluster = self.control_plane.cluster_snapshot().await;
        let mut rows = Vec::new();
        for constraint in self.information_schema_constraints_snapshot(session, &cluster)? {
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

    async fn information_schema_constraint_table_usage_rows(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let cluster = self.control_plane.cluster_snapshot().await;
        let mut rows = Vec::new();
        for constraint in self.information_schema_constraints_snapshot(session, &cluster)? {
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

    async fn information_schema_referential_constraints_rows(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let cluster = self.control_plane.cluster_snapshot().await;
        Ok(self
            .information_schema_constraints_snapshot(session, &cluster)?
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

    fn information_schema_constraints_snapshot(
        &self,
        session: &analyticsdb_core::SessionContext,
        cluster: &analyticsdb_control::ClusterSnapshot,
    ) -> Result<Vec<InformationSchemaConstraintRow>> {
        let mut constraints = Vec::new();
        let mut relations = cluster
            .relations
            .iter()
            .filter(|relation| {
                relation.kind == CatalogRelationKind::Table && relation.database == session.database
            })
            .collect::<Vec<_>>();
        relations.sort_by(|left, right| {
            (left.schema.as_str(), left.name.as_str())
                .cmp(&(right.schema.as_str(), right.name.as_str()))
        });

        for relation in relations {
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

            for relation_constraint in &relation.constraints {
                match relation_constraint.kind {
                    CatalogTableConstraintKind::PrimaryKey => {
                        constraints.push(InformationSchemaConstraintRow {
                            table_catalog: relation.database.clone(),
                            table_schema: relation.schema.clone(),
                            table_name: relation.name.clone(),
                            columns: relation_constraint.columns.clone(),
                            constraint_name: relation_constraint.name.clone(),
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
                            columns: relation_constraint.columns.clone(),
                            constraint_name: relation_constraint.name.clone(),
                            constraint_type: "FOREIGN KEY".to_string(),
                            referenced_catalog: relation_constraint.referenced_database.clone(),
                            referenced_schema: relation_constraint.referenced_schema.clone(),
                            referenced_table,
                            referenced_unique_constraint_name,
                        });
                    }
                    CatalogTableConstraintKind::Unique => {
                        constraints.push(InformationSchemaConstraintRow {
                            table_catalog: relation.database.clone(),
                            table_schema: relation.schema.clone(),
                            table_name: relation.name.clone(),
                            columns: relation_constraint.columns.clone(),
                            constraint_name: relation_constraint.name.clone(),
                            constraint_type: "UNIQUE".to_string(),
                            referenced_catalog: None,
                            referenced_schema: None,
                            referenced_table: None,
                            referenced_unique_constraint_name: None,
                        })
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

fn catalog_columns_from_schema(schema: &Schema) -> Vec<CatalogColumn> {
    schema
        .fields()
        .iter()
        .map(|field| CatalogColumn {
            name: field.name().to_string(),
            data_type: field.data_type().to_string(),
            nullable: field.is_nullable(),
            default_value: field.metadata().get("default_value").cloned(),
        })
        .collect()
}

fn build_arrow_schema_from_definitions(columns: &[TableColumnDefinition], is_external: bool) -> Result<Schema> {
    if !is_external && columns.is_empty() {
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

            let mut metadata = HashMap::new();
            if let Some(dv) = &column.default_value {
                metadata.insert("default_value".to_string(), dv.clone());
            }

            Ok(Field::new(
                column.name.clone(),
                sql_type_to_arrow_data_type(&column.data_type)?,
                column.nullable,
            ).with_metadata(metadata))
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
            TableConstraintDefinition::Unique { name, columns } => {
                if columns.is_empty() {
                    anyhow::bail!("UNIQUE constraint requires at least one column")
                }
                Ok(CatalogTableConstraint {
                    name: name
                        .clone()
                        .unwrap_or_else(|| format!("{table_name}_{}_key", columns.join("_"))),
                    kind: CatalogTableConstraintKind::Unique,
                    columns: columns.clone(),
                    referenced_database: None,
                    referenced_schema: None,
                    referenced_table: None,
                    referenced_columns: Vec::new(),
                })
            }
        })
        .collect()
}

fn sql_type_to_arrow_data_type(raw: &str) -> Result<DataType> {
    let trimmed = raw.trim();
    let normalized = trimmed.to_ascii_uppercase();
    
    // Extract the base type: e.g. "VARCHAR(50)" -> "VARCHAR", "INT PRIMARY KEY" -> "INT"
    let base_type = normalized
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()
        .unwrap_or("");

    let data_type = match base_type {
        "BOOL" | "BOOLEAN" => DataType::Boolean,
        "FLOAT" | "FLOAT4" | "REAL" | "FLOAT32" => DataType::Float32,
        "DOUBLE" | "PRECISION" | "FLOAT8" | "FLOAT64" => DataType::Float64,
        "SMALLINT" | "INT2" | "INT" | "INTEGER" | "INT4" | "INT32" => DataType::Int32,
        "BIGINT" | "INT8" | "INT64" => DataType::Int64,
        "TIMESTAMP" | "TIMESTAMPTZ" => {
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None)
        }
        "STRING" | "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER" | "VARYING" | "UTF8" => DataType::Utf8,
        _ => {
            if normalized.contains("DOUBLE PRECISION") {
                DataType::Float64
            } else if normalized.contains("CHARACTER VARYING") {
                DataType::Utf8
            } else {
                anyhow::bail!(
                    "Unsupported SQL column type '{}' in the current prototype",
                    trimmed
                )
            }
        }
    };

    Ok(data_type)
}

fn persist_table_snapshot(path: &Path, schema: &Schema, batches: &[RecordBatch]) -> Result<usize> {
    if !path.exists() {
        fs::create_dir_all(path)?;
    }

    let file_name = format!("{}.parquet", uuid::Uuid::now_v7());
    let file_path = path.join(file_name);
    let row_count = batches.iter().map(|b| b.num_rows()).sum();

    write_parquet_file(&file_path, schema, batches)?;

    Ok(row_count)
}

fn persist_empty_table_snapshot(path: &Path, schema: &Schema) -> Result<()> {
    if !path.exists() {
        fs::create_dir_all(path)?;
    }

    let file_name = "_empty.parquet";
    let file_path = path.join(file_name);

    write_parquet_file(&file_path, schema, &[])
}

fn write_parquet_file(path: &Path, schema: &Schema, batches: &[RecordBatch]) -> Result<()> {
    let file = fs::File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props))?;

    for batch in batches {
        writer.write(batch)?;
    }

    writer.close()?;
    Ok(())
}

fn append_rows_to_table_snapshot(
    path: &Path,
    schema: &Schema,
    selected_columns: Option<&[String]>,
    rows: &[Vec<String>],
) -> Result<usize> {
    if !path.exists() {
        fs::create_dir_all(path)?;
    }

    let batch = parse_rows_to_batch(schema, selected_columns, rows)?;
    let row_count = batch.num_rows();

    let file_name = format!("{}.parquet", uuid::Uuid::now_v7());
    let file_path = path.join(file_name);

    write_parquet_file(&file_path, schema, &[batch])?;

    Ok(row_count)
}

fn parse_rows_to_batch(
    schema: &Schema,
    selected_columns: Option<&[String]>,
    rows: &[Vec<String>],
) -> Result<RecordBatch> {
    let mut column_order = Vec::new();

    match selected_columns {
        Some(selected_columns) => {
            let mut seen = BTreeSet::new();

            for selected_column in selected_columns {
                let normalized_name = selected_column.to_ascii_lowercase();
                if !seen.insert(normalized_name) {
                    anyhow::bail!("Duplicate column '{}' in INSERT target", selected_column);
                }

                let column_index = schema
                    .fields()
                    .iter()
                    .position(|field| field.name().eq_ignore_ascii_case(selected_column))
                    .ok_or_else(|| anyhow::anyhow!("Unknown column '{}'", selected_column))?;
                column_order.push(column_index);
            }
        }
        None => column_order.extend(0..schema.fields().len()),
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for row in rows {
        if row.len() != column_order.len() {
            anyhow::bail!(
                "Expected {} value(s) per row, found {}",
                column_order.len(),
                row.len()
            );
        }
    }

    for (column_index, field) in schema.fields().iter().enumerate() {
        let mut values = Vec::with_capacity(rows.len());

        let input_index = column_order.iter().position(|&idx| idx == column_index);

        for row in rows {
            let raw_value = if let Some(idx) = input_index {
                row.get(idx).map(|s| s.as_str())
            } else {
                None
            };

            let value = match raw_value {
                Some(v) if v.eq_ignore_ascii_case("NULL") => None,
                Some(v) => {
                    if v.starts_with('\'') && v.ends_with('\'') {
                        Some(v[1..v.len() - 1].replace("''", "'"))
                    } else {
                        Some(v.to_string())
                    }
                }
                None => field.metadata().get("default_value").cloned(),
            };

            if value.is_none() && !field.is_nullable() {
                anyhow::bail!(
                    "Column '{}' must be provided because it is NOT NULL and has no DEFAULT",
                    field.name()
                );
            }
            values.push(value);
        }

        let array: ArrayRef = match field.data_type() {
            DataType::Boolean => Arc::new(BooleanArray::from(
                values
                    .into_iter()
                    .map(|v| {
                        v.map(|s| s.eq_ignore_ascii_case("TRUE"))
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::Int32 => Arc::new(Int32Array::from(
                values
                    .into_iter()
                    .map(|v| v.and_then(|s| s.parse::<i32>().ok()))
                    .collect::<Vec<_>>(),
            )),
            DataType::Int64 => Arc::new(Int64Array::from(
                values
                    .into_iter()
                    .map(|v| v.and_then(|s| s.parse::<i64>().ok()))
                    .collect::<Vec<_>>(),
            )),
            DataType::Float32 => Arc::new(Float32Array::from(
                values
                    .into_iter()
                    .map(|v| v.and_then(|s| s.parse::<f32>().ok()))
                    .collect::<Vec<_>>(),
            )),
            DataType::Float64 => Arc::new(Float64Array::from(
                values
                    .into_iter()
                    .map(|v| v.and_then(|s| s.parse::<f64>().ok()))
                    .collect::<Vec<_>>(),
            )),
            DataType::Utf8 => Arc::new(StringArray::from(values)),
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, _) => {
                Arc::new(datafusion::arrow::array::TimestampNanosecondArray::from(
                    values
                        .into_iter()
                        .map(|v| {
                            v.and_then(|s| {
                                if s.eq_ignore_ascii_case("CURRENT_TIMESTAMP")
                                    || s.eq_ignore_ascii_case("CURRENT_TIMESTAMP()")
                                    || s.eq_ignore_ascii_case("NOW()")
                                {
                                    std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .ok()
                                        .map(|d| d.as_nanos() as i64)
                                } else {
                                    let s_clean = if s.starts_with('\'') && s.ends_with('\'') {
                                        &s[1..s.len() - 1]
                                    } else {
                                        &s
                                    };
                                    chrono::DateTime::parse_from_rfc3339(s_clean)
                                        .map(|dt| dt.timestamp_nanos_opt().unwrap_or(0))
                                        .or_else(|_| {
                                            chrono::NaiveDateTime::parse_from_str(
                                                s_clean,
                                                "%Y-%m-%d %H:%M:%S",
                                            )
                                            .map(|dt| {
                                                dt.and_utc().timestamp_nanos_opt().unwrap_or(0)
                                            })
                                        })
                                        .ok()
                                }
                            })
                        })
                        .collect::<Vec<_>>(),
                ))
            }
            unsupported => anyhow::bail!("Unsupported data type for Parquet migration: {unsupported:?}"),
        };
        columns.push(array);
    }

    Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
}

fn load_persisted_table_snapshot(path: &Path) -> Result<RecordBatch> {
    // Note: This loads the entire directory into one batch for prototype simplicity.
    // Real engines would stream this.
    let mut batches = Vec::new();
    let mut schema = None;

    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let file_path = entry.path();
            if file_path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                let file = fs::File::open(file_path)?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
                let mut reader = builder.build()?;
                if schema.is_none() {
                    schema = Some(reader.schema());
                }
                while let Some(batch) = reader.next() {
                    batches.push(batch?);
                }
            }
        }
    }

    let schema = schema.ok_or_else(|| anyhow::anyhow!("No Parquet files found in {:?}", path))?;
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    Ok(datafusion::arrow::compute::concat_batches(&schema, &batches)?)
}

async fn register_persisted_tables_comprehensive(
    context: &SessionContext,
    control_plane: &ControlPlane,
    session: &analyticsdb_core::SessionContext,
) -> Result<()> {
    let snapshot = control_plane.cluster_snapshot().await;
    for table in snapshot.relations {
        if table.kind != CatalogRelationKind::Table {
            continue;
        }
        let Some(storage_path) = table.storage_path.as_deref() else {
             continue;
        };

        let mut targets = Vec::new();
        // Target 1: The database-named catalog (standard catalog.schema.table resolution)
        if let Some(catalog) = context.catalog(&table.database) {
            targets.push(catalog);
        }

        // Target 2: The default catalog if it matches session.database (for schema.table or table)
        if table.database == session.database {
            if let Some(catalog) = context.catalog("datafusion") {
                targets.push(catalog);
            }
        }

        for catalog in targets {
            if catalog.schema(&table.schema).is_none() {
                 catalog.register_schema(&table.schema, Arc::new(MemorySchemaProvider::new()))?;
            }
            let schema = catalog.schema(&table.schema).unwrap();

            if schema.table(&table.name).await?.is_some() {
                 continue;
            }

            // Both managed and external Parquet tables use the same SQL registration path now.
            // This provides native DataFusion performance for both.
            let statement = format!(
                "CREATE EXTERNAL TABLE IF NOT EXISTS \"{}\".\"{}\".\"{}\" STORED AS PARQUET LOCATION '{}'",
                table.database, table.schema, table.name, storage_path
            );
            if let Ok(df) = context.sql(&statement).await {
                let _ = df.collect().await;
            }

            if table.database == session.database {
                let default_stmt = format!(
                    "CREATE EXTERNAL TABLE IF NOT EXISTS \"{}\".\"{}\" STORED AS PARQUET LOCATION '{}'",
                    table.schema, table.name, storage_path
                );
                if let Ok(df) = context.sql(&default_stmt).await {
                    let _ = df.collect().await;
                }
            }
        }
    }

    Ok(())
}

async fn register_persisted_views_comprehensive(
    context: &SessionContext,
    control_plane: &ControlPlane,
    session: &analyticsdb_core::SessionContext,
) -> Result<()> {
    let snapshot = control_plane.cluster_snapshot().await;
    for view in snapshot.relations {
        if view.kind != CatalogRelationKind::View {
            continue;
        }
        let Some(definition_sql) = view.definition_sql.as_deref() else {
            continue;
        };

        let rewritten_sql = sql_rewriter::rewrite_sql_for_postgres_compatibility(
            definition_sql,
            control_plane,
            session,
        )
        .await?;

        // Prototype simplification for views: register via SQL to handle cross-catalog correctly
        let mut names = Vec::new();
        // Fully qualified
        names.push(format!("\"{}\".\"{}\".\"{}\"", view.database, view.schema, view.name));
        
        if view.database == session.database {
            // Short names for current DB
            names.push(format!("\"{}\".\"{}\"", view.schema, view.name));
            names.push(format!("\"{}\"", view.name));
        }

        for name in names {
            let statement = format!("CREATE VIEW {} AS {}", name, rewritten_sql);
            // We ignore errors here because some names might overlap or fail planning 
            // if dependencies aren't registered yet. A real engine would use a DAG.
            if let Ok(df) = context.sql(&statement).await {
                let _ = df.collect().await;
            }
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

    use super::PrototypeEngine;

    #[tokio::test]
    async fn executes_scalar_select() {
        let engine = PrototypeEngine::new().expect("engine should initialize");
        let response = engine
            .execute_query(&QueryRequest {
                sql: "SELECT 42 AS answer".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .await
            .expect("query should execute");

        assert!(response.query_id.starts_with("q-"));
        assert_eq!(response.coordinator_node_id, "control-1");
        assert_eq!(response.session.database, "postgres");
        assert_eq!(response.columns, vec!["answer"]);
        assert_eq!(response.rows, vec![vec!["42".to_string()]]);
        assert!(response.message.contains("1 row(s)"));
    }

    #[tokio::test]
    async fn rejects_unknown_schema_before_execution() {
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
            .await
            .expect_err("unknown schema should fail");

        assert!(error.to_string().contains("Unknown schema"));
    }

    #[tokio::test]
    async fn executes_metadata_show_databases() {
        let engine = PrototypeEngine::new().expect("engine should initialize");
        let response = engine
            .execute_query(&QueryRequest {
                sql: "SHOW DATABASES".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .await
            .expect("metadata query should execute");

        assert_eq!(response.columns, vec!["database_name"]);
        assert!(response
            .rows
            .iter()
            .any(|row| row == &vec!["postgres".to_string()]));
        assert!(response.message.contains("database(s) listed"));
    }

    #[tokio::test]
    async fn executes_metadata_alter_user_password() {
        let engine = PrototypeEngine::new().expect("engine should initialize");

        let response = engine
            .execute_query(&QueryRequest {
                sql: "ALTER USER analytics_reader PASSWORD 'reader-v2'".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .await
            .expect("alter user password should execute through metadata path");
        assert!(response.message.contains("rotated successfully"));

        let control_plane = engine.control_plane();
        let stale = control_plane
            .validate_credentials("analytics_reader", Some("analytics_reader"))
            .await
            .expect_err("old password should be invalidated after ALTER USER");
        assert!(stale.to_string().contains("Invalid credentials"));
        control_plane
            .validate_credentials("analytics_reader", Some("reader-v2"))
            .await
            .expect("new password should be valid");
    }

    #[tokio::test]
    async fn executes_persisted_view_query() {
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
        .await
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE VIEW daily_metrics AS SELECT 7 AS metric".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .await
            .expect("view creation should succeed");

        let reloaded = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .await
        .expect("engine should reload");

        let response = reloaded
            .execute_query(&QueryRequest {
                sql: "SELECT * FROM daily_metrics".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .await
            .expect("persisted view should be queryable");

        assert_eq!(response.columns, vec!["metric"]);
        assert_eq!(response.rows, vec![vec!["7".to_string()]]);

        let _ = std::fs::remove_file(catalog_path);
    }

    #[tokio::test]
    async fn executes_persisted_table_query() {
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
        .await
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics AS SELECT 11 AS metric, 'ok' AS status".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .await
            .expect("table creation should succeed");

        let reloaded = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .await
        .expect("engine should reload");

        let response = reloaded
            .execute_query(&QueryRequest {
                sql: "SELECT metric, status FROM fact_metrics".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .await
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

    #[tokio::test]
    async fn creates_explicit_table_inserts_rows_and_queries_it() {
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
        .await
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
            .await
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
            .await
            .expect("insert should succeed");

        let reloaded = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .await
        .expect("engine should reload");

        let response = reloaded
            .execute_query(&QueryRequest {
                sql: "SELECT metric, status, is_hot FROM fact_metrics ORDER BY metric".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .await
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
            .await
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

    #[tokio::test]
    async fn inserts_with_column_list_and_omitted_nullable_columns() {
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
        .await
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
            .await
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
            .await
            .expect("insert should succeed");

        let response = engine
            .execute_query(&QueryRequest {
                sql: "SELECT metric, status, score, active FROM fact_metrics".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .await
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
            .await
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

    #[tokio::test]
    async fn rejects_insert_with_wrong_column_count() {
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
        .await
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT)".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .await
            .expect("table definition should succeed");

        let error = engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO fact_metrics VALUES (11)".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .await
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

    #[tokio::test]
    async fn rejects_insert_when_not_null_column_is_omitted() {
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
        .await
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT)".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .await
            .expect("table definition should succeed");

        let error = engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO fact_metrics (status) VALUES ('ok')".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .await
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

    #[tokio::test]
    async fn persists_managed_table_in_columnar_layout() {
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
        .await
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics AS SELECT 11 AS metric, 'ok' AS status".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .await
            .expect("table creation should succeed");

        let managed_dir = catalog_path.with_file_name(format!(
            "{}.managed",
            catalog_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("catalog file stem should be present")
        ));
        let table_dir = managed_dir.join("postgres__public__fact_metrics.table.parquet");
        assert!(table_dir.exists());
        assert!(table_dir.is_dir());

        let mut parquet_files = std::fs::read_dir(&table_dir)
            .expect("should be able to read table directory")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("parquet"));

        assert!(parquet_files.next().is_some());

        let _ = std::fs::remove_file(&catalog_path);
        let _ = std::fs::remove_dir_all(managed_dir);
    }

    #[tokio::test]
    async fn describes_persisted_table_columns() {
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
        .await
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics AS SELECT 11 AS metric, 'ok' AS status".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .await
            .expect("table creation should succeed");

        let response = engine
            .execute_query(&QueryRequest {
                sql: "DESCRIBE fact_metrics".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .await
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

    #[tokio::test]
    async fn exposes_pg_catalog_tables_views_and_namespace_rows() {
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
        .await
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
            .await
            .expect("schema creation should succeed");
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE SCHEMA alpha".to_string(),
                session: session.clone(),
            })
            .await
            .expect("alpha schema creation should succeed");
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE reporting.fact_metrics (metric BIGINT NOT NULL, status TEXT)"
                    .to_string(),
                session: session.clone(),
            })
            .await
            .expect("table creation should succeed");
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE VIEW reporting.daily_metrics AS SELECT metric, status FROM reporting.fact_metrics"
                    .to_string(),
                session: session.clone(),
            })
            .await
            .expect("view creation should succeed");
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE alpha.fact_alpha (metric BIGINT NOT NULL, status TEXT)"
                    .to_string(),
                session: session.clone(),
            })
            .await
            .expect("alpha table creation should succeed");
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE VIEW alpha.daily_alpha AS SELECT metric, status FROM alpha.fact_alpha"
                    .to_string(),
                session: session.clone(),
            })
            .await
            .expect("alpha view creation should succeed");
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE reporting.dim_metrics (metric BIGINT NOT NULL, label TEXT, CONSTRAINT dim_metrics_pkey PRIMARY KEY (metric))"
                    .to_string(),
                session: session.clone(),
            })
            .await
            .expect("dim_metrics table creation should succeed");
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE reporting.fact_events (metric_id BIGINT NOT NULL, CONSTRAINT fact_events_metric_fk FOREIGN KEY (metric_id) REFERENCES reporting.dim_metrics(metric))"
                    .to_string(),
                session: session.clone(),
            })
            .await
            .expect("fact_events table creation should succeed");

        let tables = engine
            .execute_query(&QueryRequest {
                sql: "SELECT * FROM pg_catalog.pg_tables".to_string(),
                session: session.clone(),
            })
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
