use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use analyticsdb_control::{
    parse_metadata_statement, AlterDatabaseOperation, AlterObjectOperation, AlterTableOperation,
    CatalogColumn, CatalogRelationKind, CatalogTableConstraint, CatalogTableConstraintKind,
    ControlPlane, MetadataStatement, QueryAdmission, TableColumnDefinition,
    TableConstraintDefinition,
};
use analyticsdb_core::{QueryRequest, QueryResponse, SessionContext, StatementOutcome};
use anyhow::{bail, Result};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, RecordBatch, RecordBatchReader, StringArray, UInt16Array, UInt32Array,
    UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::catalog::{CatalogProvider, MemorySchemaProvider};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::options::ParquetReadOptions;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::{SessionConfig, SessionContext as DfSessionContext};
use datafusion_common::config::TableParquetOptions;
use datafusion_common::TableReference;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::SendableRecordBatchStream;
use futures::stream;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

pub mod functions;
pub mod postgres_compatibility;
pub mod sql_rewriter;
pub mod system_catalog;

use functions::register_postgres_functions;
use system_catalog::PgCatalogSchemaProvider;

const INSERT_SELECT_PARQUET_ROW_GROUP_SIZE: usize = 1_048_576;

fn sanitize_error<E: Into<anyhow::Error>>(e: E) -> anyhow::Error {
    let e = e.into();
    let msg = e.to_string();
    if msg.contains("datafusion.") {
        anyhow::anyhow!(msg.replace("datafusion.", ""))
    } else {
        e
    }
}

fn session_context_cache_key(session: &SessionContext) -> String {
    [
        session.user.as_str(),
        session.role.as_str(),
        session.database.as_str(),
        session.schema.as_str(),
        session.auth_method.as_str(),
        match session.protocol {
            analyticsdb_core::Protocol::Embedded => "embedded",
            analyticsdb_core::Protocol::PostgreSql => "postgresql",
            analyticsdb_core::Protocol::ArrowFlightSql => "flight-sql",
        },
        format!("{:?}", session.transaction_status).as_str(),
    ]
    .join("\u{1f}")
}

pub struct PrototypeEngine {
    control_plane: Arc<ControlPlane>,
    session_context_cache: Arc<tokio::sync::RwLock<HashMap<String, DfSessionContext>>>,
}

impl std::fmt::Debug for PrototypeEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrototypeEngine")
            .field("control_plane", &self.control_plane)
            .field("session_context_cache", &"<cached session contexts>")
            .finish()
    }
}

impl Clone for PrototypeEngine {
    fn clone(&self) -> Self {
        Self {
            control_plane: Arc::clone(&self.control_plane),
            session_context_cache: Arc::clone(&self.session_context_cache),
        }
    }
}

pub struct QueryExecutionResult {
    pub query_id: String,
    pub coordinator_node_id: String,
    pub session: SessionContext,
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub message: String,
    pub outcome: StatementOutcome,
    pub execution_time_ms: u128,
}

impl QueryExecutionResult {
    pub fn to_query_response(&self) -> QueryResponse {
        let columns = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let mut rows = Vec::new();
        for batch in &self.batches {
            for row_idx in 0..batch.num_rows() {
                let mut row = Vec::new();
                for col_idx in 0..batch.num_columns() {
                    let array = batch.column(col_idx);
                    row.push(array_value_to_string(array, row_idx).unwrap_or_default());
                }
                rows.push(row);
            }
        }
        QueryResponse {
            query_id: self.query_id.clone(),
            coordinator_node_id: self.coordinator_node_id.clone(),
            session: self.session.clone(),
            columns,
            rows,
            message: self.message.clone(),
            execution_time_ms: self.execution_time_ms,
        }
    }
}

pub struct QueryExecutionStream {
    pub query_id: String,
    pub coordinator_node_id: String,
    pub session: SessionContext,
    pub schema: SchemaRef,
    pub stream: SendableRecordBatchStream,
    pub message: String,
    pub outcome: StatementOutcome,
    pub execution_time_ms: u128,
}

#[derive(Debug, Clone)]
struct InsertSelectStatement {
    database: Option<String>,
    schema: Option<String>,
    name: String,
    columns: Option<Vec<String>>,
    query_sql: String,
}

fn rows_outcome() -> StatementOutcome {
    StatementOutcome::Rows
}

fn command_outcome(tag: impl Into<String>, rows_affected: u64) -> StatementOutcome {
    StatementOutcome::Command {
        tag: tag.into(),
        rows_affected,
    }
}

fn utf8_schema(columns: &[&str]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|column| Field::new(*column, DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ))
}

fn metadata_statement_schema(statement: &MetadataStatement) -> Option<SchemaRef> {
    match statement {
        MetadataStatement::ShowDatabases => Some(utf8_schema(&["database_name"])),
        MetadataStatement::ShowSchemas { .. } => Some(utf8_schema(&["schema_name"])),
        MetadataStatement::ShowNodes => {
            Some(utf8_schema(&["node_id", "kind", "status", "last_heartbeat_ms"]))
        }
        MetadataStatement::ShowTables { .. } => Some(utf8_schema(&["table_name"])),
        MetadataStatement::ShowViews { .. } => Some(utf8_schema(&["view_name"])),
        MetadataStatement::ShowColumns { .. } => {
            Some(utf8_schema(&["column_name", "data_type", "nullable", "default"]))
        }
        MetadataStatement::InformationSchemaSchemata { .. } => Some(utf8_schema(&[
            "catalog_name",
            "schema_name",
            "schema_owner",
            "default_character_set_catalog",
            "default_character_set_schema",
            "default_character_set_name",
            "sql_path",
        ])),
        MetadataStatement::InformationSchemaTables { .. } => Some(utf8_schema(&[
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
        ])),
        MetadataStatement::InformationSchemaColumns { .. } => Some(utf8_schema(&[
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
        ])),
        MetadataStatement::InformationSchemaViews { .. } => Some(utf8_schema(&[
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
        ])),
        MetadataStatement::InformationSchemaTableConstraints { .. } => Some(utf8_schema(&[
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
        ])),
        MetadataStatement::InformationSchemaKeyColumnUsage { .. } => Some(utf8_schema(&[
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "ordinal_position",
            "position_in_unique_constraint",
        ])),
        MetadataStatement::InformationSchemaConstraintColumnUsage { .. } => Some(utf8_schema(&[
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
        ])),
        MetadataStatement::InformationSchemaConstraintTableUsage { .. } => Some(utf8_schema(&[
            "table_catalog",
            "table_schema",
            "table_name",
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
        ])),
        MetadataStatement::InformationSchemaReferentialConstraints { .. } => Some(utf8_schema(&[
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
            "unique_constraint_catalog",
            "unique_constraint_schema",
            "unique_constraint_name",
            "match_option",
            "update_rule",
            "delete_rule",
        ])),
        _ => None,
    }
}

impl PrototypeEngine {
    pub async fn from_catalog_path(catalog_path: &str) -> Result<Self> {
        Ok(Self {
            control_plane: Arc::new(ControlPlane::from_catalog_path(catalog_path).await?),
            session_context_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        })
    }

    pub fn control_plane(&self) -> Arc<ControlPlane> {
        Arc::clone(&self.control_plane)
    }

    pub async fn list_databases(&self, session: &SessionContext) -> Result<Vec<String>> {
        self.control_plane.list_databases(session).await
    }

    pub async fn list_schemas(&self, session: &SessionContext, database: Option<&str>) -> Result<Vec<String>> {
        self.control_plane.list_schemas(session, database).await
    }

    pub async fn list_relations(&self, session: &SessionContext, database: Option<&str>, schema: Option<&str>, kind: CatalogRelationKind) -> Result<Vec<analyticsdb_control::CatalogRelation>> {
        self.control_plane.list_relations(session, database, schema, kind).await
    }

    pub async fn execute_query(&self, request: &QueryRequest) -> Result<QueryExecutionResult> {
        let started = Instant::now();
        self.control_plane
            .validate_session(&request.session)
            .await?;
        let admission = self.control_plane.admit_query(&request.session).await?;

        if let Some(statement) = parse_insert_select_statement(&request.sql)? {
            return self
                .execute_insert_select(request, statement, admission, started)
                .await;
        }

        if let Some(statement) = parse_metadata_statement(&request.sql) {
            let result = self
                .execute_metadata_query(request, statement, admission, started)
                .await?;
            if matches!(result.outcome, StatementOutcome::Command { .. }) {
                self.invalidate_session_contexts().await;
            }
            return Ok(result);
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
        let dataframe = context.sql(&sql).await.map_err(sanitize_error)?;
        let schema = Arc::new(dataframe.schema().as_arrow().as_ref().clone());
        let batches = dataframe.collect().await.map_err(sanitize_error)?;
        let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        let outcome = if schema.fields().is_empty() {
            StatementOutcome::Command {
                tag: "OK".to_string(),
                rows_affected: 0,
            }
        } else {
            StatementOutcome::Rows
        };

        Ok(QueryExecutionResult {
            query_id: admission.query_id,
            coordinator_node_id: admission.coordinator_node_id,
            session,
            schema,
            batches,
            message: format!("Query executed successfully. {row_count} row(s) returned."),
            outcome,
            execution_time_ms: started.elapsed().as_millis(),
        })
    }

    pub async fn execute_query_stream(&self, request: &QueryRequest) -> Result<QueryExecutionStream> {
        let started = Instant::now();
        let admission = self.control_plane.admit_query(&request.session).await?;

        if parse_insert_select_statement(&request.sql)?.is_some() {
            anyhow::bail!(
                "statement query cannot execute SQL that does not return rows; execute it as a statement update"
            );
        }

        if let Some(statement) = parse_metadata_statement(&request.sql) {
            if metadata_statement_schema(&statement).is_none() {
                anyhow::bail!(
                    "statement query cannot execute SQL that does not return rows; execute it as a statement update"
                );
            }

            let execution = self
                .execute_metadata_query(request, statement, admission, started)
                .await?;
            let schema = Arc::clone(&execution.schema);
            let batches = if execution.batches.is_empty() {
                vec![RecordBatch::new_empty(Arc::clone(&schema))]
            } else {
                execution.batches
            };
            let batch_stream = stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>));

            return Ok(QueryExecutionStream {
                query_id: execution.query_id,
                coordinator_node_id: execution.coordinator_node_id,
                session: execution.session,
                schema: Arc::clone(&schema),
                stream: Box::pin(RecordBatchStreamAdapter::new(schema, batch_stream)),
                message: execution.message,
                outcome: StatementOutcome::Rows,
                execution_time_ms: execution.execution_time_ms,
            });
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
        let dataframe = context.sql(&sql).await.map_err(sanitize_error)?;
        let schema = Arc::new(dataframe.schema().as_arrow().as_ref().clone());
        if schema.fields().is_empty() {
            anyhow::bail!(
                "statement query cannot execute SQL that does not return rows; execute it as a statement update"
            );
        }

        let stream = dataframe.execute_stream().await.map_err(sanitize_error)?;

        Ok(QueryExecutionStream {
            query_id: admission.query_id,
            coordinator_node_id: admission.coordinator_node_id,
            session,
            schema,
            stream,
            message: "Query stream opened successfully.".to_string(),
            outcome: StatementOutcome::Rows,
            execution_time_ms: started.elapsed().as_millis(),
        })
    }

    pub async fn plan_query_schema(&self, request: &QueryRequest) -> Result<Option<SchemaRef>> {
        self.control_plane
            .validate_session(&request.session)
            .await?;

        if let Some(statement) = parse_metadata_statement(&request.sql) {
            return Ok(metadata_statement_schema(&statement));
        }

        let control_plane = Arc::clone(&self.control_plane);
        let sql = sql_rewriter::rewrite_sql_for_postgres_compatibility(
            &request.sql,
            &control_plane,
            &request.session,
        )
        .await?;

        let context = self.create_session_context(&request.session).await?;
        let dataframe = context.sql(&sql).await.map_err(sanitize_error)?;
        let schema = Arc::new(dataframe.schema().as_arrow().as_ref().clone());
        if schema.fields().is_empty() {
            Ok(None)
        } else {
            Ok(Some(schema))
        }
    }

    async fn execute_insert_select(
        &self,
        request: &QueryRequest,
        statement: InsertSelectStatement,
        admission: QueryAdmission,
        started: Instant,
    ) -> Result<QueryExecutionResult> {
        let relation = self
            .control_plane
            .table_relation(
                &request.session,
                statement.database.as_deref(),
                statement.schema.as_deref(),
                &statement.name,
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

        let context = self.create_session_context(&request.session).await?;
        let rewritten_query_sql = sql_rewriter::rewrite_sql_for_postgres_compatibility(
            &statement.query_sql,
            &self.control_plane,
            &request.session,
        )
        .await?;
        let source_dataframe = context
            .sql(&rewritten_query_sql)
            .await
            .map_err(sanitize_error)?;
        let source_schema = Arc::new(source_dataframe.schema().as_arrow().clone());
        let projected_sql = build_insert_select_projection_sql(
            &rewritten_query_sql,
            &source_schema,
            &relation.columns,
            statement.columns.as_deref(),
        )?;
        let projected_dataframe = context.sql(&projected_sql).await.map_err(sanitize_error)?;
        if !Path::new(storage_path).exists() {
            fs::create_dir_all(storage_path)?;
        }
        let inserted_row_count =
            write_dataframe_to_table_snapshot(projected_dataframe, storage_path).await?;

        Ok(QueryExecutionResult {
            query_id: admission.query_id,
            coordinator_node_id: admission.coordinator_node_id,
            session: request.session.clone(),
            schema: Arc::new(Schema::empty()),
            batches: Vec::new(),
            message: format!(
                "Inserted {inserted_row_count} row(s) into '{}.{}.{}'.",
                relation.database, relation.schema, relation.name
            ),
            outcome: StatementOutcome::Command {
                tag: "INSERT".to_string(),
                rows_affected: inserted_row_count as u64,
            },
            execution_time_ms: started.elapsed().as_millis(),
        })
    }

    async fn execute_metadata_query(
        &self,
        request: &QueryRequest,
        statement: MetadataStatement,
        admission: QueryAdmission,
        started: Instant,
    ) -> Result<QueryExecutionResult> {
        let (schema, batches, message, outcome, new_session) = match statement {
            MetadataStatement::CreateDatabase { .. }
            | MetadataStatement::CreateAggregate { .. }
            | MetadataStatement::CreateCollation { .. }
            | MetadataStatement::CreateConversion { .. }
            | MetadataStatement::CreateFunction { .. }
            | MetadataStatement::AlterFunction { .. }
            | MetadataStatement::DropFunction { .. }
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
                        rows_outcome(),
                        request.session.clone(),
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
                        rows_outcome(),
                        request.session.clone(),
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
                        rows_outcome(),
                        request.session.clone(),
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
                        rows_outcome(),
                        request.session.clone(),
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
                        rows_outcome(),
                        request.session.clone(),
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
                        rows_outcome(),
                        request.session.clone(),
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
                        rows_outcome(),
                        request.session.clone(),
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
                        rows_outcome(),
                        request.session.clone(),
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
                        rows_outcome(),
                        request.session.clone(),
                    )
                }
                _ => {
                    let (message, new_session) = self
                        .control_plane
                        .execute_metadata_statement(&request.session, &statement)
                        .await?;
                    (
                        Arc::new(Schema::empty()),
                        Vec::new(),
                        message,
                        command_outcome("OK", 0),
                        new_session,
                    )
                }
            },
            MetadataStatement::CreateView {
                database,
                schema,
                name,
                definition_sql,
            } => {
                // Determine schema of the view query
                let session = SessionContext {
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
                    let rewritten_query_sql = sql_rewriter::rewrite_sql_for_postgres_compatibility(
                        &query_sql,
                        &self.control_plane,
                        &session,
                    )
                    .await?;
                    let dataframe = context
                        .sql(&rewritten_query_sql)
                        .await
                        .map_err(sanitize_error)?;
                    let arrow_schema = Arc::new(dataframe.schema().as_arrow().clone());
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
                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    message,
                    command_outcome("CREATE VIEW", 0),
                    request.session.clone(),
                )
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
                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    message,
                    command_outcome("CREATE TABLE", 0),
                    request.session.clone(),
                )
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

                let context = self.create_session_context(&request.session).await?;
                let rewritten_query_sql = sql_rewriter::rewrite_sql_for_postgres_compatibility(
                    &query_sql,
                    &self.control_plane,
                    &request.session,
                )
                .await?;
                let dataframe = context
                    .sql(&rewritten_query_sql)
                    .await
                    .map_err(sanitize_error)?;
                let arrow_schema = Arc::new(dataframe.schema().as_arrow().clone());
                let columns_metadata = catalog_columns_from_schema(&arrow_schema);
                let row_count = write_dataframe_to_table_snapshot(dataframe, storage_path.to_str().unwrap()).await?;

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
                    command_outcome("CREATE TABLE", 0),
                    request.session.clone(),
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

                persist_empty_table_snapshot(storage_path.to_str().unwrap(), &arrow_schema)?;

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

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    created_message,
                    command_outcome("CREATE TABLE", 0),
                    request.session.clone(),
                )
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

                let column_definitions: Vec<TableColumnDefinition> = relation
                    .columns
                    .iter()
                    .map(|column| TableColumnDefinition {
                        name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        nullable: column.nullable,
                        default_value: column.default_value.clone(),
                    })
                    .collect();
                let arrow_schema = build_arrow_schema_from_definitions(&column_definitions, false)?;
                let batch = build_record_batch_from_rows(
                    &arrow_schema,
                    &relation.columns,
                    columns,
                    &rows,
                )?;
                let row_count = batch.num_rows();

                if !Path::new(storage_path).exists() {
                    fs::create_dir_all(storage_path)?;
                }
                append_record_batch_to_table_snapshot(batch, storage_path).await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!(
                        "Inserted {row_count} row(s) into '{}.{}.{}'.",
                        relation.database, relation.schema, relation.name
                    ),
                    command_outcome("INSERT", row_count as u64),
                    request.session.clone(),
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
                let storage_path = relation.storage_path.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Managed table '{}.{}.{}' is missing a storage path",
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })?;

                let context = self.create_session_context(&request.session).await?;
                let full_table_name = format!(
                    "\"{}\".\"{}\".\"{}\"",
                    relation.database, relation.schema, relation.name
                );
                let filter_clause = selection_sql
                    .as_ref()
                    .map(|sql| format!("WHERE {sql}"))
                    .unwrap_or_default();
                let mut update_expressions = Vec::new();
                for column in &relation.columns {
                    if let Some((_, value_sql)) =
                        assignments.iter().find(|(name, _)| name == &column.name)
                    {
                        update_expressions.push(format!("{value_sql} AS \"{}\"", column.name));
                    } else {
                        update_expressions.push(format!("\"{}\"", column.name));
                    }
                }

                let update_sql = format!(
                    "SELECT {} FROM {full_table_name} {filter_clause}",
                    update_expressions.join(", ")
                );
                let rewritten_update_sql = sql_rewriter::rewrite_sql_for_postgres_compatibility(
                    &update_sql,
                    &self.control_plane,
                    &request.session,
                )
                .await?;
                let updated_dataframe = context
                    .sql(&rewritten_update_sql)
                    .await
                    .map_err(sanitize_error)?;
                let row_count =
                    write_dataframe_to_table_snapshot(updated_dataframe, storage_path).await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!(
                        "Updated {row_count} row(s) in '{}.{}.{}'.",
                        relation.database, relation.schema, relation.name
                    ),
                    command_outcome("UPDATE", row_count as u64),
                    request.session.clone(),
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
                let storage_path = relation.storage_path.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Managed table '{}.{}.{}' is missing a storage path",
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })?;

                let context = self.create_session_context(&request.session).await?;
                let full_table_name = format!(
                    "\"{}\".\"{}\".\"{}\"",
                    relation.database, relation.schema, relation.name
                );
                let filter_clause = selection_sql
                    .as_ref()
                    .map(|sql| format!("WHERE NOT ({sql})"))
                    .unwrap_or_default();
                let delete_sql = format!("SELECT * FROM {full_table_name} {filter_clause}");
                let rewritten_delete_sql = sql_rewriter::rewrite_sql_for_postgres_compatibility(
                    &delete_sql,
                    &self.control_plane,
                    &request.session,
                )
                .await?;
                let remaining_dataframe = context
                    .sql(&rewritten_delete_sql)
                    .await
                    .map_err(sanitize_error)?;
                let row_count =
                    write_dataframe_to_table_snapshot(remaining_dataframe, storage_path).await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!(
                        "DELETE completed for '{}.{}.{}'.",
                        relation.database, relation.schema, relation.name
                    ),
                    command_outcome("DELETE", row_count as u64),
                    request.session.clone(),
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
                let storage_path = relation.storage_path.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Managed table '{}.{}.{}' is missing a storage path",
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })?;

                let column_definitions: Vec<TableColumnDefinition> = relation
                    .columns
                    .iter()
                    .map(|column| TableColumnDefinition {
                        name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        nullable: column.nullable,
                        default_value: column.default_value.clone(),
                    })
                    .collect();
                let arrow_schema = build_arrow_schema_from_definitions(&column_definitions, false)?;

                persist_empty_table_snapshot(storage_path, &arrow_schema)?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!(
                        "TRUNCATE completed for '{}.{}.{}'.",
                        relation.database, relation.schema, relation.name
                    ),
                    command_outcome("TRUNCATE", 0),
                    request.session.clone(),
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
                        self.control_plane
                            .add_column(
                                &request.session,
                                database.as_deref(),
                                schema.as_deref(),
                                &name,
                                CatalogColumn {
                                    name: column.name.clone(),
                                    data_type: column.data_type.clone(),
                                    nullable: column.nullable,
                                    default_value: column.default_value.clone(),
                                },
                            )
                            .await?;

                        (
                            Arc::new(Schema::empty()),
                            Vec::new(),
                            format!(
                                "ALTER TABLE completed. Column '{}' added to '{}.{}.{}'.",
                                column.name, relation.database, relation.schema, relation.name
                            ),
                            command_outcome("ALTER TABLE", 0),
                            request.session.clone(),
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

                        (
                            Arc::new(Schema::empty()),
                            Vec::new(),
                            format!(
                                "ALTER TABLE completed. Relation '{}.{}.{}' renamed to '{}'.",
                                relation.database, relation.schema, relation.name, new_name
                            ),
                            command_outcome("ALTER TABLE", 0),
                            request.session.clone(),
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

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!(
                        "ALTER SCHEMA completed. Schema '{}.{}' renamed to '{}'.",
                        database_name, name, new_name
                    ),
                    command_outcome("ALTER SCHEMA", 0),
                    request.session.clone(),
                )
            }
            MetadataStatement::AlterDatabase { name, operation } => match &operation {
                AlterDatabaseOperation::Rename { new_name } => {
                    // 1. Get all tables in this database to update their physical paths
                    let relations = self
                        .control_plane
                        .list_relations_for_database(&request.session, &name, CatalogRelationKind::Table)
                        .await?;

                    // 2. Rename database in catalog
                    let (msg, new_session) = self
                        .control_plane
                        .execute_metadata_statement(
                            &request.session,
                            &MetadataStatement::AlterDatabase {
                                name: name.clone(),
                                operation: AlterDatabaseOperation::Rename {
                                    new_name: new_name.clone(),
                                },
                            },
                        )
                        .await?;

                    // 3. Physically rename managed directories
                    for relation in relations {
                        if let Some(storage_path_str) = &relation.storage_path {
                            let old_path = Path::new(storage_path_str);
                            if old_path.exists() {
                                let file_name = old_path.file_name().unwrap().to_str().unwrap();
                                let old_prefix = format!("{}__{}__", name, relation.schema);
                                let new_prefix = format!("{}__{}__", new_name, relation.schema);
                                let new_file_name = file_name.replace(&old_prefix, &new_prefix);
                                let new_path = old_path.with_file_name(new_file_name);

                                fs::rename(old_path, &new_path)?;

                                self.control_plane
                                    .update_relation_storage_path(
                                        &request.session,
                                        Some(&new_name),
                                        Some(&relation.schema),
                                        &relation.name,
                                        new_path.to_str().unwrap(),
                                    )
                                    .await?;
                            }
                        }
                    }

                    (
                        Arc::new(Schema::empty()),
                        Vec::new(),
                        msg,
                        command_outcome("ALTER DATABASE", 0),
                        new_session,
                    )
                }
                _ => {
                    let (message, new_session) = self
                        .control_plane
                        .execute_metadata_statement(
                            &request.session,
                            &MetadataStatement::AlterDatabase {
                                name: name.clone(),
                                operation: operation.clone(),
                            },
                        )
                        .await?;
                    (
                        Arc::new(Schema::empty()),
                        Vec::new(),
                        message,
                        command_outcome("ALTER DATABASE", 0),
                        new_session,
                    )
                }
            },
            MetadataStatement::AlterAggregate {
                database,
                schema,
                name,
                operation,
            } => {
                let (msg, new_session) = self
                    .control_plane
                    .execute_metadata_statement(
                        &request.session,
                        &MetadataStatement::AlterAggregate {
                            database,
                            schema,
                            name,
                            operation,
                        },
                    )
                    .await?;
                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    msg,
                    command_outcome("ALTER AGGREGATE", 0),
                    new_session,
                )
            }
            MetadataStatement::AlterCollation {
                database,
                schema,
                name,
                operation,
            } => {
                let (msg, new_session) = self
                    .control_plane
                    .execute_metadata_statement(
                        &request.session,
                        &MetadataStatement::AlterCollation {
                            database,
                            schema,
                            name,
                            operation,
                        },
                    )
                    .await?;
                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    msg,
                    command_outcome("ALTER COLLATION", 0),
                    new_session,
                )
            }
            MetadataStatement::AlterConversion {
                database,
                schema,
                name,
                operation,
            } => {
                let (msg, new_session) = self
                    .control_plane
                    .execute_metadata_statement(
                        &request.session,
                        &MetadataStatement::AlterConversion {
                            database,
                            schema,
                            name,
                            operation,
                        },
                    )
                    .await?;
                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    msg,
                    command_outcome("ALTER CONVERSION", 0),
                    new_session,
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
                    rows_outcome(),
                    request.session.clone(),
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
                    rows_outcome(),
                    request.session.clone(),
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
                            format!("{:?}", node.status),
                            node.last_heartbeat_at_epoch_ms.to_string(),
                        ]
                    })
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(
                    &["node_id", "kind", "status", "last_heartbeat_ms"],
                    &rows,
                )?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} node(s) listed successfully."),
                    rows_outcome(),
                    request.session.clone(),
                )
            }
            MetadataStatement::ShowTables { database, schema } => {
                let relations = self
                    .control_plane
                    .list_relations(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        CatalogRelationKind::Table,
                    )
                    .await?;
                let rows = relations
                    .into_iter()
                    .map(|rel| vec![rel.name])
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(&["table_name"], &rows)?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} table(s) listed successfully."),
                    rows_outcome(),
                    request.session.clone(),
                )
            }
            MetadataStatement::ShowViews { database, schema } => {
                let relations = self
                    .control_plane
                    .list_relations(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        CatalogRelationKind::View,
                    )
                    .await?;
                let rows = relations
                    .into_iter()
                    .map(|rel| vec![rel.name])
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(&["view_name"], &rows)?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} view(s) listed successfully."),
                    rows_outcome(),
                    request.session.clone(),
                )
            }
            MetadataStatement::ShowColumns {
                database,
                schema,
                table,
            } => {
                let relation = self
                    .control_plane
                    .table_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &table,
                    )
                    .await?;
                let rows = relation
                    .columns
                    .into_iter()
                    .map(|col| {
                        vec![
                            col.name,
                            col.data_type,
                            col.nullable.to_string(),
                            col.default_value.unwrap_or_default(),
                        ]
                    })
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(
                    &["column_name", "data_type", "nullable", "default"],
                    &rows,
                )?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} column(s) listed successfully."),
                    rows_outcome(),
                    request.session.clone(),
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

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    message,
                    command_outcome("DROP TABLE", 0),
                    request.session.clone(),
                )
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

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    message,
                    command_outcome("DROP VIEW", 0),
                    request.session.clone(),
                )
            }
            MetadataStatement::DropDatabase { name, if_exists } => {
                let message = self
                    .control_plane
                    .drop_database(&request.session, &name, if_exists)
                    .await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    message,
                    command_outcome("DROP DATABASE", 0),
                    request.session.clone(),
                )
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

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    message,
                    command_outcome("DROP SCHEMA", 0),
                    request.session.clone(),
                )
            }
        };

        Ok(QueryExecutionResult {
            query_id: admission.query_id,
            coordinator_node_id: admission.coordinator_node_id,
            session: new_session,
            schema,
            batches,
            message,
            outcome,
            execution_time_ms: started.elapsed().as_millis(),
        })
    }

    pub async fn execute_query_batches(&self, request: &QueryRequest) -> Result<QueryExecutionResult> {
        self.execute_query(request).await
    }

    async fn invalidate_session_contexts(&self) {
        let mut cache = self.session_context_cache.write().await;
        cache.clear();
    }

    async fn create_session_context(&self, session: &SessionContext) -> Result<DfSessionContext> {
        let key = session_context_cache_key(session);
        {
            let cache = self.session_context_cache.read().await;
            if let Some(ctx) = cache.get(&key) {
                return Ok(ctx.clone());
            }
        }

        let config = SessionConfig::new()
            .with_default_catalog_and_schema(&session.database, &session.schema)
            .with_target_partitions(1);

        let ctx = DfSessionContext::new_with_config(config);
        
        let provider = Arc::new(system_catalog::AnalyticsCatalogProvider::new(
            Arc::clone(&self.control_plane),
            session.clone(),
        ));
        ctx.register_catalog(&session.database, provider.clone());

        let pg_catalog = Arc::new(PgCatalogSchemaProvider::new(
            Arc::clone(&self.control_plane),
        ));
        provider.register_schema("pg_catalog", pg_catalog)?;

        register_postgres_functions(&ctx);

        let mut cache = self.session_context_cache.write().await;
        cache.insert(key, ctx.clone());
        Ok(ctx)
    }

    async fn information_schema_schemata_rows(&self, session: &SessionContext) -> Result<Vec<Vec<String>>> {
        let schemas = self.control_plane.list_schemas(session, None).await?;
        Ok(schemas
            .into_iter()
            .map(|s| {
                vec![
                    session.database.clone(),
                    s,
                    session.user.clone(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                ]
            })
            .collect())
    }

    async fn information_schema_tables_rows(&self, session: &SessionContext) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_relations(session, None, None, CatalogRelationKind::Table).await?;
        let views = self.control_plane.list_relations(session, None, None, CatalogRelationKind::View).await?;
        
        let mut rows = Vec::new();
        for rel in relations {
            rows.push(vec![
                rel.database,
                rel.schema,
                rel.name,
                "BASE TABLE".to_string(),
                String::new(), String::new(), String::new(), String::new(), String::new(),
                "YES".to_string(), "NO".to_string(), String::new(),
            ]);
        }
        for rel in views {
            rows.push(vec![
                rel.database,
                rel.schema,
                rel.name,
                "VIEW".to_string(),
                String::new(), String::new(), String::new(), String::new(), String::new(),
                "NO".to_string(), "NO".to_string(), String::new(),
            ]);
        }
        Ok(rows)
    }

    async fn information_schema_columns_rows(&self, session: &SessionContext) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;
        let mut rows = Vec::new();
        for rel in relations {
            for (i, col) in rel.columns.into_iter().enumerate() {
                rows.push(vec![
                    rel.database.clone(),
                    rel.schema.clone(),
                    rel.name.clone(),
                    col.name,
                    (i + 1).to_string(),
                    col.default_value.unwrap_or_default(),
                    if col.nullable { "YES".to_string() } else { "NO".to_string() },
                    col.data_type,
                    String::new(), String::new(), String::new(), String::new(), String::new(), String::new(),
                ]);
            }
        }
        Ok(rows)
    }

    async fn information_schema_views_rows(&self, session: &SessionContext) -> Result<Vec<Vec<String>>> {
        let views = self.control_plane.list_relations(session, None, None, CatalogRelationKind::View).await?;
        Ok(views.into_iter().map(|v| {
            vec![
                v.database, v.schema, v.name,
                v.definition_sql.unwrap_or_default(),
                "NONE".to_string(), "NO".to_string(), "NO".to_string(),
                "NO".to_string(), "NO".to_string(), "NO".to_string(),
            ]
        }).collect())
    }

    async fn information_schema_table_constraints_rows(&self, session: &SessionContext) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;
        let mut rows = Vec::new();
        for rel in relations {
            for constraint in rel.constraints {
                rows.push(vec![
                    rel.database.clone(), rel.schema.clone(), constraint.name.clone(),
                    rel.database.clone(), rel.schema.clone(), rel.name.clone(),
                    format!("{:?}", constraint.kind).to_ascii_uppercase(),
                    "NO".to_string(), "NO".to_string(), "YES".to_string(), String::new(),
                ]);
            }
            // Add NOT NULL constraints
            for col in rel.columns {
                if !col.nullable {
                    let cname = format!("{}_{}_not_null", rel.name, col.name);
                    rows.push(vec![
                        rel.database.clone(), rel.schema.clone(), cname,
                        rel.database.clone(), rel.schema.clone(), rel.name.clone(),
                        "CHECK".to_string(), "NO".to_string(), "NO".to_string(), "YES".to_string(), String::new(),
                    ]);
                }
            }
        }
        Ok(rows)
    }

    async fn information_schema_key_column_usage_rows(&self, session: &SessionContext) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;
        let mut rows = Vec::new();
        for rel in relations {
            for constraint in rel.constraints {
                if matches!(constraint.kind, CatalogTableConstraintKind::PrimaryKey | CatalogTableConstraintKind::ForeignKey | CatalogTableConstraintKind::Unique) {
                    for (i, col) in constraint.columns.into_iter().enumerate() {
                        rows.push(vec![
                            rel.database.clone(), rel.schema.clone(), constraint.name.clone(),
                            rel.database.clone(), rel.schema.clone(), rel.name.clone(),
                            col, (i + 1).to_string(), String::new(),
                        ]);
                    }
                }
            }
        }
        Ok(rows)
    }

    async fn information_schema_constraint_column_usage_rows(&self, session: &SessionContext) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;
        let mut rows = Vec::new();
        for rel in relations {
            // Include NOT NULL constraints
            for col in &rel.columns {
                if !col.nullable {
                    let cname = format!("{}_{}_not_null", rel.name, col.name);
                    rows.push(vec![
                        rel.database.clone(), rel.schema.clone(), rel.name.clone(),
                        col.name.clone(), rel.database.clone(), rel.schema.clone(), cname,
                    ]);
                }
            }
            // Include explicit constraints
            for constraint in rel.constraints {
                for col in constraint.columns {
                    rows.push(vec![
                        rel.database.clone(), rel.schema.clone(), rel.name.clone(),
                        col, rel.database.clone(), rel.schema.clone(), constraint.name.clone(),
                    ]);
                }
            }
        }
        Ok(rows)
    }

    async fn information_schema_constraint_table_usage_rows(&self, session: &SessionContext) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;
        let mut rows = Vec::new();
        for rel in relations {
             // NOT NULLs
             for col in &rel.columns {
                if !col.nullable {
                    let cname = format!("{}_{}_not_null", rel.name, col.name);
                    rows.push(vec![
                        rel.database.clone(), rel.schema.clone(), rel.name.clone(),
                        rel.database.clone(), rel.schema.clone(), cname,
                    ]);
                }
            }
            for constraint in rel.constraints {
                rows.push(vec![
                    rel.database.clone(), rel.schema.clone(), rel.name.clone(),
                    rel.database.clone(), rel.schema.clone(), constraint.name.clone(),
                ]);
            }
        }
        Ok(rows)
    }

    async fn information_schema_referential_constraints_rows(&self, session: &SessionContext) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;
        let mut rows = Vec::new();
        for rel in relations {
            for constraint in rel.constraints {
                if let CatalogTableConstraintKind::ForeignKey = constraint.kind {
                    let referenced_database = constraint.referenced_database.as_ref();
                    let referenced_schema = constraint.referenced_schema.as_ref();
                    let referenced_table = constraint.referenced_table.as_ref();
                    rows.push(vec![
                        rel.database.clone(), rel.schema.clone(), constraint.name.clone(),
                        referenced_database.cloned().unwrap_or_else(|| session.database.clone()),
                        referenced_schema.cloned().unwrap_or_else(|| session.schema.clone()),
                        referenced_table.cloned().unwrap_or_default(),
                        "MATCH SIMPLE".to_string(), "NO ACTION".to_string(), "NO ACTION".to_string(),
                    ]);
                }
            }
        }
        Ok(rows)
    }
}

fn build_arrow_schema_from_definitions(
    columns: &[TableColumnDefinition],
    _is_view: bool,
) -> Result<SchemaRef> {
    let mut fields = Vec::new();
    for col in columns {
        let dt = match col.data_type.to_ascii_uppercase().as_str() {
            "INT" | "INTEGER" | "INT4" => DataType::Int32,
            "BIGINT" | "INT8" => DataType::Int64,
            "TEXT" | "VARCHAR" | "STRING" => DataType::Utf8,
            "BOOLEAN" | "BOOL" => DataType::Boolean,
            "FLOAT4" | "REAL" => DataType::Float32,
            "FLOAT8" | "DOUBLE PRECISION" => DataType::Float64,
            _ => DataType::Utf8,
        };
        fields.push(Field::new(&col.name, dt, col.nullable));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn catalog_columns_from_schema(schema: &SchemaRef) -> Vec<CatalogColumn> {
    schema
        .fields()
        .iter()
        .map(|f| CatalogColumn {
            name: f.name().clone(),
            data_type: format!("{:?}", f.data_type()),
            nullable: f.is_nullable(),
            default_value: None,
        })
        .collect()
}

fn catalog_constraints_from_definitions(
    _table: &str,
    _database: Option<&str>,
    _schema: Option<&str>,
    _session: &SessionContext,
    defs: &[TableConstraintDefinition],
) -> Result<Vec<CatalogTableConstraint>> {
    let mut out = Vec::new();
    for def in defs {
        let (kind, ref_db, ref_sch, ref_tab, ref_cols) = match def {
            TableConstraintDefinition::PrimaryKey { .. } => {
                (CatalogTableConstraintKind::PrimaryKey, None, None, None, Vec::new())
            }
            TableConstraintDefinition::ForeignKey {
                referenced_database,
                referenced_schema,
                referenced_table,
                referenced_columns,
                ..
            } => (
                CatalogTableConstraintKind::ForeignKey,
                referenced_database.clone(),
                referenced_schema.clone(),
                Some(referenced_table.clone()),
                referenced_columns.clone(),
            ),
            TableConstraintDefinition::Unique { .. } => {
                (CatalogTableConstraintKind::Unique, None, None, None, Vec::new())
            }
        };
        let name_opt = match def {
            TableConstraintDefinition::PrimaryKey { name, .. } => name.clone(),
            TableConstraintDefinition::ForeignKey { name, .. } => name.clone(),
            TableConstraintDefinition::Unique { name, .. } => name.clone(),
        };
        let columns = match def {
            TableConstraintDefinition::PrimaryKey { columns, .. } => columns.clone(),
            TableConstraintDefinition::ForeignKey { columns, .. } => columns.clone(),
            TableConstraintDefinition::Unique { columns, .. } => columns.clone(),
        };
        out.push(CatalogTableConstraint {
            name: name_opt.unwrap_or_else(|| "auto_constraint".to_string()),
            columns,
            kind,
            referenced_database: ref_db,
            referenced_schema: ref_sch,
            referenced_table: ref_tab,
            referenced_columns: ref_cols,
        });
    }
    Ok(out)
}

fn build_record_batch_from_rows(
    schema: &SchemaRef,
    _catalog_columns: &[CatalogColumn],
    target_columns: Option<Vec<String>>,
    rows: &[Vec<String>],
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (i, field) in schema.fields().iter().enumerate() {
        let mut values = Vec::new();
        let target_idx = if let Some(ref names) = target_columns {
            names.iter().position(|n| n == field.name())
        } else {
            Some(i)
        };

        for row in rows {
            if let Some(idx) = target_idx {
                values.push(Some(row[idx].clone()));
            } else {
                values.push(None);
            }
        }
        let array: ArrayRef = Arc::new(StringArray::from(values));
        // Cast to actual type
        let casted = datafusion::arrow::compute::cast(&array, field.data_type())?;
        columns.push(casted);
    }
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

async fn write_dataframe_to_table_snapshot(df: datafusion::dataframe::DataFrame, path: &str) -> Result<usize> {
    if !Path::new(path).exists() {
        fs::create_dir_all(path)?;
    }
    let options = DataFrameWriteOptions::new();
    let results = df.write_parquet(path, options, None).await?;
    let count: usize = results.iter().map(|b| b.num_rows()).sum();
    Ok(count)
}

fn persist_empty_table_snapshot(path: &str, schema: &SchemaRef) -> Result<()> {
    if !Path::new(path).exists() {
        fs::create_dir_all(path)?;
    }
    let file_path = Path::new(path).join("empty.parquet");
    let file = fs::File::create(file_path)?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(schema), None)?;
    writer.close()?;
    Ok(())
}

async fn append_record_batch_to_table_snapshot(batch: RecordBatch, path: &str) -> Result<()> {
    let file_path = Path::new(path).join(format!("{}.parquet", uuid::Uuid::now_v7()));
    let file = fs::File::create(file_path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn utf8_record_batch(columns: &[&str], rows: &[Vec<String>]) -> Result<RecordBatch> {
    let schema = utf8_schema(columns);
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for i in 0..columns.len() {
        let values: Vec<Option<String>> = rows.iter().map(|r| Some(r[i].clone())).collect();
        arrays.push(Arc::new(StringArray::from(values)));
    }
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn execute_pg_catalog_select(
    _sql: &str,
    _table: &str,
    columns: &[&str],
    rows: &[Vec<String>],
) -> Result<(RecordBatch, usize)> {
    let batch = utf8_record_batch(columns, rows)?;
    let count = rows.len();
    Ok((batch, count))
}

fn parse_insert_select_statement(sql: &str) -> Result<Option<InsertSelectStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("INSERT INTO ") {
        return Ok(None);
    }
    // Very naive parser for prototype
    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    if parts.len() < 4 { return Ok(None); }
    let name = parts[2].to_string();
    let select_idx = upper.find("SELECT ").ok_or_else(|| anyhow::anyhow!("INSERT INTO currently only supports SELECT sources"))?;
    let query_sql = trimmed[select_idx..].to_string();
    
    Ok(Some(InsertSelectStatement {
        database: None,
        schema: None,
        name,
        columns: None,
        query_sql,
    }))
}

fn build_insert_select_projection_sql(
    _query_sql: &str,
    _source_schema: &SchemaRef,
    _table_columns: &[CatalogColumn],
    _target_columns: Option<&[String]>,
) -> Result<String> {
    // For prototype, we just return the query_sql if it matches. 
    // This is handled by DataFusion.
    Ok(_query_sql.to_string())
}
