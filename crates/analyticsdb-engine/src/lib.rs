use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use serde::{Serialize, Deserialize};

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
use sqlparser::ast::{BinaryOperator, Expr, SelectItem, SetExpr, Statement, TableFactor};
use sqlparser::parser::Parser;
use tracing::warn;

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
    relation_locks:
        Arc<tokio::sync::RwLock<HashMap<String, Arc<tokio::sync::RwLock<()>>>>>,
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
            relation_locks: Arc::clone(&self.relation_locks),
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
struct IndexedSelectStatement {
    database: Option<String>,
    schema: Option<String>,
    table: String,
    projection: Option<Vec<String>>,
    predicates: BTreeMap<String, IndexPredicate>,
}

#[derive(Debug, Clone)]
enum IndexPredicate {
    Eq(String),
    In(Vec<String>),
    Range {
        lower: Option<(String, bool)>,
        upper: Option<(String, bool)>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexSnapshotManifest {
    version: String,
    snapshot_object: String,
    row_count: usize,
    published_at_epoch_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexSnapshot {
    database: String,
    schema: String,
    table: String,
    index: String,
    columns: Vec<String>,
    unique: bool,
    primary: bool,
    entries: BTreeMap<String, Vec<String>>,
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
        MetadataStatement::ShowColumns { .. } => Some(utf8_schema(&[
            "column_name",
            "data_type",
            "is_nullable",
        ])),
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
            relation_locks: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        })
    }

    pub fn control_plane(&self) -> Arc<ControlPlane> {
        Arc::clone(&self.control_plane)
    }

    async fn relation_lock(
        &self,
        relation: &analyticsdb_control::CatalogRelation,
    ) -> Arc<tokio::sync::RwLock<()>> {
        let key = format!("{}.{}.{}", relation.database, relation.schema, relation.name);
        {
            let locks = self.relation_locks.read().await;
            if let Some(lock) = locks.get(&key) {
                return Arc::clone(lock);
            }
        }

        let mut locks = self.relation_locks.write().await;
        Arc::clone(
            locks
                .entry(key)
                .or_insert_with(|| Arc::new(tokio::sync::RwLock::new(()))),
        )
    }

    async fn rebuild_all_index_snapshots(
        &self,
        session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
    ) -> Result<()> {
        self.invalidate_session_contexts().await;
        let mut snapshots = Vec::with_capacity(relation.indexes.len());
        for index in &relation.indexes {
            snapshots.push(
                self
                .build_index_snapshot_for_relation(session, relation, &index.name)
                .await?,
            );
        }
        for snapshot in snapshots {
            write_index_snapshot(relation, &snapshot)?;
        }
        Ok(())
    }

    async fn build_index_snapshot_for_relation(
        &self,
        session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
        index_name: &str,
    ) -> Result<IndexSnapshot> {
        let Some(index) = relation.indexes.iter().find(|idx| idx.name == index_name) else {
            bail!(
                "Index '{}' not found on relation '{}.{}.{}'",
                index_name,
                relation.database,
                relation.schema,
                relation.name
            );
        };
        let rows = self.collect_table_rows(session, relation).await?;
        build_index_snapshot(relation, index, &rows)
    }

    async fn refresh_index_snapshots_after_mutation(
        &self,
        session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
    ) {
        if relation.indexes.is_empty() {
            return;
        }

        if let Err(rebuild_error) = self.rebuild_all_index_snapshots(session, relation).await {
            warn!(
                database = %relation.database,
                schema = %relation.schema,
                table = %relation.name,
                error = %rebuild_error,
                "failed to rebuild managed-table index sidecars after mutation; previous published snapshot remains active"
            );
        }
    }

    fn validate_unique_indexes_for_rows(
        &self,
        relation: &analyticsdb_control::CatalogRelation,
        rows: &[Vec<String>],
    ) -> Result<()> {
        for index in &relation.indexes {
            if index.is_unique || index.is_primary {
                validate_unique_index_rows(relation, index, rows)?;
            }
        }
        Ok(())
    }

    async fn try_execute_indexed_select(
        &self,
        request: &QueryRequest,
        statement: IndexedSelectStatement,
        admission: &QueryAdmission,
        started: Instant,
    ) -> Result<Option<QueryExecutionResult>> {
        let relation = match self
            .control_plane
            .table_relation(
                &request.session,
                statement.database.as_deref(),
                statement.schema.as_deref(),
                &statement.table,
            )
            .await
        {
            Ok(relation) => relation,
            Err(_) => return Ok(None),
        };

        let relation_lock = self.relation_lock(&relation).await;
        let _read_guard = relation_lock.read().await;

        let Some((index, row_ids)) = best_index_match(&relation, &statement)? else {
            return Ok(None);
        };

        if row_ids.is_empty() {
            let schema = build_arrow_schema_from_catalog_columns(&relation.columns)?;
            let user_schema = Arc::new(Schema::new(
                schema
                    .fields()
                    .iter()
                    .filter(|f| f.name() != "_row_id")
                    .cloned()
                    .collect::<Vec<_>>(),
            ));
            return Ok(Some(QueryExecutionResult {
                query_id: admission.query_id.clone(),
                coordinator_node_id: admission.coordinator_node_id.clone(),
                session: request.session.clone(),
                schema: user_schema,
                batches: vec![],
                message: format!(
                    "Query executed successfully using index '{}'. 0 row(s) returned.",
                    index.name
                ),
                outcome: StatementOutcome::Rows,
                execution_time_ms: started.elapsed().as_millis(),
            }));
        }

        // Use a clean DataFusion SessionContext to filter by _row_id
        let context = DfSessionContext::new();

        let storage_path = relation.storage_path.as_ref().ok_or_else(|| anyhow::anyhow!("Missing storage path"))?;
        let full_schema = build_arrow_schema_from_catalog_columns(&relation.columns)?;
        let table_path = listing_table_url_for_storage_location(storage_path)?;
        let config = datafusion::datasource::listing::ListingTableConfig::new(table_path)
            .with_listing_options(datafusion::datasource::listing::ListingOptions::new(Arc::new(datafusion::datasource::file_format::parquet::ParquetFormat::default())))
            .with_schema(full_schema);
        let table = datafusion::datasource::listing::ListingTable::try_new(config)?;
        context.register_table("indexed_table", Arc::new(table))?;

        let projection_sql = match &statement.projection {
            Some(cols) => cols
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", "),
            None => relation
                .columns
                .iter()
                .filter(|c| c.name != "_row_id")
                .map(|c| format!("\"{}\"", c.name))
                .collect::<Vec<_>>()
                .join(", "),
        };

        let row_ids_literal = row_ids
            .iter()
            .map(|id| format!("'{}'", id))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {} FROM indexed_table WHERE \"_row_id\" IN ({})",
            projection_sql, row_ids_literal
        );

        let df = context.sql(&sql).await.map_err(sanitize_error)?;
        let schema = df.schema().as_arrow().clone().into();
        let batches = df.collect().await?;
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        Ok(Some(QueryExecutionResult {
            query_id: admission.query_id.clone(),
            coordinator_node_id: admission.coordinator_node_id.clone(),
            session: request.session.clone(),
            schema,
            batches,
            message: format!(
                "Query executed successfully using index '{}'. {row_count} row(s) returned.",
                index.name
            ),
            outcome: StatementOutcome::Rows,
            execution_time_ms: started.elapsed().as_millis(),
        }))
    }

    async fn ensure_unique_indexes_after_batch_append(
        &self,
        session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
        batch: &RecordBatch,
    ) -> Result<()> {
        if !relation
            .indexes
            .iter()
            .any(|index| index.is_unique || index.is_primary)
        {
            return Ok(());
        }

        let mut rows = self.collect_table_rows(session, relation).await?;
        rows.extend(record_batch_rows(batch)?);

        for index in &relation.indexes {
            if index.is_unique || index.is_primary {
                validate_unique_index_rows(relation, index, &rows)?;
            }
        }

        Ok(())
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

        if let Some(statement) = parse_indexed_select_statement(&request.sql)? {
            if let Some(result) = self
                .try_execute_indexed_select(request, statement, &admission, started)
                .await?
            {
                return Ok(result);
            }
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

        if let Some(statement) = parse_indexed_select_statement(&request.sql)? {
            if let Some(execution) = self
                .try_execute_indexed_select(request, statement, &admission, started)
                .await?
            {
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
        let storage_path = local_managed_storage_path(storage_path)?;
        let relation_lock = self.relation_lock(&relation).await;
        let _write_guard = relation_lock.write().await;

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
        let (inserted_row_count, prepared_batches) =
            prepare_dataframe_batches_for_storage(projected_dataframe).await?;
        let mut projected_rows = Vec::new();
        for batch in &prepared_batches {
            projected_rows.extend(record_batch_rows(batch)?);
        }
        if !relation.indexes.is_empty() {
            let mut existing_rows = self.collect_table_rows(&request.session, &relation).await?;
            existing_rows.extend(projected_rows.clone());
            self.validate_unique_indexes_for_rows(&relation, &existing_rows)?;
        }

        if !storage_path.exists() {
            fs::create_dir_all(&storage_path)?;
        }
        for batch in prepared_batches {
            append_record_batch_to_table_snapshot(batch, &storage_path).await?;
        }
        self.refresh_index_snapshots_after_mutation(&request.session, &relation)
            .await;

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
            MetadataStatement::CreateIndex {
                ref database,
                ref schema,
                ref table,
                ref name,
                ref columns,
                unique,
            } => {
                let preview_relation = self
                    .control_plane
                    .preview_create_index(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        table,
                        name,
                        columns,
                        unique,
                    )
                    .await?;
                let relation_lock = self.relation_lock(&preview_relation).await;
                let _write_guard = relation_lock.write().await;
                let snapshot = self
                    .build_index_snapshot_for_relation(&request.session, &preview_relation, name)
                    .await?;
                write_index_snapshot(&preview_relation, &snapshot)?;

                let (message, _new_session) = match self
                    .control_plane
                    .execute_metadata_statement(&request.session, &statement)
                    .await
                {
                    Ok(result) => result,
                    Err(error) => {
                        let _ = remove_index_snapshot(&preview_relation, name);
                        return Err(error);
                    }
                };

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    message,
                    command_outcome("CREATE INDEX", 0),
                    request.session.clone(),
                )
            }
            MetadataStatement::AlterIndex {
                ref database,
                ref schema,
                ref name,
                ref operation,
            } => {
                let relation = self
                    .control_plane
                    .index_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        name,
                    )
                    .await?;
                let relation_lock = self.relation_lock(&relation).await;
                let _write_guard = relation_lock.write().await;

                let new_name = match operation {
                    AlterObjectOperation::Rename { new_name } => {
                        new_name.clone()
                    }
                    _ => anyhow::bail!("Unsupported index operation"),
                };
                let mut preview_relation = relation.clone();
                let index = preview_relation
                    .indexes
                    .iter_mut()
                    .find(|index| index.name == *name)
                    .ok_or_else(|| anyhow::anyhow!("Index '{}' not found", name))?;
                index.name = new_name.clone();
                let snapshot = self
                    .build_index_snapshot_for_relation(
                        &request.session,
                        &preview_relation,
                        &new_name,
                    )
                    .await?;
                write_index_snapshot(&preview_relation, &snapshot)?;

                let (message, _new_session) = match self
                    .control_plane
                    .execute_metadata_statement(&request.session, &statement)
                    .await
                {
                    Ok(result) => result,
                    Err(error) => {
                        let _ = remove_index_snapshot(&preview_relation, &new_name);
                        return Err(error);
                    }
                };
                let _ = remove_index_snapshot(&relation, name);

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    message,
                    command_outcome("ALTER INDEX", 0),
                    request.session.clone(),
                )
            }
            MetadataStatement::DropIndex {
                ref database,
                ref schema,
                ref name,
                if_exists,
                cascade: _,
            } => {
                let relation = match self
                    .control_plane
                    .index_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        name,
                    )
                    .await
                {
                    Ok(relation) => Some(relation),
                    Err(_) if if_exists => None,
                    Err(error) => return Err(error),
                };
                if let Some(relation) = &relation {
                    let relation_lock = self.relation_lock(relation).await;
                    let _write_guard = relation_lock.write().await;

                    let (message, _new_session) = self
                        .control_plane
                        .execute_metadata_statement(&request.session, &statement)
                        .await?;

                    let _ = remove_index_snapshot(relation, name);

                    (
                        Arc::new(Schema::empty()),
                        Vec::new(),
                        message,
                        command_outcome("DROP INDEX", 0),
                        request.session.clone(),
                    )
                } else {
                    let (message, _new_session) = self
                        .control_plane
                        .execute_metadata_statement(&request.session, &statement)
                        .await?;
                    (
                        Arc::new(Schema::empty()),
                        Vec::new(),
                        message,
                        command_outcome("DROP INDEX", 0),
                        request.session.clone(),
                    )
                }
            }
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
                let context = DfSessionContext::new();
                let table_path = datafusion::datasource::listing::ListingTableUrl::parse(&location)?;
                let config = datafusion::datasource::listing::ListingTableConfig::new(table_path)
                    .with_listing_options(datafusion::datasource::listing::ListingOptions::new(Arc::new(datafusion::datasource::file_format::parquet::ParquetFormat::default())))
                    .infer_schema(&context.state()).await?;
                let arrow_schema = config.file_schema.clone().ok_or_else(|| anyhow::anyhow!("Failed to infer schema for external table"))?;
                let columns = catalog_columns_from_schema(&arrow_schema);

                let message = self
                    .control_plane
                    .register_external_table(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                        &location,
                        format,
                        columns,
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
                let storage_location = self
                    .control_plane
                    .managed_table_storage_location(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;
                let storage_path = local_managed_storage_path(&storage_location.to_string_lossy())?;

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
                let row_count = write_dataframe_to_table_snapshot(dataframe, &storage_path).await?;

                let created_message = self
                    .control_plane
                    .register_managed_table(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                        &storage_location,
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
                let storage_location = self
                    .control_plane
                    .managed_table_storage_location(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;
                let storage_path = local_managed_storage_path(&storage_location.to_string_lossy())?;
                let arrow_schema = build_arrow_schema_from_definitions(&columns, false)?;

                persist_empty_table_snapshot(&storage_path, &arrow_schema)?;

                let created_message = self
                    .control_plane
                    .register_managed_table(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                        &storage_location,
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
                let relation = self
                    .control_plane
                    .table_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;
                self.refresh_index_snapshots_after_mutation(&request.session, &relation)
                    .await;

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
                let storage_path = local_managed_storage_path(storage_path)?;
                let relation_lock = self.relation_lock(&relation).await;
                let _write_guard = relation_lock.write().await;

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
                self.ensure_unique_indexes_after_batch_append(&request.session, &relation, &batch)
                    .await?;

                if !storage_path.exists() {
                    fs::create_dir_all(&storage_path)?;
                }
                append_record_batch_to_table_snapshot(batch.clone(), &storage_path).await?;
                self.refresh_index_snapshots_after_mutation(&request.session, &relation)
                    .await;

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
                let storage_path = local_managed_storage_path(storage_path)?;
                let relation_lock = self.relation_lock(&relation).await;
                let _write_guard = relation_lock.write().await;

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
                let updated_batches = updated_dataframe.clone().collect().await?;
                let mut updated_rows = Vec::new();
                for batch in &updated_batches {
                    updated_rows.extend(record_batch_rows(batch)?);
                }
                self.validate_unique_indexes_for_rows(&relation, &updated_rows)?;

                let row_count =
                    write_dataframe_to_table_snapshot(updated_dataframe, &storage_path).await?;
                self.refresh_index_snapshots_after_mutation(&request.session, &relation)
                    .await;

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
                let storage_path = local_managed_storage_path(storage_path)?;
                let relation_lock = self.relation_lock(&relation).await;
                let _write_guard = relation_lock.write().await;

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
                    write_dataframe_to_table_snapshot(remaining_dataframe, &storage_path).await?;
                self.refresh_index_snapshots_after_mutation(&request.session, &relation)
                    .await;

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
                let storage_path = local_managed_storage_path(storage_path)?;
                let relation_lock = self.relation_lock(&relation).await;
                let _write_guard = relation_lock.write().await;

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

                persist_empty_table_snapshot(&storage_path, &arrow_schema)?;
                self.refresh_index_snapshots_after_mutation(&request.session, &relation)
                    .await;

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
                    AlterTableOperation::AddConstraint { constraint } => {
                        let relation_lock = self.relation_lock(&relation).await;
                        let _write_guard = relation_lock.write().await;
                        let catalog_constraints = catalog_constraints_from_definitions(
                            &name,
                            database.as_deref(),
                            schema.as_deref(),
                            &request.session,
                            &[constraint],
                        )?;

                        let mut message = String::new();
                        for cat_con in catalog_constraints {
                            let preview_relation = self
                                .control_plane
                                .preview_add_constraint(
                                    &request.session,
                                    database.as_deref(),
                                    schema.as_deref(),
                                    &name,
                                    &cat_con,
                                )
                                .await?;
                            let staged_index_names = preview_relation
                                .indexes
                                .iter()
                                .filter(|index| {
                                    relation
                                        .indexes
                                        .iter()
                                        .all(|existing| existing.name != index.name)
                                })
                                .map(|index| index.name.clone())
                                .collect::<Vec<_>>();

                            for index_name in &staged_index_names {
                                let snapshot = self
                                    .build_index_snapshot_for_relation(
                                        &request.session,
                                        &preview_relation,
                                        index_name,
                                    )
                                    .await?;
                                write_index_snapshot(&preview_relation, &snapshot)?;
                            }

                            message = match self
                                .control_plane
                                .add_constraint(
                                    &request.session,
                                    database.as_deref(),
                                    schema.as_deref(),
                                    &name,
                                    cat_con.clone(),
                                )
                                .await
                            {
                                Ok(message) => message,
                                Err(error) => {
                                    for index_name in &staged_index_names {
                                        let _ = remove_index_snapshot(&preview_relation, index_name);
                                    }
                                    return Err(error);
                                }
                            };
                        }

                        (
                            Arc::new(Schema::empty()),
                            Vec::new(),
                            message,
                            command_outcome("ALTER TABLE", 0),
                            request.session.clone(),
                        )
                    }
                    AlterTableOperation::RenameTable { new_name } => {
                        let relation_lock = self.relation_lock(&relation).await;
                        let _write_guard = relation_lock.write().await;
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
                            let old_path = local_managed_storage_path(storage_path_str)?;
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
                                        &storage_location_from_local_path(
                                            &new_path,
                                            Some(storage_path_str),
                                        ),
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
                    AlterTableOperation::DropColumn {
                        column_name,
                        if_exists,
                        cascade: _,
                    } => {
                        let message = self
                            .control_plane
                            .drop_column(
                                &request.session,
                                database.as_deref(),
                                schema.as_deref(),
                                &name,
                                &column_name,
                                if_exists,
                            )
                            .await?;

                        (
                            Arc::new(Schema::empty()),
                            Vec::new(),
                            message,
                            command_outcome("ALTER TABLE", 0),
                            request.session.clone(),
                        )
                    }
                    AlterTableOperation::RenameColumn { old_name, new_name } => {
                        let message = self
                            .control_plane
                            .rename_column(
                                &request.session,
                                database.as_deref(),
                                schema.as_deref(),
                                &name,
                                &old_name,
                                &new_name,
                            )
                            .await?;

                        (
                            Arc::new(Schema::empty()),
                            Vec::new(),
                            message,
                            command_outcome("ALTER TABLE", 0),
                            request.session.clone(),
                        )
                    }
                    AlterTableOperation::DropConstraint {
                        name: constraint_name,
                        if_exists,
                        cascade,
                    } => {
                        // 1. Identify sidecar indexes that will be dropped
                        let preview_result = self
                            .control_plane
                            .preview_drop_constraint(
                                &request.session,
                                database.as_deref(),
                                schema.as_deref(),
                                &name,
                                &constraint_name,
                                cascade,
                            )
                            .await;

                        match preview_result {
                            Ok(preview_relation) => {
                                let dropped_index_names = relation
                                    .indexes
                                    .iter()
                                    .filter(|existing| {
                                        !preview_relation
                                            .indexes
                                            .iter()
                                            .any(|preview| preview.name == existing.name)
                                    })
                                    .map(|i| i.name.clone())
                                    .collect::<Vec<_>>();

                                // 2. Perform the drop in catalog
                                let message = self
                                    .control_plane
                                    .drop_constraint(
                                        &request.session,
                                        database.as_deref(),
                                        schema.as_deref(),
                                        &name,
                                        &constraint_name,
                                        if_exists,
                                        cascade,
                                    )
                                    .await?;

                                // 3. Physically remove dropped index snapshots
                                for index_name in dropped_index_names {
                                    let _ = remove_index_snapshot(&relation, &index_name);
                                }

                                (
                                    Arc::new(Schema::empty()),
                                    Vec::new(),
                                    message,
                                    command_outcome("ALTER TABLE", 0),
                                    request.session.clone(),
                                )
                            }
                            Err(e) => {
                                if if_exists && e.to_string().contains("not found") {
                                    (
                                        Arc::new(Schema::empty()),
                                        Vec::new(),
                                        format!(
                                            "Constraint '{}' does not exist, skipping.",
                                            constraint_name
                                        ),
                                        command_outcome("ALTER TABLE", 0),
                                        request.session.clone(),
                                    )
                                } else {
                                    return Err(e);
                                }
                            }
                        }
                    }
                    AlterTableOperation::AlterColumn {
                        column_name,
                        operation,
                    } => {
                        let message = self
                            .control_plane
                            .alter_column(
                                &request.session,
                                database.as_deref(),
                                schema.as_deref(),
                                &name,
                                &column_name,
                                operation,
                            )
                            .await?;

                        (
                            Arc::new(Schema::empty()),
                            Vec::new(),
                            message,
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
                        let old_path = local_managed_storage_path(storage_path_str)?;
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
                                    &storage_location_from_local_path(
                                        &new_path,
                                        Some(storage_path_str),
                                    ),
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
                            let old_path = local_managed_storage_path(storage_path_str)?;
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
                                        &storage_location_from_local_path(
                                            &new_path,
                                            Some(storage_path_str),
                                        ),
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
                    .filter(|col| col.name != "_row_id")
                    .map(|col| {
                        vec![
                            col.name,
                            col.data_type,
                            if col.nullable { "YES".to_string() } else { "NO".to_string() },
                        ]
                    })
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(
                    &["column_name", "data_type", "is_nullable"],
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
                    let relation_lock = self.relation_lock(&rel).await;
                    let _write_guard = relation_lock.write().await;
                    // For managed tables, delete the storage directory
                    if rel.external_format.is_none() {
                        if let Some(path_str) = &rel.storage_path {
                            let path = local_managed_storage_path(path_str)?;
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

    async fn collect_table_rows(
        &self,
        _session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
    ) -> Result<Vec<Vec<String>>> {
        let context = DfSessionContext::new();
        let storage_path = relation.storage_path.as_ref().ok_or_else(|| anyhow::anyhow!("Missing storage path"))?;
        let full_schema = build_arrow_schema_from_catalog_columns(&relation.columns)?;
        let table_path = listing_table_url_for_storage_location(storage_path)?;
        let config = datafusion::datasource::listing::ListingTableConfig::new(table_path)
            .with_listing_options(datafusion::datasource::listing::ListingOptions::new(Arc::new(datafusion::datasource::file_format::parquet::ParquetFormat::default())))
            .with_schema(full_schema);
        let table = datafusion::datasource::listing::ListingTable::try_new(config)?;
        context.register_table("target_table", Arc::new(table))?;

        let projection = relation
            .columns
            .iter()
            .map(|c| format!("\"{}\"", c.name))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT {} FROM target_table", projection);

        let dataframe = context.sql(&sql).await.map_err(sanitize_error)?;
        let batches = dataframe.collect().await.map_err(sanitize_error)?;
        let mut rows = Vec::new();
        for batch in &batches {
            rows.extend(record_batch_rows(batch)?);
        }
        Ok(rows)
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
        let dt = catalog_data_type(&col.data_type);
        fields.push(Field::new(&col.name, dt, col.nullable));
    }

    if !fields.iter().any(|f| f.name() == "_row_id") {
        fields.push(Field::new("_row_id", DataType::Utf8, false));
    }

    Ok(Arc::new(Schema::new(fields)))
}

fn build_arrow_schema_from_catalog_columns(columns: &[CatalogColumn]) -> Result<SchemaRef> {
    let definitions = columns
        .iter()
        .map(|c| TableColumnDefinition {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            nullable: c.nullable,
            default_value: c.default_value.clone(),
        })
        .collect::<Vec<_>>();
    build_arrow_schema_from_definitions(&definitions, false)
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

        if field.name() == "_row_id" {
            for _ in 0..rows.len() {
                values.push(Some(uuid::Uuid::now_v7().to_string()));
            }
        } else {
            let target_idx = if let Some(ref names) = target_columns {
                names.iter().position(|n| n == field.name())
            } else {
                Some(i)
            };

            for row in rows {
                if let Some(idx) = target_idx {
                    if idx < row.len() {
                        if row[idx].trim().eq_ignore_ascii_case("NULL") {
                            values.push(None);
                        } else {
                            values.push(Some(normalize_insert_value(
                                &row[idx],
                                field.data_type(),
                            )));
                        }
                    } else {
                        values.push(None);
                    }
                } else {
                    values.push(None);
                }
            }
        }

        let array: ArrayRef = Arc::new(StringArray::from(values));
        // Cast to actual type
        let casted = datafusion::arrow::compute::cast(&array, field.data_type())?;
        columns.push(casted);
    }
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

fn normalize_insert_value(raw: &str, _data_type: &DataType) -> String {
    let trimmed = raw.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        return trimmed[1..trimmed.len() - 1].replace("''", "'");
    }
    trimmed.to_string()
}

async fn write_dataframe_to_table_snapshot(
    df: datafusion::dataframe::DataFrame,
    path: &Path,
) -> Result<usize> {
    if !path.exists() {
        fs::create_dir_all(path)?;
    }
    clear_table_snapshot_files(path)?;
    let (row_count, batches) = prepare_dataframe_batches_for_storage(df).await?;
    for batch in batches {
        append_record_batch_to_table_snapshot(batch, path).await?;
    }
    Ok(row_count)
}

async fn prepare_dataframe_batches_for_storage(
    df: datafusion::dataframe::DataFrame,
) -> Result<(usize, Vec<RecordBatch>)> {
    let schema = df.schema();
    let has_row_id = schema.fields().iter().any(|f| f.name() == "_row_id");

    if has_row_id {
        let batches = df.collect().await?;
        let mut row_count = 0;
        let mut new_batches = Vec::new();

        for batch in batches {
            let num_rows = batch.num_rows();
            row_count += num_rows;
            let mut columns = batch.columns().to_vec();

            let row_id_col_idx = batch.schema().fields().iter().position(|f| f.name() == "_row_id").unwrap();
            let row_id_array = batch.column(row_id_col_idx);

            // Check if it's the AUTO_UUID placeholder
            let mut needs_replace = false;
            if let Some(strings) = row_id_array.as_any().downcast_ref::<StringArray>() {
                if strings.len() > 0 && strings.value(0) == "AUTO_UUID" {
                    needs_replace = true;
                }
            }

            if needs_replace {
                let row_ids: Vec<String> = (0..num_rows)
                    .map(|_| uuid::Uuid::now_v7().to_string())
                    .collect();
                columns[row_id_col_idx] = Arc::new(StringArray::from(row_ids));
                new_batches.push(RecordBatch::try_new(batch.schema(), columns)?);
            } else {
                new_batches.push(batch);
            }
        }

        Ok((row_count, new_batches))
    } else {
        // We need to inject _row_id
        let batches = df.collect().await?;
        let mut row_count = 0;
        let mut new_batches = Vec::new();

        for batch in batches {
            let num_rows = batch.num_rows();
            row_count += num_rows;
            let mut columns = batch.columns().to_vec();
            let row_ids: Vec<String> = (0..num_rows)
                .map(|_| uuid::Uuid::now_v7().to_string())
                .collect();
            columns.push(Arc::new(StringArray::from(row_ids)));

            let mut fields = batch.schema().fields().to_vec();
            fields.push(Arc::new(Field::new("_row_id", DataType::Utf8, false)));
            let new_schema = Arc::new(Schema::new(fields));

            new_batches.push(RecordBatch::try_new(new_schema, columns)?);
        }

        Ok((row_count, new_batches))
    }
}

fn persist_empty_table_snapshot(path: &Path, schema: &SchemaRef) -> Result<()> {
    if !path.exists() {
        fs::create_dir_all(path)?;
    }
    clear_table_snapshot_files(path)?;
    let file_path = path.join("empty.parquet");
    let file = fs::File::create(file_path)?;
    let writer = ArrowWriter::try_new(file, Arc::clone(schema), None)?;
    writer.close()?;
    Ok(())
}

fn clear_table_snapshot_files(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();
        if entry_path.is_file() && entry_path.extension().and_then(|value| value.to_str()) == Some("parquet") {
            fs::remove_file(entry_path)?;
        }
    }
    Ok(())
}

async fn append_record_batch_to_table_snapshot(batch: RecordBatch, path: &Path) -> Result<()> {
    let file_path = path.join(format!("{}.parquet", uuid::Uuid::now_v7()));
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

fn parse_indexed_select_statement(sql: &str) -> Result<Option<IndexedSelectStatement>> {
    let dialect = PostgreSqlDialect {};
    let statements = match Parser::parse_sql(&dialect, sql) {
        Ok(statements) => statements,
        Err(_) => return Ok(None),
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return Ok(None);
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
        || select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !matches!(
            &select.group_by,
            sqlparser::ast::GroupByExpr::Expressions(expressions, modifiers)
                if expressions.is_empty() && modifiers.is_empty()
        )
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
    {
        return Ok(None);
    }
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Ok(None);
    }
    let TableFactor::Table { name, .. } = &select.from[0].relation else {
        return Ok(None);
    };
    let idents = name
        .0
        .iter()
        .map(|ident| ident.to_string())
        .collect::<Vec<_>>();
    let (database, schema, table) = match idents.as_slice() {
        [table] => (None, None, table.clone()),
        [schema, table] => (None, Some(schema.clone()), table.clone()),
        [database, schema, table] => (Some(database.clone()), Some(schema.clone()), table.clone()),
        _ => return Ok(None),
    };

    let projection = select_projection_columns(&select.projection)?;
    let Some(selection) = &select.selection else {
        return Ok(None);
    };

    let mut predicates = BTreeMap::new();
    if !extract_index_predicates(selection, &mut predicates)? || predicates.is_empty() {
        return Ok(None);
    }

    Ok(Some(IndexedSelectStatement {
        database,
        schema,
        table,
        projection,
        predicates,
    }))
}

fn select_projection_columns(projection: &[SelectItem]) -> Result<Option<Vec<String>>> {
    let mut columns = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard(_) => return Ok(None),
            SelectItem::UnnamedExpr(Expr::Identifier(identifier)) => {
                columns.push(identifier.to_string());
            }
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                columns.push(parts.last().unwrap().to_string());
            }
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(identifier),
                alias,
            } if identifier == alias => {
                columns.push(identifier.to_string());
            }
            SelectItem::ExprWithAlias {
                expr: Expr::CompoundIdentifier(parts),
                alias,
            } if parts.last().unwrap() == alias => {
                columns.push(alias.to_string());
            }
            _ => return Ok(None),
        }
    }
    Ok(Some(columns))
}

fn extract_index_predicates(
    expr: &Expr,
    predicates: &mut BTreeMap<String, IndexPredicate>,
) -> Result<bool> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(
                extract_index_predicates(left, predicates)?
                    && extract_index_predicates(right, predicates)?,
            ),
            BinaryOperator::Eq => Ok(
                store_eq_predicate(predicates, left, right)
                    .or_else(|| store_eq_predicate(predicates, right, left))
                    .is_some(),
            ),
            BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq => Ok(
                store_range_binary_predicate(predicates, left, op, right, true)
                    .or_else(|| {
                        store_range_binary_predicate(predicates, right, op, left, false)
                    })
                    .is_some(),
            ),
            _ => Ok(false),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } if !negated => Ok(store_in_predicate(predicates, expr, list).is_ok()),
        Expr::Nested(inner) => extract_index_predicates(inner, predicates),
        _ => Ok(false),
    }
}

fn literal_index_value(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(value) => Some(normalize_index_literal(&value.to_string())),
        Expr::UnaryOp { op, expr } => {
            let value = literal_index_value(expr)?;
            match op {
                sqlparser::ast::UnaryOperator::Minus => Some(format!("-{value}")),
                sqlparser::ast::UnaryOperator::Plus => Some(value),
                _ => None,
            }
        }
        _ => None,
    }
}

fn normalize_index_literal(value: &str) -> String {
    value.trim().trim_matches('\'').replace("''", "'")
}

fn index_predicate_column(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(identifier) => Some(identifier.to_string()),
        Expr::CompoundIdentifier(parts) => parts.last().map(ToString::to_string),
        _ => None,
    }
}

fn store_eq_predicate(
    predicates: &mut BTreeMap<String, IndexPredicate>,
    column_expr: &Expr,
    value_expr: &Expr,
) -> Option<()> {
    let column = index_predicate_column(column_expr)?;
    let value = literal_index_value(value_expr)?;
    if predicates.contains_key(&column) {
        return None;
    }
    predicates.insert(column, IndexPredicate::Eq(value));
    Some(())
}

fn store_in_predicate(
    predicates: &mut BTreeMap<String, IndexPredicate>,
    expr: &Expr,
    list: &[Expr],
) -> Result<()> {
    let Some(column) = index_predicate_column(expr) else {
        anyhow::bail!("unsupported index IN predicate");
    };
    if predicates.contains_key(&column) {
        anyhow::bail!("duplicate predicates on indexed column '{}'", column);
    }
    let mut values = Vec::with_capacity(list.len());
    for item in list {
        let Some(value) = literal_index_value(item) else {
            anyhow::bail!("unsupported index IN predicate");
        };
        values.push(value);
    }
    predicates.insert(column, IndexPredicate::In(values));
    Ok(())
}

fn store_range_binary_predicate(
    predicates: &mut BTreeMap<String, IndexPredicate>,
    column_expr: &Expr,
    operator: &BinaryOperator,
    value_expr: &Expr,
    column_on_left: bool,
) -> Option<()> {
    let column = index_predicate_column(column_expr)?;
    let value = literal_index_value(value_expr)?;
    let mut lower = None;
    let mut upper = None;
    match (operator, column_on_left) {
        (BinaryOperator::Gt, true) | (BinaryOperator::Lt, false) => {
            lower = Some((value, false))
        }
        (BinaryOperator::GtEq, true) | (BinaryOperator::LtEq, false) => {
            lower = Some((value, true))
        }
        (BinaryOperator::Lt, true) | (BinaryOperator::Gt, false) => {
            upper = Some((value, false))
        }
        (BinaryOperator::LtEq, true) | (BinaryOperator::GtEq, false) => {
            upper = Some((value, true))
        }
        _ => return None,
    }

    match predicates.get_mut(&column) {
        None => {
            predicates.insert(column, IndexPredicate::Range { lower, upper });
            Some(())
        }
        Some(IndexPredicate::Range {
            lower: existing_lower,
            upper: existing_upper,
        }) => {
            if let Some(bound) = lower {
                if existing_lower.is_some() {
                    return None;
                }
                *existing_lower = Some(bound);
            }
            if let Some(bound) = upper {
                if existing_upper.is_some() {
                    return None;
                }
                *existing_upper = Some(bound);
            }
            Some(())
        }
        Some(_) => None,
    }
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
    let Some(select_idx) = upper.find("SELECT ") else {
        return Ok(None);
    };
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

fn best_index_match(
    relation: &analyticsdb_control::CatalogRelation,
    statement: &IndexedSelectStatement,
) -> Result<Option<(analyticsdb_control::CatalogIndex, Vec<String>)>> {
    let mut best_match: Option<(analyticsdb_control::CatalogIndex, Vec<String>, usize, bool)> = None;

    for index in &relation.indexes {
        let Some(snapshot) = read_index_snapshot(relation, &index.name)? else {
            continue;
        };
        let Some((score, has_range, row_ids)) =
            candidate_row_ids_from_snapshot(relation, index, &snapshot, &statement.predicates)?
        else {
            continue;
        };

        let replace = match &best_match {
            None => true,
            Some((best_index, best_row_ids, best_score, best_has_range)) => {
                score > *best_score
                    || (score == *best_score && !has_range && *best_has_range)
                    || (score == *best_score
                        && has_range == *best_has_range
                        && index.is_unique
                        && !best_index.is_unique)
                    || (score == *best_score
                        && has_range == *best_has_range
                        && index.is_unique == best_index.is_unique
                        && row_ids.len() < best_row_ids.len())
            }
        };

        if replace {
            best_match = Some((index.clone(), row_ids, score, has_range));
        }
    }

    Ok(best_match.map(|(index, row_ids, _, _)| (index, row_ids)))
}

fn candidate_row_ids_from_snapshot(
    relation: &analyticsdb_control::CatalogRelation,
    index: &analyticsdb_control::CatalogIndex,
    snapshot: &IndexSnapshot,
    predicates: &BTreeMap<String, IndexPredicate>,
) -> Result<Option<(usize, bool, Vec<String>)>> {
    let mut matched_prefix_len = 0usize;
    let mut has_range = false;
    let mut covered_predicate_columns = 0usize;

    for column in &index.columns {
        let Some(predicate) = find_index_predicate(predicates, column) else {
            break;
        };
        covered_predicate_columns += 1;
        match predicate {
            IndexPredicate::Eq(_) | IndexPredicate::In(_) => {
                matched_prefix_len += 1;
            }
            IndexPredicate::Range { .. } => {
                has_range = true;
                break;
            }
        }
    }

    if matched_prefix_len == 0 && !has_range {
        return Ok(None);
    }
    if covered_predicate_columns != predicates.len() {
        return Ok(None);
    }

    let column_types = index
        .columns
        .iter()
        .map(|column| {
            relation
                .columns
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(column))
                .map(|catalog_column| catalog_data_type(&catalog_column.data_type))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Column '{}' not found in relation '{}.{}.{}'",
                        column,
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut row_ids = Vec::new();
    for (composite_key, entry_row_ids) in &snapshot.entries {
        let values = composite_key.split('\u{1f}').collect::<Vec<_>>();
        let mut matched = true;
        for (position, index_column) in index.columns.iter().enumerate() {
            let Some(predicate) = find_index_predicate(predicates, index_column) else {
                break;
            };
            let value = values.get(position).copied().unwrap_or_default();
            if !index_value_satisfies_predicate(value, predicate, &column_types[position])? {
                matched = false;
                break;
            }
            if matches!(predicate, IndexPredicate::Range { .. }) {
                break;
            }
        }

        if matched {
            row_ids.extend(entry_row_ids.iter().cloned());
        }
    }

    Ok(Some((matched_prefix_len, has_range, row_ids)))
}

fn find_index_predicate<'a>(
    predicates: &'a BTreeMap<String, IndexPredicate>,
    column: &str,
) -> Option<&'a IndexPredicate> {
    predicates
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(column))
        .map(|(_, predicate)| predicate)
}

fn index_value_satisfies_predicate(
    candidate: &str,
    predicate: &IndexPredicate,
    data_type: &DataType,
) -> Result<bool> {
    match predicate {
        IndexPredicate::Eq(value) => Ok(compare_index_values(candidate, value, data_type)? == Ordering::Equal),
        IndexPredicate::In(values) => Ok(values.iter().any(|value| {
            compare_index_values(candidate, value, data_type)
                .map(|ordering| ordering == Ordering::Equal)
                .unwrap_or(false)
        })),
        IndexPredicate::Range { lower, upper } => {
            if let Some((lower_value, inclusive)) = lower {
                let ordering = compare_index_values(candidate, lower_value, data_type)?;
                if ordering == Ordering::Less || (!inclusive && ordering == Ordering::Equal) {
                    return Ok(false);
                }
            }
            if let Some((upper_value, inclusive)) = upper {
                let ordering = compare_index_values(candidate, upper_value, data_type)?;
                if ordering == Ordering::Greater || (!inclusive && ordering == Ordering::Equal) {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}

fn catalog_data_type(data_type: &str) -> DataType {
    match data_type.to_ascii_uppercase().as_str() {
        "INT" | "INTEGER" | "INT4" | "INT32" => DataType::Int32,
        "BIGINT" | "INT8" | "INT64" => DataType::Int64,
        "TEXT" | "VARCHAR" | "STRING" | "UTF8" => DataType::Utf8,
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "FLOAT4" | "REAL" | "FLOAT32" => DataType::Float32,
        "FLOAT8" | "DOUBLE PRECISION" | "FLOAT64" => DataType::Float64,
        _ => DataType::Utf8,
    }
}

fn compare_index_values(left: &str, right: &str, data_type: &DataType) -> Result<Ordering> {
    Ok(match data_type {
        DataType::Int32 => left.parse::<i32>()?.cmp(&right.parse::<i32>()?),
        DataType::Int64 => left.parse::<i64>()?.cmp(&right.parse::<i64>()?),
        DataType::Float32 => left
            .parse::<f32>()?
            .partial_cmp(&right.parse::<f32>()?)
            .ok_or_else(|| anyhow::anyhow!("cannot compare NaN float values"))?,
        DataType::Float64 => left
            .parse::<f64>()?
            .partial_cmp(&right.parse::<f64>()?)
            .ok_or_else(|| anyhow::anyhow!("cannot compare NaN float values"))?,
        DataType::Boolean => left.parse::<bool>()?.cmp(&right.parse::<bool>()?),
        _ => left.cmp(right),
    })
}

fn build_index_snapshot(
    relation: &analyticsdb_control::CatalogRelation,
    index: &analyticsdb_control::CatalogIndex,
    rows: &[Vec<String>],
) -> Result<IndexSnapshot> {
    validate_unique_index_rows(relation, index, rows)?;
    let index_positions = index_column_positions(relation, &index.columns)?;
    let row_id_pos = relation
        .columns
        .iter()
        .position(|c| c.name == "_row_id")
        .ok_or_else(|| anyhow::anyhow!("_row_id column missing from relation"))?;

    let mut entries: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in rows {
        let key = index_positions
            .iter()
            .map(|pos| row.get(*pos).cloned().unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\u{1f}");
        let row_id = row
            .get(row_id_pos)
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        entries.entry(key).or_default().push(row_id);
    }
    Ok(IndexSnapshot {
        database: relation.database.clone(),
        schema: relation.schema.clone(),
        table: relation.name.clone(),
        index: index.name.clone(),
        columns: index.columns.clone(),
        unique: index.is_unique,
        primary: index.is_primary,
        entries,
    })
}

fn validate_unique_index_rows(
    relation: &analyticsdb_control::CatalogRelation,
    index: &analyticsdb_control::CatalogIndex,
    rows: &[Vec<String>],
) -> Result<()> {
    if !index.is_unique && !index.is_primary {
        return Ok(());
    }

    let index_positions = index_column_positions(relation, &index.columns)?;
    let mut seen = HashMap::new();
    for row in rows {
        let key = index_positions
            .iter()
            .map(|pos| row.get(*pos).cloned().unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\u{1f}");
        *seen.entry(key).or_insert(0) += 1;
    }

    if let Some((key, _)) = seen.into_iter().find(|(_, count)| *count > 1) {
        anyhow::bail!(
            "Unique index '{}' on '{}.{}.{}' would contain duplicate key '{}'",
            index.name,
            relation.database,
            relation.schema,
            relation.name,
            key.replace('\u{1f}', ",")
        );
    }
    Ok(())
}

fn local_managed_storage_path(storage_location: &str) -> Result<PathBuf> {
    if let Some(path) = storage_location.strip_prefix("file://") {
        return Ok(PathBuf::from(path));
    }
    if storage_location.contains("://") {
        anyhow::bail!(
            "Managed-table storage location '{}' is not writable through the local filesystem prototype",
            storage_location
        );
    }
    Ok(PathBuf::from(storage_location))
}

fn storage_location_from_local_path(path: &Path, original_location: Option<&str>) -> String {
    if original_location.is_some_and(|location| location.starts_with("file://")) {
        format!("file://{}", path.to_string_lossy())
    } else {
        path.to_string_lossy().to_string()
    }
}

fn listing_table_url_for_storage_location(
    storage_location: &str,
) -> Result<datafusion::datasource::listing::ListingTableUrl> {
    datafusion::datasource::listing::ListingTableUrl::parse(storage_location)
        .map_err(Into::into)
}

fn index_snapshot_root(
    relation: &analyticsdb_control::CatalogRelation,
    index_name: &str,
) -> Result<PathBuf> {
    let storage_path = relation.storage_path.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "Managed table '{}.{}.{}' is missing a storage path",
            relation.database,
            relation.schema,
            relation.name
        )
    })?;
    Ok(local_managed_storage_path(storage_path)?
        .join(".analyticsdb_indexes")
        .join(index_name))
}

fn index_snapshot_manifest_path(
    relation: &analyticsdb_control::CatalogRelation,
    index_name: &str,
) -> Result<PathBuf> {
    Ok(index_snapshot_root(relation, index_name)?.join("manifest.json"))
}

fn read_index_snapshot(
    relation: &analyticsdb_control::CatalogRelation,
    index_name: &str,
) -> Result<Option<IndexSnapshot>> {
    let root = index_snapshot_root(relation, index_name)?;
    let manifest_path = index_snapshot_manifest_path(relation, index_name)?;
    if !manifest_path.exists() {
        return Ok(None);
    }

    let manifest: IndexSnapshotManifest = serde_json::from_str(&fs::read_to_string(manifest_path)?)?;
    let snapshot_path = root.join(manifest.snapshot_object);
    if !snapshot_path.exists() {
        anyhow::bail!(
            "Published index snapshot for '{}.{}.{}' is missing its data object",
            relation.database,
            relation.schema,
            relation.name
        );
    }
    Ok(Some(serde_json::from_str(&fs::read_to_string(snapshot_path)?)?))
}

fn write_json_atomically(path: &Path, contents: &str) -> Result<()> {
    let parent = path.parent().ok_or_else(|| anyhow::anyhow!("path '{}' has no parent", path.display()))?;
    fs::create_dir_all(parent)?;
    let temp_path = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|file_name| file_name.to_str())
            .unwrap_or("index-snapshot"),
        uuid::Uuid::now_v7()
    ));
    fs::write(&temp_path, contents)?;
    fs::rename(temp_path, path)?;
    Ok(())
}

fn prune_old_index_snapshot_versions(versions_dir: &Path, current_snapshot_object: &str) -> Result<()> {
    let current_file_name = Path::new(current_snapshot_object)
        .file_name()
        .map(|value| value.to_owned());
    for entry in fs::read_dir(versions_dir)? {
        let entry = entry?;
        let entry_path = entry.path();
        if entry_path.is_file()
            && current_file_name
                .as_ref()
                .map(|name| entry_path.file_name() != Some(name.as_os_str()))
                .unwrap_or(true)
        {
            fs::remove_file(entry_path)?;
        }
    }
    Ok(())
}

fn write_index_snapshot(
    relation: &analyticsdb_control::CatalogRelation,
    snapshot: &IndexSnapshot,
) -> Result<()> {
    let root = index_snapshot_root(relation, &snapshot.index)?;
    let versions_dir = root.join("versions");
    fs::create_dir_all(&versions_dir)?;

    let version = uuid::Uuid::now_v7().to_string();
    let snapshot_object = format!("versions/{version}.json");
    let snapshot_path = root.join(&snapshot_object);
    write_json_atomically(&snapshot_path, &serde_json::to_string_pretty(snapshot)?)?;

    let manifest = IndexSnapshotManifest {
        version,
        snapshot_object: snapshot_object.clone(),
        row_count: snapshot.entries.values().map(Vec::len).sum(),
        published_at_epoch_ms: chrono::Utc::now().timestamp_millis(),
    };
    write_json_atomically(
        &index_snapshot_manifest_path(relation, &snapshot.index)?,
        &serde_json::to_string_pretty(&manifest)?,
    )?;
    prune_old_index_snapshot_versions(&versions_dir, &snapshot_object)?;
    Ok(())
}

fn remove_index_snapshot(
    relation: &analyticsdb_control::CatalogRelation,
    index_name: &str,
) -> Result<()> {
    let path = index_snapshot_root(relation, index_name)?;
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    Ok(())
}

fn index_column_positions(
    relation: &analyticsdb_control::CatalogRelation,
    columns: &[String],
) -> Result<Vec<usize>> {
    columns
        .iter()
        .map(|column| {
            relation
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(column))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Column '{}' not found in table '{}.{}.{}'",
                        column,
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })
        })
        .collect()
}

fn record_batch_rows(batch: &RecordBatch) -> Result<Vec<Vec<String>>> {
    let mut rows = Vec::new();
    for row_idx in 0..batch.num_rows() {
        let mut row = Vec::new();
        for col_idx in 0..batch.num_columns() {
            row.push(array_value_to_string(batch.column(col_idx), row_idx).unwrap_or_default());
        }
        rows.push(row);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use analyticsdb_core::{Protocol, QueryRequest, SessionContext};

    fn temp_catalog_path() -> String {
        let mut path = std::env::temp_dir();
        path.push(format!("analyticsdb-engine-test-{}.json", uuid::Uuid::now_v7()));
        path.to_string_lossy().into_owned()
    }

    fn cleanup_catalog_artifacts(catalog_path: &str) {
        let _ = std::fs::remove_file(catalog_path);
        let mut managed_dir = std::path::PathBuf::from(catalog_path);
        let stem = managed_dir
            .file_stem()
            .and_then(|value| value.to_str())
            .expect("catalog path should have a file stem")
            .to_string();
        managed_dir.set_file_name(format!("{stem}.managed"));
        let _ = std::fs::remove_dir_all(managed_dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_primary_key_inserts_keep_table_and_index_consistent() {
        let catalog_path = temp_catalog_path();
        let engine = PrototypeEngine::from_catalog_path(&catalog_path)
            .await
            .expect("engine should initialize");
        let session = SessionContext {
            protocol: Protocol::Embedded,
            ..SessionContext::default()
        };

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE customers (id BIGINT PRIMARY KEY, name TEXT)".to_string(),
                session: session.clone(),
            })
            .await
            .expect("table should be created");

        let engine_a = engine.clone();
        let engine_b = engine.clone();
        let session_a = session.clone();
        let session_b = session.clone();
        let request_a = QueryRequest {
            sql: "INSERT INTO customers VALUES (1, 'one')".to_string(),
            session: session_a,
        };
        let request_b = QueryRequest {
            sql: "INSERT INTO customers VALUES (1, 'duplicate')".to_string(),
            session: session_b,
        };

        let (insert_a, insert_b) = tokio::join!(
            engine_a.execute_query(&request_a),
            engine_b.execute_query(&request_b)
        );

        let successes = usize::from(insert_a.is_ok()) + usize::from(insert_b.is_ok());
        assert_eq!(successes, 1, "exactly one concurrent insert should succeed");
        assert!(
            insert_a
                .as_ref()
                .err()
                .or_else(|| insert_b.as_ref().err())
                .map(|error| error.to_string().contains("duplicate key"))
                .unwrap_or(true),
            "one insert should fail with duplicate-key enforcement"
        );

        let result = engine
            .execute_query(&QueryRequest {
                sql: "SELECT id, name FROM customers WHERE id = 1".to_string(),
                session,
            })
            .await
            .expect("indexed select should succeed");
        let response = result.to_query_response();

        assert_eq!(response.rows.len(), 1);
        assert!(response.message.contains("using index"));

        cleanup_catalog_artifacts(&catalog_path);
    }
}
