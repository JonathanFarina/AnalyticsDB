use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use object_store::path::Path as OPath;
use object_store::{ObjectStore, ObjectStoreExt};

use analyticsdb_control::{
    parse_metadata_statement, AlterDatabaseOperation, AlterObjectOperation, AlterTableOperation,
    CatalogColumn, CatalogRelationKind, CatalogTableConstraint, CatalogTableConstraintKind,
    ControlPlane, MetadataStatement, QueryAdmission, ReindexTarget, TableColumnDefinition,
    TableConstraintDefinition,
};
use analyticsdb_core::{QueryRequest, QueryResponse, SessionContext, StatementOutcome};
use anyhow::{bail, Result};
use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::MemTable;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_physical_plan::streaming::StreamingTableExec;

struct PartitionStreamImpl {
    schema: SchemaRef,
    stream: Arc<tokio::sync::Mutex<Option<SendableRecordBatchStream>>>,
}

impl std::fmt::Debug for PartitionStreamImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionStreamImpl")
            .field("schema", &self.schema)
            .field("stream", &"<stream>")
            .finish()
    }
}

impl PartitionStream for PartitionStreamImpl {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, _ctx: Arc<datafusion::execution::TaskContext>) -> SendableRecordBatchStream {
        let mut guard = self
            .stream
            .try_lock()
            .expect("PartitionStream scanned twice");
        guard
            .take()
            .expect("PartitionStream can only be executed once")
    }
}

struct StreamingTableProvider {
    schema: SchemaRef,
    stream: Arc<tokio::sync::Mutex<Option<SendableRecordBatchStream>>>,
}

impl std::fmt::Debug for StreamingTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingTableProvider")
            .field("schema", &self.schema)
            .field("stream", &"<stream>")
            .finish()
    }
}

#[async_trait::async_trait]
impl TableProvider for StreamingTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let stream = PartitionStreamImpl {
            schema: Arc::clone(&self.schema),
            stream: Arc::clone(&self.stream),
        };

        Ok(Arc::new(StreamingTableExec::try_new(
            Arc::clone(&self.schema),
            vec![Arc::new(stream) as Arc<dyn PartitionStream>],
            projection,
            None,
            true, // is_infinite
            limit,
        )?))
    }
}
use datafusion::error::DataFusionError;
use datafusion::logical_expr::ExprSchemable;
use datafusion::prelude::{
    col, lit, ParquetReadOptions, SessionConfig, SessionContext as DfSessionContext,
};
use datafusion::scalar::ScalarValue;
use datafusion_functions_aggregate::expr_fn::count;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::SendableRecordBatchStream;
use futures::{stream, StreamExt};
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, SelectItem,
    SetExpr, Statement, TableFactor,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tracing::{info, warn};

pub mod distributed;
pub mod functions;
pub mod postgres_compatibility;
pub mod sql_rewriter;
pub mod storage;
pub mod system_catalog;

pub use distributed::{
    ExecutePartitionRequest, ExecutePartitionWriteAck, ExecutePartitionWriteRequest,
    PartitionClient,
};

use functions::register_postgres_functions;
use system_catalog::PgCatalogSchemaProvider;

#[allow(dead_code)]
const INSERT_SELECT_PARQUET_ROW_GROUP_SIZE: usize = 1_048_576;

/// Returns a `SessionConfig` with `schema_force_view_types = false` so DataFusion
/// reads Parquet string columns as `Utf8` / `Binary` rather than the newer
/// `Utf8View` / `BinaryView` types that many Arrow Flight clients don't yet support.
fn base_session_config() -> SessionConfig {
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = false;
    config.options_mut().execution.target_partitions = num_cpus::get();
    config
}

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

use dashmap::DashMap;

pub struct FileListCache {
    cache: DashMap<String, (u64, Vec<(String, u64)>)>, // table_key -> (epoch, files)
    epochs: DashMap<String, u64>,                      // table_key -> current_epoch
}

impl FileListCache {
    pub fn new() -> Self {
        Self {
            cache: DashMap::new(),
            epochs: DashMap::new(),
        }
    }

    pub async fn get_or_list(
        &self,
        table_key: &str,
        store: &Arc<dyn ObjectStore>,
        prefix: &object_store::path::Path,
    ) -> Result<Vec<(String, u64)>> {
        let current_epoch = self.epochs.get(table_key).map(|r| *r.value()).unwrap_or(0);
        if let Some(entry) = self.cache.get(table_key) {
            let (cached_epoch, files) = entry.value();
            if *cached_epoch == current_epoch {
                return Ok(files.clone());
            }
        }

        let files = storage::list_parquet_files_with_sizes(store, prefix).await?;
        self.cache
            .insert(table_key.to_string(), (current_epoch, files.clone()));
        Ok(files)
    }

    pub fn invalidate(&self, table_key: &str) {
        let new_epoch = self
            .epochs
            .get(table_key)
            .map(|r| *r.value() + 1)
            .unwrap_or(1);
        self.epochs.insert(table_key.to_string(), new_epoch);
    }
}

pub struct PrototypeEngine {
    control_plane: Arc<ControlPlane>,
    session_context_cache: Arc<tokio::sync::RwLock<HashMap<String, DfSessionContext>>>,
    relation_locks: Arc<tokio::sync::RwLock<HashMap<String, Arc<tokio::sync::RwLock<()>>>>>,
    partition_client: Arc<PartitionClient>,
    file_list_cache: Arc<FileListCache>,
}

impl std::fmt::Debug for PrototypeEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrototypeEngine")
            .field("control_plane", &self.control_plane)
            .field("session_context_cache", &"<cached session contexts>")
            .field("partition_client", &"<partition client>")
            .field("file_list_cache", &"<file list cache>")
            .finish()
    }
}

impl Clone for PrototypeEngine {
    fn clone(&self) -> Self {
        Self {
            control_plane: Arc::clone(&self.control_plane),
            session_context_cache: Arc::clone(&self.session_context_cache),
            relation_locks: Arc::clone(&self.relation_locks),
            partition_client: Arc::clone(&self.partition_client),
            file_list_cache: Arc::clone(&self.file_list_cache),
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
    entries_object: String,
    row_count: usize,
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
        MetadataStatement::ShowNodes => Some(utf8_schema(&[
            "node_id",
            "kind",
            "status",
            "last_heartbeat_ms",
        ])),
        MetadataStatement::ShowTables { .. } => Some(utf8_schema(&["table_name"])),
        MetadataStatement::ShowViews { .. } => Some(utf8_schema(&["view_name"])),
        MetadataStatement::ShowColumns { .. } => {
            Some(utf8_schema(&["column_name", "data_type", "is_nullable"]))
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

fn metadata_statement_sql(statement: &MetadataStatement) -> Option<&str> {
    match statement {
        MetadataStatement::InformationSchemaSchemata { sql }
        | MetadataStatement::InformationSchemaTables { sql }
        | MetadataStatement::InformationSchemaColumns { sql }
        | MetadataStatement::InformationSchemaViews { sql }
        | MetadataStatement::InformationSchemaTableConstraints { sql }
        | MetadataStatement::InformationSchemaKeyColumnUsage { sql }
        | MetadataStatement::InformationSchemaConstraintColumnUsage { sql }
        | MetadataStatement::InformationSchemaConstraintTableUsage { sql }
        | MetadataStatement::InformationSchemaReferentialConstraints { sql } => Some(sql),
        _ => None,
    }
}

fn projected_metadata_schema(sql: &str, base_schema: &SchemaRef) -> Result<SchemaRef> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    let Some(Statement::Query(query)) = statements.first() else {
        return Ok(Arc::clone(base_schema));
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(Arc::clone(base_schema));
    };

    if select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard(_)))
    {
        return Ok(Arc::clone(base_schema));
    }

    let mut fields = Vec::new();
    for item in &select.projection {
        let name = match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => ident.to_string(),
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) => idents
                .last()
                .map(|ident| ident.to_string())
                .unwrap_or_else(|| item.to_string()),
            SelectItem::ExprWithAlias { alias, .. } => alias.to_string(),
            SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => {
                return Ok(Arc::clone(base_schema));
            }
            _ => item.to_string(),
        };
        fields.push(Field::new(name, DataType::Utf8, false));
    }

    Ok(Arc::new(Schema::new(fields)))
}

impl PrototypeEngine {
    pub fn new() -> Result<Self> {
        let control_plane = Arc::new(ControlPlane::new_bootstrap());
        let mut partition_client = PartitionClient::new(Arc::clone(&control_plane));
        partition_client.set_compute_eligible(true);
        let partition_client = Arc::new(partition_client);
        Ok(Self {
            control_plane,
            session_context_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            relation_locks: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            partition_client,
            file_list_cache: Arc::new(FileListCache::new()),
        })
    }

    pub async fn from_catalog_path(catalog_path: &str) -> Result<Self> {
        let control_plane = Arc::new(ControlPlane::from_catalog_path(catalog_path).await?);
        let mut partition_client = PartitionClient::new(Arc::clone(&control_plane));
        partition_client.set_compute_eligible(true);
        let partition_client = Arc::new(partition_client);
        Ok(Self {
            control_plane,
            session_context_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            relation_locks: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            partition_client,
            file_list_cache: Arc::new(FileListCache::new()),
        })
    }

    pub fn control_plane(&self) -> Arc<ControlPlane> {
        Arc::clone(&self.control_plane)
    }

    /// Executes an `ExecutePartitionRequest` and returns a stream of `RecordBatch`es.
    pub async fn execute_partition_stream(
        &self,
        req: &distributed::ExecutePartitionRequest,
    ) -> Result<SendableRecordBatchStream> {
        let ctx = DfSessionContext::new_with_config(base_session_config());
        register_postgres_functions(&ctx);

        if !req.partition_files.is_empty() {
            let paths = req
                .partition_files
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let partition_df = if req.source_columns.is_empty() {
                ctx.read_parquet(paths, ParquetReadOptions::default())
                    .await
                    .map_err(sanitize_error)?
            } else {
                let source_schema =
                    build_partition_read_schema(&ctx, paths.clone(), &req.source_columns).await?;
                ctx.read_parquet(
                    paths,
                    ParquetReadOptions::default().schema(source_schema.as_ref()),
                )
                .await
                .map_err(sanitize_error)?
            };
            ctx.register_table("__partition__", partition_df.into_view())
                .map_err(sanitize_error)?;
        }

        let df = ctx.sql(&req.sql).await.map_err(sanitize_error)?;
        df.execute_stream().await.map_err(sanitize_error)
    }

    /// Executes an `ExecutePartitionRequest` on behalf of a Coordinator.
    ///
    /// The worker runs `req.sql` in a fresh DataFusion session, bypassing
    /// admission control (the Coordinator has already admitted the query).
    /// The SQL must be fully self-contained — if it references specific Parquet
    /// files it should use DataFusion's `read_parquet([…])` function directly.
    pub async fn execute_partition(
        &self,
        req: &distributed::ExecutePartitionRequest,
    ) -> Result<QueryExecutionResult> {
        let started = std::time::Instant::now();
        let stream = self.execute_partition_stream(req).await?;
        let schema = stream.schema();
        let batches = datafusion::physical_plan::common::collect(stream)
            .await
            .map_err(sanitize_error)?;
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        Ok(QueryExecutionResult {
            query_id: req.query_id.clone(),
            coordinator_node_id: String::new(),
            session: req.session.clone(),
            schema,
            batches,
            message: format!("{row_count} row(s) from partition."),
            outcome: StatementOutcome::Rows,
            execution_time_ms: started.elapsed().as_millis(),
        })
    }

    /// Executes an `ExecutePartitionWriteRequest` on behalf of a Coordinator.
    ///
    /// Runs `req.sql` in a fresh DataFusion session, writes each output batch
    /// as a distinct UUID-named Parquet file under `req.write_prefix` in the
    /// shared object store, and returns an acknowledgment with the written file
    /// paths and total row count.
    pub async fn execute_distributed_write_partition(
        &self,
        req: &distributed::ExecutePartitionWriteRequest,
    ) -> Result<distributed::ExecutePartitionWriteAck> {
        let (store, prefix) = storage::store_for_location(&req.write_prefix)?;

        let ctx = DfSessionContext::new_with_config(base_session_config());
        register_postgres_functions(&ctx);

        // Use the Rust API to read partition files directly — DataFusion does not
        // expose `read_parquet([...])` as a SQL table function in this version.
        let paths = req
            .partition_files
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let df = if req.source_columns.is_empty() {
            ctx.read_parquet(paths, ParquetReadOptions::default())
                .await
                .map_err(sanitize_error)?
        } else {
            let source_schema =
                build_partition_read_schema(&ctx, paths.clone(), &req.source_columns).await?;
            ctx.read_parquet(
                paths,
                ParquetReadOptions::default().schema(source_schema.as_ref()),
            )
            .await
            .map_err(sanitize_error)?
        };
        let batches = df.collect().await.map_err(sanitize_error)?;

        let mut written_files = Vec::new();
        let mut total_rows = 0usize;

        let mut current_batch = Vec::new();
        let mut current_rows = 0;

        for batch in batches {
            let (row_count, prepared) = prepare_batch_for_storage(batch)?;
            if row_count == 0 {
                continue;
            }

            current_batch.push(prepared);
            current_rows += row_count;
            total_rows += row_count;

            if current_rows >= INSERT_SELECT_PARQUET_ROW_GROUP_SIZE {
                let file_name = format!("{}.parquet", uuid::Uuid::now_v7());
                let key = prefix.clone().join(file_name.as_str());
                let schema = current_batch[0].schema();
                storage::write_parquet_batches(&store, &key, schema, &current_batch).await?;
                written_files.push(format!("/{}", key.as_ref()));
                current_batch.clear();
                current_rows = 0;
            }
        }

        if !current_batch.is_empty() {
            let file_name = format!("{}.parquet", uuid::Uuid::now_v7());
            let key = prefix.clone().join(file_name.as_str());
            let schema = current_batch[0].schema();
            storage::write_parquet_batches(&store, &key, schema, &current_batch).await?;
            written_files.push(format!("/{}", key.as_ref()));
        }

        Ok(distributed::ExecutePartitionWriteAck {
            written_files,
            row_count: total_rows,
        })
    }

    async fn relation_lock(
        &self,
        relation: &analyticsdb_control::CatalogRelation,
    ) -> Arc<tokio::sync::RwLock<()>> {
        let key = format!(
            "{}.{}.{}",
            relation.database, relation.schema, relation.name
        );
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
        let (store, table_prefix) = table_store_prefix(relation)?;
        for index in &relation.indexes {
            println!("rebuilding index snapshot for: {}", index.name);
            let version = uuid::Uuid::now_v7().to_string();
            let snapshot = self
                .build_index_snapshot_for_relation(session, relation, &index.name, &version)
                .await?;
            println!("writing index snapshot for: {}", index.name);
            write_index_snapshot(&store, &table_prefix, &snapshot, &version).await?;
        }
        Ok(())
    }

    async fn physical_migrate_relation(
        &self,
        _session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
    ) -> Result<()> {
        let Some(storage_path) = &relation.storage_path else {
            return Ok(());
        };

        let (store, prefix) = storage::store_for_location(storage_path)?;
        let files = storage::list_parquet_files(&store, &prefix).await?;

        for file_path in files {
            let ctx = DfSessionContext::new_with_config(base_session_config());
            let df = ctx
                .read_parquet(vec![file_path.as_str()], ParquetReadOptions::default())
                .await
                .map_err(sanitize_error)?;

            // When reading with default options, DataFusion infers the schema from the file.
            // We want to write it back with the NEW schema from the catalog.
            let full_schema = build_arrow_schema_from_catalog_columns(&relation.columns)?;

            // Collect existing data and write it back with the new schema.
            // DataFusion will handle missing columns by filling with NULLs.
            let batches = df.collect().await.map_err(sanitize_error)?;
            let bytes = storage::encode_parquet_batches(full_schema, &batches)?;
            let key = OPath::parse(file_path.trim_start_matches('/'))?;
            store.put(&key, bytes.into()).await?;
        }

        Ok(())
    }

    async fn physical_migrate_rename_column(
        &self,
        _session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        let Some(storage_path) = &relation.storage_path else {
            return Ok(());
        };

        let (store, prefix) = storage::store_for_location(storage_path)?;
        let files = storage::list_parquet_files(&store, &prefix).await?;

        for file_path in files {
            let ctx = DfSessionContext::new_with_config(base_session_config());
            let df = ctx
                .read_parquet(vec![file_path.as_str()], ParquetReadOptions::default())
                .await
                .map_err(sanitize_error)?;

            let mut projection = Vec::new();
            for field in df.schema().fields() {
                if field.name() == old_name {
                    projection.push(datafusion::prelude::col(field.name()).alias(new_name));
                } else {
                    projection.push(datafusion::prelude::col(field.name()));
                }
            }
            let renamed_df = df.select(projection).map_err(sanitize_error)?;
            let schema = Arc::new(renamed_df.schema().as_arrow().as_ref().clone());
            let batches = renamed_df.collect().await.map_err(sanitize_error)?;

            let bytes = storage::encode_parquet_batches(schema, &batches)?;
            let key = OPath::parse(file_path.trim_start_matches('/'))?;
            store.put(&key, bytes.into()).await?;
        }

        Ok(())
    }

    async fn build_index_snapshot_for_relation(
        &self,
        session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
        index_name: &str,
        version: &str,
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

        let df_context = self.create_session_context(session).await?;
        let table_name = format!(
            "\"{}\".\"{}\".\"{}\"",
            relation.database, relation.schema, relation.name
        );
        let index_cols = index
            .columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {}, \"_row_id\" FROM {} ORDER BY {}",
            index_cols, table_name, index_cols
        );
        let df = df_context.sql(&sql).await.map_err(sanitize_error)?;

        // Validate uniqueness if needed
        if index.is_unique || index.is_primary {
            let check_sql = format!(
                "SELECT COUNT(*) as count FROM (SELECT {} FROM {} GROUP BY {} HAVING COUNT(*) > 1)",
                index_cols, table_name, index_cols
            );
            let check_df = df_context.sql(&check_sql).await.map_err(sanitize_error)?;
            let results = check_df.collect().await.map_err(sanitize_error)?;
            let count_val = results
                .iter()
                .map(|b| {
                    if b.num_rows() > 0 {
                        let col = b.column(0);
                        let arr = col
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::Int64Array>()
                            .unwrap();
                        arr.value(0)
                    } else {
                        0
                    }
                })
                .sum::<i64>();
            if count_val > 0 {
                anyhow::bail!(
                    "Unique index '{}' on '{}.{}.{}' would contain duplicate keys",
                    index.name,
                    relation.database,
                    relation.schema,
                    relation.name
                );
            }
        }

        let (store, table_prefix) = table_store_prefix(relation)?;
        let data_key = index_data_key(&table_prefix, index_name, version);

        let sort_exprs = index
            .columns
            .iter()
            .map(|c| col(c).sort(true, true))
            .collect::<Vec<_>>();

        let sorted_batches = df
            .clone()
            .sort(sort_exprs)
            .map_err(sanitize_error)?
            .collect()
            .await
            .map_err(sanitize_error)?;

        let schema = if let Some(first) = sorted_batches.first() {
            first.schema()
        } else {
            Arc::new(df.schema().as_arrow().clone())
        };
        storage::write_parquet_batches(&store, &data_key, schema, &sorted_batches).await?;

        let row_count_df = df.aggregate(vec![], vec![count(col("_row_id")).alias("count")])?;
        let row_count_results = row_count_df.collect().await.map_err(sanitize_error)?;
        let row_count = row_count_results
            .iter()
            .map(|b| {
                if b.num_rows() > 0 {
                    let col = b.column(0);
                    let arr = col
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::Int64Array>()
                        .unwrap();
                    arr.value(0) as usize
                } else {
                    0
                }
            })
            .sum::<usize>();

        Ok(IndexSnapshot {
            database: relation.database.clone(),
            schema: relation.schema.clone(),
            table: relation.name.clone(),
            index: index.name.clone(),
            columns: index.columns.clone(),
            unique: index.is_unique,
            primary: index.is_primary,
            entries_object: version.to_string(),
            row_count,
        })
    }

    async fn refresh_index_snapshots_after_mutation(
        &self,
        session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
    ) {
        if relation.indexes.is_empty() {
            return;
        }

        let engine = self.clone();
        let session = session.clone();
        let relation = relation.clone();

        tokio::spawn(async move {
            if let Err(rebuild_error) = engine
                .rebuild_all_index_snapshots(&session, &relation)
                .await
            {
                warn!(
                    database = %relation.database,
                    schema = %relation.schema,
                    table = %relation.name,
                    error = %rebuild_error,
                    "failed to rebuild managed-table index sidecars in background; previous published snapshot remains active"
                );
            }
        });
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

        let Some((index, row_ids)) = self
            .best_index_match(&request.session, &relation, &statement)
            .await?
        else {
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
        let context = DfSessionContext::new_with_config(base_session_config());

        let storage_path = relation
            .storage_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Missing storage path"))?;
        let full_schema = build_arrow_schema_from_catalog_columns(&relation.columns)?;
        let table_path = listing_table_url_for_storage_location(storage_path)?;
        let config = datafusion::datasource::listing::ListingTableConfig::new(table_path)
            .with_listing_options(datafusion::datasource::listing::ListingOptions::new(
                Arc::new(datafusion::datasource::file_format::parquet::ParquetFormat::default()),
            ))
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

    async fn validate_batch_against_table_uniqueness(
        &self,
        _session: &SessionContext,
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

        for index in &relation.indexes {
            if !index.is_unique && !index.is_primary {
                continue;
            }

            let (store, table_prefix) = table_store_prefix(relation)?;
            let Some(snapshot) = read_index_snapshot(&store, &table_prefix, &index.name).await?
            else {
                continue;
            };

            // Use a fresh context for internal validation to avoid catalog registration issues
            let df_context = DfSessionContext::new_with_config(base_session_config());
            let new_batch_df = df_context.read_batch(batch.clone())?;

            let data_key = index_data_key(&table_prefix, &index.name, &snapshot.entries_object);
            if !storage::object_exists(&store, &data_key).await? {
                continue;
            }

            let data_local_path = format!("/{}", data_key.as_ref());
            let snapshot_df = df_context
                .read_parquet(&data_local_path, Default::default())
                .await
                .map_err(sanitize_error)?;

            let index_cols = index.columns.iter().map(|c| col(c)).collect::<Vec<_>>();

            // 1. Check for duplicates within the new batch
            let batch_dup_df = new_batch_df
                .clone()
                .aggregate(index_cols.clone(), vec![count(lit(1)).alias("count")])?
                .filter(col("count").gt(lit(1)))?;
            let batch_dup_results = batch_dup_df.collect().await.map_err(sanitize_error)?;
            if batch_dup_results.iter().any(|b| b.num_rows() > 0) {
                anyhow::bail!(
                    "Unique index '{}' on '{}.{}.{}' would contain duplicate keys within the new batch",
                    index.name,
                    relation.database,
                    relation.schema,
                    relation.name
                );
            }

            // 2. Check for duplicates against existing data (the index snapshot)
            let join_on_cols = index.columns.iter().map(|c| c.as_str()).collect::<Vec<_>>();
            let join_df = new_batch_df.join(
                snapshot_df,
                datafusion::prelude::JoinType::LeftSemi,
                &join_on_cols,
                &join_on_cols,
                None,
            )?;

            let join_results = join_df.collect().await.map_err(sanitize_error)?;
            let count = join_results.iter().map(|b| b.num_rows()).sum::<usize>();
            println!("validate uniqueness join count: {}", count);
            if count > 0 {
                anyhow::bail!(
                    "Unique index '{}' on '{}.{}.{}' would contain duplicate keys (violation against existing data)",
                    index.name,
                    relation.database,
                    relation.schema,
                    relation.name
                );
            }
        }

        Ok(())
    }

    pub async fn list_databases(&self, session: &SessionContext) -> Result<Vec<String>> {
        self.control_plane.list_databases(session).await
    }

    pub async fn list_schemas(
        &self,
        session: &SessionContext,
        database: Option<&str>,
    ) -> Result<Vec<String>> {
        self.control_plane.list_schemas(session, database).await
    }

    pub async fn list_relations(
        &self,
        session: &SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        kind: CatalogRelationKind,
    ) -> Result<Vec<analyticsdb_control::CatalogRelation>> {
        self.control_plane
            .list_relations(session, database, schema, kind)
            .await
    }

    /// Attempts to execute `request.sql` as a distributed scatter-gather query stream.
    async fn try_execute_distributed_select_stream(
        &self,
        request: &QueryRequest,
        admission: &QueryAdmission,
        started: Instant,
    ) -> Result<Option<QueryExecutionStream>> {
        // Only attempt distribution for plain SELECT statements.
        let Some((db, schema_name, table_name)) = parse_plain_select_table(&request.sql) else {
            return Ok(None);
        };

        // Resolve the managed relation; fall through if not found.
        let relation = match self
            .control_plane
            .table_relation(
                &request.session,
                db.as_deref(),
                schema_name.as_deref(),
                &table_name,
            )
            .await
        {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        let Some(storage_location) = relation.storage_path.as_deref() else {
            return Ok(None);
        };

        // Enumerate the Parquet files backing the table.
        let (store, prefix) = storage::store_for_location(storage_location)?;
        let table_key = format!(
            "{}.{}.{}",
            relation.database, relation.schema, relation.name
        );
        let files = self
            .file_list_cache
            .get_or_list(&table_key, &store, &prefix)
            .await?;

        let aggregate_plan = distributed_aggregate_plan(&request.sql, &table_name);
        if aggregate_plan.is_none() && select_projection_contains_function(&request.sql) {
            return Ok(None);
        }

        // Calculate optimal worker count based on data size and file count.
        let total_size: u64 = files.iter().map(|(_, size)| *size).sum();
        let file_count = files.len();
        let partition_client = Arc::clone(&self.partition_client);

        let mut attempts = 0;
        const MAX_ATTEMPTS: usize = 3;

        while attempts < MAX_ATTEMPTS {
            attempts += 1;

            // Discover available Compute nodes.
            let mut compute_nodes = partition_client.list_compute_nodes().await?;
            if compute_nodes.is_empty() {
                warn!("[coordinator] No compute nodes available for distributed query; falling back to local execution.");
                return Ok(None);
            }

            let optimal_worker_count = distributed::calculate_optimal_worker_count(
                total_size,
                file_count,
                compute_nodes.len(),
            );

            // If heuristic says 1 node and it's just the coordinator, might as well run locally
            // unless we want to use the partition executor anyway. For now, if N=1 and we have
            // workers, we'll still pick one worker to keep the distributed path exercised.

            // Select a subset of nodes (round-robin selection could be added here,
            // but for now we just take the first N).
            compute_nodes.truncate(optimal_worker_count);

            if files.is_empty() {
                let schema = build_arrow_schema_from_catalog_columns(&relation.columns)?;
                return self
                    .execute_coordinator_select_over_partition_batches(
                        request,
                        admission,
                        started,
                        &table_name,
                        schema,
                        Vec::new(),
                        compute_nodes.len(),
                        None,
                    )
                    .await
                    .map(Some);
            }

            // Partition files across available workers (greedy size-aware).
            let chunks =
                distributed::partition_files_for_workers(files.clone(), compute_nodes.len());

            let node_list: Vec<&str> = compute_nodes
                .iter()
                .map(distributed::PartitionClient::node_channel_endpoint)
                .collect();
            info!(
                "[coordinator] Distributed SELECT on '{}' (attempt {}): {} file(s) across {} worker(s) [{} of {} available]: [{}]",
                table_name,
                attempts,
                files.len(),
                compute_nodes.len(),
                optimal_worker_count,
                partition_client.list_compute_nodes().await?.len(),
                node_list.join(", ")
            );

            let worker_sql = aggregate_plan
                .as_ref()
                .map(|(worker_sql, _)| worker_sql.clone())
                .unwrap_or_else(|| {
                    rewrite_sql_for_partition(&request.sql, &table_name)
                        .unwrap_or_else(|| "SELECT * FROM __partition__".to_string())
                });

            // Build tasks.
            let worker_tasks: Vec<_> = chunks
                .into_iter()
                .zip(compute_nodes.iter())
                .map(|(chunk_files, node)| {
                    let req = distributed::ExecutePartitionRequest {
                        query_id: admission.query_id.clone(),
                        sql: worker_sql.clone(),
                        session: request.session.clone(),
                        partition_files: chunk_files,
                        source_columns: relation.columns.clone(),
                    };
                    (node, req)
                })
                .collect();

            // Dispatch all concurrently.
            let mut dispatch_futures = Vec::new();
            for (node, req) in &worker_tasks {
                let endpoint =
                    distributed::PartitionClient::node_channel_endpoint(node).to_string();
                let pc = Arc::clone(&partition_client);
                let node_id = node.id.clone();
                dispatch_futures.push(async move {
                    match pc.execute_on_node(&endpoint, &req).await {
                        Ok(stream) => Ok((node_id, stream)),
                        Err(e) => Err((node_id, e)),
                    }
                });
            }

            let dispatch_results = futures::future::join_all(dispatch_futures).await;
            let mut worker_streams = Vec::new();
            let mut failed_node_id = None;

            for res in dispatch_results {
                match res {
                    Ok((node_id, stream)) => {
                        // Wrap stream to catch mid-flight failures and identify the node.
                        let node_id_inner = node_id.clone();
                        worker_streams.push(stream.map(move |batch_res| {
                            batch_res.map_err(|e| (node_id_inner.clone(), e))
                        }));
                    }
                    Err((node_id, e)) => {
                        warn!(
                            "[coordinator] Failed to dispatch to node '{}': {}",
                            node_id, e
                        );
                        failed_node_id = Some(node_id);
                        break;
                    }
                }
            }

            if let Some(node_id) = failed_node_id {
                info!(
                    "[coordinator] Marking node '{}' as unavailable and retrying query...",
                    node_id
                );
                let _ = partition_client.mark_node_unavailable(&node_id).await;
                continue;
            }

            // Merge all worker streams into one concurrent stream.
            let mut merged_stream = futures::stream::select_all(worker_streams);
            let base_schema = build_arrow_schema_from_catalog_columns(&relation.columns)?;

            if aggregate_plan.is_some() {
                // For aggregates, we must materialize the (small) partial results to finalize.
                let mut all_batches = Vec::new();
                let mut stream_failed_node_id = None;

                while let Some(batch_res) = merged_stream.next().await {
                    match batch_res {
                        Ok(batch) => all_batches.push(batch),
                        Err((node_id, e)) => {
                            warn!(
                                "[coordinator] Node '{}' failed during streaming: {}",
                                node_id, e
                            );
                            stream_failed_node_id = Some(node_id);
                            break;
                        }
                    }
                }

                if let Some(node_id) = stream_failed_node_id {
                    info!(
                        "[coordinator] Marking node '{}' as unavailable and retrying query...",
                        node_id
                    );
                    let _ = partition_client.mark_node_unavailable(&node_id).await;
                    continue;
                }

                // Success!
                let schema = all_batches
                    .first()
                    .map(|batch| batch.schema())
                    .unwrap_or(Arc::clone(&base_schema));

                return self
                    .execute_coordinator_select_over_partition_batches(
                        request,
                        admission,
                        started,
                        &table_name,
                        schema,
                        all_batches,
                        compute_nodes.len(),
                        aggregate_plan.map(|(_, final_sql)| final_sql),
                    )
                    .await
                    .map(Some);
            } else {
                // For plain SELECTs (large results), use the TRUE STREAMING path.
                let df_stream = merged_stream.map(|res| match res {
                    Ok(batch) => Ok(batch),
                    Err((node_id, e)) => Err(DataFusionError::Execution(format!(
                        "Node {node_id} failed: {e}"
                    ))),
                });

                let partition_stream: SendableRecordBatchStream = Box::pin(
                    RecordBatchStreamAdapter::new(Arc::clone(&base_schema), df_stream),
                );

                return self
                    .execute_coordinator_select_over_partition_stream(
                        request,
                        admission,
                        started,
                        &table_name,
                        base_schema,
                        partition_stream,
                        compute_nodes.len(),
                        None,
                    )
                    .await
                    .map(Some);
            }
        }

        warn!("[coordinator] Distributed query failed after {} attempts; falling back to local execution.", MAX_ATTEMPTS);
        Ok(None)
    }

    async fn execute_coordinator_select_over_partition_stream(
        &self,
        request: &QueryRequest,
        admission: &QueryAdmission,
        started: Instant,
        table_name: &str,
        partition_schema: SchemaRef,
        partition_stream: SendableRecordBatchStream,
        worker_count: usize,
        final_sql_override: Option<String>,
    ) -> Result<QueryExecutionStream> {
        let final_sql = final_sql_override.unwrap_or_else(|| {
            rewrite_sql_for_partition(&request.sql, table_name)
                .unwrap_or_else(|| "SELECT * FROM __partition__".to_string())
        });
        let context = DfSessionContext::new_with_config(base_session_config());
        register_postgres_functions(&context);

        let table = StreamingTableProvider {
            schema: Arc::clone(&partition_schema),
            stream: Arc::new(tokio::sync::Mutex::new(Some(partition_stream))),
        };
        context.register_table("__partition__", Arc::new(table))?;

        let dataframe = context.sql(&final_sql).await.map_err(sanitize_error)?;
        let schema = Arc::new(dataframe.schema().as_arrow().as_ref().clone());
        let stream = dataframe.execute_stream().await.map_err(sanitize_error)?;

        Ok(QueryExecutionStream {
            query_id: admission.query_id.clone(),
            coordinator_node_id: admission.coordinator_node_id.clone(),
            session: request.session.clone(),
            schema,
            stream,
            message: format!(
                "Distributed query: coordinator streaming result from {worker_count} node(s)."
            ),
            outcome: StatementOutcome::Rows,
            execution_time_ms: started.elapsed().as_millis(),
        })
    }

    async fn execute_coordinator_select_over_partition_batches(
        &self,
        request: &QueryRequest,
        admission: &QueryAdmission,
        started: Instant,
        table_name: &str,
        partition_schema: SchemaRef,
        partition_batches: Vec<RecordBatch>,
        worker_count: usize,
        final_sql_override: Option<String>,
    ) -> Result<QueryExecutionStream> {
        let final_sql = final_sql_override.unwrap_or_else(|| {
            rewrite_sql_for_partition(&request.sql, table_name)
                .unwrap_or_else(|| "SELECT * FROM __partition__".to_string())
        });
        let context = DfSessionContext::new_with_config(base_session_config());
        register_postgres_functions(&context);
        let partitions = if partition_batches.is_empty() {
            vec![vec![RecordBatch::new_empty(Arc::clone(&partition_schema))]]
        } else {
            vec![partition_batches]
        };
        let table = MemTable::try_new(Arc::clone(&partition_schema), partitions)?;
        context.register_table("__partition__", Arc::new(table))?;

        let dataframe = context.sql(&final_sql).await.map_err(sanitize_error)?;
        let schema = Arc::new(dataframe.schema().as_arrow().as_ref().clone());
        let stream = dataframe.execute_stream().await.map_err(sanitize_error)?;

        Ok(QueryExecutionStream {
            query_id: admission.query_id.clone(),
            coordinator_node_id: admission.coordinator_node_id.clone(),
            session: request.session.clone(),
            schema,
            stream,
            message: format!(
                "Distributed query: coordinator finalized result from {worker_count} node(s)."
            ),
            outcome: StatementOutcome::Rows,
            execution_time_ms: started.elapsed().as_millis(),
        })
    }

    /// Attempts to execute `request.sql` as a distributed scatter-gather query.
    ///
    /// Returns `None` when:
    /// - the SQL is not a plain `SELECT … FROM <table>` targeting a managed table, or
    /// - there are no Ready Compute nodes available (falls through to local execution).
    ///
    /// When Compute nodes are available the query is rewritten to use
    /// `read_parquet([…])` syntax so workers don't need catalog access, then
    /// dispatched concurrently.  RecordBatches from all workers are concatenated.
    async fn try_execute_distributed_select(
        &self,
        request: &QueryRequest,
        admission: &QueryAdmission,
        started: Instant,
    ) -> Result<Option<QueryExecutionResult>> {
        let Some(exec_stream) = self
            .try_execute_distributed_select_stream(request, admission, started)
            .await?
        else {
            return Ok(None);
        };

        let schema = exec_stream.schema;
        let batches = datafusion::physical_plan::common::collect(exec_stream.stream)
            .await
            .map_err(sanitize_error)?;

        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        Ok(Some(QueryExecutionResult {
            query_id: exec_stream.query_id,
            coordinator_node_id: exec_stream.coordinator_node_id,
            session: exec_stream.session,
            schema,
            batches,
            message: format!(
                "Distributed query: {row_count} row(s) returned from {} nodes.",
                // We don't have the node count easily here without re-calculating,
                // but we can just use the message from exec_stream or a generic one.
                "multiple"
            ),
            outcome: exec_stream.outcome,
            execution_time_ms: started.elapsed().as_millis(),
        }))
    }

    /// Attempts to execute `INSERT INTO target SELECT … FROM source` in a distributed fashion.
    ///
    /// Returns `None` (fall through to local single-node execution) when:
    /// - the SELECT source is not a plain managed table, or
    /// - the target table has unique/primary-key indexes (cross-partition duplicate
    ///   checking is not yet implemented), or
    /// - there are no Ready Compute nodes.
    ///
    /// When distribution is possible, each worker receives a subset of the source
    /// Parquet files and writes its output directly to the target table's shared
    /// object store prefix.  The Coordinator then updates index sidecars.
    async fn try_execute_distributed_insert_select(
        &self,
        request: &QueryRequest,
        statement: &InsertSelectStatement,
        admission: &QueryAdmission,
        started: Instant,
    ) -> Result<Option<QueryExecutionResult>> {
        // Only distribute when the SELECT source is a plain table reference.
        let Some((src_db, src_schema, src_table)) = parse_plain_select_table(&statement.query_sql)
        else {
            return Ok(None);
        };

        // Resolve the target relation.
        let target_relation = match self
            .control_plane
            .table_relation(
                &request.session,
                statement.database.as_deref(),
                statement.schema.as_deref(),
                &statement.name,
            )
            .await
        {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        // Skip distribution when uniqueness constraints exist — cross-partition
        // duplicate validation requires a separate merge step not yet implemented.
        if target_relation
            .indexes
            .iter()
            .any(|idx| idx.is_unique || idx.is_primary)
        {
            return Ok(None);
        }

        let Some(target_storage) = target_relation.storage_path.as_deref() else {
            return Ok(None);
        };

        // Resolve the source relation and its Parquet files.
        let source_relation = match self
            .control_plane
            .table_relation(
                &request.session,
                src_db.as_deref(),
                src_schema.as_deref(),
                &src_table,
            )
            .await
        {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        let Some(source_storage) = source_relation.storage_path.as_deref() else {
            return Ok(None);
        };

        let (src_store, src_prefix) = storage::store_for_location(source_storage)?;
        let src_key = format!(
            "{}.{}.{}",
            source_relation.database, source_relation.schema, source_relation.name
        );
        let source_files = self
            .file_list_cache
            .get_or_list(&src_key, &src_store, &src_prefix)
            .await?;

        if source_files.is_empty() {
            // Nothing to insert.
            return Ok(Some(QueryExecutionResult {
                query_id: admission.query_id.clone(),
                coordinator_node_id: admission.coordinator_node_id.clone(),
                session: request.session.clone(),
                schema: Arc::new(Schema::empty()),
                batches: vec![],
                message: format!(
                    "Inserted 0 row(s) into '{}.{}.{}' (source table is empty).",
                    target_relation.database, target_relation.schema, target_relation.name
                ),
                outcome: StatementOutcome::Command {
                    tag: "INSERT".to_string(),
                    rows_affected: 0,
                },
                execution_time_ms: started.elapsed().as_millis(),
            }));
        }

        let total_size: u64 = source_files.iter().map(|(_, size)| *size).sum();
        let file_count = source_files.len();
        let partition_client = Arc::clone(&self.partition_client);

        let mut attempts = 0;
        const MAX_ATTEMPTS: usize = 3;

        while attempts < MAX_ATTEMPTS {
            attempts += 1;

            // Discover Ready Compute nodes.
            let mut compute_nodes = partition_client.list_compute_nodes().await?;
            if compute_nodes.is_empty() {
                warn!("[coordinator] No compute nodes available for distributed insert; falling back to local execution.");
                return Ok(None);
            }

            let optimal_worker_count = distributed::calculate_optimal_worker_count(
                total_size,
                file_count,
                compute_nodes.len(),
            );
            compute_nodes.truncate(optimal_worker_count);

            // Partition source files across workers (greedy size-aware).
            let chunks =
                distributed::partition_files_for_workers(source_files.clone(), compute_nodes.len());

            // Acquire the write lock on the target relation.
            let relation_lock = self.relation_lock(&target_relation).await;
            let _write_guard = relation_lock.write().await;

            // Build one ExecutePartitionWriteRequest per worker chunk.
            let worker_tasks: Vec<_> = chunks
                .into_iter()
                .zip(compute_nodes.iter())
                .map(|(chunk_files, node)| {
                    let req = distributed::ExecutePartitionWriteRequest {
                        query_id: admission.query_id.clone(),
                        // sql is unused when partition_files is non-empty; kept for tracing.
                        sql: format!("SELECT * FROM partition ({} files)", chunk_files.len()),
                        session: request.session.clone(),
                        partition_files: chunk_files,
                        source_columns: source_relation.columns.clone(),
                        write_prefix: target_storage.to_string(),
                    };
                    (node, req)
                })
                .collect();

            // Dispatch all concurrently.
            let mut dispatch_futures = Vec::new();
            for (node, req) in &worker_tasks {
                let endpoint =
                    distributed::PartitionClient::node_channel_endpoint(node).to_string();
                let pc = Arc::clone(&partition_client);
                let node_id = node.id.clone();
                dispatch_futures.push(async move {
                    match pc.write_on_node(&endpoint, &req).await {
                        Ok(ack) => Ok((node_id, ack)),
                        Err(e) => Err((node_id, e)),
                    }
                });
            }

            let dispatch_results = futures::future::join_all(dispatch_futures).await;
            let mut all_acks = Vec::new();
            let mut failed_node_id = None;

            for res in dispatch_results {
                match res {
                    Ok((_, ack)) => all_acks.push(ack),
                    Err((node_id, e)) => {
                        warn!(
                            "[coordinator] Node '{}' failed during distributed write: {}",
                            node_id, e
                        );
                        failed_node_id = Some(node_id);
                        break;
                    }
                }
            }

            if let Some(node_id) = failed_node_id {
                info!("[coordinator] Marking node '{}' as unavailable and retrying distributed insert...", node_id);
                let _ = partition_client.mark_node_unavailable(&node_id).await;
                continue;
            }

            // Success!
            let total_rows: usize = all_acks.iter().map(|a| a.row_count).sum();

            // Commit: refresh index sidecars now that new Parquet files are visible.
            self.refresh_index_snapshots_after_mutation(&request.session, &target_relation)
                .await;

            let table_key = format!(
                "{}.{}.{}",
                target_relation.database, target_relation.schema, target_relation.name
            );
            self.file_list_cache.invalidate(&table_key);

            return Ok(Some(QueryExecutionResult {
                query_id: admission.query_id.clone(),
                coordinator_node_id: admission.coordinator_node_id.clone(),
                session: request.session.clone(),
                schema: Arc::new(Schema::empty()),
                batches: vec![],
                message: format!(
                    "Distributed insert: {total_rows} row(s) inserted into '{}.{}.{}' via {} node(s) (attempt {}).",
                    target_relation.database,
                    target_relation.schema,
                    target_relation.name,
                    compute_nodes.len(),
                    attempts,
                ),
                outcome: StatementOutcome::Command {
                    tag: "INSERT".to_string(),
                    rows_affected: total_rows as u64,
                },
                execution_time_ms: started.elapsed().as_millis(),
            }));
        }

        warn!("[coordinator] Distributed insert failed after {} attempts; falling back to local execution.", MAX_ATTEMPTS);
        Ok(None)
    }

    async fn prepare_query_request(
        &self,
        request: &QueryRequest,
    ) -> Result<(QueryRequest, QueryAdmission, Instant)> {
        let started = Instant::now();
        self.control_plane
            .validate_session(&request.session)
            .await?;
        let admission = self.control_plane.admit_query(&request.session).await?;

        let control_plane = Arc::clone(&self.control_plane);
        let sql = sql_rewriter::rewrite_sql_for_postgres_compatibility(
            &request.sql,
            &control_plane,
            &request.session,
        )
        .await?;
        let mut request = request.clone();
        request.sql = sql;
        Ok((request, admission, started))
    }

    pub async fn execute_query(&self, request: &QueryRequest) -> Result<QueryExecutionResult> {
        let (request, admission, started) = self.prepare_query_request(request).await?;

        if let Some(statement) = parse_insert_select_statement(&request.sql)? {
            return self
                .execute_insert_select(&request, statement, admission, started)
                .await;
        }

        if let Some(statement) = parse_indexed_select_statement(&request.sql)? {
            if let Some(result) = self
                .try_execute_indexed_select(&request, statement, &admission, started)
                .await?
            {
                return Ok(result);
            }
        }

        if let Some(statement) = parse_metadata_statement(&request.sql) {
            let result = self
                .execute_metadata_query(&request, statement, admission, started)
                .await?;
            if matches!(result.outcome, StatementOutcome::Command { .. }) {
                self.invalidate_session_contexts().await;
            }
            return Ok(result);
        }

        if let Some(result) = self
            .try_execute_distributed_select(&request, &admission, started)
            .await?
        {
            return Ok(result);
        }

        let session = request.session.clone();
        let context = self.create_session_context(&session).await?;
        let dataframe = context.sql(&request.sql).await.map_err(sanitize_error)?;
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

    pub async fn execute_query_stream(
        &self,
        request: &QueryRequest,
    ) -> Result<QueryExecutionStream> {
        let (request, admission, started) = self.prepare_query_request(request).await?;

        if let Some(statement) = parse_insert_select_statement(&request.sql)? {
            let execution = self
                .execute_insert_select(&request, statement, admission, started)
                .await?;
            let schema = Arc::new(Schema::empty());
            let batch_stream =
                stream::iter(vec![].into_iter().map(Ok::<RecordBatch, DataFusionError>));

            return Ok(QueryExecutionStream {
                query_id: execution.query_id,
                coordinator_node_id: execution.coordinator_node_id,
                session: execution.session,
                schema: Arc::clone(&schema),
                stream: Box::pin(RecordBatchStreamAdapter::new(schema, batch_stream)),
                message: execution.message,
                outcome: execution.outcome,
                execution_time_ms: execution.execution_time_ms,
            });
        }

        if let Some(statement) = parse_indexed_select_statement(&request.sql)? {
            if let Some(execution) = self
                .try_execute_indexed_select(&request, statement, &admission, started)
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
            let execution = self
                .execute_metadata_query(&request, statement, admission, started)
                .await?;
            if matches!(execution.outcome, StatementOutcome::Command { .. }) {
                self.invalidate_session_contexts().await;
            }
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
                outcome: execution.outcome,
                execution_time_ms: execution.execution_time_ms,
            });
        }

        if let Some(result) = self
            .try_execute_distributed_select_stream(&request, &admission, started)
            .await?
        {
            return Ok(result);
        }

        let session = request.session.clone();

        let context = self.create_session_context(&session).await?;
        let dataframe = context.sql(&request.sql).await.map_err(sanitize_error)?;
        let schema = Arc::new(dataframe.schema().as_arrow().as_ref().clone());

        let stream = dataframe.execute_stream().await.map_err(sanitize_error)?;
        let outcome = if schema.fields().is_empty() {
            StatementOutcome::Command {
                tag: "OK".to_string(),
                rows_affected: 0,
            }
        } else {
            StatementOutcome::Rows
        };

        Ok(QueryExecutionStream {
            query_id: admission.query_id,
            coordinator_node_id: admission.coordinator_node_id,
            session,
            schema,
            stream,
            message: "Query stream opened successfully.".to_string(),
            outcome,
            execution_time_ms: started.elapsed().as_millis(),
        })
    }

    pub async fn plan_query_schema(&self, request: &QueryRequest) -> Result<Option<SchemaRef>> {
        self.control_plane
            .validate_session(&request.session)
            .await?;

        if parse_insert_select_statement(&request.sql)?.is_some() {
            return Ok(Some(Arc::new(Schema::empty())));
        }

        if let Some(statement) = parse_metadata_statement(&request.sql) {
            let base_schema =
                metadata_statement_schema(&statement).unwrap_or_else(|| Arc::new(Schema::empty()));
            if let Some(sql) = metadata_statement_sql(&statement) {
                return Ok(Some(projected_metadata_schema(sql, &base_schema)?));
            }
            return Ok(Some(base_schema));
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
        // Attempt distributed execution first; fall through on None.
        if let Some(result) = self
            .try_execute_distributed_insert_select(request, &statement, &admission, started)
            .await?
        {
            return Ok(result);
        }

        let relation = self
            .control_plane
            .table_relation(
                &request.session,
                statement.database.as_deref(),
                statement.schema.as_deref(),
                &statement.name,
            )
            .await?;
        let storage_location = relation.storage_path.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "Managed table '{}.{}.{}' is missing a storage path",
                relation.database,
                relation.schema,
                relation.name
            )
        })?;
        let (store, prefix) = storage::store_for_location(storage_location)?;
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

        // Align columns with the table
        let projected_dataframe = if let Some(target_cols) = statement.columns {
            if source_dataframe.schema().fields().len() != target_cols.len() {
                anyhow::bail!(
                    "INSERT has {} target columns but SELECT has {} source columns",
                    target_cols.len(),
                    source_dataframe.schema().fields().len()
                );
            }

            let mut projections = Vec::new();
            for table_col in &relation.columns {
                if table_col.name == "_row_id" {
                    continue;
                }

                if let Some(pos) = target_cols.iter().position(|c| c == &table_col.name) {
                    projections.push(
                        datafusion::prelude::col(source_dataframe.schema().field(pos).name())
                            .alias(table_col.name.clone()),
                    );
                } else {
                    // If not in target list, use NULL
                    projections.push(
                        datafusion::prelude::lit(ScalarValue::Null)
                            .cast_to(
                                &catalog_data_type(&table_col.data_type),
                                source_dataframe.schema(),
                            )?
                            .alias(table_col.name.clone()),
                    );
                }
            }
            source_dataframe.select(projections)?
        } else {
            // No target columns specified, match by position (excluding _row_id)
            let visible_table_columns: Vec<_> = relation
                .columns
                .iter()
                .filter(|c| c.name != "_row_id")
                .collect();
            if source_dataframe.schema().fields().len() != visible_table_columns.len() {
                anyhow::bail!(
                    "INSERT into table with {} columns but SELECT has {} source columns",
                    visible_table_columns.len(),
                    source_dataframe.schema().fields().len()
                );
            }

            let mut projections = Vec::new();
            for (i, table_col) in visible_table_columns.iter().enumerate() {
                projections.push(
                    datafusion::prelude::col(source_dataframe.schema().field(i).name())
                        .alias(table_col.name.clone()),
                );
            }
            source_dataframe.select(projections)?
        };

        let mut stream = projected_dataframe
            .execute_stream()
            .await
            .map_err(sanitize_error)?;
        let mut inserted_row_count = 0;
        let mut current_batch = Vec::new();
        let mut current_rows = 0;

        use futures::StreamExt;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(sanitize_error)?;
            let (batch_row_count, prepared_batch) = prepare_batch_for_storage(batch)?;

            if batch_row_count == 0 {
                continue;
            }

            if !relation.indexes.is_empty() {
                self.validate_batch_against_table_uniqueness(
                    &request.session,
                    &relation,
                    &prepared_batch,
                )
                .await?;
            }

            current_batch.push(prepared_batch);
            current_rows += batch_row_count;
            inserted_row_count += batch_row_count;

            if current_rows >= INSERT_SELECT_PARQUET_ROW_GROUP_SIZE {
                let schema = current_batch[0].schema();
                let key = prefix
                    .clone()
                    .join(format!("{}.parquet", uuid::Uuid::now_v7()).as_str());
                storage::write_parquet_batches(&store, &key, schema, &current_batch).await?;
                current_batch.clear();
                current_rows = 0;
            }
        }

        if !current_batch.is_empty() {
            let schema = current_batch[0].schema();
            let key = prefix
                .clone()
                .join(format!("{}.parquet", uuid::Uuid::now_v7()).as_str());
            storage::write_parquet_batches(&store, &key, schema, &current_batch).await?;
        }

        self.refresh_index_snapshots_after_mutation(&request.session, &relation)
            .await;

        let table_key = format!(
            "{}.{}.{}",
            relation.database, relation.schema, relation.name
        );
        self.file_list_cache.invalidate(&table_key);

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
            | MetadataStatement::CreateUser { .. }
            | MetadataStatement::DropUser { .. }
            | MetadataStatement::CreateGroup { .. }
            | MetadataStatement::AlterGroup { .. }
            | MetadataStatement::DropGroup { .. }
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
                concurrently: _,
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
                let (idx_store, idx_prefix) = table_store_prefix(&preview_relation)?;
                let version = uuid::Uuid::now_v7().to_string();
                let snapshot = self
                    .build_index_snapshot_for_relation(
                        &request.session,
                        &preview_relation,
                        name,
                        &version,
                    )
                    .await?;
                write_index_snapshot(&idx_store, &idx_prefix, &snapshot, &version).await?;

                let (message, _new_session) = match self
                    .control_plane
                    .execute_metadata_statement(&request.session, &statement)
                    .await
                {
                    Ok(result) => result,
                    Err(error) => {
                        let _ = remove_index_snapshot(&idx_store, &idx_prefix, name).await;
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
                    AlterObjectOperation::Rename { new_name } => new_name.clone(),
                    _ => anyhow::bail!("Unsupported index operation"),
                };
                let mut preview_relation = relation.clone();
                let index = preview_relation
                    .indexes
                    .iter_mut()
                    .find(|index| index.name == *name)
                    .ok_or_else(|| anyhow::anyhow!("Index '{}' not found", name))?;
                index.name = new_name.clone();
                let (idx_store, idx_prefix) = table_store_prefix(&preview_relation)?;
                let version = uuid::Uuid::now_v7().to_string();
                let snapshot = self
                    .build_index_snapshot_for_relation(
                        &request.session,
                        &preview_relation,
                        &new_name,
                        &version,
                    )
                    .await?;
                write_index_snapshot(&idx_store, &idx_prefix, &snapshot, &version).await?;

                let message = match self
                    .control_plane
                    .rename_index(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &relation.name,
                        name,
                        &new_name,
                    )
                    .await
                {
                    Ok(msg) => msg,
                    Err(error) => {
                        let _ = remove_index_snapshot(&idx_store, &idx_prefix, &new_name).await;
                        return Err(error);
                    }
                };
                let _ = remove_index_snapshot(&idx_store, &idx_prefix, name).await;

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

                    if let Ok((idx_store, idx_prefix)) = table_store_prefix(relation) {
                        let _ = remove_index_snapshot(&idx_store, &idx_prefix, name).await;
                    }

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
            MetadataStatement::Reindex { ref target } => match target {
                ReindexTarget::Index {
                    database,
                    schema,
                    name,
                    concurrently: _,
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

                    self.invalidate_session_contexts().await;
                    let (idx_store, idx_prefix) = table_store_prefix(&relation)?;
                    let version = uuid::Uuid::now_v7().to_string();
                    let snapshot = self
                        .build_index_snapshot_for_relation(
                            &request.session,
                            &relation,
                            name,
                            &version,
                        )
                        .await?;
                    write_index_snapshot(&idx_store, &idx_prefix, &snapshot, &version).await?;

                    (
                        Arc::new(Schema::empty()),
                        Vec::new(),
                        format!("Index '{}' reindexed successfully.", name),
                        command_outcome("REINDEX", 0),
                        request.session.clone(),
                    )
                }
                ReindexTarget::Table {
                    database,
                    schema,
                    name,
                    concurrently: _,
                } => {
                    let relation = self
                        .control_plane
                        .table_relation(
                            &request.session,
                            database.as_deref(),
                            schema.as_deref(),
                            name,
                        )
                        .await?;
                    let relation_lock = self.relation_lock(&relation).await;
                    let _write_guard = relation_lock.write().await;

                    self.rebuild_all_index_snapshots(&request.session, &relation)
                        .await?;

                    (
                        Arc::new(Schema::empty()),
                        Vec::new(),
                        format!(
                            "Reindexed {} index(es) on '{}.{}.{}'.",
                            relation.indexes.len(),
                            relation.database,
                            relation.schema,
                            relation.name
                        ),
                        command_outcome("REINDEX", 0),
                        request.session.clone(),
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
                let context = DfSessionContext::new_with_config(base_session_config());
                let table_path =
                    datafusion::datasource::listing::ListingTableUrl::parse(&location)?;
                let config = datafusion::datasource::listing::ListingTableConfig::new(table_path)
                    .with_listing_options(datafusion::datasource::listing::ListingOptions::new(
                        Arc::new(
                            datafusion::datasource::file_format::parquet::ParquetFormat::default(),
                        ),
                    ))
                    .infer_schema(&context.state())
                    .await?;
                let arrow_schema = config
                    .file_schema
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("Failed to infer schema for external table"))?;
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
                let location_str = storage_location.to_string_lossy().into_owned();
                let (store, prefix) = storage::store_for_location(&location_str)?;

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
                let row_count =
                    write_dataframe_to_table_snapshot(dataframe, &store, &prefix).await?;

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
            MetadataStatement::SelectInto {
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
                let location_str = storage_location.to_string_lossy().into_owned();
                let (store, prefix) = storage::store_for_location(&location_str)?;

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
                let row_count =
                    write_dataframe_to_table_snapshot(dataframe, &store, &prefix).await?;

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
                    format!("{created_message} {row_count} row(s) materialized by SELECT INTO."),
                    command_outcome("SELECT INTO", row_count as u64),
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
                let location_str = storage_location.to_string_lossy().into_owned();
                let (store, prefix) = storage::store_for_location(&location_str)?;
                let arrow_schema = build_arrow_schema_from_definitions(&columns, false)?;

                persist_empty_table_snapshot(&store, &prefix, &arrow_schema).await?;

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
                let storage_location = relation.storage_path.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Managed table '{}.{}.{}' is missing a storage path",
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })?;
                let (store, prefix) = storage::store_for_location(storage_location)?;
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
                let expected_values = columns.as_ref().map_or_else(
                    || {
                        relation
                            .columns
                            .iter()
                            .filter(|c| c.name != "_row_id")
                            .count()
                    },
                    Vec::len,
                );
                for row in &rows {
                    if row.len() != expected_values {
                        bail!(
                            "Expected {expected_values} value(s) per row, found {}",
                            row.len()
                        );
                    }
                }
                let batch =
                    build_record_batch_from_rows(&arrow_schema, &relation.columns, columns, &rows)?;
                let (row_count, prepared_batch) = prepare_batch_for_storage(batch)?;

                self.validate_batch_against_table_uniqueness(
                    &request.session,
                    &relation,
                    &prepared_batch,
                )
                .await?;

                storage::append_parquet_batch(&store, &prefix, prepared_batch).await?;
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
                let storage_location = relation.storage_path.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Managed table '{}.{}.{}' is missing a storage path",
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })?;
                let (store, prefix) = storage::store_for_location(storage_location)?;
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
                    write_dataframe_to_table_snapshot(updated_dataframe, &store, &prefix).await?;
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
                let storage_location = relation.storage_path.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Managed table '{}.{}.{}' is missing a storage path",
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })?;
                let (store, prefix) = storage::store_for_location(storage_location)?;
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
                    write_dataframe_to_table_snapshot(remaining_dataframe, &store, &prefix).await?;
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
                let storage_location = relation.storage_path.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Managed table '{}.{}.{}' is missing a storage path",
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })?;
                let (store, prefix) = storage::store_for_location(storage_location)?;
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

                persist_empty_table_snapshot(&store, &prefix, &arrow_schema).await?;
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
                        let result = self
                            .control_plane
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
                            .await;

                        if result.is_ok() {
                            let updated_relation = self
                                .control_plane
                                .find_relation(
                                    &request.session,
                                    database.as_deref(),
                                    schema.as_deref(),
                                    &name,
                                )
                                .await?;
                            let _ = self
                                .physical_migrate_relation(&request.session, &updated_relation)
                                .await;
                        }

                        let _ = result?;

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
                        // ...
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

                            let (idx_store, idx_prefix) = table_store_prefix(&preview_relation)?;
                            for index_name in &staged_index_names {
                                let version = uuid::Uuid::now_v7().to_string();
                                let snapshot = self
                                    .build_index_snapshot_for_relation(
                                        &request.session,
                                        &preview_relation,
                                        index_name,
                                        &version,
                                    )
                                    .await?;
                                write_index_snapshot(&idx_store, &idx_prefix, &snapshot, &version)
                                    .await?;
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
                                        let _ = remove_index_snapshot(
                                            &idx_store,
                                            &idx_prefix,
                                            index_name,
                                        )
                                        .await;
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
                            let (store, old_prefix) =
                                storage::store_for_location(storage_path_str)?;
                            // Calculate new storage location by replacing the table name part.
                            // Managed tables use names like <db>__<schema>__<table>.table.parquet
                            let old_suffix = format!("{}.table.parquet", name);
                            let new_suffix = format!("{}.table.parquet", new_name);
                            let new_location_str =
                                storage_path_str.replace(&old_suffix, &new_suffix);
                            let (_, new_prefix) = storage::store_for_location(&new_location_str)?;
                            storage::rename_prefix(&store, &old_prefix, &new_prefix).await?;

                            // 3. Update the storage path in catalog after physical rename
                            self.control_plane
                                .update_relation_storage_path(
                                    &request.session,
                                    database.as_deref(),
                                    schema.as_deref(),
                                    &new_name,
                                    &new_location_str,
                                )
                                .await?;
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
                        let result = self
                            .control_plane
                            .drop_column(
                                &request.session,
                                database.as_deref(),
                                schema.as_deref(),
                                &name,
                                &column_name,
                                if_exists,
                            )
                            .await;

                        if result.is_ok() {
                            let updated_relation = self
                                .control_plane
                                .find_relation(
                                    &request.session,
                                    database.as_deref(),
                                    schema.as_deref(),
                                    &name,
                                )
                                .await?;
                            let _ = self
                                .physical_migrate_relation(&request.session, &updated_relation)
                                .await;
                        }

                        let message = result?;

                        (
                            Arc::new(Schema::empty()),
                            Vec::new(),
                            message,
                            command_outcome("ALTER TABLE", 0),
                            request.session.clone(),
                        )
                    }
                    AlterTableOperation::RenameColumn { old_name, new_name } => {
                        let relation_lock = self.relation_lock(&relation).await;
                        let _write_guard = relation_lock.write().await;

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

                        // Physically rewrite the table to apply the rename
                        let updated_relation = self
                            .control_plane
                            .find_relation(
                                &request.session,
                                database.as_deref(),
                                schema.as_deref(),
                                &name,
                            )
                            .await?;
                        let _ = self
                            .physical_migrate_rename_column(
                                &request.session,
                                &updated_relation,
                                &old_name,
                                &new_name,
                            )
                            .await;

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
                        // ...
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
                                if let Ok((idx_store, idx_prefix)) = table_store_prefix(&relation) {
                                    for index_name in dropped_index_names {
                                        let _ = remove_index_snapshot(
                                            &idx_store,
                                            &idx_prefix,
                                            &index_name,
                                        )
                                        .await;
                                    }
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
                        let result = self
                            .control_plane
                            .alter_column(
                                &request.session,
                                database.as_deref(),
                                schema.as_deref(),
                                &name,
                                &column_name,
                                operation,
                            )
                            .await;

                        if result.is_ok() {
                            let updated_relation = self
                                .control_plane
                                .find_relation(
                                    &request.session,
                                    database.as_deref(),
                                    schema.as_deref(),
                                    &name,
                                )
                                .await?;
                            let _ = self
                                .physical_migrate_relation(&request.session, &updated_relation)
                                .await;
                        }

                        let message = result?;

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
                        let (store, old_obj_prefix) =
                            storage::store_for_location(storage_path_str)?;
                        let old_part = format!("{}__{}__", database_name, name);
                        let new_part = format!("{}__{}__", database_name, new_name);
                        let new_location_str = storage_path_str.replace(&old_part, &new_part);
                        let (_, new_obj_prefix) = storage::store_for_location(&new_location_str)?;
                        storage::rename_prefix(&store, &old_obj_prefix, &new_obj_prefix).await?;
                        self.control_plane
                            .update_relation_storage_path(
                                &request.session,
                                Some(database_name),
                                Some(&new_name),
                                &relation.name,
                                &new_location_str,
                            )
                            .await?;
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
                        .list_relations_for_database(
                            &request.session,
                            &name,
                            CatalogRelationKind::Table,
                        )
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
                            let (store, old_obj_prefix) =
                                storage::store_for_location(storage_path_str)?;
                            let old_part = format!("{}__{}__", name, relation.schema);
                            let new_part = format!("{}__{}__", new_name, relation.schema);
                            let new_location_str = storage_path_str.replace(&old_part, &new_part);
                            let (_, new_obj_prefix) =
                                storage::store_for_location(&new_location_str)?;
                            storage::rename_prefix(&store, &old_obj_prefix, &new_obj_prefix)
                                .await?;
                            self.control_plane
                                .update_relation_storage_path(
                                    &request.session,
                                    Some(&new_name),
                                    Some(&relation.schema),
                                    &relation.name,
                                    &new_location_str,
                                )
                                .await?;
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
                let batch =
                    utf8_record_batch(&["node_id", "kind", "status", "last_heartbeat_ms"], &rows)?;
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
                            if col.nullable {
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
                    // For managed tables, delete all storage objects
                    if rel.external_format.is_none() {
                        if let Some(path_str) = &rel.storage_path {
                            let (store, prefix) = storage::store_for_location(path_str)?;
                            storage::delete_prefix(&store, &prefix).await?;
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

    pub async fn execute_query_batches(
        &self,
        request: &QueryRequest,
    ) -> Result<QueryExecutionResult> {
        self.execute_query(request).await
    }

    #[allow(dead_code)]
    async fn collect_table_rows(
        &self,
        _session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
    ) -> Result<Vec<Vec<String>>> {
        let context = DfSessionContext::new_with_config(base_session_config());
        let storage_path = relation
            .storage_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Missing storage path"))?;
        let full_schema = build_arrow_schema_from_catalog_columns(&relation.columns)?;
        let table_path = listing_table_url_for_storage_location(storage_path)?;
        let config = datafusion::datasource::listing::ListingTableConfig::new(table_path)
            .with_listing_options(datafusion::datasource::listing::ListingOptions::new(
                Arc::new(datafusion::datasource::file_format::parquet::ParquetFormat::default()),
            ))
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

        let config = base_session_config()
            .with_default_catalog_and_schema(&session.database, &session.schema);

        let ctx = DfSessionContext::new_with_config(config);

        let databases = self
            .control_plane
            .list_databases(session)
            .await
            .unwrap_or_else(|_| vec![session.database.clone()]);
        for database in databases {
            let provider_session = SessionContext {
                database: database.clone(),
                ..session.clone()
            };
            let provider = Arc::new(system_catalog::AnalyticsCatalogProvider::new(
                Arc::clone(&self.control_plane),
                provider_session,
            ));

            if database == session.database {
                let pg_catalog = Arc::new(PgCatalogSchemaProvider::new(Arc::clone(
                    &self.control_plane,
                )));
                provider.register_schema("pg_catalog", pg_catalog)?;
            }

            ctx.register_catalog(&database, provider);
        }

        register_postgres_functions(&ctx);

        let mut cache = self.session_context_cache.write().await;
        cache.insert(key, ctx.clone());
        Ok(ctx)
    }

    async fn information_schema_schemata_rows(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<Vec<String>>> {
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

    async fn information_schema_tables_rows(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;

        let mut rows = Vec::new();
        for rel in relations {
            let (table_type, is_insertable) = match rel.kind {
                CatalogRelationKind::Table => ("BASE TABLE", "YES"),
                CatalogRelationKind::View => ("VIEW", "NO"),
            };
            rows.push(vec![
                rel.database,
                rel.schema,
                rel.name,
                table_type.to_string(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                is_insertable.to_string(),
                "NO".to_string(),
                String::new(),
            ]);
        }
        Ok(rows)
    }

    async fn information_schema_columns_rows(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;
        let mut rows = Vec::new();
        for rel in relations {
            for (i, col) in rel
                .columns
                .into_iter()
                .filter(|column| column.name != "_row_id")
                .enumerate()
            {
                rows.push(vec![
                    rel.database.clone(),
                    rel.schema.clone(),
                    rel.name.clone(),
                    col.name,
                    (i + 1).to_string(),
                    col.default_value.unwrap_or_default(),
                    if col.nullable {
                        "YES".to_string()
                    } else {
                        "NO".to_string()
                    },
                    col.data_type.to_ascii_lowercase(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                ]);
            }
        }
        Ok(rows)
    }

    async fn information_schema_views_rows(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let views = self
            .control_plane
            .list_all_relations(session)
            .await?
            .into_iter()
            .filter(|relation| relation.kind == CatalogRelationKind::View)
            .collect::<Vec<_>>();
        Ok(views
            .into_iter()
            .map(|v| {
                vec![
                    v.database,
                    v.schema,
                    v.name,
                    v.definition_sql.unwrap_or_default(),
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
        session: &SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;
        let mut rows = Vec::new();
        for rel in relations {
            for constraint in rel.constraints {
                rows.push(vec![
                    rel.database.clone(),
                    rel.schema.clone(),
                    constraint.name.clone(),
                    rel.database.clone(),
                    rel.schema.clone(),
                    rel.name.clone(),
                    format!("{:?}", constraint.kind).to_ascii_uppercase(),
                    "NO".to_string(),
                    "NO".to_string(),
                    "YES".to_string(),
                    String::new(),
                ]);
            }
            // Add NOT NULL constraints
            for col in rel.columns {
                if !col.nullable {
                    let cname = format!("{}_{}_not_null", rel.name, col.name);
                    rows.push(vec![
                        rel.database.clone(),
                        rel.schema.clone(),
                        cname,
                        rel.database.clone(),
                        rel.schema.clone(),
                        rel.name.clone(),
                        "CHECK".to_string(),
                        "NO".to_string(),
                        "NO".to_string(),
                        "YES".to_string(),
                        String::new(),
                    ]);
                }
            }
        }
        Ok(rows)
    }

    async fn information_schema_key_column_usage_rows(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;
        let mut rows = Vec::new();
        for rel in relations {
            for constraint in rel.constraints {
                if matches!(
                    constraint.kind,
                    CatalogTableConstraintKind::PrimaryKey
                        | CatalogTableConstraintKind::ForeignKey
                        | CatalogTableConstraintKind::Unique
                ) {
                    for (i, col) in constraint.columns.into_iter().enumerate() {
                        rows.push(vec![
                            rel.database.clone(),
                            rel.schema.clone(),
                            constraint.name.clone(),
                            rel.database.clone(),
                            rel.schema.clone(),
                            rel.name.clone(),
                            col,
                            (i + 1).to_string(),
                            String::new(),
                        ]);
                    }
                }
            }
        }
        Ok(rows)
    }

    async fn information_schema_constraint_column_usage_rows(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;
        let mut rows = Vec::new();
        for rel in relations {
            // Include NOT NULL constraints
            for col in &rel.columns {
                if !col.nullable {
                    let cname = format!("{}_{}_not_null", rel.name, col.name);
                    rows.push(vec![
                        rel.database.clone(),
                        rel.schema.clone(),
                        rel.name.clone(),
                        col.name.clone(),
                        rel.database.clone(),
                        rel.schema.clone(),
                        cname,
                    ]);
                }
            }
            // Include explicit constraints
            for constraint in rel.constraints {
                for col in constraint.columns {
                    rows.push(vec![
                        rel.database.clone(),
                        rel.schema.clone(),
                        rel.name.clone(),
                        col,
                        rel.database.clone(),
                        rel.schema.clone(),
                        constraint.name.clone(),
                    ]);
                }
            }
        }
        Ok(rows)
    }

    async fn information_schema_constraint_table_usage_rows(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;
        let mut rows = Vec::new();
        for rel in relations {
            // NOT NULLs
            for col in &rel.columns {
                if !col.nullable {
                    let cname = format!("{}_{}_not_null", rel.name, col.name);
                    rows.push(vec![
                        rel.database.clone(),
                        rel.schema.clone(),
                        rel.name.clone(),
                        rel.database.clone(),
                        rel.schema.clone(),
                        cname,
                    ]);
                }
            }
            for constraint in rel.constraints {
                rows.push(vec![
                    rel.database.clone(),
                    rel.schema.clone(),
                    rel.name.clone(),
                    rel.database.clone(),
                    rel.schema.clone(),
                    constraint.name.clone(),
                ]);
            }
        }
        Ok(rows)
    }

    async fn information_schema_referential_constraints_rows(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;
        let mut rows = Vec::new();
        for rel in &relations {
            for constraint in &rel.constraints {
                if let CatalogTableConstraintKind::ForeignKey = constraint.kind {
                    let referenced_database = constraint
                        .referenced_database
                        .clone()
                        .unwrap_or_else(|| session.database.clone());
                    let referenced_schema = constraint
                        .referenced_schema
                        .clone()
                        .unwrap_or_else(|| session.schema.clone());
                    let referenced_table = constraint.referenced_table.clone().unwrap_or_default();
                    let unique_constraint_name = relations
                        .iter()
                        .find(|candidate| {
                            candidate.database == referenced_database
                                && candidate.schema == referenced_schema
                                && candidate.name == referenced_table
                        })
                        .and_then(|candidate| {
                            candidate.constraints.iter().find(|candidate_constraint| {
                                matches!(
                                    candidate_constraint.kind,
                                    CatalogTableConstraintKind::PrimaryKey
                                        | CatalogTableConstraintKind::Unique
                                )
                            })
                        })
                        .map(|constraint| constraint.name.clone())
                        .unwrap_or_else(|| referenced_table.clone());
                    rows.push(vec![
                        rel.database.clone(),
                        rel.schema.clone(),
                        constraint.name.clone(),
                        referenced_database,
                        referenced_schema,
                        unique_constraint_name,
                        "MATCH SIMPLE".to_string(),
                        "NO ACTION".to_string(),
                        "NO ACTION".to_string(),
                    ]);
                }
            }
        }
        Ok(rows)
    }

    async fn best_index_match(
        &self,
        session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
        statement: &IndexedSelectStatement,
    ) -> Result<Option<(analyticsdb_control::CatalogIndex, Vec<String>)>> {
        let mut best_match: Option<(analyticsdb_control::CatalogIndex, Vec<String>, usize, bool)> =
            None;

        let (idx_store, idx_prefix) = table_store_prefix(relation)?;
        for index in &relation.indexes {
            let Some(snapshot) = read_index_snapshot(&idx_store, &idx_prefix, &index.name).await?
            else {
                continue;
            };
            let Some((score, has_range, row_ids)) = self
                .candidate_row_ids_from_snapshot(
                    session,
                    relation,
                    index,
                    &snapshot,
                    &statement.predicates,
                )
                .await?
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

    async fn candidate_row_ids_from_snapshot(
        &self,
        _session: &SessionContext,
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

        // Use a fresh context for internal index lookups to avoid catalog registration issues
        let df_context = DfSessionContext::new_with_config(base_session_config());
        let (store, table_prefix) = table_store_prefix(relation)?;
        let data_key = index_data_key(&table_prefix, &index.name, &snapshot.entries_object);
        if !storage::object_exists(&store, &data_key).await? {
            return Ok(None);
        }

        let data_local_path = format!("/{}", data_key.as_ref());
        let mut df = df_context
            .read_parquet(&data_local_path, Default::default())
            .await
            .map_err(sanitize_error)?;

        for (col_name, predicate) in predicates {
            let col_expr = col(col_name);
            let filter_expr = match predicate {
                IndexPredicate::Eq(val) => col_expr.eq(lit(val.clone())),
                IndexPredicate::In(vals) => {
                    col_expr.in_list(vals.iter().map(|v| lit(v.clone())).collect(), false)
                }
                IndexPredicate::Range { lower, upper } => {
                    let mut expr = lit(true);
                    if let Some((val, inclusive)) = lower {
                        let lower_expr = if *inclusive {
                            col_expr.clone().gt_eq(lit(val.clone()))
                        } else {
                            col_expr.clone().gt(lit(val.clone()))
                        };
                        expr = expr.and(lower_expr);
                    }
                    if let Some((val, inclusive)) = upper {
                        let upper_expr = if *inclusive {
                            col_expr.clone().lt_eq(lit(val.clone()))
                        } else {
                            col_expr.clone().lt(lit(val.clone()))
                        };
                        expr = expr.and(upper_expr);
                    }
                    expr
                }
            };
            df = df.filter(filter_expr).map_err(sanitize_error)?;
        }

        let batches = df
            .select(vec![col("_row_id")])
            .map_err(sanitize_error)?
            .collect()
            .await
            .map_err(sanitize_error)?;

        let mut row_ids = Vec::new();
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                if let Some(val) = array_value_to_string(batch.column(0), row_idx).ok() {
                    row_ids.push(val);
                }
            }
        }

        Ok(Some((matched_prefix_len, has_range, row_ids)))
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
        fields.push(Field::new("_row_id", DataType::Utf8, true));
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

async fn build_partition_read_schema(
    ctx: &DfSessionContext,
    paths: Vec<&str>,
    columns: &[CatalogColumn],
) -> Result<SchemaRef> {
    let mut schema = build_arrow_schema_from_catalog_columns(columns)?;
    if !schema
        .fields()
        .iter()
        .any(|field| matches!(field.data_type(), DataType::Utf8))
    {
        return Ok(schema);
    }

    let sample_df = ctx
        .read_parquet(paths, ParquetReadOptions::default())
        .await
        .map_err(sanitize_error)?;
    let inferred_schema = Arc::new(sample_df.schema().as_arrow().as_ref().clone());
    let sample_batches = sample_df
        .limit(0, Some(1024))
        .map_err(sanitize_error)?
        .collect()
        .await
        .map_err(sanitize_error)?;

    schema = refine_utf8_partition_schema_from_sample(&schema, &inferred_schema, &sample_batches);
    Ok(schema)
}

fn refine_utf8_partition_schema_from_sample(
    schema: &SchemaRef,
    inferred_schema: &SchemaRef,
    sample_batches: &[RecordBatch],
) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            if !matches!(field.data_type(), DataType::Utf8) || field.name() == "_row_id" {
                return Arc::clone(field);
            }

            if let Ok(inferred_idx) = inferred_schema.index_of(field.name()) {
                let inferred_field = inferred_schema.field(inferred_idx);
                if !matches!(inferred_field.data_type(), DataType::Utf8) {
                    return Arc::new(Field::new(
                        field.name(),
                        inferred_field.data_type().clone(),
                        field.is_nullable(),
                    ));
                }
            }

            match infer_utf8_partition_column_type(field.name(), sample_batches) {
                Some(data_type) => {
                    Arc::new(Field::new(field.name(), data_type, field.is_nullable()))
                }
                None => Arc::clone(field),
            }
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

fn infer_utf8_partition_column_type(
    column_name: &str,
    sample_batches: &[RecordBatch],
) -> Option<DataType> {
    let mut saw_value = false;
    let mut all_numeric = true;
    let mut all_date = true;

    for batch in sample_batches {
        let Ok(idx) = batch.schema().index_of(column_name) else {
            return None;
        };
        let Some(values) = batch.column(idx).as_any().downcast_ref::<StringArray>() else {
            return None;
        };

        for row in 0..values.len() {
            if values.is_null(row) {
                continue;
            }
            let value = values.value(row).trim();
            if value.is_empty() {
                all_numeric = false;
                all_date = false;
                continue;
            }
            saw_value = true;
            if value.parse::<f64>().is_err() {
                all_numeric = false;
            }
            if chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_err() {
                all_date = false;
            }
        }
    }

    if !saw_value {
        return None;
    }
    if all_date {
        Some(DataType::Date32)
    } else if all_numeric {
        Some(DataType::Float64)
    } else {
        None
    }
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
            TableConstraintDefinition::PrimaryKey { .. } => (
                CatalogTableConstraintKind::PrimaryKey,
                None,
                None,
                None,
                Vec::new(),
            ),
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
            TableConstraintDefinition::Unique { .. } => (
                CatalogTableConstraintKind::Unique,
                None,
                None,
                None,
                Vec::new(),
            ),
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
    catalog_columns: &[CatalogColumn],
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
                            if !field.is_nullable() {
                                bail!(
                                    "Column '{}' must be provided because it is NOT NULL",
                                    field.name()
                                );
                            }
                            values.push(None);
                        } else {
                            values.push(Some(normalize_insert_value(&row[idx], field.data_type())));
                        }
                    } else if let Some(default_value) =
                        default_value_for_column(catalog_columns, field.name(), field.data_type())
                    {
                        values.push(Some(default_value));
                    } else if !field.is_nullable() {
                        bail!(
                            "Column '{}' must be provided because it is NOT NULL",
                            field.name()
                        );
                    } else {
                        values.push(None);
                    }
                } else if let Some(default_value) =
                    default_value_for_column(catalog_columns, field.name(), field.data_type())
                {
                    values.push(Some(default_value));
                } else if !field.is_nullable() {
                    bail!(
                        "Column '{}' must be provided because it is NOT NULL",
                        field.name()
                    );
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

fn default_value_for_column(
    catalog_columns: &[CatalogColumn],
    column_name: &str,
    data_type: &DataType,
) -> Option<String> {
    let raw = catalog_columns
        .iter()
        .find(|column| column.name == column_name)?
        .default_value
        .as_deref()
        .unwrap_or_else(|| {
            if column_name.eq_ignore_ascii_case("created_at") {
                "CURRENT_TIMESTAMP"
            } else {
                ""
            }
        })
        .trim();
    if raw.is_empty() {
        return None;
    }

    if raw.eq_ignore_ascii_case("CURRENT_TIMESTAMP()")
        || raw.eq_ignore_ascii_case("CURRENT_TIMESTAMP")
        || raw.eq_ignore_ascii_case("NOW()")
    {
        return Some(chrono::Utc::now().to_rfc3339());
    }

    Some(normalize_insert_value(raw, data_type))
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
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
) -> Result<usize> {
    let mut stream = df.execute_stream().await.map_err(sanitize_error)?;
    let mut row_count = 0;
    let mut prepared_batches = Vec::new();

    use futures::StreamExt;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(sanitize_error)?;
        let (batch_row_count, prepared_batch) = prepare_batch_for_storage(batch)?;
        row_count += batch_row_count;
        prepared_batches.push(prepared_batch);
    }

    storage::clear_parquet_files(store, prefix).await?;

    for prepared_batch in prepared_batches {
        storage::append_parquet_batch(store, prefix, prepared_batch).await?;
    }
    Ok(row_count)
}

fn prepare_batch_for_storage(batch: RecordBatch) -> Result<(usize, RecordBatch)> {
    let num_rows = batch.num_rows();
    let has_row_id = batch
        .schema()
        .fields()
        .iter()
        .any(|f| f.name() == "_row_id");

    if has_row_id {
        let mut columns = batch.columns().to_vec();
        let row_id_col_idx = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == "_row_id")
            .unwrap();
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
            Ok((num_rows, RecordBatch::try_new(batch.schema(), columns)?))
        } else {
            Ok((num_rows, batch))
        }
    } else {
        // We need to inject _row_id
        let mut columns = batch.columns().to_vec();
        let row_ids: Vec<String> = (0..num_rows)
            .map(|_| uuid::Uuid::now_v7().to_string())
            .collect();
        columns.push(Arc::new(StringArray::from(row_ids)));

        let mut fields = batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("_row_id", DataType::Utf8, true)));
        let new_schema = Arc::new(Schema::new(fields));

        Ok((num_rows, RecordBatch::try_new(new_schema, columns)?))
    }
}

async fn persist_empty_table_snapshot(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
    schema: &SchemaRef,
) -> Result<()> {
    storage::clear_parquet_files(store, prefix).await?;
    let key = prefix.clone().join("empty.parquet");
    storage::write_empty_parquet(store, &key, schema).await
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
    sql: &str,
    _table: &str,
    columns: &[&str],
    rows: &[Vec<String>],
) -> Result<(RecordBatch, usize)> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    let Some(Statement::Query(query)) = statements.first() else {
        let batch = utf8_record_batch(columns, rows)?;
        return Ok((batch, rows.len()));
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        let batch = utf8_record_batch(columns, rows)?;
        return Ok((batch, rows.len()));
    };

    let mut filtered_rows = rows.to_vec();
    if let Some(selection) = &select.selection {
        filtered_rows.retain(|row| metadata_row_matches(selection, columns, row));
    }

    if let Some(order_by) = &query.order_by {
        if let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind {
            filtered_rows.sort_by(|left, right| {
                for order_expr in exprs {
                    let Some(column_name) = metadata_expr_column_name(&order_expr.expr) else {
                        continue;
                    };
                    let Some(idx) = columns.iter().position(|c| *c == column_name) else {
                        continue;
                    };
                    let ord = left[idx].cmp(&right[idx]);
                    if ord != std::cmp::Ordering::Equal {
                        return if order_expr.options.asc == Some(false) {
                            ord.reverse()
                        } else {
                            ord
                        };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
    }

    let projected = metadata_projection_indices(select, columns)?;
    let projected_columns = projected
        .iter()
        .map(|idx| columns[*idx])
        .collect::<Vec<_>>();
    let projected_rows = filtered_rows
        .iter()
        .map(|row| {
            projected
                .iter()
                .map(|idx| row[*idx].clone())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let batch = utf8_record_batch(&projected_columns, &projected_rows)?;
    let count = projected_rows.len();
    Ok((batch, count))
}

fn metadata_projection_indices(
    select: &sqlparser::ast::Select,
    columns: &[&str],
) -> Result<Vec<usize>> {
    if select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard(_)))
    {
        return Ok((0..columns.len()).collect());
    }

    let mut indices = Vec::new();
    for item in &select.projection {
        let Some(column_name) = (match item {
            SelectItem::UnnamedExpr(expr) => metadata_expr_column_name(expr),
            SelectItem::ExprWithAlias { expr, .. } => metadata_expr_column_name(expr),
            _ => None,
        }) else {
            bail!("Unsupported metadata projection '{}'", item);
        };
        let Some(index) = columns.iter().position(|c| *c == column_name) else {
            bail!("Unknown metadata projection column '{}'", column_name);
        };
        indices.push(index);
    }
    Ok(indices)
}

fn metadata_row_matches(expr: &Expr, columns: &[&str], row: &[String]) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } if matches!(op, BinaryOperator::Eq) => {
            let Some(column_name) = metadata_expr_column_name(left) else {
                return true;
            };
            let Some(idx) = columns.iter().position(|c| *c == column_name) else {
                return true;
            };
            metadata_literal_value(right)
                .map(|value| row[idx] == value)
                .unwrap_or(true)
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let Some(column_name) = metadata_expr_column_name(expr) else {
                return true;
            };
            let Some(idx) = columns.iter().position(|c| *c == column_name) else {
                return true;
            };
            let matched = list
                .iter()
                .filter_map(metadata_literal_value)
                .any(|value| row[idx] == value);
            if *negated {
                !matched
            } else {
                matched
            }
        }
        Expr::Nested(expr) => metadata_row_matches(expr, columns, row),
        Expr::BinaryOp { left, op, right } if matches!(op, BinaryOperator::And) => {
            metadata_row_matches(left, columns, row) && metadata_row_matches(right, columns, row)
        }
        _ => true,
    }
}

fn metadata_expr_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.as_str()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.value.as_str()),
        _ => None,
    }
}

fn metadata_literal_value(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(value) => match &value.value {
            sqlparser::ast::Value::SingleQuotedString(value)
            | sqlparser::ast::Value::DoubleQuotedString(value)
            | sqlparser::ast::Value::Number(value, _) => Some(value.clone()),
            _ => None,
        },
        _ => None,
    }
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

    let projection = match select_projection_columns(&select.projection)? {
        Some(p) => Some(p),
        None => {
            // If select_projection_columns returns None, it might be a wildcard (*)
            // or a complex projection. We only support wildcards in indexed select
            // if all columns are simple.
            if select
                .projection
                .iter()
                .any(|item| !matches!(item, SelectItem::Wildcard(_)))
            {
                return Ok(None);
            }
            None
        }
    };
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
            BinaryOperator::And => Ok(extract_index_predicates(left, predicates)?
                && extract_index_predicates(right, predicates)?),
            BinaryOperator::Eq => Ok(store_eq_predicate(predicates, left, right)
                .or_else(|| store_eq_predicate(predicates, right, left))
                .is_some()),
            BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq => Ok(store_range_binary_predicate(
                predicates, left, op, right, true,
            )
            .or_else(|| store_range_binary_predicate(predicates, right, op, left, false))
            .is_some()),
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
        (BinaryOperator::Gt, true) | (BinaryOperator::Lt, false) => lower = Some((value, false)),
        (BinaryOperator::GtEq, true) | (BinaryOperator::LtEq, false) => lower = Some((value, true)),
        (BinaryOperator::Lt, true) | (BinaryOperator::Gt, false) => upper = Some((value, false)),
        (BinaryOperator::LtEq, true) | (BinaryOperator::GtEq, false) => upper = Some((value, true)),
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

/// Returns `(database, schema, table)` if `sql` is a plain SELECT that reads
/// exactly one managed table and has no sub-queries, CTEs, or JOINs.
///
/// The result is used by `try_execute_distributed_select` to decide whether the
/// query can be fanned out to Compute nodes.  Conservative: returns `None` for
/// anything that doesn't look like `SELECT … FROM [db.][ schema.]table [WHERE …]`.
fn parse_plain_select_table(sql: &str) -> Option<(Option<String>, Option<String>, String)> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, trimmed).ok()?;
    let statement = statements.into_iter().next()?;
    let Statement::Query(query) = statement else {
        return None;
    };
    // Reject CTEs.
    if query.with.is_some() {
        return None;
    }
    let SetExpr::Select(select) = *query.body else {
        return None;
    };
    // Exactly one FROM item, no JOINs.
    if select.from.len() != 1 {
        return None;
    }
    let table_with_joins = &select.from[0];
    if !table_with_joins.joins.is_empty() {
        return None;
    }
    let TableFactor::Table { name, .. } = &table_with_joins.relation else {
        return None;
    };
    let parts: Vec<String> = name.0.iter().map(|i| i.to_string()).collect();
    match parts.as_slice() {
        [table] => Some((None, None, table.clone())),
        [schema, table] => Some((None, Some(schema.clone()), table.clone())),
        [db, schema, table] => Some((Some(db.clone()), Some(schema.clone()), table.clone())),
        _ => None,
    }
}

/// Rewrites `sql` so that the FROM clause referencing `source_table` (any level
/// of qualification) is replaced with `__partition__`.  The caller registers
/// the worker's local Parquet files as a DataFusion table named `__partition__`
/// before executing the returned SQL, which preserves WHERE, LIMIT, projections,
/// and other clauses so DataFusion can push them into the scan.
///
/// Returns `None` if the SQL cannot be parsed or the table is not found.
fn rewrite_sql_for_partition(sql: &str, source_table: &str) -> Option<String> {
    use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

    let trimmed = sql.trim().trim_end_matches(';').trim();
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, trimmed).ok()?;
    let statement = statements.get_mut(0)?;

    let Statement::Query(ref mut query) = statement else {
        return None;
    };
    let SetExpr::Select(ref mut select) = *query.body else {
        return None;
    };

    let mut replaced = false;
    for twj in &mut select.from {
        if let TableFactor::Table {
            ref mut name,
            ref mut alias,
            ..
        } = twj.relation
        {
            let last = name.0.last().and_then(|p| match p {
                ObjectNamePart::Identifier(id) => Some(id.value.clone()),
                _ => None,
            });
            if last
                .as_deref()
                .map(|t| t.eq_ignore_ascii_case(source_table))
                .unwrap_or(false)
            {
                *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                    "__partition__",
                ))]);
                // Remove any alias so the query can reference columns directly.
                *alias = None;
                replaced = true;
            }
        }
    }

    if replaced {
        Some(statement.to_string())
    } else {
        None
    }
}

fn distributed_aggregate_plan(sql: &str, source_table: &str) -> Option<(String, String)> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let mut partition_sql = rewrite_sql_for_partition(trimmed, source_table)?;

    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, &partition_sql).ok()?;
    let statement = statements.get_mut(0)?;
    let Statement::Query(ref mut query) = statement else {
        return None;
    };
    let SetExpr::Select(ref mut select) = *query.body else {
        return None;
    };
    if !matches!(&select.group_by, GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty())
        || select.having.is_some()
        || select.distinct.is_some()
    {
        return None;
    }

    let mut worker_items = Vec::new();
    let mut final_items = Vec::new();
    for (idx, item) in select.projection.iter().enumerate() {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, quote_sql_identifier(&expr.to_string())),
            SelectItem::ExprWithAlias { expr, alias } => {
                (expr, quote_sql_identifier(alias.value.as_str()))
            }
            _ => return None,
        };
        let aggregate = aggregate_expr_parts(expr)?;
        let output = format!("__adb_agg_{idx}");
        match aggregate.function.as_str() {
            "count" => {
                worker_items.push(format!(
                    "COUNT({}) AS {}",
                    aggregate.argument,
                    quote_sql_identifier(&output)
                ));
                final_items.push(format!("SUM({}) AS {alias}", quote_sql_identifier(&output)));
            }
            "sum" => {
                worker_items.push(format!(
                    "SUM({}) AS {}",
                    aggregate.argument,
                    quote_sql_identifier(&output)
                ));
                final_items.push(format!("SUM({}) AS {alias}", quote_sql_identifier(&output)));
            }
            "min" => {
                worker_items.push(format!(
                    "MIN({}) AS {}",
                    aggregate.argument,
                    quote_sql_identifier(&output)
                ));
                final_items.push(format!("MIN({}) AS {alias}", quote_sql_identifier(&output)));
            }
            "max" => {
                worker_items.push(format!(
                    "MAX({}) AS {}",
                    aggregate.argument,
                    quote_sql_identifier(&output)
                ));
                final_items.push(format!("MAX({}) AS {alias}", quote_sql_identifier(&output)));
            }
            "avg" => {
                let sum_output = format!("{output}_sum");
                let count_output = format!("{output}_count");
                worker_items.push(format!(
                    "SUM({}) AS {}, COUNT({}) AS {}",
                    aggregate.argument,
                    quote_sql_identifier(&sum_output),
                    aggregate.argument,
                    quote_sql_identifier(&count_output)
                ));
                final_items.push(format!(
                    "SUM({}) / SUM({}) AS {alias}",
                    quote_sql_identifier(&sum_output),
                    quote_sql_identifier(&count_output)
                ));
            }
            _ => return None,
        }
    }

    select.projection = vec![SelectItem::UnnamedExpr(Expr::Value(
        sqlparser::ast::Value::Number("1".to_string(), false).into(),
    ))];
    partition_sql = statement.to_string();
    let re = regex::Regex::new(r"(?is)^\s*SELECT\s+1\s+FROM\s+").ok()?;
    let worker_sql = re
        .replace(
            &partition_sql,
            format!("SELECT {} FROM ", worker_items.join(", ")).as_str(),
        )
        .to_string();
    let final_sql = format!("SELECT {} FROM __partition__", final_items.join(", "));
    Some((worker_sql, final_sql))
}

fn quote_sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn select_projection_contains_function(sql: &str) -> bool {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let dialect = PostgreSqlDialect {};
    let Ok(statements) = Parser::parse_sql(&dialect, trimmed) else {
        return false;
    };
    let Some(Statement::Query(query)) = statements.first() else {
        return false;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select.projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(Expr::Function(_)) => true,
        SelectItem::ExprWithAlias {
            expr: Expr::Function(_),
            ..
        } => true,
        _ => false,
    })
}

struct AggregateExprParts {
    function: String,
    argument: String,
}

fn aggregate_expr_parts(expr: &Expr) -> Option<AggregateExprParts> {
    let Expr::Function(function) = expr else {
        return None;
    };
    if function.filter.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
        || !matches!(function.parameters, FunctionArguments::None)
    {
        return None;
    }

    let function_name = function
        .name
        .to_string()
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .trim_matches('"')
        .to_ascii_lowercase();

    let FunctionArguments::List(args) = &function.args else {
        return None;
    };
    if args.duplicate_treatment.is_some() || !args.clauses.is_empty() || args.args.len() != 1 {
        return None;
    }
    let FunctionArg::Unnamed(arg) = &args.args[0] else {
        return None;
    };
    let argument = match arg {
        FunctionArgExpr::Wildcard => "*".to_string(),
        FunctionArgExpr::Expr(expr) => expr.to_string(),
        FunctionArgExpr::QualifiedWildcard(_) => return None,
    };
    if argument == "*" && function_name != "count" {
        return None;
    }

    Some(AggregateExprParts {
        function: function_name,
        argument,
    })
}

fn parse_insert_select_statement(sql: &str) -> Result<Option<InsertSelectStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("INSERT INTO ") {
        return Ok(None);
    }

    // Use regex to find SELECT and extract table name. Use (?is) for case-insensitive and dot-matches-all.
    let re =
        regex::Regex::new(r"(?is)INSERT\s+INTO\s+([^\s\(\)]+)\s*(\([^\)]+\))?\s*(SELECT\s+.*)")?;
    if let Some(caps) = re.captures(trimmed) {
        let name = caps
            .get(1)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();
        let columns = caps.get(2).map(|m| {
            m.as_str()
                .trim_start_matches('(')
                .trim_end_matches(')')
                .split(',')
                .map(|s| s.trim().to_string())
                .collect::<Vec<_>>()
        });
        let query_sql = caps
            .get(3)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();

        return Ok(Some(InsertSelectStatement {
            database: None,
            schema: None,
            name,
            columns,
            query_sql,
        }));
    }

    Ok(None)
}

fn table_store_prefix(
    relation: &analyticsdb_control::CatalogRelation,
) -> Result<(Arc<dyn ObjectStore>, OPath)> {
    let location = relation.storage_path.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "Managed table '{}.{}.{}' is missing a storage path",
            relation.database,
            relation.schema,
            relation.name
        )
    })?;
    storage::store_for_location(location)
}

fn index_manifest_key(table_prefix: &OPath, index_name: &str) -> OPath {
    table_prefix
        .clone()
        .join(".analyticsdb_indexes")
        .join(index_name)
        .join("manifest.json")
}

fn index_version_metadata_key(table_prefix: &OPath, index_name: &str, version: &str) -> OPath {
    table_prefix
        .clone()
        .join(".analyticsdb_indexes")
        .join(index_name)
        .join("versions")
        .join(version)
        .join("metadata.json")
}

fn index_data_key(table_prefix: &OPath, index_name: &str, entries_object: &str) -> OPath {
    table_prefix
        .clone()
        .join(".analyticsdb_indexes")
        .join(index_name)
        .join("versions")
        .join(entries_object)
        .join("data.parquet")
}

fn index_prefix_key(table_prefix: &OPath, index_name: &str) -> OPath {
    table_prefix
        .clone()
        .join(".analyticsdb_indexes")
        .join(index_name)
}

fn listing_table_url_for_storage_location(
    storage_location: &str,
) -> Result<datafusion::datasource::listing::ListingTableUrl> {
    datafusion::datasource::listing::ListingTableUrl::parse(storage_location).map_err(Into::into)
}

async fn read_index_snapshot(
    store: &Arc<dyn ObjectStore>,
    table_prefix: &OPath,
    index_name: &str,
) -> Result<Option<IndexSnapshot>> {
    let manifest_key = index_manifest_key(table_prefix, index_name);
    let Some(manifest_json) = storage::read_json(store, &manifest_key).await? else {
        return Ok(None);
    };
    let manifest: IndexSnapshotManifest = serde_json::from_str(&manifest_json)?;
    let metadata_key = index_version_metadata_key(table_prefix, index_name, &manifest.version);
    let Some(metadata_json) = storage::read_json(store, &metadata_key).await? else {
        anyhow::bail!(
            "Published index snapshot for index '{}' is missing its metadata object",
            index_name
        );
    };
    Ok(Some(serde_json::from_str(&metadata_json)?))
}

async fn write_index_snapshot(
    store: &Arc<dyn ObjectStore>,
    table_prefix: &OPath,
    snapshot: &IndexSnapshot,
    version: &str,
) -> Result<()> {
    let metadata_key = index_version_metadata_key(table_prefix, &snapshot.index, version);
    storage::write_json(
        store,
        &metadata_key,
        &serde_json::to_string_pretty(snapshot)?,
    )
    .await?;

    let manifest = IndexSnapshotManifest {
        version: version.to_string(),
        snapshot_object: snapshot.entries_object.clone(),
        row_count: snapshot.row_count,
        published_at_epoch_ms: chrono::Utc::now().timestamp_millis(),
    };
    let manifest_key = index_manifest_key(table_prefix, &snapshot.index);
    storage::write_json(
        store,
        &manifest_key,
        &serde_json::to_string_pretty(&manifest)?,
    )
    .await?;
    Ok(())
}

async fn remove_index_snapshot(
    store: &Arc<dyn ObjectStore>,
    table_prefix: &OPath,
    index_name: &str,
) -> Result<()> {
    let prefix = index_prefix_key(table_prefix, index_name);
    storage::delete_prefix(store, &prefix).await
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
        path.push(format!(
            "analyticsdb-engine-test-{}.json",
            uuid::Uuid::now_v7()
        ));
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn test_regex_insert() {
        let sql = "INSERT INTO orders (id, customer_name, order_value, date_of_purchase)
SELECT
  n,
  'Customer ' || n,
  ROUND((10 + random() * 990)::numeric, 2),
  NOW() - (random() * INTERVAL '5 years')
FROM generate_series(1, 1000000) AS s(n)
";
        let result = crate::parse_insert_select_statement(sql).unwrap();
        assert!(result.is_some(), "Parser failed to match INSERT statement");
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

    #[tokio::test(flavor = "multi_thread")]
    async fn create_unique_index_concurrently_works() {
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
            .unwrap();

        engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO customers VALUES (1, 'one'), (2, 'two')".to_string(),
                session: session.clone(),
            })
            .await
            .unwrap();

        // Test CREATE UNIQUE INDEX CONCURRENTLY
        let result = engine
            .execute_query(&QueryRequest {
                sql: "CREATE UNIQUE INDEX CONCURRENTLY customers_name_idx ON customers (name)"
                    .to_string(),
                session: session.clone(),
            })
            .await
            .expect("CREATE UNIQUE INDEX CONCURRENTLY should succeed");

        assert!(result
            .message
            .contains("Index 'customers_name_idx' created successfully"));

        // Verify index is used
        let result = engine
            .execute_query(&QueryRequest {
                sql: "SELECT id, name FROM customers WHERE name = 'one'".to_string(),
                session: session.clone(),
            })
            .await
            .expect("Query should succeed");
        let response = result.to_query_response();
        assert_eq!(response.rows.len(), 1);
        assert!(response
            .message
            .contains("using index 'customers_name_idx'"));

        cleanup_catalog_artifacts(&catalog_path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_index_join_pg_class_works() {
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
                sql: "CREATE TABLE test_idx (id INT)".to_string(),
                session: session.clone(),
            })
            .await
            .unwrap();

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE INDEX test_idx_idx ON test_idx (id)".to_string(),
                session: session.clone(),
            })
            .await
            .unwrap();

        let result = engine
            .execute_query(&QueryRequest {
                sql: "SELECT relname, indisvalid FROM pg_index i JOIN pg_class c ON i.indexrelid = c.oid WHERE relname = 'test_idx_idx'".to_string(),
                session: session.clone(),
            })
            .await
            .expect("Query should succeed");

        let response = result.to_query_response();
        assert_eq!(response.rows.len(), 1);
        assert_eq!(response.rows[0][0], "test_idx_idx");
        assert_eq!(response.rows[0][1], "true");

        cleanup_catalog_artifacts(&catalog_path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn repro_user_insert_error() {
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
                sql: "CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_name TEXT NOT NULL, order_value NUMERIC(12,2) NOT NULL, date_of_purchase DATE NOT NULL)".to_string(),
                session: session.clone(),
            })
            .await
            .unwrap();

        let sql = "INSERT INTO orders (id, customer_name, order_value, date_of_purchase)
SELECT
  n,
  'Customer ' || n,
  ROUND((10 + random() * 990)::numeric, 2),
  NOW() - (random() * INTERVAL '5 years')
FROM generate_series(1, 1000) AS s(n)";

        let result = engine
            .execute_query(&QueryRequest {
                sql: sql.to_string(),
                session: session.clone(),
            })
            .await;

        cleanup_catalog_artifacts(&catalog_path);
        result.expect("INSERT … SELECT with rewritten interval expression should succeed");
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn repro_datafusion_insert_error() {
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
                sql: "CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_name TEXT NOT NULL, order_value NUMERIC(12,2) NOT NULL, date_of_purchase DATE NOT NULL)".to_string(),
                session: session.clone(),
            })
            .await
            .unwrap();

        let sql = "INSERT INTO orders (id, customer_name, order_value, date_of_purchase)
SELECT n, 'Customer ' || n, ROUND((10 + random() * 990)::numeric, 2), NOW() - (random() * INTERVAL '5 years')
FROM generate_series(1, 10) AS s(n)";

        // Force execution through DataFusion
        let context = engine.create_session_context(&session).await.unwrap();
        let result = context.sql(sql).await;

        match result {
            Ok(_) => println!("Success"),
            Err(e) => println!("DataFusion error: {}", e),
        }
        cleanup_catalog_artifacts(&catalog_path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_partition_runs_scalar_sql() {
        let catalog_path = temp_catalog_path();
        let engine = PrototypeEngine::from_catalog_path(&catalog_path)
            .await
            .expect("engine should initialize");

        let req = crate::distributed::ExecutePartitionRequest {
            query_id: "test-partition-q1".to_string(),
            sql: "SELECT 1 + 1 AS result".to_string(),
            session: SessionContext {
                protocol: Protocol::Embedded,
                ..SessionContext::default()
            },
            partition_files: vec![],
            source_columns: vec![],
        };

        let result = engine
            .execute_partition(&req)
            .await
            .expect("partition should execute");
        assert_eq!(result.query_id, "test-partition-q1");
        assert_eq!(
            result.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            1
        );
        cleanup_catalog_artifacts(&catalog_path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_partition_uses_catalog_schema_for_order_aggregates() {
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
                sql: "CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_name TEXT NOT NULL, order_value NUMERIC(12,2) NOT NULL, date_of_purchase DATE NOT NULL)".to_string(),
                session: session.clone(),
            })
            .await
            .expect("orders table should be created");
        engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO orders (id, customer_name, order_value, date_of_purchase) SELECT 1, 'A', CAST(10.50 AS NUMERIC(12,2)), CAST('2024-01-15' AS DATE) UNION ALL SELECT 2, 'B', CAST(20.25 AS NUMERIC(12,2)), CAST('2019-06-01' AS DATE)".to_string(),
                session: session.clone(),
            })
            .await
            .expect("orders should be inserted");

        let relation = engine
            .control_plane()
            .table_relation(&session, None, None, "orders")
            .await
            .expect("orders relation should exist");
        let storage_path = relation
            .storage_path
            .clone()
            .expect("orders should have managed storage");
        let (store, prefix) = crate::storage::store_for_location(&storage_path).unwrap();
        let partition_files = crate::storage::list_parquet_files(&store, &prefix)
            .await
            .unwrap();
        assert!(
            !partition_files.is_empty(),
            "orders table must have parquet files"
        );

        let req = crate::distributed::ExecutePartitionRequest {
            query_id: "test-partition-orders-agg".to_string(),
            sql: "SELECT COUNT(*) AS order_count, SUM(order_value) AS total_order_value, AVG(order_value) AS avg_order_value, MIN(date_of_purchase) AS first_order_date, MAX(date_of_purchase) AS last_order_date FROM __partition__ WHERE date_of_purchase >= DATE '2020-01-01'".to_string(),
            session: session.clone(),
            partition_files: partition_files.clone(),
            source_columns: relation.columns.clone(),
        };

        let result = engine
            .execute_partition(&req)
            .await
            .expect("partition aggregate should use catalog schema");
        let response = result.to_query_response();
        assert_eq!(response.rows.len(), 1);
        assert_eq!(response.rows[0][0], "1");
        assert_eq!(response.rows[0][1], "10.5000000000");

        let wrong_utf8_columns = relation
            .columns
            .iter()
            .map(|column| {
                let mut column = column.clone();
                if column.name == "order_value" || column.name == "date_of_purchase" {
                    column.data_type = "Utf8".to_string();
                }
                column
            })
            .collect::<Vec<_>>();
        let req = crate::distributed::ExecutePartitionRequest {
            query_id: "test-partition-orders-agg-wrong-catalog".to_string(),
            sql: "SELECT COUNT(*) AS order_count, SUM(order_value) AS total_order_value, AVG(order_value) AS avg_order_value, MIN(date_of_purchase) AS first_order_date, MAX(date_of_purchase) AS last_order_date FROM __partition__ WHERE date_of_purchase >= DATE '2020-01-01'".to_string(),
            session: session.clone(),
            partition_files,
            source_columns: wrong_utf8_columns,
        };
        let result = engine
            .execute_partition(&req)
            .await
            .expect("partition aggregate should refine wrong string catalog metadata");
        let response = result.to_query_response();
        assert_eq!(response.rows.len(), 1);
        assert_eq!(response.rows[0][0], "1");

        cleanup_catalog_artifacts(&catalog_path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_partition_refines_string_backed_order_aggregate_columns() {
        let catalog_path = temp_catalog_path();
        let engine = PrototypeEngine::from_catalog_path(&catalog_path)
            .await
            .expect("engine should initialize");
        let session = SessionContext {
            protocol: Protocol::Embedded,
            ..SessionContext::default()
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("customer_name", DataType::Utf8, false),
            Field::new("order_value", DataType::Utf8, false),
            Field::new("date_of_purchase", DataType::Utf8, false),
            Field::new("_row_id", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["A", "B"])),
                Arc::new(StringArray::from(vec!["10.50", "20.25"])),
                Arc::new(StringArray::from(vec!["2024-01-15", "2019-06-01"])),
                Arc::new(StringArray::from(vec!["r1", "r2"])),
            ],
        )
        .unwrap();

        let write_dir = std::env::temp_dir().join(format!(
            "analyticsdb-string-backed-orders-{}",
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(&write_dir).unwrap();
        let storage_path = format!("file://{}", write_dir.display());
        let (store, prefix) = crate::storage::store_for_location(&storage_path).unwrap();
        crate::storage::append_parquet_batch(&store, &prefix, batch)
            .await
            .unwrap();
        let partition_files = crate::storage::list_parquet_files(&store, &prefix)
            .await
            .unwrap();

        let req = crate::distributed::ExecutePartitionRequest {
            query_id: "test-partition-string-backed-orders-agg".to_string(),
            sql: "SELECT COUNT(*) AS order_count, SUM(order_value) AS total_order_value, AVG(order_value) AS avg_order_value, MIN(date_of_purchase) AS first_order_date, MAX(date_of_purchase) AS last_order_date FROM __partition__ WHERE date_of_purchase >= DATE '2020-01-01'".to_string(),
            session,
            partition_files,
            source_columns: vec![
                CatalogColumn {
                    name: "id".to_string(),
                    data_type: "Int64".to_string(),
                    nullable: true,
                    default_value: None,
                },
                CatalogColumn {
                    name: "customer_name".to_string(),
                    data_type: "Utf8".to_string(),
                    nullable: false,
                    default_value: None,
                },
                CatalogColumn {
                    name: "order_value".to_string(),
                    data_type: "Utf8".to_string(),
                    nullable: false,
                    default_value: None,
                },
                CatalogColumn {
                    name: "date_of_purchase".to_string(),
                    data_type: "Utf8".to_string(),
                    nullable: false,
                    default_value: None,
                },
                CatalogColumn {
                    name: "_row_id".to_string(),
                    data_type: "Utf8".to_string(),
                    nullable: false,
                    default_value: None,
                },
            ],
        };

        let result = engine
            .execute_partition(&req)
            .await
            .expect("string-backed partition aggregate should be refined");
        let response = result.to_query_response();
        assert_eq!(response.rows.len(), 1);
        assert_eq!(response.rows[0][0], "1");
        assert_eq!(response.rows[0][1], "10.5");

        let _ = std::fs::remove_dir_all(write_dir);
        cleanup_catalog_artifacts(&catalog_path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn coordinator_finalizes_distributed_count_to_one_row() {
        let catalog_path = temp_catalog_path();
        let engine = PrototypeEngine::from_catalog_path(&catalog_path)
            .await
            .expect("engine should initialize");
        let session = SessionContext {
            protocol: Protocol::Embedded,
            ..SessionContext::default()
        };
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch_a = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(datafusion::arrow::array::Int64Array::from(vec![
                1, 2,
            ]))],
        )
        .unwrap();
        let batch_b = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(datafusion::arrow::array::Int64Array::from(vec![
                3,
            ]))],
        )
        .unwrap();
        let request = QueryRequest {
            sql: "SELECT COUNT(*) FROM customers".to_string(),
            session: session.clone(),
        };
        let admission = QueryAdmission {
            query_id: "test-distributed-count-finalize".to_string(),
            coordinator_node_id: "coord-test".to_string(),
        };

        let result = engine
            .execute_coordinator_select_over_partition_batches(
                &request,
                &admission,
                Instant::now(),
                "customers",
                schema,
                vec![batch_a, batch_b],
                3,
                None,
            )
            .await
            .expect("coordinator should finalize distributed count");
        let schema = Arc::clone(&result.schema);
        let batches = datafusion::physical_plan::common::collect(result.stream)
            .await
            .unwrap();
        let response = QueryExecutionResult {
            query_id: admission.query_id,
            coordinator_node_id: admission.coordinator_node_id,
            session,
            schema,
            batches,
            message: String::new(),
            outcome: StatementOutcome::Rows,
            execution_time_ms: 0,
        }
        .to_query_response();

        assert_eq!(response.rows, vec![vec!["3".to_string()]]);
        cleanup_catalog_artifacts(&catalog_path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn coordinator_sums_distributed_partial_count_to_one_row() {
        let catalog_path = temp_catalog_path();
        let engine = PrototypeEngine::from_catalog_path(&catalog_path)
            .await
            .expect("engine should initialize");
        let session = SessionContext {
            protocol: Protocol::Embedded,
            ..SessionContext::default()
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            "__adb_agg_0",
            DataType::Int64,
            false,
        )]));
        let partials = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(datafusion::arrow::array::Int64Array::from(vec![
                101, 203, 307,
            ]))],
        )
        .unwrap();
        let request = QueryRequest {
            sql: "SELECT COUNT(*) FROM customers".to_string(),
            session: session.clone(),
        };
        let admission = QueryAdmission {
            query_id: "test-distributed-partial-count-finalize".to_string(),
            coordinator_node_id: "coord-test".to_string(),
        };
        let (_, final_sql) = distributed_aggregate_plan(&request.sql, "customers").unwrap();

        let result = engine
            .execute_coordinator_select_over_partition_batches(
                &request,
                &admission,
                Instant::now(),
                "customers",
                schema,
                vec![partials],
                3,
                Some(final_sql),
            )
            .await
            .expect("coordinator should sum partial counts");
        let schema = Arc::clone(&result.schema);
        let batches = datafusion::physical_plan::common::collect(result.stream)
            .await
            .unwrap();
        let response = QueryExecutionResult {
            query_id: admission.query_id,
            coordinator_node_id: admission.coordinator_node_id,
            session,
            schema,
            batches,
            message: String::new(),
            outcome: StatementOutcome::Rows,
            execution_time_ms: 0,
        }
        .to_query_response();

        assert_eq!(response.rows, vec![vec!["611".to_string()]]);
        cleanup_catalog_artifacts(&catalog_path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn coordinator_combines_multiple_distributed_aggregates() {
        let catalog_path = temp_catalog_path();
        let engine = PrototypeEngine::from_catalog_path(&catalog_path)
            .await
            .expect("engine should initialize");
        let session = SessionContext {
            protocol: Protocol::Embedded,
            ..SessionContext::default()
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("__adb_agg_0", DataType::Int64, false),
            Field::new("__adb_agg_1", DataType::Float64, true),
            Field::new("__adb_agg_2_sum", DataType::Float64, true),
            Field::new("__adb_agg_2_count", DataType::Int64, false),
            Field::new("__adb_agg_3", DataType::Int64, true),
            Field::new("__adb_agg_4", DataType::Int64, true),
        ]));
        let partials = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![2, 3])),
                Arc::new(datafusion::arrow::array::Float64Array::from(vec![
                    30.0, 120.0,
                ])),
                Arc::new(datafusion::arrow::array::Float64Array::from(vec![
                    30.0, 120.0,
                ])),
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![2, 3])),
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![10, 5])),
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![20, 60])),
            ],
        )
        .unwrap();
        let request = QueryRequest {
            sql: "SELECT COUNT(*) AS n, SUM(amount) AS total, AVG(amount) AS avg_amount, MIN(id) AS first_id, MAX(id) AS last_id FROM customers".to_string(),
            session: session.clone(),
        };
        let admission = QueryAdmission {
            query_id: "test-distributed-multi-agg-finalize".to_string(),
            coordinator_node_id: "coord-test".to_string(),
        };
        let (worker_sql, final_sql) =
            distributed_aggregate_plan(&request.sql, "customers").unwrap();
        assert_eq!(
            worker_sql,
            "SELECT COUNT(*) AS \"__adb_agg_0\", SUM(amount) AS \"__adb_agg_1\", SUM(amount) AS \"__adb_agg_2_sum\", COUNT(amount) AS \"__adb_agg_2_count\", MIN(id) AS \"__adb_agg_3\", MAX(id) AS \"__adb_agg_4\" FROM __partition__"
        );

        let result = engine
            .execute_coordinator_select_over_partition_batches(
                &request,
                &admission,
                Instant::now(),
                "customers",
                schema,
                vec![partials],
                2,
                Some(final_sql),
            )
            .await
            .expect("coordinator should combine aggregate partials");
        let schema = Arc::clone(&result.schema);
        let batches = datafusion::physical_plan::common::collect(result.stream)
            .await
            .unwrap();
        let response = QueryExecutionResult {
            query_id: admission.query_id,
            coordinator_node_id: admission.coordinator_node_id,
            session,
            schema,
            batches,
            message: String::new(),
            outcome: StatementOutcome::Rows,
            execution_time_ms: 0,
        }
        .to_query_response();

        assert_eq!(
            response.rows,
            vec![vec![
                "5".to_string(),
                "150.0".to_string(),
                "30.0".to_string(),
                "5".to_string(),
                "60".to_string()
            ]]
        );
        cleanup_catalog_artifacts(&catalog_path);
    }

    #[test]
    fn parse_plain_select_table_simple() {
        assert_eq!(
            parse_plain_select_table("SELECT * FROM my_table"),
            Some((None, None, "my_table".to_string()))
        );
    }

    #[test]
    fn parse_plain_select_table_qualified() {
        assert_eq!(
            parse_plain_select_table("SELECT id FROM public.users"),
            Some((None, Some("public".to_string()), "users".to_string()))
        );
    }

    #[test]
    fn parse_plain_select_table_fully_qualified() {
        assert_eq!(
            parse_plain_select_table("SELECT * FROM mydb.public.orders"),
            Some((
                Some("mydb".to_string()),
                Some("public".to_string()),
                "orders".to_string()
            ))
        );
    }

    #[test]
    fn parse_plain_select_table_rejects_join() {
        assert_eq!(
            parse_plain_select_table("SELECT * FROM a JOIN b ON a.id = b.id"),
            None
        );
    }

    #[test]
    fn parse_plain_select_table_rejects_subquery() {
        assert_eq!(
            parse_plain_select_table("SELECT * FROM (SELECT 1) sub"),
            None
        );
    }

    #[test]
    fn parse_plain_select_table_rejects_cte() {
        assert_eq!(
            parse_plain_select_table("WITH cte AS (SELECT 1) SELECT * FROM cte"),
            None
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn distributed_select_falls_through_to_local_when_no_compute_nodes() {
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
                sql: "CREATE TABLE dist_test (id INT, val TEXT)".to_string(),
                session: session.clone(),
            })
            .await
            .unwrap();

        engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO dist_test SELECT 1, 'hello'".to_string(),
                session: session.clone(),
            })
            .await
            .unwrap();

        // No compute nodes registered → falls through to local DataFusion execution.
        let result = engine
            .execute_query(&QueryRequest {
                sql: "SELECT * FROM dist_test".to_string(),
                session: session.clone(),
            })
            .await
            .expect("local fallback should succeed");

        assert_eq!(
            result.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            1
        );
        cleanup_catalog_artifacts(&catalog_path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_distributed_write_partition_writes_parquet() {
        let catalog_path = temp_catalog_path();
        let engine = PrototypeEngine::from_catalog_path(&catalog_path)
            .await
            .expect("engine should initialize");
        let session = SessionContext {
            protocol: Protocol::Embedded,
            ..SessionContext::default()
        };

        // Create a source table and populate it.
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE write_src (n INT)".to_string(),
                session: session.clone(),
            })
            .await
            .unwrap();
        engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO write_src SELECT * FROM generate_series(1, 5) AS s(n)"
                    .to_string(),
                session: session.clone(),
            })
            .await
            .unwrap();

        // Find where the source table's parquet files live.
        let src_relation = engine
            .control_plane()
            .table_relation(&session, None, None, "write_src")
            .await
            .unwrap();
        let src_path = src_relation.storage_path.clone().unwrap();
        let (src_store, src_prefix) = crate::storage::store_for_location(&src_path).unwrap();
        let src_files = crate::storage::list_parquet_files(&src_store, &src_prefix)
            .await
            .unwrap();
        assert!(
            !src_files.is_empty(),
            "source table must have parquet files"
        );

        // Create a target directory for the worker to write into.
        let write_dir =
            std::env::temp_dir().join(format!("analyticsdb-write-test-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&write_dir).unwrap();
        let write_prefix = format!("file://{}", write_dir.display());

        let req = crate::distributed::ExecutePartitionWriteRequest {
            query_id: "write-test-1".to_string(),
            sql: format!("SELECT * FROM partition ({} files)", src_files.len()),
            session: session.clone(),
            partition_files: src_files,
            source_columns: src_relation.columns.clone(),
            write_prefix,
        };

        let ack = engine
            .execute_distributed_write_partition(&req)
            .await
            .expect("write partition should succeed");

        assert_eq!(ack.row_count, 5);
        assert!(!ack.written_files.is_empty());

        cleanup_catalog_artifacts(&catalog_path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn distributed_insert_falls_through_to_local_when_no_compute_nodes() {
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
                sql: "CREATE TABLE ins_src (x INT)".to_string(),
                session: session.clone(),
            })
            .await
            .unwrap();
        engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO ins_src SELECT * FROM generate_series(1, 3) AS s(x)".to_string(),
                session: session.clone(),
            })
            .await
            .unwrap();
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE ins_dst (x INT)".to_string(),
                session: session.clone(),
            })
            .await
            .unwrap();

        // No compute nodes → falls through to local single-node execution.
        let result = engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO ins_dst SELECT * FROM ins_src".to_string(),
                session: session.clone(),
            })
            .await
            .expect("local insert fallback should succeed");

        assert!(matches!(
            result.outcome,
            analyticsdb_core::StatementOutcome::Command { ref tag, rows_affected: 3 }
            if tag == "INSERT"
        ));

        cleanup_catalog_artifacts(&catalog_path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn distributed_select_retries_and_falls_back_on_node_failure() {
        let catalog_path = temp_catalog_path();
        let engine = PrototypeEngine::from_catalog_path(&catalog_path)
            .await
            .expect("engine should initialize");
        let session = SessionContext {
            protocol: Protocol::Embedded,
            ..SessionContext::default()
        };

        // Create a table with some data.
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fail_test (id INT)".to_string(),
                session: session.clone(),
            })
            .await
            .unwrap();
        engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO fail_test SELECT * FROM generate_series(1, 10) AS s(id)"
                    .to_string(),
                session: session.clone(),
            })
            .await
            .unwrap();

        // Register a bogus compute node that will fail connection.
        engine
            .control_plane()
            .register_node(analyticsdb_control::ClusterNode {
                id: "bogus-node".to_string(),
                role: analyticsdb_control::NodeRole::Compute,
                endpoint: "http://127.0.0.1:1".to_string(), // Invalid port
                status: analyticsdb_control::NodeStatus::Ready,
                last_heartbeat_at_epoch_ms: 0,
                ..analyticsdb_control::ClusterNode::default()
            })
            .await
            .unwrap();

        // Query should still succeed by retrying and eventually falling back to local.
        let result = engine
            .execute_query(&QueryRequest {
                sql: "SELECT * FROM fail_test".to_string(),
                session: session.clone(),
            })
            .await
            .expect("Query should succeed via fallback despite bogus node");

        assert_eq!(
            result.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            10
        );

        // Verify the node was marked Unavailable.
        let nodes = engine.control_plane().list_nodes().await.unwrap();
        let bogus = nodes.iter().find(|n| n.id == "bogus-node").unwrap();
        assert_eq!(bogus.status, analyticsdb_control::NodeStatus::Unavailable);

        cleanup_catalog_artifacts(&catalog_path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_table_provider_projects_batches_to_declared_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["one", "two"])),
            ],
        )
        .unwrap();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream::iter(vec![Ok::<_, DataFusionError>(batch)]),
        ));
        let table = StreamingTableProvider {
            schema: Arc::clone(&schema),
            stream: Arc::new(tokio::sync::Mutex::new(Some(stream))),
        };
        let context = DfSessionContext::new_with_config(base_session_config());
        context
            .register_table("__partition__", Arc::new(table))
            .unwrap();

        let dataframe = context.sql("SELECT name FROM __partition__").await.unwrap();
        let stream = dataframe.execute_stream().await.unwrap();
        let result = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].schema().fields().len(), 1);
        assert_eq!(result[0].schema().field(0).name(), "name");
    }

    #[test]
    fn optimal_worker_count_heuristic() {
        use crate::distributed::calculate_optimal_worker_count;

        // Small data, few files -> 1 node (coordinator)
        assert_eq!(calculate_optimal_worker_count(1024, 2, 10), 1);

        // Large data, many files -> subset of nodes
        // 1GB / 128MB = 8 nodes
        assert_eq!(
            calculate_optimal_worker_count(1024 * 1024 * 1024, 20, 20),
            10
        ); // file count / 2 is 10

        // 10GB / 128MB = 80 nodes, clamped to available 20
        assert_eq!(
            calculate_optimal_worker_count(10 * 1024 * 1024 * 1024, 100, 20),
            20
        );

        // Moderate data, many files
        // 100MB / 128MB = 1 node, but 20 files / 2 = 10 nodes
        assert_eq!(
            calculate_optimal_worker_count(100 * 1024 * 1024, 20, 20),
            10
        );
    }
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

fn catalog_data_type(data_type: &str) -> DataType {
    let upper = data_type.to_ascii_uppercase();
    if upper.starts_with("NUMERIC") || upper.starts_with("DECIMAL") {
        // Default to Decimal128(38, 10) for prototype if no precision/scale specified
        // or parse them if we want to be more precise
        return DataType::Decimal128(38, 10);
    }
    match upper.as_str() {
        "INT" | "INTEGER" | "INT4" | "INT32" => DataType::Int32,
        "BIGINT" | "INT8" | "INT64" => DataType::Int64,
        "TEXT" | "VARCHAR" | "STRING" | "UTF8" => DataType::Utf8,
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "FLOAT4" | "REAL" | "FLOAT32" => DataType::Float32,
        "FLOAT8" | "DOUBLE PRECISION" | "FLOAT64" => DataType::Float64,
        "DATE" => DataType::Date32,
        _ => DataType::Utf8,
    }
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
    let mut seen = std::collections::HashMap::new();

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
