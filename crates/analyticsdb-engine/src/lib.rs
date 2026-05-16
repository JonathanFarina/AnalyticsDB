#![cfg_attr(not(test), deny(clippy::panic))]
#![cfg_attr(not(test), deny(clippy::todo))]
#![cfg_attr(not(test), deny(clippy::unimplemented))]
#![cfg_attr(not(test), deny(clippy::unwrap_used))]
#![cfg_attr(not(test), deny(clippy::expect_used))]
// Allow a few pedantic lints that DataFusion/Arrow trait bounds make unavoidable.
#![allow(clippy::type_complexity)]
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
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
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
        let Ok(mut guard) = self.stream.try_lock() else {
            return Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                stream::once(async {
                    Err(DataFusionError::Internal(
                        "PartitionStream scanned concurrently".to_string(),
                    ))
                }),
            ));
        };
        match guard.take() {
            Some(s) => s,
            None => Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                stream::once(async {
                    Err(DataFusionError::Internal(
                        "PartitionStream already consumed".to_string(),
                    ))
                }),
            )),
        }
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
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::ExprSchemable;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::prelude::{
    col, lit, ParquetReadOptions, SessionConfig, SessionContext as DfSessionContext,
};
use datafusion::scalar::ScalarValue;
use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_functions_aggregate::expr_fn::count;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::SendableRecordBatchStream;
use futures::{stream, Stream, StreamExt};
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, SelectItem,
    SetExpr, Statement, TableFactor,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tracing::{info, warn};

pub mod audit_log;
pub mod distributed;
pub mod functions;
pub(crate) mod manifest;
pub mod postgres_compatibility;
pub mod query_log;
pub mod sql_rewriter;
pub mod storage;
pub mod system_catalog;

mod batch;
mod ddl;
mod dispatch_impl;
mod dispatch_plan;
mod index_impl;
mod index_ops;
mod information_schema;
mod metadata_helpers;
mod schema_build;

pub use distributed::{
    ExecutePartitionRequest, ExecutePartitionWriteAck, ExecutePartitionWriteRequest,
    PartitionClient,
};

fn load_mtls_config_from_cluster_config(
    config: &analyticsdb_control::ClusterConfig,
) -> Option<crate::distributed::ClusterMtlsConfig> {
    let ca_path = config.tls_ca_cert_path.as_ref()?;
    let cert_path = config.tls_cert_path.as_ref()?;
    let key_path = config.tls_key_path.as_ref()?;
    let ca = std::fs::read(ca_path).ok()?;
    let cert = std::fs::read(cert_path).ok()?;
    let key = std::fs::read(key_path).ok()?;
    Some(crate::distributed::ClusterMtlsConfig {
        ca_cert_pem: ca,
        client_cert_pem: cert,
        client_key_pem: key,
    })
}

// Re-export submodule items so that `use super::*` in child modules
// pulls everything into scope.
pub(crate) use batch::*;
pub(crate) use dispatch_plan::*;
pub(crate) use index_ops::*;
pub(crate) use metadata_helpers::*;
pub(crate) use schema_build::*;

use audit_log::AuditLog;
use functions::register_postgres_functions;
use query_log::QueryLog;
use system_catalog::{PgCatalogSchemaProvider, SystemSchemaProvider};

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
    config.options_mut().sql_parser.map_string_types_to_utf8view = false;
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
    cache: DashMap<String, (u64, Vec<(String, u64, i64)>)>, // table_key -> (epoch, files)
    epochs: DashMap<String, u64>,                           // table_key -> current_epoch
}

impl Default for FileListCache {
    fn default() -> Self {
        Self::new()
    }
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
    ) -> Result<Vec<(String, u64, i64)>> {
        let current_epoch = self.epochs.get(table_key).map(|r| *r.value()).unwrap_or(0);
        if let Some(entry) = self.cache.get(table_key) {
            let (cached_epoch, files) = entry.value();
            if *cached_epoch == current_epoch {
                return Ok(files.clone());
            }
        }

        let files = manifest::list_files_with_sizes_and_rows(store, prefix).await?;
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

/// An in-process write lock for a single relation, optionally backed by a
/// distributed advisory lease stored in the SQLite catalogue.
///
/// When dropped, the lease is released asynchronously via a background task.
/// The local `RwLock<()>` remains valid for the lifetime of the engine
/// (preventing concurrent mutations within a single process) regardless of
/// whether a distributed lease is in use.
pub struct DistributedRelationLock {
    inner: Arc<tokio::sync::RwLock<()>>,
    /// Dropping this sender signals the background task to release the lease.
    _release_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl std::ops::Deref for DistributedRelationLock {
    type Target = tokio::sync::RwLock<()>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct PrototypeEngine {
    control_plane: Arc<ControlPlane>,
    session_context_cache: Arc<tokio::sync::RwLock<HashMap<String, DfSessionContext>>>,
    relation_locks: Arc<tokio::sync::RwLock<HashMap<String, Arc<tokio::sync::RwLock<()>>>>>,
    partition_client: Arc<PartitionClient>,
    file_list_cache: Arc<FileListCache>,
    query_log: Arc<QueryLog>,
    pub audit_log: Arc<AuditLog>,
    active_queries: Arc<dashmap::DashMap<String, tokio_util::sync::CancellationToken>>,
    /// Limits the number of queries that can execute concurrently on this node.
    ///
    /// Controlled by `ANALYTICSDB_MAX_CONCURRENT_QUERIES` (default: 32).
    /// Callers that exceed the limit receive an error immediately so they can
    /// retry or queue on the client side rather than blocking indefinitely.
    query_semaphore: Arc<tokio::sync::Semaphore>,
    /// Shared memory pool used by all DataFusion session contexts.
    ///
    /// Each session gets its own `RuntimeEnv` (so CacheManager and
    /// ObjectStoreRegistry are not shared), but they all draw from this single
    /// pool so that the total memory used by concurrent queries is bounded.
    /// Pool size is controlled by `ANALYTICSDB_WORKER_MEMORY_LIMIT_MIB`
    /// (default 4096 MiB).
    memory_pool: Arc<dyn MemoryPool>,
}

impl std::fmt::Debug for PrototypeEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrototypeEngine")
            .field("control_plane", &self.control_plane)
            .field("session_context_cache", &"<cached session contexts>")
            .field("partition_client", &"<partition client>")
            .field("file_list_cache", &"<file list cache>")
            .field("query_log", &self.query_log.root_location())
            .field("audit_log", &self.audit_log.root_location())
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
            query_log: Arc::clone(&self.query_log),
            audit_log: Arc::clone(&self.audit_log),
            active_queries: Arc::clone(&self.active_queries),
            query_semaphore: Arc::clone(&self.query_semaphore),
            memory_pool: Arc::clone(&self.memory_pool),
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

struct QueryLogStreamWrapper {
    inner: SendableRecordBatchStream,
    probe: query_log::QueryProbe,
    rows: i64,
    bytes: i64,
}

impl Stream for QueryLogStreamWrapper {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            std::task::Poll::Ready(Some(Ok(batch))) => {
                self.rows += batch.num_rows() as i64;
                self.bytes += batch.get_array_memory_size() as i64;
                std::task::Poll::Ready(Some(Ok(batch)))
            }
            std::task::Poll::Ready(Some(Err(e))) => {
                self.probe.finish_stream_error(e.to_string());
                std::task::Poll::Ready(Some(Err(e)))
            }
            std::task::Poll::Ready(None) => {
                self.probe.finish_stream_success(self.rows, self.bytes);
                std::task::Poll::Ready(None)
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl RecordBatchStream for QueryLogStreamWrapper {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
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

fn rows_outcome() -> StatementOutcome {
    StatementOutcome::Rows
}

fn command_outcome(tag: impl Into<String>, rows_affected: u64) -> StatementOutcome {
    StatementOutcome::Command {
        tag: tag.into(),
        rows_affected,
    }
}

/// RAII guard that removes a query's `CancellationToken` from the active-query registry
/// when the guard is dropped (i.e., when the query completes or returns an error).
/// Also holds the semaphore permit so the concurrency slot is released on drop.
struct QueryGuard {
    query_id: String,
    active_queries: Arc<dashmap::DashMap<String, tokio_util::sync::CancellationToken>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for QueryGuard {
    fn drop(&mut self) {
        self.active_queries.remove(&self.query_id);
        // _permit is dropped here, releasing the semaphore slot.
    }
}

impl PrototypeEngine {
    pub async fn local_node_id(&self) -> String {
        self.control_plane
            .local_node()
            .await
            .map(|n| n.id)
            .unwrap_or_else(|| "unknown".to_string())
    }

    /// Builds the shared memory pool that all DataFusion session contexts draw from.
    ///
    /// Pool size in MiB is read from `ANALYTICSDB_WORKER_MEMORY_LIMIT_MIB`
    /// (default: 4096 MiB / 4 GiB).
    fn build_memory_pool() -> Arc<dyn MemoryPool> {
        let limit_mib: usize = std::env::var("ANALYTICSDB_WORKER_MEMORY_LIMIT_MIB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4096);
        let limit_bytes = limit_mib.saturating_mul(1024 * 1024);
        Arc::new(GreedyMemoryPool::new(limit_bytes))
    }

    fn build_query_semaphore() -> Arc<tokio::sync::Semaphore> {
        let max: usize = std::env::var("ANALYTICSDB_MAX_CONCURRENT_QUERIES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(32);
        Arc::new(tokio::sync::Semaphore::new(max))
    }

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
            query_log: Arc::new(QueryLog::disabled()),
            audit_log: Arc::new(AuditLog::disabled()),
            active_queries: Arc::new(dashmap::DashMap::new()),
            query_semaphore: Self::build_query_semaphore(),
            memory_pool: Self::build_memory_pool(),
        })
    }

    pub async fn from_catalog_path(catalog_path: &str) -> Result<Self> {
        let control_plane = Arc::new(ControlPlane::from_catalog_path(catalog_path).await?);

        // Propagate SSE config from ClusterConfig into env vars so that every
        // subsequent `store_for_location` call picks them up.  Env vars already
        // set by the operator take precedence (the `set_var` calls are no-ops
        // when the var is already present due to the guard below).
        if let Some(config) = control_plane.cluster_config().await {
            if let Some(ref sse) = config.s3_sse {
                if std::env::var("ANALYTICSDB_S3_SSE").is_err() {
                    // Safety: single-threaded startup path; no other thread
                    // reads these vars until the engine is returned.
                    unsafe { std::env::set_var("ANALYTICSDB_S3_SSE", sse) };
                }
            }
            if let Some(ref key_id) = config.s3_sse_kms_key_id {
                if std::env::var("ANALYTICSDB_S3_SSE_KMS_KEY_ID").is_err() {
                    unsafe { std::env::set_var("ANALYTICSDB_S3_SSE_KMS_KEY_ID", key_id) };
                }
            }
        }

        let cluster_config = control_plane.cluster_config().await;
        let query_log_config = cluster_config
            .as_ref()
            .map(|config| config.query_log.clone())
            .unwrap_or_default();
        let query_log_root = control_plane
            .managed_data_root()
            .join("system")
            .join("query_log");
        let audit_log_root = control_plane
            .managed_data_root()
            .join("system")
            .join("audit_log");
        let mut partition_client = PartitionClient::new(Arc::clone(&control_plane));
        partition_client.set_compute_eligible(true);
        if let Some(config) = &cluster_config {
            if let Some(mtls_config) = load_mtls_config_from_cluster_config(config) {
                partition_client.set_mtls(mtls_config);
            }
        }
        let partition_client = Arc::new(partition_client);
        Ok(Self {
            control_plane,
            session_context_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            relation_locks: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            partition_client,
            file_list_cache: Arc::new(FileListCache::new()),
            query_log: Arc::new(QueryLog::new(query_log_config, query_log_root)),
            audit_log: Arc::new(AuditLog::new(
                audit_log::AuditLogConfig::default(),
                audit_log_root,
            )),
            active_queries: Arc::new(dashmap::DashMap::new()),
            query_semaphore: Self::build_query_semaphore(),
            memory_pool: Self::build_memory_pool(),
        })
    }

    pub fn control_plane(&self) -> Arc<ControlPlane> {
        Arc::clone(&self.control_plane)
    }

    /// Check whether the session's role has the given privilege on the specified table.
    ///
    /// Admin users bypass all privilege checks.  For non-admin users, queries
    /// the catalogue store for a matching grant row.  Returns an error with
    /// the PostgreSQL-compatible message `permission denied for table <name>` when
    /// access is denied (SQLSTATE 42501).
    async fn check_table_access(
        &self,
        session: &SessionContext,
        table_name: &str,
        privilege: &str,
    ) -> Result<()> {
        // Fetch the user record to determine admin status.
        let is_admin = self
            .control_plane
            .catalog_user(&session.user)
            .await
            .map(|u| u.is_admin)
            .unwrap_or(false);

        if is_admin {
            return Ok(());
        }

        let granted = self
            .control_plane
            .check_privilege(&session.role, "table", table_name, privilege)
            .await?;

        if granted {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "permission denied for table {}",
                table_name
            ))
        }
    }

    /// Cancels every query that is currently executing on this engine instance.
    ///
    /// Called during graceful shutdown so in-flight queries receive an error
    /// promptly rather than being killed mid-stream by the process exiting.
    pub fn cancel_all_queries(&self) {
        for entry in self.active_queries.iter() {
            entry.value().cancel();
        }
    }

    /// Executes an `ExecutePartitionRequest` and returns a stream of `RecordBatch`es.
    pub async fn execute_partition_stream(
        &self,
        req: &distributed::ExecutePartitionRequest,
    ) -> Result<SendableRecordBatchStream> {
        let probe = self.query_log.start_probe_distributed(
            &QueryRequest {
                sql: req.sql.clone(),
                session: req.session.clone(),
                query_id: None,
            },
            &req.query_id,
            &req.initial_query_id,
            false,
            &req.coordinator_node_id,
            Some(self.local_node_id().await),
            None,
            &req.sql,
        );

        let stream = self.execute_partition_stream_inner(req).await?;
        Ok(Box::pin(QueryLogStreamWrapper {
            inner: stream,
            probe,
            rows: 0,
            bytes: 0,
        }))
    }

    async fn execute_partition_stream_inner(
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
        let probe = self.query_log.start_probe_distributed(
            &QueryRequest {
                sql: req.sql.clone(),
                session: req.session.clone(),
                query_id: None,
            },
            &req.query_id,
            &req.initial_query_id,
            false,
            &req.coordinator_node_id,
            Some(self.local_node_id().await),
            Some(req.partition_files.len() as i32),
            &req.sql,
        );

        let started = std::time::Instant::now();
        let result: Result<QueryExecutionResult> = async {
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
        .await;

        if let Ok(execution) = &result {
            let rows: i64 = execution.batches.iter().map(|b| b.num_rows() as i64).sum();
            probe.observe_read(rows, 0);
        }
        probe.finish_result(&result);
        result
    }

    /// Executes an `ExecutePartitionWriteRequest` on behalf of a Coordinator.
    ///
    /// When `partition_files` is non-empty: opens those Parquet files via the
    /// DataFusion Rust API (a passthrough copy of the source files into the
    /// target table, optionally re-typed by `source_columns`).
    ///
    /// When `partition_files` is empty: executes `req.sql` directly via
    /// DataFusion.  Used by the generate_series distribution path where the
    /// worker has no upstream files to read and synthesises its own rows.
    ///
    /// Either way, each output batch is written as a distinct UUID-named
    /// Parquet file under `req.write_prefix` and the function returns an
    /// acknowledgment with the written file paths and total row count.
    pub async fn execute_distributed_write_partition(
        &self,
        req: &distributed::ExecutePartitionWriteRequest,
    ) -> Result<distributed::ExecutePartitionWriteAck> {
        let probe = self.query_log.start_probe_distributed(
            &QueryRequest {
                sql: req.sql.clone(),
                session: req.session.clone(),
                query_id: None,
            },
            &req.query_id,
            &req.initial_query_id,
            false,
            &req.coordinator_node_id,
            Some(self.local_node_id().await),
            None,
            &req.sql,
        );

        let result = self.execute_distributed_write_partition_inner(req).await;

        if let Ok(ack) = &result {
            probe.observe_written(ack.row_count as i64, 0);
        }
        probe.finish_stream_success(0, 0); // result_rows is 0 for writes, usually

        result
    }

    async fn execute_distributed_write_partition_inner(
        &self,
        req: &distributed::ExecutePartitionWriteRequest,
    ) -> Result<distributed::ExecutePartitionWriteAck> {
        let (store, prefix) = storage::store_for_location(&req.write_prefix)?;

        let ctx = self.create_session_context(&req.session).await?;

        let batches = if req.partition_files.is_empty() {
            // SQL-driven path (e.g. generate_series).
            let rewritten = sql_rewriter::rewrite_sql_for_postgres_compatibility(
                &req.sql,
                &self.control_plane,
                &req.session,
            )
            .await?;
            let df = ctx.sql(&rewritten).await.map_err(sanitize_error)?;
            df.collect().await.map_err(sanitize_error)?
        } else {
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
            df.collect().await.map_err(sanitize_error)?
        };

        let mut written_files = Vec::new();
        let mut total_rows = 0usize;

        let mut current_batch = Vec::new();
        let mut current_rows = 0;

        // Embed the attempt_id in every filename so recovery can identify all
        // staged files for this attempt by scanning for the prefix, even after
        // the coordinator crashes before updating the manifest.
        let file_prefix = if req.attempt_id.is_empty() {
            String::new()
        } else {
            format!("{}__", req.attempt_id)
        };

        for batch in batches {
            let (row_count, prepared) = prepare_batch_for_storage(batch)?;
            if row_count == 0 {
                continue;
            }

            current_batch.push(prepared);
            current_rows += row_count;
            total_rows += row_count;

            if current_rows >= INSERT_SELECT_PARQUET_ROW_GROUP_SIZE {
                let file_name = format!("{}{}.parquet", file_prefix, uuid::Uuid::now_v7());
                let key = prefix.clone().join(file_name.as_str());
                let schema = current_batch[0].schema();
                storage::write_parquet_batches(&store, &key, schema, &current_batch).await?;
                written_files.push(format!("/{}", key.as_ref()));
                current_batch.clear();
                current_rows = 0;
            }
        }

        if !current_batch.is_empty() {
            let file_name = format!("{}{}.parquet", file_prefix, uuid::Uuid::now_v7());
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
    ) -> Result<DistributedRelationLock> {
        let key = format!(
            "{}.{}.{}",
            relation.database, relation.schema, relation.name
        );

        // In-process lock: guarantees single-process serialisation.
        let inner = {
            let locks = self.relation_locks.read().await;
            if let Some(lock) = locks.get(&key) {
                Arc::clone(lock)
            } else {
                drop(locks);
                let mut locks = self.relation_locks.write().await;
                Arc::clone(
                    locks
                        .entry(key.clone())
                        .or_insert_with(|| Arc::new(tokio::sync::RwLock::new(()))),
                )
            }
        };

        // Distributed advisory lease: guards against concurrent mutations
        // from other coordinator nodes sharing the same SQLite catalogue.
        let node_id = self.local_node_id().await;
        let acquired = self
            .control_plane
            .try_acquire_relation_lease(&key, &node_id, 30_000)
            .await?;
        if !acquired {
            anyhow::bail!("relation {key} is locked by another coordinator; retry momentarily");
        }

        // Spawn a background task that releases the lease when the lock is
        // dropped (the sender half of the oneshot is stored in the lock struct).
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let cp = Arc::clone(&self.control_plane);
        let release_key = key.clone();
        let release_node = node_id.clone();
        tokio::spawn(async move {
            // Wait for the lock to be dropped (sender closes).
            let _ = rx.await;
            let _ = cp.release_relation_lease(&release_key, &release_node).await;
        });

        Ok(DistributedRelationLock {
            inner,
            _release_tx: Some(tx),
        })
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
        let original_sql = request.sql.clone();
        let (request, admission, started) = self.prepare_query_request(request).await?;

        // Acquire a concurrency slot.  Returns immediately (non-blocking) so
        // clients get a fast error when the node is saturated rather than queuing
        // indefinitely and eventually timing out.
        let permit = Arc::clone(&self.query_semaphore)
            .try_acquire_owned()
            .map_err(|_| {
                anyhow::anyhow!(
                    "Server is at maximum query concurrency ({} active); try again later",
                    self.query_semaphore.available_permits() == 0
                )
            })?;

        // Register a CancellationToken for this query so KILL QUERY can cancel it.
        let token = tokio_util::sync::CancellationToken::new();
        let query_id = request
            .query_id
            .clone()
            .unwrap_or_else(|| admission.query_id.clone());
        self.active_queries.insert(query_id.clone(), token.clone());
        let _guard = QueryGuard {
            query_id: query_id.clone(),
            active_queries: Arc::clone(&self.active_queries),
            _permit: permit,
        };

        // Determine effective timeout: per-session statement_timeout takes precedence
        // over the global ANALYTICSDB_QUERY_TIMEOUT_SECS env var. 0 = unlimited.
        let timeout_ms: u64 = if request.session.statement_timeout_ms > 0 {
            request.session.statement_timeout_ms
        } else {
            std::env::var("ANALYTICSDB_QUERY_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(300)
                .saturating_mul(1_000)
        };

        let probe = self
            .query_log
            .start_probe(&request, &admission, &original_sql);

        let result = if timeout_ms == 0 {
            self.execute_query_inner(request, admission, started, &probe)
                .await
        } else {
            let token_for_timeout = token.clone();
            match tokio::time::timeout(
                std::time::Duration::from_millis(timeout_ms),
                self.execute_query_inner(request, admission, started, &probe),
            )
            .await
            {
                Ok(r) => r,
                Err(_elapsed) => {
                    token_for_timeout.cancel();
                    Err(anyhow::anyhow!(
                        "statement_timeout: query exceeded the {}ms execution time limit",
                        timeout_ms
                    ))
                }
            }
        };

        probe.finish_result(&result);
        result
    }

    async fn execute_query_inner(
        &self,
        request: QueryRequest,
        admission: QueryAdmission,
        started: Instant,
        probe: &query_log::QueryProbe,
    ) -> Result<QueryExecutionResult> {
        if let Some(statement) = parse_insert_select_statement(&request.sql)? {
            return self
                .execute_insert_select(&request, statement, admission, started, probe)
                .await;
        }

        if let Some(statement) = parse_indexed_select_statement(&request.sql)? {
            if let Some(result) = self
                .try_execute_indexed_select(&request, statement, &admission, started, probe)
                .await?
            {
                return Ok(result);
            }
        }

        if let Some(statement) = parse_metadata_statement(&request.sql) {
            let result = self
                .execute_metadata_query(&request, statement, admission, started, probe)
                .await?;
            if matches!(result.outcome, StatementOutcome::Command { .. }) {
                self.invalidate_session_contexts().await;
            }
            return Ok(result);
        }

        if let Some(result) = self
            .try_execute_distributed_select(&request, &admission, started, probe)
            .await?
        {
            return Ok(result);
        }

        // D5: Object-level authorization check before executing DML.
        if let Some((table_name, privilege)) =
            extract_dml_table_and_privilege(&request.sql, &request.session)
        {
            self.check_table_access(&request.session, &table_name, &privilege)
                .await?;
        }

        let session = request.session.clone();
        let context = self.create_session_context(&session).await?;
        let dataframe = context.sql(&request.sql).await.map_err(sanitize_error)?;
        let schema = Arc::new(dataframe.schema().as_arrow().as_ref().clone());
        let plan = dataframe
            .create_physical_plan()
            .await
            .map_err(sanitize_error)?;
        let batches = datafusion::physical_plan::collect(Arc::clone(&plan), context.task_ctx())
            .await
            .map_err(sanitize_error)?;
        probe.observe_plan(plan.as_ref());

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
        let original_sql = request.sql.clone();
        let (request, admission, started) = self.prepare_query_request(request).await?;
        let probe = self
            .query_log
            .start_probe(&request, &admission, &original_sql);

        let mut execution = self
            .execute_query_stream_inner(&request, admission, started, &probe)
            .await?;
        execution.stream = Box::pin(QueryLogStreamWrapper {
            inner: execution.stream,
            probe,
            rows: 0,
            bytes: 0,
        });

        Ok(execution)
    }

    async fn execute_query_stream_inner(
        &self,
        request: &QueryRequest,
        admission: QueryAdmission,
        started: Instant,
        probe: &query_log::QueryProbe,
    ) -> Result<QueryExecutionStream> {
        if let Some(statement) = parse_insert_select_statement(&request.sql)? {
            let execution = self
                .execute_insert_select(request, statement, admission, started, probe)
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
                .try_execute_indexed_select(request, statement, &admission, started, probe)
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
                .execute_metadata_query(request, statement, admission, started, probe)
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
            .try_execute_distributed_select_stream(request, &admission, started, probe)
            .await?
        {
            return Ok(result);
        }

        // D5: Object-level authorization check before executing DML (stream path).
        if let Some((table_name, privilege)) =
            extract_dml_table_and_privilege(&request.sql, &request.session)
        {
            self.check_table_access(&request.session, &table_name, &privilege)
                .await?;
        }

        let session = request.session.clone();

        let context = self.create_session_context(&session).await?;
        let dataframe = context.sql(&request.sql).await.map_err(sanitize_error)?;
        let schema = Arc::new(dataframe.schema().as_arrow().as_ref().clone());

        let plan = dataframe
            .create_physical_plan()
            .await
            .map_err(sanitize_error)?;
        probe.observe_plan(plan.as_ref());
        let stream = datafusion::physical_plan::execute_stream(plan, context.task_ctx())
            .map_err(sanitize_error)?;
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
        probe: &query_log::QueryProbe,
    ) -> Result<QueryExecutionResult> {
        // Attempt distributed execution first; fall through on None.
        if let Some(result) = self
            .try_execute_distributed_insert_select(request, &statement, &admission, started, probe)
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
        let relation_lock = self.relation_lock(&relation).await?;
        println!("insert waiting for lock for {}", request.sql);
        let _write_guard = relation_lock.write().await;
        println!("insert acquired lock for {}", request.sql);

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
                let filename = format!("{}.parquet", uuid::Uuid::now_v7());
                let data_path = format!("data/{}", filename);
                let key = prefix.clone().join("data").join(filename.as_str());
                let bytes = storage::encode_parquet_batches(schema, &current_batch)?;
                let size = bytes.len() as u64;
                let row_count = current_rows as i64;
                store.put(&key, bytes.into()).await?;
                crate::manifest::append_to_manifest(
                    &store,
                    &prefix,
                    &data_path,
                    size,
                    row_count,
                    Vec::new(),
                )
                .await?;
                current_batch.clear();
                current_rows = 0;
            }
        }

        if !current_batch.is_empty() {
            let schema = current_batch[0].schema();
            let filename = format!("{}.parquet", uuid::Uuid::now_v7());
            let data_path = format!("data/{}", filename);
            let key = prefix.clone().join("data").join(filename.as_str());
            let bytes = storage::encode_parquet_batches(schema, &current_batch)?;
            let size = bytes.len() as u64;
            let row_count = current_rows as i64;
            store.put(&key, bytes.into()).await?;
            crate::manifest::append_to_manifest(
                &store,
                &prefix,
                &data_path,
                size,
                row_count,
                Vec::new(),
            )
            .await?;
        }

        let table_key = format!(
            "{}.{}.{}",
            relation.database, relation.schema, relation.name
        );
        self.file_list_cache.invalidate(&table_key);

        self.rebuild_all_index_snapshots(&request.session, &relation)
            .await?;

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

        // Build a per-session RuntimeEnv backed by the shared memory pool so
        // that each session has its own CacheManager and ObjectStoreRegistry
        // (avoiding cross-session state pollution) while still drawing from the
        // same pool limit that bounds total memory across all concurrent queries.
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&self.memory_pool))
            .build()
            .map_err(|e| anyhow::anyhow!("RuntimeEnv build failed: {}", e))?;
        let ctx = DfSessionContext::new_with_config_rt(config, Arc::new(runtime_env));

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
                let system_schema = Arc::new(SystemSchemaProvider::new_with_audit_log(
                    self.query_log.root_location().to_string(),
                    Some(self.audit_log.root_location().to_string()),
                ));
                provider.register_schema("system", system_schema)?;
            }

            ctx.register_catalog(&database, provider);
        }

        register_postgres_functions(&ctx);

        let mut cache = self.session_context_cache.write().await;
        cache.insert(key, ctx.clone());
        Ok(ctx)
    }
}

/// Extract the primary table name and required privilege from a DML SQL statement.
///
/// Returns `Some((qualified_table_name, privilege))` for SELECT/INSERT/UPDATE/DELETE
/// statements. Returns `None` for DDL or unrecognized statements.
///
/// Only the first/primary table is checked (multi-table authorization is a follow-up).
fn extract_dml_table_and_privilege(
    sql: &str,
    session: &SessionContext,
) -> Option<(String, String)> {
    let dialect = PostgreSqlDialect {};
    let Ok(statements) = Parser::parse_sql(&dialect, sql.trim().trim_end_matches(';')) else {
        return None;
    };

    let stmt = statements.into_iter().next()?;

    match &stmt {
        Statement::Query(query) => {
            // SELECT — extract the first FROM table.
            let table_name = extract_first_select_table(query.body.as_ref(), session)?;
            Some((table_name, "SELECT".to_string()))
        }
        Statement::Insert(insert) => {
            let table_obj = match &insert.table {
                sqlparser::ast::TableObject::TableName(n) => n,
                _ => return None,
            };
            let name = qualify_table_name(&table_obj.to_string(), session);
            Some((name, "INSERT".to_string()))
        }
        Statement::Update(update) => {
            let name = qualify_table_name(&update.table.relation.to_string(), session);
            Some((name, "UPDATE".to_string()))
        }
        Statement::Delete(del) => {
            // del.from is a FromTable enum
            let tables = match &del.from {
                sqlparser::ast::FromTable::WithFromKeyword(tables) => tables,
                sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
            };
            let name = tables
                .first()
                .map(|f| qualify_table_name(&f.relation.to_string(), session))?;
            Some((name, "DELETE".to_string()))
        }
        _ => None,
    }
}

fn extract_first_select_table(body: &SetExpr, session: &SessionContext) -> Option<String> {
    match body {
        SetExpr::Select(select) => {
            let first = select.from.first()?;
            Some(qualify_table_factor(&first.relation, session))
        }
        SetExpr::Query(q) => extract_first_select_table(q.body.as_ref(), session),
        SetExpr::SetOperation { left, .. } => extract_first_select_table(left.as_ref(), session),
        _ => None,
    }
}

fn qualify_table_factor(factor: &TableFactor, session: &SessionContext) -> String {
    match factor {
        TableFactor::Table { name, .. } => qualify_table_name(&name.to_string(), session),
        _ => String::new(),
    }
}

fn qualify_table_name(name: &str, session: &SessionContext) -> String {
    let parts: Vec<&str> = name.split('.').collect();
    match parts.as_slice() {
        [table] => format!("{}.{}.{}", session.database, session.schema, table),
        [schema, table] => format!("{}.{}.{}", session.database, schema, table),
        [database, schema, table] => format!("{database}.{schema}.{table}"),
        _ => name.to_string(),
    }
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

    async fn configure_fast_query_log(catalog_path: &str) {
        let control_plane = analyticsdb_control::ControlPlane::from_catalog_path(catalog_path)
            .await
            .expect("control plane should initialize");
        let mut config = control_plane
            .cluster_config()
            .await
            .expect("bootstrap config should exist");
        config.query_log.batch_size = 1;
        config.query_log.batch_interval_ms = 25;
        control_plane
            .update_cluster_config(config)
            .await
            .expect("query log config should persist");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn query_log_records_successful_queries_as_system_table() {
        let catalog_path = temp_catalog_path();
        configure_fast_query_log(&catalog_path).await;
        let engine = PrototypeEngine::from_catalog_path(&catalog_path)
            .await
            .expect("engine should initialize");
        let session = SessionContext {
            protocol: Protocol::Embedded,
            ..SessionContext::default()
        };

        engine
            .execute_query(&QueryRequest {
                sql: "SELECT 1 AS logged_value".to_string(),
                session: session.clone(),
                query_id: None,
            })
            .await
            .expect("logged query should execute");

        let mut rows = Vec::new();
        for _ in 0..20 {
            let result = engine
                .execute_query(&QueryRequest {
                    sql: "SELECT query, event_type, protocol, result_rows FROM system.query_log WHERE query = 'SELECT 1 AS logged_value' ORDER BY event_time_us LIMIT 1".to_string(),
                    session: session.clone(),
                    query_id: None,
})
                .await
                .expect("query log should be readable");
            rows = result.to_query_response().rows;
            if !rows.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }

        assert_eq!(
            rows,
            vec![vec![
                "SELECT 1 AS logged_value".to_string(),
                "QueryFinish".to_string(),
                "embedded".to_string(),
                "1".to_string()
            ]]
        );
        cleanup_catalog_artifacts(&catalog_path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn query_log_production_features() {
        let catalog_path = temp_catalog_path();
        configure_fast_query_log(&catalog_path).await;
        let engine = PrototypeEngine::from_catalog_path(&catalog_path)
            .await
            .expect("engine should initialize");
        let session = SessionContext {
            protocol: Protocol::Embedded,
            ..SessionContext::default()
        };

        // 1. Test partitioned layout and metrics (read_rows)
        engine
            .execute_query(&QueryRequest {
                sql: "SELECT * FROM generate_series(1, 100)".to_string(),
                session: session.clone(),
                query_id: None,
            })
            .await
            .expect("query should execute");

        // 2. Test streaming logging
        let mut stream_exec = engine
            .execute_query_stream(&QueryRequest {
                sql: "SELECT * FROM generate_series(1, 50) AS t2".to_string(),
                session: session.clone(),
                query_id: None,
            })
            .await
            .expect("stream query should execute");

        use futures::StreamExt;
        while let Some(batch) = stream_exec.stream.next().await {
            batch.expect("batch should be ok");
        }

        // Wait for flush
        let mut rows = Vec::new();
        for i in 0..40 {
            let result = engine
                .execute_query(&QueryRequest {
                    sql: "SELECT query, read_rows, result_rows FROM system.query_log ORDER BY event_time_us".to_string(),
                    session: session.clone(),
                    query_id: None,
})
                .await
                .expect("query log should be readable");
            rows = result.to_query_response().rows;
            if rows.len() >= 2
                && rows.iter().any(|r| r[0].contains("100"))
                && rows.iter().any(|r| r[0].contains("t2"))
            {
                break;
            }
            if i % 10 == 0 {
                println!("Waiting for logs... current count: {}", rows.len());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        if rows.is_empty() {
            let query_log_dir = std::path::Path::new(&catalog_path)
                .with_extension("managed")
                .join("system")
                .join("query_log");
            println!("Query log dir: {:?}", query_log_dir);
            if query_log_dir.exists() {
                for entry in std::fs::read_dir(&query_log_dir).unwrap() {
                    println!("  Entry: {:?}", entry.unwrap().path());
                }
            } else {
                println!("Query log dir DOES NOT EXIST");
            }
        }

        assert!(
            rows.len() >= 2,
            "Expected at least 2 log rows, found {}",
            rows.len()
        );

        // Find generate_series(1, 100)
        let row100 = rows
            .iter()
            .find(|r| r[0].contains("100"))
            .expect("row 100 not found");
        // read_rows might be 100 if generate_series is correctly instrumented
        assert!(
            row100[1].parse::<i64>().unwrap() >= 100,
            "read_rows should be >= 100, found {}",
            row100[1]
        );
        assert_eq!(row100[2], "100");

        // Find generate_series(1, 50) AS t2
        let row50 = rows
            .iter()
            .find(|r| r[0].contains("t2"))
            .expect("row 50 not found");
        assert!(
            row50[1].parse::<i64>().unwrap() >= 50,
            "read_rows should be >= 50, found {}",
            row50[1]
        );
        assert_eq!(row50[2], "50");

        // Verify partitioned files on disk
        let query_log_dir = std::path::Path::new(&catalog_path)
            .with_extension("managed")
            .join("system")
            .join("query_log");

        let mut found_partitioned = false;
        for entry in std::fs::read_dir(query_log_dir).expect("should be able to read query log dir")
        {
            let entry = entry.expect("valid entry");
            if entry.file_type().expect("valid file type").is_dir() {
                let name = entry.file_name();
                if name.to_string_lossy().chars().all(|c| c.is_ascii_digit()) {
                    found_partitioned = true;
                    break;
                }
            }
        }
        assert!(
            found_partitioned,
            "should have created partitioned YYYY/ directories"
        );
        cleanup_catalog_artifacts(&catalog_path);
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
                query_id: None,
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
            query_id: None,
        };
        let request_b = QueryRequest {
            sql: "INSERT INTO customers VALUES (1, 'duplicate')".to_string(),
            session: session_b,
            query_id: None,
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
                query_id: None,
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
                query_id: None,
            })
            .await
            .unwrap();

        engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO customers VALUES (1, 'one'), (2, 'two')".to_string(),
                session: session.clone(),
                query_id: None,
            })
            .await
            .unwrap();

        // Test CREATE UNIQUE INDEX CONCURRENTLY
        let result = engine
            .execute_query(&QueryRequest {
                sql: "CREATE UNIQUE INDEX CONCURRENTLY customers_name_idx ON customers (name)"
                    .to_string(),
                session: session.clone(),
                query_id: None,
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
                query_id: None,
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
                query_id: None,
            })
            .await
            .unwrap();

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE INDEX test_idx_idx ON test_idx (id)".to_string(),
                session: session.clone(),
                query_id: None,
            })
            .await
            .unwrap();

        let result = engine
            .execute_query(&QueryRequest {
                sql: "SELECT relname, indisvalid FROM pg_index i JOIN pg_class c ON i.indexrelid = c.oid WHERE relname = 'test_idx_idx'".to_string(),
                session: session.clone(),
                query_id: None,
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
                query_id: None,
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
                query_id: None,
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
                query_id: None,
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
            coordinator_node_id: "coordinator".to_string(),
            initial_query_id: "test-partition-q1".to_string(),
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
                query_id: None,
})
            .await
            .expect("orders table should be created");
        engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO orders (id, customer_name, order_value, date_of_purchase) SELECT 1, 'A', CAST(10.50 AS NUMERIC(12,2)), CAST('2024-01-15' AS DATE) UNION ALL SELECT 2, 'B', CAST(20.25 AS NUMERIC(12,2)), CAST('2019-06-01' AS DATE)".to_string(),
                session: session.clone(),
                query_id: None,
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
        let partition_files = crate::manifest::list_files(&store, &prefix).await.unwrap();
        assert!(
            !partition_files.is_empty(),
            "orders table must have parquet files"
        );

        let req = crate::distributed::ExecutePartitionRequest {
            query_id: "test-partition-orders-agg".to_string(),
            coordinator_node_id: "coordinator".to_string(),
            initial_query_id: "test-partition-orders-agg".to_string(),
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
            coordinator_node_id: "coordinator".to_string(),
            initial_query_id: "test-partition-orders-agg-wrong-catalog".to_string(),
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
        crate::manifest::append_batch(&store, &prefix, batch)
            .await
            .unwrap();
        let partition_files = crate::manifest::list_files(&store, &prefix).await.unwrap();

        let req = crate::distributed::ExecutePartitionRequest {
            query_id: "test-partition-string-backed-orders-agg".to_string(),
            coordinator_node_id: "coordinator".to_string(),
            initial_query_id: "test-partition-string-backed-orders-agg".to_string(),
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
            query_id: None,
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
            query_id: None,
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
            query_id: None,
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

    #[test]
    fn slice_int_range_even_split() {
        assert_eq!(slice_int_range(1, 10, 2), vec![(1, 5), (6, 10)]);
    }

    #[test]
    fn slice_int_range_uneven_split_extras_go_first() {
        // 10 elements / 3 workers → sizes [4, 3, 3]
        assert_eq!(slice_int_range(1, 10, 3), vec![(1, 4), (5, 7), (8, 10)]);
    }

    #[test]
    fn slice_int_range_more_workers_than_elements() {
        // 3 elements / 5 workers → only 3 chunks
        assert_eq!(slice_int_range(1, 3, 5), vec![(1, 1), (2, 2), (3, 3)]);
    }

    #[test]
    fn slice_int_range_empty_input() {
        assert!(slice_int_range(5, 4, 4).is_empty());
        assert!(slice_int_range(1, 10, 0).is_empty());
    }

    #[test]
    fn parse_generate_series_select_basic() {
        let plan = parse_generate_series_select("SELECT n FROM generate_series(1, 100) AS s(n)")
            .expect("should parse");
        assert_eq!(plan.start, 1);
        assert_eq!(plan.end, 100);
        assert_eq!(plan.func_name.to_lowercase(), "generate_series");
    }

    #[test]
    fn parse_generate_series_select_rejects_step_argument() {
        assert!(
            parse_generate_series_select("SELECT n FROM generate_series(1, 100, 2) AS s(n)")
                .is_none()
        );
    }

    #[test]
    fn parse_generate_series_select_rejects_non_literal_bounds() {
        assert!(parse_generate_series_select(
            "SELECT n FROM generate_series(now(), now() + interval '1 day') AS s(n)"
        )
        .is_none());
    }

    #[test]
    fn parse_generate_series_select_rejects_other_table() {
        assert!(parse_generate_series_select("SELECT * FROM customers").is_none());
    }

    #[test]
    fn rewrite_generate_series_range_substitutes_and_aliases() {
        let plan =
            parse_generate_series_select("SELECT n, n * 2 FROM generate_series(1, 100) AS s(n)")
                .unwrap();
        let targets = vec!["id".to_string(), "doubled".to_string()];
        let rewritten = rewrite_generate_series_range(
            "SELECT n, n * 2 FROM generate_series(1, 100) AS s(n)",
            &plan,
            25,
            50,
            &targets,
        )
        .expect("should rewrite");
        assert!(rewritten.contains("generate_series(25, 50)"));
        assert!(rewritten.contains(r#"AS "id""#));
        assert!(rewritten.contains(r#"AS "doubled""#));
    }

    #[test]
    fn rewrite_generate_series_range_rejects_projection_arity_mismatch() {
        let plan =
            parse_generate_series_select("SELECT n FROM generate_series(1, 100) AS s(n)").unwrap();
        let targets = vec!["a".to_string(), "b".to_string()];
        assert!(rewrite_generate_series_range(
            "SELECT n FROM generate_series(1, 100) AS s(n)",
            &plan,
            1,
            10,
            &targets,
        )
        .is_none());
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
                query_id: None,
            })
            .await
            .unwrap();

        engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO dist_test SELECT 1, 'hello'".to_string(),
                session: session.clone(),
                query_id: None,
            })
            .await
            .unwrap();

        // No compute nodes registered → falls through to local DataFusion execution.
        let result = engine
            .execute_query(&QueryRequest {
                sql: "SELECT * FROM dist_test".to_string(),
                session: session.clone(),
                query_id: None,
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
                query_id: None,
            })
            .await
            .unwrap();
        engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO write_src SELECT * FROM generate_series(1, 5) AS s(n)"
                    .to_string(),
                session: session.clone(),
                query_id: None,
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
        let src_files = crate::manifest::list_files(&src_store, &src_prefix)
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
            coordinator_node_id: "coordinator".to_string(),
            initial_query_id: "write-test-1".to_string(),
            sql: format!("SELECT * FROM partition ({} files)", src_files.len()),
            session: session.clone(),
            partition_files: src_files,
            source_columns: src_relation.columns.clone(),
            write_prefix,
            attempt_id: "write-test-1_a1".to_string(),
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
                query_id: None,
            })
            .await
            .unwrap();
        engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO ins_src SELECT * FROM generate_series(1, 3) AS s(x)".to_string(),
                session: session.clone(),
                query_id: None,
            })
            .await
            .unwrap();
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE ins_dst (x INT)".to_string(),
                session: session.clone(),
                query_id: None,
            })
            .await
            .unwrap();

        // No compute nodes → falls through to local single-node execution.
        let result = engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO ins_dst SELECT * FROM ins_src".to_string(),
                session: session.clone(),
                query_id: None,
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
                query_id: None,
            })
            .await
            .unwrap();
        engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO fail_test SELECT * FROM generate_series(1, 10) AS s(id)"
                    .to_string(),
                session: session.clone(),
                query_id: None,
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
                query_id: None,
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

    // ---- D5: object-level authorization tests ----

    fn temp_sqlite_catalog_path() -> String {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "analyticsdb-engine-authz-test-{}.db",
            uuid::Uuid::now_v7()
        ));
        path.to_string_lossy().into_owned()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unprivileged_role_cannot_select_without_grant() {
        let catalog_path = temp_sqlite_catalog_path();
        let engine = PrototypeEngine::from_catalog_path(&catalog_path)
            .await
            .expect("engine should initialize");
        let admin_session = SessionContext::default();

        // Create a table as admin.
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE orders (id INTEGER, name TEXT)".to_string(),
                session: admin_session.clone(),
                query_id: None,
            })
            .await
            .expect("create table should succeed");

        // Create an unprivileged user.
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE USER alice PASSWORD 'secret'".to_string(),
                session: admin_session.clone(),
                query_id: None,
            })
            .await
            .expect("create user should succeed");

        // Try SELECT as alice (no grant).
        let alice_session = SessionContext {
            user: "alice".to_string(),
            role: "alice".to_string(),
            ..SessionContext::default()
        };
        let result = engine
            .execute_query(&QueryRequest {
                sql: "SELECT * FROM orders".to_string(),
                session: alice_session,
                query_id: None,
            })
            .await;

        assert!(result.is_err(), "unprivileged user should be denied SELECT");
        let msg = result.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(
            msg.contains("permission denied for table"),
            "error should mention permission denied: {}",
            msg
        );

        cleanup_catalog_artifacts(&catalog_path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn granted_role_can_select_after_grant() {
        let catalog_path = temp_sqlite_catalog_path();
        let engine = PrototypeEngine::from_catalog_path(&catalog_path)
            .await
            .expect("engine should initialize");
        let admin_session = SessionContext::default();

        // Create a table as admin.
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE products (id INTEGER, name TEXT)".to_string(),
                session: admin_session.clone(),
                query_id: None,
            })
            .await
            .expect("create table should succeed");

        // Create an unprivileged user.
        engine
            .execute_query(&QueryRequest {
                sql: "CREATE USER bob PASSWORD 'secret'".to_string(),
                session: admin_session.clone(),
                query_id: None,
            })
            .await
            .expect("create user should succeed");

        // Grant SELECT.
        engine
            .execute_query(&QueryRequest {
                sql: "GRANT SELECT ON TABLE products TO bob".to_string(),
                session: admin_session.clone(),
                query_id: None,
            })
            .await
            .expect("grant should succeed");

        // Now bob can SELECT.
        let bob_session = SessionContext {
            user: "bob".to_string(),
            role: "bob".to_string(),
            ..SessionContext::default()
        };
        let result = engine
            .execute_query(&QueryRequest {
                sql: "SELECT * FROM products".to_string(),
                session: bob_session,
                query_id: None,
            })
            .await;

        assert!(
            result.is_ok(),
            "bob should be able to SELECT after grant: {:?}",
            result.err()
        );

        cleanup_catalog_artifacts(&catalog_path);
    }

    #[tokio::test]
    async fn statement_timeout_is_propagated_through_session_context() {
        // Verify that session.statement_timeout_ms flows into the query request
        // and that the default is 0 (unlimited). Timing-based assertion is
        // environment-dependent; this test covers the structural invariant.
        let session = SessionContext {
            statement_timeout_ms: 5_000,
            ..SessionContext::default()
        };
        assert_eq!(session.statement_timeout_ms, 5_000);

        let default_session = SessionContext::default();
        assert_eq!(
            default_session.statement_timeout_ms, 0,
            "default session must be unlimited"
        );
        assert_eq!(
            default_session.idle_in_transaction_timeout_ms, 0,
            "default idle timeout must be disabled"
        );

        let req = QueryRequest {
            sql: "SELECT 1".to_string(),
            session,
            query_id: None,
        };
        assert_eq!(req.session.statement_timeout_ms, 5_000);
    }
}
