use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, LazyLock};

use analyticsdb_control::{QueryAdmission, QueryLogConfig};
use analyticsdb_core::{Protocol, QueryRequest, SessionContext, StatementOutcome};
use anyhow::Result;
use chrono::{Datelike, TimeZone, Utc};
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Int32Array, Int64Array, ListBuilder, StringArray, StringBuilder,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use object_store::ObjectStoreExt;
use regex::Regex;
use sqlparser::ast::{SetExpr, Statement, TableFactor};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio::sync::mpsc;
use tracing::debug;

use crate::{storage, QueryExecutionResult};
use std::time::{Duration, Instant};

// Static regexes — compiled once; patterns are validated literals so .expect() is safe.
#[allow(clippy::expect_used)]
static RE_STRINGS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"'([^']|'')*'").expect("valid string regex"));
#[allow(clippy::expect_used)]
static RE_NUMBERS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b\d+(\.\d+)?\b").expect("valid number regex"));

#[derive(Debug, Clone)]
pub struct QueryLog {
    config: QueryLogConfig,
    root_location: String,
    sender: Option<mpsc::UnboundedSender<QueryLogRecord>>,
}

impl QueryLog {
    /// Trigger manual vacuum of expired query log partitions.
    pub async fn vacuum(&self) -> Result<()> {
        cleanup_expired_logs(&self.config, &self.root_location).await
    }

    pub fn new(config: QueryLogConfig, root: PathBuf) -> Self {
        let root_location = format!("file://{}", root.display());
        if let Err(error) = std::fs::create_dir_all(&root) {
            debug!(
                "query log disabled because root directory '{}' could not be created: {}",
                root.display(),
                error
            );
            return Self {
                config,
                root_location,
                sender: None,
            };
        }

        if !config.enabled {
            return Self {
                config,
                root_location,
                sender: None,
            };
        }

        let (sender, receiver) = mpsc::unbounded_channel();
        let writer = QueryLogWriter::new(config.clone(), root_location.clone(), receiver);
        tokio::spawn(async move {
            writer.run().await;
        });

        Self {
            config,
            root_location,
            sender: Some(sender),
        }
    }

    pub fn disabled() -> Self {
        Self {
            config: QueryLogConfig {
                enabled: false,
                ..QueryLogConfig::default()
            },
            root_location: "file://analyticsdb-catalog.managed/system/query_log".to_string(),
            sender: None,
        }
    }

    pub fn root_location(&self) -> &str {
        &self.root_location
    }

    pub fn start_probe(
        &self,
        request: &QueryRequest,
        admission: &QueryAdmission,
        original_sql: &str,
    ) -> QueryProbe {
        self.start_probe_distributed(
            request,
            &admission.query_id,
            &admission.query_id,
            true,
            &admission.coordinator_node_id,
            None,
            None,
            original_sql,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn start_probe_distributed(
        &self,
        request: &QueryRequest,
        query_id: &str,
        initial_query_id: &str,
        is_initial_query: bool,
        coordinator_node_id: &str,
        worker_node_id: Option<String>,
        distributed_partition_count: Option<i32>,
        original_sql: &str,
    ) -> QueryProbe {
        let Some(sender) = &self.sender else {
            return QueryProbe::noop();
        };
        if self.config.sample_rate <= 0.0 {
            return QueryProbe::noop();
        }
        if self.config.sample_rate < 1.0 {
            let sample = stable_sample_fraction(initial_query_id);
            if sample >= self.config.sample_rate {
                return QueryProbe::noop();
            }
        }

        let query_start_time_us = Utc::now().timestamp_micros();
        let (query, query_truncated) =
            truncate_query(original_sql, self.config.max_query_length_bytes);
        let tables = extract_tables(original_sql, &request.session);
        let normalized_query_hash = normalized_query_hash(original_sql);
        let query_kind = query_kind(original_sql);

        QueryProbe {
            inner: Some(Arc::new(QueryProbeInner {
                sender: sender.clone(),
                finished: AtomicBool::new(false),
                started_at: Instant::now(),
                query_start_time_us,
                query_id: query_id.to_string(),
                initial_query_id: initial_query_id.to_string(),
                is_initial_query,
                query_kind,
                query,
                query_truncated,
                normalized_query_hash,
                user: request.session.user.clone(),
                database: request.session.database.clone(),
                protocol: protocol_label(&request.session.protocol).to_string(),
                coordinator_node_id: coordinator_node_id.to_string(),
                worker_node_id,
                distributed_partition_count,
                tables,
                settings: "{}".to_string(),
                profile: "{}".to_string(),
                min_duration_ms: self.config.min_duration_ms.try_into().unwrap_or(i64::MAX),
                read_rows: AtomicI64::new(0),
                read_bytes: AtomicI64::new(0),
                written_rows: AtomicI64::new(0),
                written_bytes: AtomicI64::new(0),
                memory_peak_bytes: AtomicI64::new(0),
            })),
        }
    }
}

#[derive(Clone, Debug)]
pub struct QueryProbe {
    inner: Option<Arc<QueryProbeInner>>,
}

impl QueryProbe {
    fn noop() -> Self {
        Self { inner: None }
    }

    pub fn observe_read(&self, rows: i64, bytes: i64) {
        if let Some(inner) = &self.inner {
            inner.read_rows.fetch_add(rows, Ordering::Relaxed);
            inner.read_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    pub fn observe_written(&self, rows: i64, bytes: i64) {
        if let Some(inner) = &self.inner {
            inner.written_rows.fetch_add(rows, Ordering::Relaxed);
            inner.written_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    pub fn observe_memory(&self, bytes: i64) {
        if let Some(inner) = &self.inner {
            inner.memory_peak_bytes.fetch_max(bytes, Ordering::Relaxed);
        }
    }

    pub fn observe_plan(&self, plan: &dyn ExecutionPlan) {
        if let Some(inner) = &self.inner {
            if let Some(metrics) = plan.metrics() {
                let mut read_rows = 0;
                // Aggregate output_rows from all operators in the plan.
                // Note: generate_series and some other operators might use different names
                // or might not have metrics until execution is complete.
                for m in metrics.iter() {
                    let name = m.value().name();
                    if name == "output_rows" || name == "rows" {
                        read_rows += m.value().as_usize() as i64;
                    }
                }
                inner.read_rows.fetch_add(read_rows, Ordering::Relaxed);
            }

            // Recursively observe children
            for child in plan.children() {
                self.observe_plan(child.as_ref());
            }
        }
    }

    pub fn finish_result(&self, result: &Result<QueryExecutionResult>) {
        let Some(inner) = &self.inner else {
            return;
        };
        if inner.finished.swap(true, Ordering::AcqRel) {
            return;
        }
        let elapsed = inner.started_at.elapsed();
        let mut record = inner.base_record(elapsed, Utc::now().timestamp_micros());
        match result {
            Ok(execution) => {
                record.event_type = "QueryFinish".to_string();
                record.result_rows = execution
                    .batches
                    .iter()
                    .map(|batch| batch.num_rows() as i64)
                    .sum();
                record.result_bytes = execution
                    .batches
                    .iter()
                    .map(|batch| batch.get_array_memory_size() as i64)
                    .sum();
                if let StatementOutcome::Command { rows_affected, .. } = &execution.outcome {
                    record.written_rows = (*rows_affected).try_into().unwrap_or(i64::MAX);
                }
                if record.read_rows == 0 && record.result_rows > 0 {
                    record.read_rows = record.result_rows;
                }
            }
            Err(error) => {
                record.event_type = "ExceptionWhileProcessing".to_string();
                record.error_code = Some(1);
                record.error = Some(error.to_string());
                record.error_stack = std::env::var("RUST_BACKTRACE")
                    .ok()
                    .filter(|value| value != "0")
                    .map(|_| format!("{error:?}"));
            }
        }
        if record.duration_ms < inner_min_duration_ms(inner) {
            return;
        }
        if let Err(error) = inner.sender.send(record) {
            debug!("query log send failed: {}", error);
        }
    }

    pub fn finish_stream_success(&self, result_rows: i64, result_bytes: i64) {
        let Some(inner) = &self.inner else {
            return;
        };
        if inner.finished.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut record =
            inner.base_record(inner.started_at.elapsed(), Utc::now().timestamp_micros());
        record.event_type = "QueryFinish".to_string();
        record.result_rows = result_rows;
        record.result_bytes = result_bytes;
        if record.read_rows == 0 && record.result_rows > 0 {
            record.read_rows = record.result_rows;
        }
        if let Err(error) = inner.sender.send(record) {
            debug!("query log send failed: {}", error);
        }
    }

    pub fn finish_stream_error(&self, error: String) {
        let Some(inner) = &self.inner else {
            return;
        };
        if inner.finished.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut record =
            inner.base_record(inner.started_at.elapsed(), Utc::now().timestamp_micros());
        record.event_type = "ExceptionWhileProcessing".to_string();
        record.error_code = Some(1);
        record.error = Some(error);
        if let Err(error) = inner.sender.send(record) {
            debug!("query log send failed: {}", error);
        }
    }
}

#[derive(Debug)]
struct QueryProbeInner {
    sender: mpsc::UnboundedSender<QueryLogRecord>,
    finished: AtomicBool,
    started_at: Instant,
    query_start_time_us: i64,
    query_id: String,
    initial_query_id: String,
    is_initial_query: bool,
    query_kind: String,
    query: String,
    query_truncated: bool,
    normalized_query_hash: i64,
    user: String,
    database: String,
    protocol: String,
    coordinator_node_id: String,
    worker_node_id: Option<String>,
    distributed_partition_count: Option<i32>,
    tables: Vec<String>,
    settings: String,
    profile: String,
    min_duration_ms: i64,

    read_rows: AtomicI64,
    read_bytes: AtomicI64,
    written_rows: AtomicI64,
    written_bytes: AtomicI64,
    memory_peak_bytes: AtomicI64,
}

fn inner_min_duration_ms(_inner: &QueryProbeInner) -> i64 {
    _inner.min_duration_ms
}

impl QueryProbeInner {
    fn base_record(&self, elapsed: Duration, event_time_us: i64) -> QueryLogRecord {
        QueryLogRecord {
            event_type: "QueryFinish".to_string(),
            event_time_us,
            query_start_time_us: self.query_start_time_us,
            query_id: self.query_id.clone(),
            initial_query_id: self.initial_query_id.clone(),
            is_initial_query: self.is_initial_query,
            query_kind: self.query_kind.clone(),
            query: self.query.clone(),
            query_truncated: self.query_truncated,
            normalized_query_hash: self.normalized_query_hash,
            duration_ms: elapsed.as_millis().try_into().unwrap_or(i64::MAX),
            read_rows: self.read_rows.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            written_rows: self.written_rows.load(Ordering::Relaxed),
            written_bytes: self.written_bytes.load(Ordering::Relaxed),
            result_rows: 0,
            result_bytes: 0,
            memory_peak_bytes: self.memory_peak_bytes.load(Ordering::Relaxed),
            error_code: None,
            error: None,
            error_stack: None,
            user: self.user.clone(),
            database: self.database.clone(),
            client_address: None,
            protocol: self.protocol.clone(),
            coordinator_node_id: self.coordinator_node_id.clone(),
            worker_node_id: self.worker_node_id.clone(),
            distributed_partition_count: self.distributed_partition_count,
            tables: self.tables.clone(),
            settings: self.settings.clone(),
            profile: self.profile.clone(),
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryLogRecord {
    pub event_type: String,
    pub event_time_us: i64,
    pub query_start_time_us: i64,
    pub query_id: String,
    pub initial_query_id: String,
    pub is_initial_query: bool,
    pub query_kind: String,
    pub query: String,
    pub query_truncated: bool,
    pub normalized_query_hash: i64,
    pub duration_ms: i64,
    pub read_rows: i64,
    pub read_bytes: i64,
    pub written_rows: i64,
    pub written_bytes: i64,
    pub result_rows: i64,
    pub result_bytes: i64,
    pub memory_peak_bytes: i64,
    pub error_code: Option<i32>,
    pub error: Option<String>,
    pub error_stack: Option<String>,
    pub user: String,
    pub database: String,
    pub client_address: Option<String>,
    pub protocol: String,
    pub coordinator_node_id: String,
    pub worker_node_id: Option<String>,
    pub distributed_partition_count: Option<i32>,
    pub tables: Vec<String>,
    pub settings: String,
    pub profile: String,
    pub engine_version: String,
}

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_time_us", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("query_start_time_us", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("query_id", DataType::Utf8, false),
        Field::new("initial_query_id", DataType::Utf8, false),
        Field::new("is_initial_query", DataType::Boolean, false),
        Field::new("query_kind", DataType::Utf8, false),
        Field::new("query", DataType::Utf8, false),
        Field::new("query_truncated", DataType::Boolean, false),
        Field::new("normalized_query_hash", DataType::Int64, false),
        Field::new("duration_ms", DataType::Int64, false),
        Field::new("read_rows", DataType::Int64, false),
        Field::new("read_bytes", DataType::Int64, false),
        Field::new("written_rows", DataType::Int64, false),
        Field::new("written_bytes", DataType::Int64, false),
        Field::new("result_rows", DataType::Int64, false),
        Field::new("result_bytes", DataType::Int64, false),
        Field::new("memory_peak_bytes", DataType::Int64, false),
        Field::new("error_code", DataType::Int32, true),
        Field::new("error", DataType::Utf8, true),
        Field::new("error_stack", DataType::Utf8, true),
        Field::new("user", DataType::Utf8, false),
        Field::new("database", DataType::Utf8, false),
        Field::new("client_address", DataType::Utf8, true),
        Field::new("protocol", DataType::Utf8, false),
        Field::new("coordinator_node_id", DataType::Utf8, false),
        Field::new("worker_node_id", DataType::Utf8, true),
        Field::new("distributed_partition_count", DataType::Int32, true),
        Field::new("tables", DataType::new_list(DataType::Utf8, true), true),
        Field::new("settings", DataType::Utf8, false),
        Field::new("profile", DataType::Utf8, false),
        Field::new("engine_version", DataType::Utf8, false),
    ]))
}

/// Standalone function to clean up expired query log partitions.
pub(crate) async fn cleanup_expired_logs(
    config: &QueryLogConfig,
    root_location: &str,
) -> Result<()> {
    if config.retention_days == 0 {
        return Ok(());
    }

    let (store, prefix) = storage::store_for_location(root_location)?;

    let expiration =
        Utc::now() - Duration::from_secs(86400 * config.retention_days as u64);

    debug!(
        "query log retention: cleaning up logs older than {}",
        expiration
    );

    let mut stream = store.list(Some(&prefix));
    while let Some(meta) = stream.next().await {
        let meta = match meta {
            Ok(m) => m,
            Err(_) => continue,
        };
        let path = meta.location.as_ref();
        let Some(rel_path) = path.strip_prefix(prefix.as_ref()) else {
            continue;
        };
        let rel_path = rel_path.trim_start_matches('/');
        // Check if path contains a date=YYYY-MM-DD partition
        let mut parts = rel_path.split('/');
        if let Some(first_part) = parts.next() {
            if first_part.starts_with("date=") {
                let date_str = &first_part[5..]; // strip "date="
                if let Ok(date) = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                    let partition_date = Utc.ymd(date.year(), date.month(), date.day());
                    if partition_date < expiration.date() {
                        if let Err(e) = store.delete(&meta.location).await {
                            debug!("failed to delete expired query log {}: {}", path, e);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn records_to_batch(records: &[QueryLogRecord]) -> Result<RecordBatch> {
    let mut tables = ListBuilder::new(StringBuilder::new());
    for record in records {
        for table in &record.tables {
            tables.values().append_value(table);
        }
        tables.append(true);
    }

    Ok(RecordBatch::try_new(
        schema(),
        vec![
            string_array(records, |r| Some(r.event_type.as_str())),
            timestamp_array(records, |r| r.event_time_us),
            int64_array(records, |r| r.event_time_us),
            timestamp_array(records, |r| r.query_start_time_us),
            string_array(records, |r| Some(r.query_id.as_str())),
            string_array(records, |r| Some(r.initial_query_id.as_str())),
            Arc::new(BooleanArray::from_iter(
                records.iter().map(|r| Some(r.is_initial_query)),
            )),
            string_array(records, |r| Some(r.query_kind.as_str())),
            string_array(records, |r| Some(r.query.as_str())),
            Arc::new(BooleanArray::from_iter(
                records.iter().map(|r| Some(r.query_truncated)),
            )),
            int64_array(records, |r| r.normalized_query_hash),
            int64_array(records, |r| r.duration_ms),
            int64_array(records, |r| r.read_rows),
            int64_array(records, |r| r.read_bytes),
            int64_array(records, |r| r.written_rows),
            int64_array(records, |r| r.written_bytes),
            int64_array(records, |r| r.result_rows),
            int64_array(records, |r| r.result_bytes),
            int64_array(records, |r| r.memory_peak_bytes),
            Arc::new(Int32Array::from(
                records.iter().map(|r| r.error_code).collect::<Vec<_>>(),
            )),
            string_array(records, |r| r.error.as_deref()),
            string_array(records, |r| r.error_stack.as_deref()),
            string_array(records, |r| Some(r.user.as_str())),
            string_array(records, |r| Some(r.database.as_str())),
            string_array(records, |r| r.client_address.as_deref()),
            string_array(records, |r| Some(r.protocol.as_str())),
            string_array(records, |r| Some(r.coordinator_node_id.as_str())),
            string_array(records, |r| r.worker_node_id.as_deref()),
            Arc::new(Int32Array::from(
                records
                    .iter()
                    .map(|r| r.distributed_partition_count)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(tables.finish()) as ArrayRef,
            string_array(records, |r| Some(r.settings.as_str())),
            string_array(records, |r| Some(r.profile.as_str())),
            string_array(records, |r| Some(r.engine_version.as_str())),
        ],
    )?)
}

fn string_array<F>(records: &[QueryLogRecord], f: F) -> ArrayRef
where
    F: Fn(&QueryLogRecord) -> Option<&str>,
{
    Arc::new(StringArray::from(records.iter().map(f).collect::<Vec<_>>()))
}

fn int64_array<F>(records: &[QueryLogRecord], f: F) -> ArrayRef
where
    F: Fn(&QueryLogRecord) -> i64,
{
    Arc::new(Int64Array::from_iter_values(records.iter().map(f)))
}

fn timestamp_array<F>(records: &[QueryLogRecord], f: F) -> ArrayRef
where
    F: Fn(&QueryLogRecord) -> i64,
{
    Arc::new(TimestampMicrosecondArray::from_iter_values(
        records.iter().map(f),
    ))
}

struct QueryLogWriter {
    config: QueryLogConfig,
    root_location: String,
    receiver: mpsc::UnboundedReceiver<QueryLogRecord>,
}

impl QueryLogWriter {
    fn new(
        config: QueryLogConfig,
        root_location: String,
        receiver: mpsc::UnboundedReceiver<QueryLogRecord>,
    ) -> Self {
        Self {
            config,
            root_location,
            receiver,
        }
    }

    async fn run(mut self) {
        let mut buffer = Vec::with_capacity(self.config.batch_size.max(1));
        let interval = Duration::from_millis(self.config.batch_interval_ms.max(1));
        let mut ticker = tokio::time::interval(interval);

        // Retention sweeper: once per hour
        let mut retention_ticker = tokio::time::interval(Duration::from_secs(3600));

        loop {
            tokio::select! {
                maybe_record = self.receiver.recv() => {
                    match maybe_record {
                        Some(record) => {
                            buffer.push(record);
                            if buffer.len() >= self.config.batch_size.max(1) {
                                self.flush(&mut buffer).await;
                            }
                        }
                        None => {
                            self.flush(&mut buffer).await;
                            break;
                        }
                    }
                }
                _ = ticker.tick() => {
                    self.flush(&mut buffer).await;
                }
                _ = retention_ticker.tick() => {
                    let _ = cleanup_expired_logs(&self.config, &self.root_location).await;
                }
            }
        }
    }

    async fn flush(&self, buffer: &mut Vec<QueryLogRecord>) {
        if buffer.is_empty() {
            return;
        }
        let records = std::mem::take(buffer);
        if let Err(error) = self.write_records(&records).await {
            debug!(
                "query log flush failed; dropping {} record(s): {}",
                records.len(),
                error
            );
        }
    }

    async fn write_records(&self, records: &[QueryLogRecord]) -> Result<()> {
        let batch = records_to_batch(records)?;
        let (store, prefix) = storage::store_for_location(&self.root_location)?;
        let event_time = Utc
            .timestamp_micros(records[0].event_time_us)
            .single()
            .unwrap_or_else(Utc::now);
        let date_partition = format!(
            "date={:04}-{:02}-{:02}",
            event_time.year(),
            event_time.month(),
            event_time.day()
        );
        let key = prefix
            .join(&*date_partition)
            .join(format!("{}.parquet", uuid::Uuid::now_v7()).as_str());
        storage::write_parquet_batches(&store, &key, schema(), &[batch]).await
    }

}

fn protocol_label(protocol: &Protocol) -> &'static str {
    match protocol {
        Protocol::Embedded => "embedded",
        Protocol::PostgreSql => "postgresql",
        Protocol::ArrowFlightSql => "flight-sql",
    }
}

fn stable_sample_fraction(query_id: &str) -> f64 {
    let mut hasher = DefaultHasher::new();
    query_id.hash(&mut hasher);
    hasher.finish() as f64 / u64::MAX as f64
}

fn truncate_query(sql: &str, max_bytes: usize) -> (String, bool) {
    if sql.len() <= max_bytes {
        return (sql.to_string(), false);
    }
    let mut end = max_bytes;
    while end > 0 && !sql.is_char_boundary(end) {
        end -= 1;
    }
    (sql[..end].to_string(), true)
}

fn normalized_query_hash(sql: &str) -> i64 {
    let normalized = normalize_query(sql);
    let mut hasher = DefaultHasher::new();
    normalized.hash(&mut hasher);
    hasher.finish() as i64
}

fn normalize_query(sql: &str) -> String {
    let without_strings = RE_STRINGS.replace_all(sql, "?");
    RE_NUMBERS
        .replace_all(&without_strings, "?")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn query_kind(sql: &str) -> String {
    let first = sql
        .split_whitespace()
        .next()
        .unwrap_or("Other")
        .trim_matches(|c: char| !c.is_ascii_alphabetic())
        .to_ascii_uppercase();
    match first.as_str() {
        "SELECT" | "WITH" | "SHOW" | "DESCRIBE" | "EXPLAIN" => "Select",
        "INSERT" => "Insert",
        "CREATE" => "Create",
        "DROP" => "Drop",
        "ALTER" => "Alter",
        _ => "Other",
    }
    .to_string()
}

fn extract_tables(sql: &str, session: &SessionContext) -> Vec<String> {
    let dialect = PostgreSqlDialect {};
    let Ok(statements) = Parser::parse_sql(&dialect, sql.trim().trim_end_matches(';')) else {
        return Vec::new();
    };
    let mut tables = Vec::new();
    for statement in statements {
        extract_statement_tables(&statement, session, &mut tables);
    }
    tables.sort();
    tables.dedup();
    tables
}

fn extract_statement_tables(
    statement: &Statement,
    session: &SessionContext,
    out: &mut Vec<String>,
) {
    match statement {
        Statement::Query(query) => extract_setexpr_tables(&query.body, session, out),
        _ => extract_keyword_table(statement.to_string().as_str(), session, out),
    }
}

fn extract_keyword_table(sql: &str, session: &SessionContext, out: &mut Vec<String>) {
    let patterns = [
        r"(?i)^\s*INSERT\s+INTO\s+([A-Za-z_][A-Za-z0-9_\.]*)",
        r"(?i)^\s*UPDATE\s+([A-Za-z_][A-Za-z0-9_\.]*)",
        r"(?i)^\s*DELETE\s+FROM\s+([A-Za-z_][A-Za-z0-9_\.]*)",
        r"(?i)^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z_][A-Za-z0-9_\.]*)",
        r"(?i)^\s*DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([A-Za-z_][A-Za-z0-9_\.]*)",
        r"(?i)^\s*ALTER\s+TABLE\s+([A-Za-z_][A-Za-z0-9_\.]*)",
    ];
    for pattern in patterns {
        let Ok(re) = Regex::new(pattern) else {
            continue;
        };
        if let Some(captures) = re.captures(sql) {
            if let Some(name) = captures.get(1) {
                out.push(qualify_object_name(name.as_str(), session));
                return;
            }
        }
    }
}

fn extract_setexpr_tables(expr: &SetExpr, session: &SessionContext, out: &mut Vec<String>) {
    match expr {
        SetExpr::Select(select) => {
            for table in &select.from {
                extract_table_factor(&table.relation, session, out);
                for join in &table.joins {
                    extract_table_factor(&join.relation, session, out);
                }
            }
        }
        SetExpr::Query(query) => extract_setexpr_tables(&query.body, session, out),
        SetExpr::SetOperation { left, right, .. } => {
            extract_setexpr_tables(left, session, out);
            extract_setexpr_tables(right, session, out);
        }
        _ => {}
    }
}

fn extract_table_factor(factor: &TableFactor, session: &SessionContext, out: &mut Vec<String>) {
    match factor {
        TableFactor::Table { name, .. } => {
            out.push(qualify_object_name(&name.to_string(), session))
        }
        TableFactor::Derived { subquery, .. } => {
            extract_setexpr_tables(&subquery.body, session, out)
        }
        _ => {}
    }
}

fn qualify_object_name(name: &str, session: &SessionContext) -> String {
    let parts = name.split('.').collect::<Vec<_>>();
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

    #[test]
    fn normalize_query_hash_ignores_literals() {
        assert_eq!(
            normalized_query_hash("SELECT * FROM t WHERE a = 42 AND b = 'hello'"),
            normalized_query_hash("select * from t where a = 7 and b = 'world'")
        );
    }

    #[test]
    fn table_extraction_qualifies_names() {
        let session = SessionContext::default();
        let tables = extract_tables(
            "SELECT * FROM public.orders JOIN analytics.customers ON orders.id = customers.id",
            &session,
        );
        assert_eq!(
            tables,
            vec![
                "postgres.analytics.customers".to_string(),
                "postgres.public.orders".to_string()
            ]
        );
    }
}
