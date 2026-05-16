//! Durable audit log for security-sensitive events.
//!
//! Follows the same pattern as `crate::query_log`: a background writer task
//! drains an unbounded channel, batches `AuditLogRecord`s into Arrow
//! `RecordBatch`es, and writes Parquet files under the managed data root.
//!
//! The audit log is disabled when `AuditLogConfig::enabled` is `false`.  In
//! that case every method is a no-op and no background task is spawned.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::{Datelike, TimeZone, Utc};
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::debug;

use crate::storage;

// ---- Configuration -----

fn default_audit_log_enabled() -> bool {
    true
}

fn default_audit_log_retention_days() -> u32 {
    90
}

fn default_audit_batch_size() -> usize {
    256
}

fn default_audit_batch_interval_ms() -> u64 {
    5000
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuditLogConfig {
    #[serde(default = "default_audit_log_enabled")]
    pub enabled: bool,
    #[serde(default = "default_audit_log_retention_days")]
    pub retention_days: u32,
    #[serde(default = "default_audit_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_audit_batch_interval_ms")]
    pub batch_interval_ms: u64,
}

impl Default for AuditLogConfig {
    fn default() -> Self {
        Self {
            enabled: default_audit_log_enabled(),
            retention_days: default_audit_log_retention_days(),
            batch_size: default_audit_batch_size(),
            batch_interval_ms: default_audit_batch_interval_ms(),
        }
    }
}

// ---- Event types ----

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditEventType {
    Login,
    LoginFailed,
    Logout,
    CreateTable,
    DropTable,
    CreateUser,
    DropUser,
    AlterUser,
    GrantPrivilege,
    RevokePrivilege,
    CreateRole,
    DropRole,
}

impl AuditEventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            AuditEventType::Login => "Login",
            AuditEventType::LoginFailed => "LoginFailed",
            AuditEventType::Logout => "Logout",
            AuditEventType::CreateTable => "CreateTable",
            AuditEventType::DropTable => "DropTable",
            AuditEventType::CreateUser => "CreateUser",
            AuditEventType::DropUser => "DropUser",
            AuditEventType::AlterUser => "AlterUser",
            AuditEventType::GrantPrivilege => "GrantPrivilege",
            AuditEventType::RevokePrivilege => "RevokePrivilege",
            AuditEventType::CreateRole => "CreateRole",
            AuditEventType::DropRole => "DropRole",
        }
    }
}

// ---- Record ----

#[derive(Debug, Clone)]
pub struct AuditLogRecord {
    pub event_type: String,
    pub event_time_us: i64,
    pub user: String,
    pub role: String,
    pub action: String,
    pub object_type: String,
    pub object_name: String,
    pub status: String,
    pub error: String,
    pub client_address: String,
    pub protocol: String,
}

impl AuditLogRecord {
    pub fn success(
        event_type: AuditEventType,
        user: impl Into<String>,
        role: impl Into<String>,
        action: impl Into<String>,
        object_type: impl Into<String>,
        object_name: impl Into<String>,
        protocol: impl Into<String>,
    ) -> Self {
        Self {
            event_type: event_type.as_str().to_string(),
            event_time_us: Utc::now().timestamp_micros(),
            user: user.into(),
            role: role.into(),
            action: action.into(),
            object_type: object_type.into(),
            object_name: object_name.into(),
            status: "success".to_string(),
            error: String::new(),
            client_address: String::new(),
            protocol: protocol.into(),
        }
    }

    pub fn error(
        event_type: AuditEventType,
        user: impl Into<String>,
        role: impl Into<String>,
        action: impl Into<String>,
        object_type: impl Into<String>,
        object_name: impl Into<String>,
        protocol: impl Into<String>,
        error: impl Into<String>,
    ) -> Self {
        Self {
            event_type: event_type.as_str().to_string(),
            event_time_us: Utc::now().timestamp_micros(),
            user: user.into(),
            role: role.into(),
            action: action.into(),
            object_type: object_type.into(),
            object_name: object_name.into(),
            status: "error".to_string(),
            error: error.into(),
            client_address: String::new(),
            protocol: protocol.into(),
        }
    }
}

// ---- Arrow schema ----

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_type", DataType::Utf8, false),
        Field::new("event_time_us", DataType::Int64, false),
        Field::new("user", DataType::Utf8, false),
        Field::new("role", DataType::Utf8, false),
        Field::new("action", DataType::Utf8, false),
        Field::new("object_type", DataType::Utf8, false),
        Field::new("object_name", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("error", DataType::Utf8, false),
        Field::new("client_address", DataType::Utf8, false),
        Field::new("protocol", DataType::Utf8, false),
    ]))
}

fn records_to_batch(records: &[AuditLogRecord]) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema(),
        vec![
            string_col(records, |r| r.event_type.as_str()),
            Arc::new(Int64Array::from_iter_values(
                records.iter().map(|r| r.event_time_us),
            )) as ArrayRef,
            string_col(records, |r| r.user.as_str()),
            string_col(records, |r| r.role.as_str()),
            string_col(records, |r| r.action.as_str()),
            string_col(records, |r| r.object_type.as_str()),
            string_col(records, |r| r.object_name.as_str()),
            string_col(records, |r| r.status.as_str()),
            string_col(records, |r| r.error.as_str()),
            string_col(records, |r| r.client_address.as_str()),
            string_col(records, |r| r.protocol.as_str()),
        ],
    )?)
}

fn string_col<F>(records: &[AuditLogRecord], f: F) -> ArrayRef
where
    F: Fn(&AuditLogRecord) -> &str,
{
    Arc::new(StringArray::from(
        records.iter().map(|r| Some(f(r))).collect::<Vec<_>>(),
    ))
}

// ---- AuditLog ----

#[derive(Debug, Clone)]
pub struct AuditLog {
    root_location: String,
    sender: Option<mpsc::UnboundedSender<AuditLogRecord>>,
}

impl AuditLog {
    pub fn new(config: AuditLogConfig, root: PathBuf) -> Self {
        let root_location = format!("file://{}", root.display());

        if let Err(error) = std::fs::create_dir_all(&root) {
            debug!(
                "audit log disabled because root directory '{}' could not be created: {}",
                root.display(),
                error
            );
            return Self {
                root_location,
                sender: None,
            };
        }

        if !config.enabled {
            return Self {
                root_location,
                sender: None,
            };
        }

        let (sender, receiver) = mpsc::unbounded_channel();
        let writer = AuditLogWriter::new(config, root_location.clone(), receiver);
        tokio::spawn(async move {
            writer.run().await;
        });

        Self {
            root_location,
            sender: Some(sender),
        }
    }

    pub fn disabled() -> Self {
        Self {
            root_location: "file://analyticsdb-catalog.managed/system/audit_log".to_string(),
            sender: None,
        }
    }

    pub fn root_location(&self) -> &str {
        &self.root_location
    }

    /// Fire an audit event.  Non-blocking — if the channel is closed the event
    /// is silently dropped.
    pub fn log_event(&self, record: AuditLogRecord) {
        if let Some(sender) = &self.sender {
            if let Err(error) = sender.send(record) {
                debug!("audit log send failed: {}", error);
            }
        }
    }
}

// ---- Background writer ----

struct AuditLogWriter {
    config: AuditLogConfig,
    root_location: String,
    receiver: mpsc::UnboundedReceiver<AuditLogRecord>,
}

impl AuditLogWriter {
    fn new(
        config: AuditLogConfig,
        root_location: String,
        receiver: mpsc::UnboundedReceiver<AuditLogRecord>,
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
        let mut retention_ticker = tokio::time::interval(Duration::from_secs(86400));

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
                    self.cleanup_expired_logs().await;
                }
            }
        }
    }

    async fn flush(&self, buffer: &mut Vec<AuditLogRecord>) {
        if buffer.is_empty() {
            return;
        }
        let records = std::mem::take(buffer);
        if let Err(error) = self.write_records(&records).await {
            debug!(
                "audit log flush failed; dropping {} record(s): {}",
                records.len(),
                error
            );
        }
    }

    async fn write_records(&self, records: &[AuditLogRecord]) -> Result<()> {
        let batch = records_to_batch(records)?;
        let (store, prefix) = storage::store_for_location(&self.root_location)?;
        let event_time = Utc
            .timestamp_micros(records[0].event_time_us)
            .single()
            .unwrap_or_else(Utc::now);
        let key = prefix
            .join(format!("{:04}", event_time.year()).as_str())
            .join(format!("{:02}", event_time.month()).as_str())
            .join(format!("{:02}", event_time.day()).as_str())
            .join(format!("{}.parquet", uuid::Uuid::now_v7()).as_str());
        storage::write_parquet_batches(&store, &key, schema(), &[batch]).await
    }

    async fn cleanup_expired_logs(&self) {
        if self.config.retention_days == 0 {
            return;
        }

        let (store, prefix) = match storage::store_for_location(&self.root_location) {
            Ok(res) => res,
            Err(_) => return,
        };

        let expiration =
            Utc::now() - Duration::from_secs(86400 * self.config.retention_days as u64);
        let expiration_prefix = format!(
            "{:04}/{:02}/{:02}/",
            expiration.year(),
            expiration.month(),
            expiration.day()
        );

        use object_store::ObjectStoreExt;
        let mut stream = store.list(Some(&prefix));
        use futures::StreamExt;
        while let Some(meta) = stream.next().await {
            let Ok(meta) = meta else { continue };
            let path = meta.location.as_ref();
            if let Some(rel_path) = path.strip_prefix(prefix.as_ref()) {
                let rel_path = rel_path.trim_start_matches('/');
                if rel_path.len() >= 10 && rel_path[0..4].chars().all(|c| c.is_ascii_digit()) {
                    let day_prefix = &rel_path[0..10];
                    if day_prefix < &expiration_prefix[0..10] {
                        if let Err(e) = store.delete(&meta.location).await {
                            debug!("failed to delete expired audit log {}: {}", path, e);
                        }
                    }
                }
            }
        }
    }
}

// ---- Unit tests ----

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_log_disabled_when_config_disabled() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let config = AuditLogConfig {
                enabled: false,
                ..AuditLogConfig::default()
            };
            let tmp =
                std::env::temp_dir().join(format!("adb-audit-disabled-{}", uuid::Uuid::now_v7()));
            let audit_log = AuditLog::new(config, tmp);
            // sender should be None — log_event is a no-op
            assert!(
                audit_log.sender.is_none(),
                "disabled audit log should have no sender"
            );
            // log_event should not panic
            audit_log.log_event(AuditLogRecord::success(
                AuditEventType::Login,
                "user",
                "role",
                "LOGIN",
                "",
                "",
                "embedded",
            ));
        });
    }

    #[test]
    fn audit_log_record_arrow_schema_has_expected_columns() {
        let s = schema();
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(names.contains(&"event_type"), "missing event_type");
        assert!(names.contains(&"event_time_us"), "missing event_time_us");
        assert!(names.contains(&"user"), "missing user");
        assert!(names.contains(&"role"), "missing role");
        assert!(names.contains(&"action"), "missing action");
        assert!(names.contains(&"object_type"), "missing object_type");
        assert!(names.contains(&"object_name"), "missing object_name");
        assert!(names.contains(&"status"), "missing status");
        assert!(names.contains(&"error"), "missing error");
        assert!(names.contains(&"client_address"), "missing client_address");
        assert!(names.contains(&"protocol"), "missing protocol");
    }
}
