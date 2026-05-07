use std::io::Cursor;
use std::sync::Arc;

use analyticsdb_control::{ClusterNode, ControlPlane, NodeRole, NodeStatus};
use analyticsdb_core::SessionContext;
use anyhow::Result;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::Action;
use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

/// Payload sent from a Coordinator to a Compute node via the `ExecutePartition`
/// Flight DoAction.  The coordinator constructs `sql` so that it is a complete,
/// self-contained DataFusion query (e.g. using `read_parquet([…])` for the
/// assigned files).  `partition_files` is included for tracing and for use by
/// future query-planning phases.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutePartitionRequest {
    /// Query identifier issued by the Coordinator's admission control.
    pub query_id: String,
    /// Complete SQL to execute on the worker node.
    pub sql: String,
    /// Session (user, role, database, schema) to run under.
    pub session: SessionContext,
    /// Parquet file paths assigned to this worker.  May be empty when the
    /// coordinator sends the original SQL without file-level partitioning.
    pub partition_files: Vec<String>,
}

/// Splits `files` into at most `num_workers` chunks using round-robin assignment.
///
/// Empty chunks are never emitted — if `files.len() < num_workers` the returned
/// Vec will have fewer entries than `num_workers`.  Returns an empty Vec when
/// `files` is empty.
pub fn partition_files_for_workers(files: Vec<String>, num_workers: usize) -> Vec<Vec<String>> {
    if files.is_empty() || num_workers == 0 {
        return Vec::new();
    }
    let buckets = num_workers.min(files.len());
    let mut chunks: Vec<Vec<String>> = (0..buckets).map(|_| Vec::new()).collect();
    for (i, file) in files.into_iter().enumerate() {
        chunks[i % buckets].push(file);
    }
    chunks
}

// ─── Arrow IPC encoding helpers ─────────────────────────────────────────────

/// Encodes a slice of `RecordBatch`es as a single Arrow IPC stream.
pub fn batches_to_ipc_bytes(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Bytes> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref())?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(Bytes::from(buf))
}

/// Decodes an Arrow IPC stream into `RecordBatch`es.
pub fn ipc_bytes_to_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    reader.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

// ─── Coordinator-side client ─────────────────────────────────────────────────

/// Used by a Coordinator to discover Compute nodes and dispatch partition tasks
/// to them over Arrow Flight.
pub struct PartitionClient {
    control_plane: Arc<ControlPlane>,
}

impl PartitionClient {
    pub fn new(control_plane: Arc<ControlPlane>) -> Self {
        Self { control_plane }
    }

    /// Returns all Ready Compute nodes available to accept partition tasks.
    pub async fn list_compute_nodes(&self) -> Result<Vec<ClusterNode>> {
        Ok(self
            .control_plane
            .list_nodes()
            .await?
            .into_iter()
            .filter(|n| n.role == NodeRole::Compute && n.status == NodeStatus::Ready)
            .collect())
    }

    /// Sends an `ExecutePartition` DoAction to a Compute node and collects the
    /// returned `RecordBatch`es.  The node endpoint must be a fully-qualified
    /// URI such as `http://host:50052` or `https://host:50052`.
    pub async fn execute_on_node(
        &self,
        node_endpoint: &str,
        req: &ExecutePartitionRequest,
    ) -> Result<Vec<RecordBatch>> {
        let channel = build_channel(node_endpoint).await?;
        let mut client = FlightSqlServiceClient::new(channel);

        let action = Action {
            r#type: "ExecutePartition".to_string(),
            body: serde_json::to_vec(req)?.into(),
        };

        let mut stream = client.do_action(action).await?;
        let mut all_batches = Vec::new();

        while let Some(result) = stream.next().await {
            let flight_result = result?;
            let batches = ipc_bytes_to_batches(&flight_result.body)?;
            all_batches.extend(batches);
        }

        Ok(all_batches)
    }
}

/// Builds a tonic transport channel to `endpoint`.  Supports both `http://`
/// and `https://` schemes; for HTTPS the system trust store is used.
async fn build_channel(endpoint: &str) -> Result<tonic::transport::Channel> {
    let mut builder = tonic::transport::Endpoint::new(endpoint.to_string())?
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(300));

    if endpoint.starts_with("https://") {
        let tls = tonic::transport::ClientTlsConfig::new().with_enabled_roots();
        builder = builder.tls_config(tls)?;
    }

    Ok(builder.connect().await?)
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let col = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        RecordBatch::try_new(schema, vec![col]).unwrap()
    }

    #[test]
    fn ipc_round_trip() {
        let batch = make_batch();
        let schema = batch.schema();
        let bytes = batches_to_ipc_bytes(&schema, &[batch.clone()]).unwrap();
        let decoded = ipc_bytes_to_batches(&bytes).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].num_rows(), 3);
    }

    #[test]
    fn ipc_round_trip_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let bytes = batches_to_ipc_bytes(&schema, &[]).unwrap();
        let decoded = ipc_bytes_to_batches(&bytes).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn execute_partition_request_serde() {
        let req = ExecutePartitionRequest {
            query_id: "q1".to_string(),
            sql: "SELECT 1".to_string(),
            session: SessionContext::default(),
            partition_files: vec!["/tmp/a.parquet".to_string()],
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: ExecutePartitionRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.query_id, "q1");
        assert_eq!(decoded.partition_files, vec!["/tmp/a.parquet"]);
    }

    #[test]
    fn partition_files_round_robin_even() {
        let files: Vec<String> = (0..6).map(|i| format!("f{i}.parquet")).collect();
        let chunks = partition_files_for_workers(files, 3);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], vec!["f0.parquet", "f3.parquet"]);
        assert_eq!(chunks[1], vec!["f1.parquet", "f4.parquet"]);
        assert_eq!(chunks[2], vec!["f2.parquet", "f5.parquet"]);
    }

    #[test]
    fn partition_files_fewer_files_than_workers() {
        let files: Vec<String> = vec!["a.parquet".to_string(), "b.parquet".to_string()];
        let chunks = partition_files_for_workers(files, 5);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0], vec!["a.parquet"]);
        assert_eq!(chunks[1], vec!["b.parquet"]);
    }

    #[test]
    fn partition_files_empty() {
        let chunks = partition_files_for_workers(vec![], 4);
        assert!(chunks.is_empty());
    }

    #[test]
    fn partition_files_single_worker() {
        let files: Vec<String> = vec!["x.parquet".to_string(), "y.parquet".to_string()];
        let chunks = partition_files_for_workers(files.clone(), 1);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], files);
    }
}
