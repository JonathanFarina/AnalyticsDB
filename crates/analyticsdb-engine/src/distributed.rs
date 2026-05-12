use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use analyticsdb_control::{CatalogColumn, ClusterNode, ControlPlane, NodeRole, NodeStatus};
use analyticsdb_core::SessionContext;
use anyhow::Result;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use futures::{Future, StreamExt};
use hyper_util::rt::tokio::TokioIo;
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
    /// Coordinator query id shared by all distributed sibling tasks.
    #[serde(default)]
    pub initial_query_id: String,
    /// Node ID of the coordinator that admitted the query.
    #[serde(default)]
    pub coordinator_node_id: String,
    /// Complete SQL to execute on the worker node.
    pub sql: String,
    /// Session (user, role, database, schema) to run under.
    pub session: SessionContext,
    /// Parquet file paths assigned to this worker.  May be empty when the
    /// coordinator sends the original SQL without file-level partitioning.
    pub partition_files: Vec<String>,
    /// Catalog schema for the source table behind `partition_files`.
    ///
    /// Workers must not rely only on Parquet schema inference, because managed
    /// table writes may need catalog types such as NUMERIC/DATE to preserve SQL
    /// semantics across distributed execution.
    #[serde(default)]
    pub source_columns: Vec<CatalogColumn>,
}

/// Splits `files` into at most `num_workers` chunks using greedy size-aware assignment.
///
/// Empty chunks are never emitted — if `files.len() < num_workers` the returned
/// Vec will have fewer entries than `num_workers`.  Returns an empty Vec when
/// `files` is empty.
pub fn partition_files_for_workers(
    files: Vec<(String, u64)>,
    num_workers: usize,
) -> Vec<Vec<String>> {
    if files.is_empty() || num_workers == 0 {
        return Vec::new();
    }
    let buckets = num_workers.min(files.len());
    let mut chunks: Vec<Vec<String>> = (0..buckets).map(|_| Vec::new()).collect();
    let mut bucket_sizes: Vec<u64> = vec![0; buckets];

    // Sort by size descending
    let mut sorted = files;
    sorted.sort_by(|a, b| b.1.cmp(&a.1));

    // Greedy assignment: assign each file to the bucket with the smallest total size.
    for (file, size) in sorted {
        let min_idx = bucket_sizes
            .iter()
            .enumerate()
            .min_by_key(|(_, s)| *s)
            .map(|(i, _)| i)
            .unwrap();
        chunks[min_idx].push(file);
        bucket_sizes[min_idx] += size;
    }
    chunks
}

// ─── Arrow IPC encoding helpers ─────────────────────────────────────────────

pub fn calculate_optimal_worker_count(
    total_size: u64,
    file_count: usize,
    available_nodes: usize,
) -> usize {
    if available_nodes <= 1 {
        return available_nodes;
    }

    // Heuristic:
    // - Pick N such that each worker has ~128MB of data.
    // - Pick N such that each worker has at least 2 files.
    // - N must be between 1 and available_nodes.

    // For very small datasets, just use 1 node (coordinator).
    if total_size < 10 * 1024 * 1024 && file_count < 4 {
        return 1;
    }

    let by_size = (total_size as f64 / (128.0 * 1024.0 * 1024.0)).ceil() as usize;
    let by_file = (file_count as f64 / 2.0).ceil() as usize;

    by_size.max(by_file).clamp(1, available_nodes)
}

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

/// Payload sent from a Coordinator to a Compute node via `ExecutePartitionWrite`.
///
/// The worker runs `sql` (a self-contained SELECT, typically using `read_parquet([…])`),
/// writes every output batch directly to `write_prefix` in the shared object store as
/// a distinct UUID-named Parquet file, then responds with an acknowledgment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutePartitionWriteRequest {
    pub query_id: String,
    #[serde(default)]
    pub initial_query_id: String,
    #[serde(default)]
    pub coordinator_node_id: String,
    pub sql: String,
    pub session: SessionContext,
    pub partition_files: Vec<String>,
    #[serde(default)]
    pub source_columns: Vec<CatalogColumn>,
    /// Absolute filesystem path prefix under which the worker writes its output files.
    pub write_prefix: String,
}

/// Acknowledgment returned by a worker after completing an `ExecutePartitionWrite`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutePartitionWriteAck {
    /// Paths of Parquet files written by this worker (absolute, relative to store root).
    pub written_files: Vec<String>,
    pub row_count: usize,
}

// ─── Coordinator-side client ─────────────────────────────────────────────────

/// Used by a Coordinator to discover Compute nodes and dispatch partition tasks
/// to them over Arrow Flight.
pub struct PartitionClient {
    control_plane: Arc<ControlPlane>,
    channels: Arc<dashmap::DashMap<String, tonic::transport::Channel>>,
    is_compute_eligible: bool,
}

impl PartitionClient {
    pub fn new(control_plane: Arc<ControlPlane>) -> Self {
        Self {
            control_plane,
            channels: Arc::new(dashmap::DashMap::new()),
            is_compute_eligible: false,
        }
    }

    pub fn set_compute_eligible(&mut self, eligible: bool) {
        self.is_compute_eligible = eligible;
    }

    async fn get_or_create_channel(&self, endpoint: &str) -> Result<tonic::transport::Channel> {
        if let Some(ch) = self.channels.get(endpoint) {
            return Ok(ch.clone());
        }
        let ch = build_channel(endpoint).await?;
        self.channels.insert(endpoint.to_string(), ch.clone());
        Ok(ch)
    }

    /// Returns all Ready Compute nodes available to accept partition tasks.
    /// If `is_compute_eligible` is true and this node is the coordinator,
    /// it includes itself in the list.
    pub async fn list_compute_nodes(&self) -> Result<Vec<ClusterNode>> {
        let mut nodes = self
            .control_plane
            .list_nodes()
            .await?
            .into_iter()
            .filter(|n| n.role == NodeRole::Compute && n.status == NodeStatus::Ready)
            .collect::<Vec<_>>();

        if self.is_compute_eligible {
            if let Some(coordinator) = self.control_plane.local_node().await {
                // If the coordinator is not already in the list (as a compute node), add it.
                if !nodes.iter().any(|n| n.id == coordinator.id) {
                    nodes.push(coordinator);
                }
            }
        }

        Ok(nodes)
    }

    pub async fn mark_node_unavailable(&self, node_id: &str) -> Result<()> {
        self.control_plane.mark_node_unavailable(node_id).await
    }

    pub fn node_channel_endpoint(node: &ClusterNode) -> &str {
        node.internal_endpoint
            .as_deref()
            .unwrap_or(node.endpoint.as_str())
    }

    /// Sends an `ExecutePartitionWrite` DoAction to a Compute node, instructing
    /// the worker to run `req.sql` and write its output directly to the shared
    /// object store under `req.write_prefix`.  Returns the acknowledgment
    /// containing the written file paths and row count.
    pub async fn write_on_node(
        &self,
        node_endpoint: &str,
        req: &ExecutePartitionWriteRequest,
    ) -> Result<ExecutePartitionWriteAck> {
        let channel = self.get_or_create_channel(node_endpoint).await?;
        let mut client = FlightServiceClient::new(channel)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);

        let action = Action {
            r#type: "ExecutePartitionWrite".to_string(),
            body: bincode::serialize(req)?.into(),
        };

        let mut stream = client.do_action(action).await?.into_inner();
        let flight_result = stream
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("Worker returned no acknowledgment"))??;
        let ack: ExecutePartitionWriteAck = serde_json::from_slice(&flight_result.body)?;
        Ok(ack)
    }

    /// Sends an `ExecutePartition` DoAction to a Compute node and returns a stream
    /// of `RecordBatch`es.  The node endpoint must be a fully-qualified
    /// URI such as `http://host:50052` or `https://host:50052`.
    pub async fn execute_on_node(
        &self,
        node_endpoint: &str,
        req: &ExecutePartitionRequest,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = Result<RecordBatch>> + Send>>> {
        let channel = self.get_or_create_channel(node_endpoint).await?;
        let mut client = FlightServiceClient::new(channel)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);

        let action = Action {
            r#type: "ExecutePartition".to_string(),
            body: bincode::serialize(req)?.into(),
        };

        let stream = client.do_action(action).await?.into_inner();
        let batch_stream = stream.then(|result| async move {
            match result {
                Ok(flight_result) => match ipc_bytes_to_batches(&flight_result.body) {
                    Ok(batches) => {
                        let res: Vec<Result<RecordBatch>> = batches.into_iter().map(Ok).collect();
                        futures::stream::iter(res)
                    }
                    Err(e) => futures::stream::iter(vec![Err(anyhow::anyhow!(e))]),
                },
                Err(e) => futures::stream::iter(vec![Err(anyhow::anyhow!(e))]),
            }
        });

        Ok(Box::pin(batch_stream.flatten()))
    }
}

/// Builds a tonic transport channel to `endpoint`.
///
/// For `http://` endpoints a plain HTTP/2 connection is used.
/// For `https://` endpoints an encrypted connection is established but
/// certificate verification is skipped — intra-cluster gRPC runs on a
/// trusted internal network and all nodes share the same cluster cert, so
/// peer verification adds no real security benefit here.
async fn build_channel(endpoint: &str) -> Result<tonic::transport::Channel> {
    let builder = tonic::transport::Endpoint::new(endpoint.to_string())?
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(300));

    if endpoint.starts_with("https://") {
        Ok(builder
            .connect_with_connector(ClusterInternalConnector::new())
            .await?)
    } else {
        Ok(builder.connect().await?)
    }
}

// ─── Insecure TLS connector for intra-cluster gRPC ──────────────────────────

#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Clone)]
struct ClusterInternalConnector {
    tls_config: Arc<rustls::ClientConfig>,
}

impl ClusterInternalConnector {
    fn new() -> Self {
        let mut cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("rustls config")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerifier))
        .with_no_client_auth();
        cfg.alpn_protocols = vec![b"h2".to_vec()];
        Self {
            tls_config: Arc::new(cfg),
        }
    }
}

impl tower::Service<http::Uri> for ClusterInternalConnector {
    type Response = TokioIo<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        let tls_config = Arc::clone(&self.tls_config);
        let host = uri.host().unwrap_or("localhost").to_string();
        let port = uri.port_u16().unwrap_or(443);
        Box::pin(async move {
            let tcp = tokio::net::TcpStream::connect(format!("{}:{}", host, port)).await?;
            let connector = tokio_rustls::TlsConnector::from(tls_config);
            let domain = rustls_pki_types::ServerName::try_from(host)
                .map_err(|e| anyhow::anyhow!("Invalid DNS name: {}", e))?
                .to_owned();
            let tls = connector.connect(domain, tcp).await?;
            Ok(TokioIo::new(tls))
        })
    }
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
            initial_query_id: "q1".to_string(),
            coordinator_node_id: "node-1".to_string(),
            sql: "SELECT 1".to_string(),
            session: SessionContext::default(),
            partition_files: vec!["/tmp/a.parquet".to_string()],
            source_columns: vec![],
        };
        let bytes = bincode::serialize(&req).unwrap();
        let decoded: ExecutePartitionRequest = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.query_id, "q1");
        assert_eq!(decoded.coordinator_node_id, "node-1");
        assert_eq!(decoded.partition_files, vec!["/tmp/a.parquet"]);
        assert!(decoded.source_columns.is_empty());
    }

    #[test]
    fn partition_client_prefers_internal_node_channel_endpoint() {
        let node = ClusterNode {
            id: "worker-1".to_string(),
            role: NodeRole::Compute,
            endpoint: "https://worker.example:50052".to_string(),
            internal_endpoint: Some("http://10.0.0.2:60052".to_string()),
            status: NodeStatus::Ready,
            last_heartbeat_at_epoch_ms: 0,
        };

        assert_eq!(
            PartitionClient::node_channel_endpoint(&node),
            "http://10.0.0.2:60052"
        );
    }

    #[test]
    fn partition_client_falls_back_to_client_endpoint_for_legacy_nodes() {
        let node = ClusterNode {
            id: "worker-1".to_string(),
            role: NodeRole::Compute,
            endpoint: "https://worker.example:50052".to_string(),
            internal_endpoint: None,
            status: NodeStatus::Ready,
            last_heartbeat_at_epoch_ms: 0,
        };

        assert_eq!(
            PartitionClient::node_channel_endpoint(&node),
            "https://worker.example:50052"
        );
    }

    #[test]
    fn partition_files_size_aware_balanced() {
        let files = vec![
            ("f1.parquet".to_string(), 100),
            ("f2.parquet".to_string(), 100),
            ("f3.parquet".to_string(), 100),
            ("f4.parquet".to_string(), 100),
            ("f5.parquet".to_string(), 100),
            ("f6.parquet".to_string(), 100),
        ];
        let chunks = partition_files_for_workers(files, 3);
        assert_eq!(chunks.len(), 3);
        // With equal sizes, it should still be balanced
        for chunk in &chunks {
            assert_eq!(chunk.len(), 2);
        }
    }

    #[test]
    fn partition_files_size_aware_greedy() {
        let files = vec![
            ("large.parquet".to_string(), 1000),
            ("small1.parquet".to_string(), 100),
            ("small2.parquet".to_string(), 100),
            ("small3.parquet".to_string(), 100),
        ];
        // 2 workers:
        // Worker 1 should get large.parquet (1000)
        // Worker 2 should get all small ones (300)
        let chunks = partition_files_for_workers(files, 2);
        assert_eq!(chunks.len(), 2);

        let chunk0 = &chunks[0];
        let chunk1 = &chunks[1];

        if chunk0.contains(&"large.parquet".to_string()) {
            assert_eq!(chunk0.len(), 1);
            assert_eq!(chunk1.len(), 3);
        } else {
            assert_eq!(chunk1.len(), 1);
            assert_eq!(chunk0.len(), 3);
        }
    }

    #[test]
    fn partition_files_fewer_files_than_workers() {
        let files = vec![("a.parquet".to_string(), 10), ("b.parquet".to_string(), 10)];
        let chunks = partition_files_for_workers(files, 5);
        assert_eq!(chunks.len(), 2);
    }

    #[test]
    fn partition_files_empty() {
        let chunks = partition_files_for_workers(vec![], 4);
        assert!(chunks.is_empty());
    }

    #[test]
    fn partition_files_single_worker() {
        let files = vec![("x.parquet".to_string(), 10), ("y.parquet".to_string(), 10)];
        let chunks = partition_files_for_workers(files.clone(), 1);
        assert_eq!(chunks.len(), 1);
        assert_eq!(
            chunks[0],
            vec!["x.parquet".to_string(), "y.parquet".to_string()]
        );
    }
}
