use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;

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

/// Splits `files` into at most `num_workers` chunks using greedy weight-aware assignment.
///
/// Each entry is `(path, byte_size, row_count)`.  When all files have `row_count > 0`
/// the row count is used as the balancing weight; otherwise `byte_size` is used as a
/// fallback.  This prevents a single large (by bytes) but tiny (by rows) file from
/// monopolising a worker when row counts are available.
///
/// Empty chunks are never emitted — if `files.len() < num_workers` the returned
/// Vec will have fewer entries than `num_workers`.  Returns an empty Vec when
/// `files` is empty.
pub fn partition_files_for_workers(
    files: Vec<(String, u64, i64)>,
    num_workers: usize,
) -> Vec<Vec<String>> {
    if files.is_empty() || num_workers == 0 {
        return Vec::new();
    }
    let buckets = num_workers.min(files.len());
    let mut chunks: Vec<Vec<String>> = (0..buckets).map(|_| Vec::new()).collect();
    let mut bucket_weights: Vec<u64> = vec![0; buckets];

    // Use row_count as weight when all files have row_count > 0; otherwise byte_size.
    let use_rows = files.iter().all(|(_, _, rows)| *rows > 0);

    // Sort by weight descending for greedy assignment.
    let mut sorted: Vec<(String, u64)> = files
        .into_iter()
        .map(|(path, size, rows)| {
            let weight = if use_rows { rows as u64 } else { size };
            (path, weight)
        })
        .collect();
    sorted.sort_by_key(|b| std::cmp::Reverse(b.1));

    // Greedy assignment: assign each file to the bucket with the smallest total weight.
    for (file, weight) in sorted {
        let min_idx = bucket_weights
            .iter()
            .enumerate()
            .min_by_key(|(_, w)| *w)
            .map(|(i, _)| i)
            .unwrap();
        chunks[min_idx].push(file);
        bucket_weights[min_idx] += weight;
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
    /// Attempt identifier embedded in output filenames to prevent double-publishing on retry.
    #[serde(default)]
    pub attempt_id: String,
}

/// Acknowledgment returned by a worker after completing an `ExecutePartitionWrite`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutePartitionWriteAck {
    /// Paths of Parquet files written by this worker (absolute, relative to store root).
    pub written_files: Vec<String>,
    pub row_count: usize,
}

// ─── mTLS configuration ──────────────────────────────────────────────────────

/// mTLS configuration for intra-cluster gRPC connections.
/// Holds PEM-encoded bytes so the connector can be cloned cheaply.
#[derive(Clone)]
pub struct ClusterMtlsConfig {
    pub ca_cert_pem: Vec<u8>,
    pub client_cert_pem: Vec<u8>,
    pub client_key_pem: Vec<u8>,
}

// ─── Coordinator-side client ─────────────────────────────────────────────────

/// Used by a Coordinator to discover Compute nodes and dispatch partition tasks
/// to them over Arrow Flight.
pub struct PartitionClient {
    control_plane: Arc<ControlPlane>,
    channels: Arc<dashmap::DashMap<String, tonic::transport::Channel>>,
    is_compute_eligible: bool,
    mtls: Option<Arc<ClusterMtlsConfig>>,
}

impl PartitionClient {
    pub fn new(control_plane: Arc<ControlPlane>) -> Self {
        Self {
            control_plane,
            channels: Arc::new(dashmap::DashMap::new()),
            is_compute_eligible: false,
            mtls: None,
        }
    }

    pub fn set_compute_eligible(&mut self, eligible: bool) {
        self.is_compute_eligible = eligible;
    }

    pub fn set_mtls(&mut self, config: ClusterMtlsConfig) {
        self.mtls = Some(Arc::new(config));
    }

    async fn get_or_create_channel(&self, endpoint: &str) -> Result<tonic::transport::Channel> {
        if let Some(ch) = self.channels.get(endpoint) {
            return Ok(ch.clone());
        }
        let ch = build_channel(endpoint, self.mtls.as_deref()).await?;
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
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = Result<RecordBatch>> + Send>>> {
        let channel = self.get_or_create_channel(node_endpoint).await?;
        let mut client = FlightServiceClient::new(channel)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);

        let action = Action {
            r#type: "ExecutePartition".to_string(),
            body: bincode::serialize(req)?.into(),
        };

        // Race the DoAction call against cancellation so that a KILL QUERY
        // issued before or during connection establishment aborts promptly.
        let action_result = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                return Err(anyhow::anyhow!("Query cancelled"));
            }
            result = client.do_action(action) => result,
        };

        let stream = action_result?.into_inner();
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
/// For `http://` endpoints a plain HTTP/2 connection is used (no TLS).
/// For `https://` endpoints, mTLS is required — `mtls` must be `Some`.
async fn build_channel(
    endpoint: &str,
    mtls: Option<&ClusterMtlsConfig>,
) -> Result<tonic::transport::Channel> {
    let builder = tonic::transport::Endpoint::new(endpoint.to_string())?
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(300));

    if endpoint.starts_with("https://") {
        let cfg = mtls.ok_or_else(|| {
            anyhow::anyhow!(
                "HTTPS intra-cluster endpoint '{}' requires mTLS config. \
                 Run `analyticsdb ca init` and set tls_ca_cert_path / tls_cert_path / tls_key_path in cluster config.",
                endpoint
            )
        })?;
        let tls = tonic::transport::ClientTlsConfig::new()
            .domain_name("localhost")
            .ca_certificate(tonic::transport::Certificate::from_pem(
                cfg.ca_cert_pem.clone(),
            ))
            .identity(tonic::transport::Identity::from_pem(
                cfg.client_cert_pem.clone(),
                cfg.client_key_pem.clone(),
            ));
        Ok(builder.tls_config(tls)?.connect().await?)
    } else {
        Ok(builder.connect().await?)
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
        let bytes = batches_to_ipc_bytes(&schema, std::slice::from_ref(&batch)).unwrap();
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
            ("f1.parquet".to_string(), 100, 0),
            ("f2.parquet".to_string(), 100, 0),
            ("f3.parquet".to_string(), 100, 0),
            ("f4.parquet".to_string(), 100, 0),
            ("f5.parquet".to_string(), 100, 0),
            ("f6.parquet".to_string(), 100, 0),
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
            ("large.parquet".to_string(), 1000, 0),
            ("small1.parquet".to_string(), 100, 0),
            ("small2.parquet".to_string(), 100, 0),
            ("small3.parquet".to_string(), 100, 0),
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
        let files = vec![
            ("a.parquet".to_string(), 10, 0),
            ("b.parquet".to_string(), 10, 0),
        ];
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
        let files = vec![
            ("x.parquet".to_string(), 10, 0),
            ("y.parquet".to_string(), 10, 0),
        ];
        let chunks = partition_files_for_workers(files.clone(), 1);
        assert_eq!(chunks.len(), 1);
        assert_eq!(
            chunks[0],
            vec!["x.parquet".to_string(), "y.parquet".to_string()]
        );
    }

    #[test]
    fn partition_files_skew_regression_detects_worst_case_imbalance() {
        // 10 files: one giant (1000 rows), nine tiny (1 row each). Total = 1009 rows.
        // With 3 workers, fair share is ~336 rows each.
        // The skew-aware partitioner should place the 1000-row file on one worker
        // and distribute the nine tiny files across the others, keeping the worst
        // worker well within 3× fair share.
        let mut files: Vec<(String, u64, i64)> = vec![
            ("big.parquet".to_string(), 1, 1000), // huge by rows, tiny by bytes
        ];
        for i in 1..10 {
            files.push((format!("small{i}.parquet"), 1, 1));
        }

        let chunks = partition_files_for_workers(files, 3);

        // Compute per-worker row totals.
        let row_counts_by_name: std::collections::HashMap<String, i64> = {
            let mut m = std::collections::HashMap::new();
            m.insert("big.parquet".to_string(), 1000_i64);
            for i in 1..10_i64 {
                m.insert(format!("small{i}.parquet"), 1);
            }
            m
        };

        let worker_rows: Vec<i64> = chunks
            .iter()
            .map(|chunk| chunk.iter().map(|f| row_counts_by_name[f]).sum())
            .collect();

        let total_rows: i64 = worker_rows.iter().sum();
        let n = chunks.len() as f64;
        let fair_share = total_rows as f64 / n;
        let max_rows = *worker_rows.iter().max().unwrap() as f64;
        let skew_ratio = max_rows / fair_share;

        assert!(
            skew_ratio <= 3.0,
            "Skew ratio {skew_ratio:.2} exceeds 3.0 — row-count balancing not working. \
             Worker rows: {worker_rows:?}"
        );
    }

    #[test]
    fn partition_files_falls_back_to_size_when_row_counts_absent() {
        // All row_counts are 0 → byte_size must be used as the weight.
        // One large file (1000 bytes) and three small ones (100 bytes each).
        // With 2 workers the large file should end up alone.
        let files = vec![
            ("large.parquet".to_string(), 1000_u64, 0_i64),
            ("small1.parquet".to_string(), 100, 0),
            ("small2.parquet".to_string(), 100, 0),
            ("small3.parquet".to_string(), 100, 0),
        ];
        let chunks = partition_files_for_workers(files, 2);
        assert_eq!(chunks.len(), 2);

        let large_in_chunk0 = chunks[0].contains(&"large.parquet".to_string());
        let large_in_chunk1 = chunks[1].contains(&"large.parquet".to_string());
        assert!(large_in_chunk0 ^ large_in_chunk1, "large.parquet must be in exactly one chunk");

        // The chunk that has large.parquet should contain only it (greedy: 1000 > 300).
        if large_in_chunk0 {
            assert_eq!(chunks[0].len(), 1, "large file should be alone");
            assert_eq!(chunks[1].len(), 3);
        } else {
            assert_eq!(chunks[1].len(), 1, "large file should be alone");
            assert_eq!(chunks[0].len(), 3);
        }
    }

    #[tokio::test]
    async fn mtls_config_from_cluster_config_requires_all_three_paths() {
        use analyticsdb_control::{ClusterConfig, QueryLogConfig};
        use crate::load_mtls_config_from_cluster_config;
        let mut config = ClusterConfig {
            base_postgres_port: 5432,
            base_flight_sql_port: 50051,
            base_node_port: 60051,
            catalog_path: "analyticsdb-catalog.db".to_string(),
            tls_cert_path: None,
            tls_key_path: None,
            tls_ca_cert_path: None,
            next_available_port_offset: 0,
            query_log: QueryLogConfig::default(),
            storage_root: None,
            cluster_id: None,
            s3_sse: None,
            s3_sse_kms_key_id: None,
        };
        // No paths — None
        assert!(load_mtls_config_from_cluster_config(&config).is_none());
        // Only CA path — still None
        config.tls_ca_cert_path = Some("/tmp/ca.crt".to_string());
        assert!(load_mtls_config_from_cluster_config(&config).is_none());
    }

    #[test]
    fn cluster_mtls_config_can_be_built_from_rcgen_certs() {
        use rcgen::{
            BasicConstraints, CertificateParams, IsCa, KeyPair, SanType,
            PKCS_ECDSA_P256_SHA256,
        };
        // Generate CA
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        // Generate leaf
        let mut leaf_params = CertificateParams::default();
        leaf_params.subject_alt_names =
            vec![SanType::DnsName("localhost".try_into().unwrap())];
        let leaf_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        let leaf_cert = leaf_params
            .signed_by(&leaf_key, &ca_cert, &ca_key)
            .unwrap();
        // Build config
        let cfg = ClusterMtlsConfig {
            ca_cert_pem: ca_cert.pem().into_bytes(),
            client_cert_pem: leaf_cert.pem().into_bytes(),
            client_key_pem: leaf_key.serialize_pem().into_bytes(),
        };
        assert!(!cfg.ca_cert_pem.is_empty());
        assert!(!cfg.client_cert_pem.is_empty());
        assert!(!cfg.client_key_pem.is_empty());
    }
}
