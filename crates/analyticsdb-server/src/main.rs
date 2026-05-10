use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use analyticsdb_control::{ClusterNode, NodeRole, NodeStatus};
use analyticsdb_engine::PrototypeEngine;
use analyticsdb_protocol::{serve_flight_sql_with_label, serve_postgres_wire};
use anyhow::{Context as AnyhowContext, Result};
use clap::Parser;
use futures::Future;
use hyper_util::rt::tokio::TokioIo;
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tracing::{error, info, warn};

const BANNER: &str = "
\x1b[1;36m     _                _       _   _          ____  ____  \x1b[0m
\x1b[1;36m    / \\   _ __   __ _| |_   _| |_(_) ___ ___|  _ \\| __ ) \x1b[0m
\x1b[1;36m   / _ \\ | '_ \\ / _` | | | | | __| |/ __/ __| | | |  _ \\ \x1b[0m
\x1b[1;36m  / ___ \\| | | | (_| | | |_| | |_| | (__\\__ \\ |_| | |_) |\x1b[0m
\x1b[1;36m /_/   \\_\\_| |_|\\__,_|_|\\__, |\\__|_|\\___|___/____/|____/ \x1b[0m
\x1b[1;36m                        |___/                            \x1b[0m
";

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
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Clone)]
struct InsecureConnector {
    tls_config: Arc<rustls::ClientConfig>,
}

impl InsecureConnector {
    fn new() -> Self {
        let mut rustls_config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("Failed to create rustls config")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerifier))
        .with_no_client_auth();

        rustls_config.alpn_protocols = vec![b"h2".to_vec()];

        Self {
            tls_config: Arc::new(rustls_config),
        }
    }
}

impl tower::Service<http::Uri> for InsecureConnector {
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
        let addr = format!("{}:{}", host, port);

        Box::pin(async move {
            let tcp = tokio::net::TcpStream::connect(addr).await?;
            let connector = tokio_rustls::TlsConnector::from(tls_config);
            let domain = rustls_pki_types::ServerName::try_from(host)
                .map_err(|e| anyhow::anyhow!("Invalid DNS name: {}", e))?
                .to_owned();
            let tls = connector.connect(domain, tcp).await?;
            Ok(TokioIo::new(tls))
        })
    }
}

#[tokio::main]
async fn main() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt().with_env_filter(filter).init();

    if let Err(error) = run().await {
        eprintln!("\n\x1b[1;31mFATAL ERROR: {error:#}\x1b[0m");
        error!("ERROR: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    println!("{}", BANNER);

    if let Some(config_path) = &cli.cluster_config {
        if !std::path::Path::new(config_path).exists() {
            anyhow::bail!("Cluster configuration file not found: {}", config_path);
        }
    } else if cli.role == NodeRole::Control && cli.join.is_none() && !cli.init_cluster {
        warn!("\x1b[1;31mNo cluster configuration provided for control node. Using bootstrap defaults.\x1b[0m");
    }

    // Addresses: explicit CLI values take precedence; joining nodes get ports
    // assigned by the coordinator, so these are optional for non-primary nodes.
    let mut pg_addr = cli
        .postgres_addr
        .clone()
        .unwrap_or_else(|| "127.0.0.1:5432".to_string());
    let mut flight_addr = cli
        .flight_sql_addr
        .clone()
        .unwrap_or_else(|| "127.0.0.1:50051".to_string());
    let mut node_addr = cli
        .node_addr
        .clone()
        .unwrap_or_else(|| "127.0.0.1:60051".to_string());
    let mut catalog_path = cli.catalog_path.clone();
    let mut node_id = cli.node_id.clone();
    let mut final_tls_cert = cli.tls_cert.clone();
    let mut final_tls_key = cli.tls_key.clone();
    // When joining with --cluster-config, load the config now so the cert
    // path is available as a trust anchor before we connect to the coordinator.
    let mut final_tls_ca_cert = cli.tls_ca_cert.clone();
    if cli.join.is_some() {
        if let Some(config_path) = &cli.cluster_config {
            let config_raw = std::fs::read_to_string(config_path)
                .with_context(|| format!("Failed to read cluster config: {}", config_path))?;
            let config: analyticsdb_control::ClusterConfig = serde_json::from_str(&config_raw)?;
            // Use the cert from the config as the CA trust anchor if not explicitly set.
            if final_tls_ca_cert.is_none() {
                final_tls_ca_cert = config.tls_cert_path.clone();
            }
            if final_tls_cert.is_none() {
                final_tls_cert = config.tls_cert_path;
            }
            if final_tls_key.is_none() {
                final_tls_key = config.tls_key_path;
            }
        }
    }

    // If joining a cluster, request configuration from the coordinator
    if let Some(coordinator_endpoint) = &cli.join {
        let is_https = coordinator_endpoint.starts_with("https");
        let normalized = if coordinator_endpoint.contains("://") {
            coordinator_endpoint.clone()
        } else {
            format!("http://{}", coordinator_endpoint)
        };

        println!(
            " \x1b[1;33mJoining cluster via coordinator at {}...\x1b[0m",
            normalized
        );

        let channel = if is_https && cli.tls_insecure {
            warn!("\x1b[1;31mTLS verification disabled for cluster join. This is insecure!\x1b[0m");
            tonic::transport::Endpoint::new(normalized)?
                .connect_timeout(std::time::Duration::from_secs(5))
                .connect_with_connector(InsecureConnector::new())
                .await
                .with_context(|| {
                    "Failed to connect to cluster coordinator using insecure TLS connector."
                })?
        } else {
            let mut endpoint = tonic::transport::Endpoint::new(normalized)?
                .connect_timeout(std::time::Duration::from_secs(5));

            if is_https || coordinator_endpoint.contains(":443") {
                let mut tls_config = tonic::transport::ClientTlsConfig::new()
                    .domain_name(cli.tls_domain.as_deref().unwrap_or("localhost"));

                let trust_anchor_path = final_tls_ca_cert.as_ref().or(final_tls_cert.as_ref());
                if let Some(cert_path) = trust_anchor_path {
                    info!(
                        "Using certificate from {} as trust anchor for joining coordinator",
                        cert_path
                    );
                    let pem = std::fs::read(cert_path)?;
                    let ca = tonic::transport::Certificate::from_pem(pem);
                    tls_config = tls_config.ca_certificate(ca);
                } else {
                    tls_config = tls_config.with_enabled_roots();
                }

                endpoint = endpoint.tls_config(tls_config)?;
            }

            endpoint.connect()
                .await
                .with_context(|| "Failed to connect to cluster coordinator. Ensure the coordinator node is running and accessible (check if TLS is required).")?
        };

        let mut client = arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

        let req = analyticsdb_control::raft::JoinRequest {
            node_id: cli.node_id.clone(),
            endpoint: "auto".to_string(),
            advertise_host: Some(cli.advertise_host.clone()),
        };
        let body = serde_json::to_vec(&req)?;
        let action = arrow_flight::Action {
            r#type: "JoinCluster".to_string(),
            body: body.into(),
        };

        let mut stream = client.do_action(action).await?;
        let res_bytes = stream
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("No response from coordinator"))??;
        let res: analyticsdb_control::raft::JoinResponse = serde_json::from_slice(&res_bytes.body)?;

        println!(
            " \x1b[1;32mJoined cluster. Assigned ID: '{}'\x1b[0m",
            res.node_id
        );
        println!(
            " \x1b[1;32m  PostgreSQL  → {}:{}\x1b[0m",
            cli.advertise_host, res.postgres_port
        );
        println!(
            " \x1b[1;32m  Flight SQL  → {}:{}\x1b[0m",
            cli.advertise_host, res.flight_sql_port
        );
        println!(
            " \x1b[1;32m  Node channel → {}:{}\x1b[0m",
            cli.advertise_host, res.node_port
        );
        info!(
            "Joined cluster. ID='{}', PG={}, Flight={}, Node={}",
            res.node_id, res.postgres_port, res.flight_sql_port, res.node_port
        );
        node_id = Some(res.node_id);
        // Bind on all interfaces so the coordinator and peers can reach this node.
        pg_addr = format!("0.0.0.0:{}", res.postgres_port);
        flight_addr = format!("0.0.0.0:{}", res.flight_sql_port);
        node_addr = format!("0.0.0.0:{}", res.node_port);
        catalog_path = res.config.catalog_path.clone();

        // Inherit TLS cert/key from the JoinResponse so the compute node's
        // flight server also serves with TLS.  The coordinator dispatches to
        // it via https:// using an insecure connector (cert verification
        // skipped for intra-cluster gRPC on a trusted internal network).
        if final_tls_cert.is_none() {
            final_tls_cert = res.config.tls_cert_path;
        }
        if final_tls_key.is_none() {
            final_tls_key = res.config.tls_key_path;
        }
    }

    let mut actual_catalog_path = catalog_path.clone();

    if let Some(config_path) = &cli.cluster_config {
        if cli.join.is_none() {
            info!("Loading cluster configuration from {}...", config_path);
            let config_raw = std::fs::read_to_string(config_path)?;
            let config: analyticsdb_control::ClusterConfig = serde_json::from_str(&config_raw)?;

            if final_tls_cert.is_none() {
                final_tls_cert = config.tls_cert_path.clone();
            }
            if final_tls_key.is_none() {
                final_tls_key = config.tls_key_path.clone();
            }
            actual_catalog_path = config.catalog_path.clone();
        }
    }

    let engine = Arc::new(PrototypeEngine::from_catalog_path(&actual_catalog_path).await?);
    engine
        .control_plane()
        .set_tls_paths(final_tls_cert.clone(), final_tls_key.clone())
        .await?;

    let node_id = node_id.unwrap_or_else(|| "standalone".to_string());

    // Joining nodes are already registered by the coordinator with the correct
    // advertised endpoints. Re-registering here would overwrite those with
    // bind addresses ("0.0.0.0:port") which are not valid peer URIs.
    if cli.join.is_none() {
        // Clear stale Compute node entries from previous runs so the
        // coordinator doesn't dispatch to workers that are no longer running.
        // Compute nodes re-register themselves when they join.
        engine.control_plane().clear_compute_nodes().await?;

        let flight_scheme = if final_tls_cert.is_some() {
            "https"
        } else {
            "http"
        };
        let flight_port = flight_addr.rsplit(':').next().unwrap_or("50051");
        let node_port = node_addr.rsplit(':').next().unwrap_or("60051");
        engine
            .control_plane()
            .register_node(ClusterNode {
                id: node_id.clone(),
                role: cli.role.clone(),
                endpoint: format!("{}://{}:{}", flight_scheme, cli.advertise_host, flight_port),
                internal_endpoint: Some(format!("http://{}:{}", cli.advertise_host, node_port)),
                status: NodeStatus::Ready,
                last_heartbeat_at_epoch_ms: 0,
            })
            .await?;

        println!(
            " \x1b[1;32mRegistered node '{}' as {:?} in control plane\x1b[0m",
            node_id, cli.role
        );
        info!(
            "Registered node '{}' as {:?} in control plane",
            node_id, cli.role
        );
    }

    let pg_listener = TcpListener::bind(&pg_addr)
        .await
        .context("Failed to bind PostgreSQL port")?;
    let flight_listener = TcpListener::bind(&flight_addr)
        .await
        .context("Failed to bind Flight SQL port")?;
    let node_listener = TcpListener::bind(&node_addr)
        .await
        .context("Failed to bind node communication port")?;

    info!("PostgreSQL protocol listening on: {}", pg_addr);
    info!("Flight SQL protocol listening on: {}", flight_addr);
    info!("Node communication channel listening on: {}", node_addr);

    let tls_config = match (&final_tls_cert, &final_tls_key) {
        (Some(cert_path), Some(key_path)) => {
            let cert = std::fs::read(cert_path)?;
            let key = std::fs::read(key_path)?;
            Some((cert, key))
        }
        (None, None) => None,
        _ => anyhow::bail!("Both --tls-cert and --tls-key must be provided to enable TLS"),
    };

    // Client-facing Flight SQL uses TLS when cert/key paths are configured.
    let flight_tls_config = tls_config.clone();

    let engine_for_flight = Arc::clone(&engine);
    let flight_handle = tokio::spawn(async move {
        if let Err(e) = serve_flight_sql_with_label(
            flight_listener,
            engine_for_flight,
            flight_tls_config,
            "Client Flight SQL",
        )
        .await
        {
            error!("Flight SQL server error: {}", e);
        }
    });

    let engine_for_node = Arc::clone(&engine);
    let node_handle = tokio::spawn(async move {
        if let Err(e) =
            serve_flight_sql_with_label(node_listener, engine_for_node, None, "Node channel").await
        {
            error!("Node communication server error: {}", e);
        }
    });

    let engine_for_pg = Arc::clone(&engine);
    let pg_handle = tokio::spawn(async move {
        if let Err(e) = serve_postgres_wire(pg_listener, engine_for_pg).await {
            error!("PostgreSQL server error: {}", e);
        }
    });

    println!("\n\x1b[1;32mStartup complete. Ready to accept connections.\x1b[0m");

    tokio::select! {
        _ = flight_handle => {},
        _ = node_handle => {},
        _ = pg_handle => {},
    }

    Ok(())
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(long)]
    node_id: Option<String>,
    #[arg(long, value_enum, default_value = "control")]
    role: NodeRole,
    #[arg(long)]
    init_cluster: bool,
    #[arg(long)]
    join: Option<String>,
    #[arg(long)]
    cluster_config: Option<String>,
    /// Address to bind the PostgreSQL wire protocol on.
    /// Non-primary nodes (--join) have this assigned automatically by the
    /// coordinator; you only need to set it explicitly for the primary node.
    #[arg(long)]
    postgres_addr: Option<String>,
    /// Address to bind the Flight SQL protocol on.
    /// Non-primary nodes (--join) have this assigned automatically by the
    /// coordinator; you only need to set it explicitly for the primary node.
    #[arg(long)]
    flight_sql_addr: Option<String>,
    /// Address to bind the node-to-node communication channel on.
    /// Non-primary nodes (--join) have this assigned automatically by the
    /// coordinator; you only need to set it explicitly for the primary node.
    #[arg(long)]
    node_addr: Option<String>,
    /// Hostname or IP that peer nodes use to reach this node.
    /// Defaults to 127.0.0.1 (suitable for single-machine clusters).
    /// Set to your network IP or DNS name for multi-host deployments.
    #[arg(long, default_value = "127.0.0.1")]
    advertise_host: String,
    #[arg(long, default_value = "analyticsdb-catalog.db")]
    catalog_path: String,
    #[arg(long)]
    tls_cert: Option<String>,
    #[arg(long)]
    tls_key: Option<String>,
    #[arg(long)]
    tls_ca_cert: Option<String>,
    #[arg(long)]
    tls_domain: Option<String>,
    #[arg(long)]
    tls_insecure: bool,
}

#[cfg(test)]
mod tests {
    use super::BANNER;

    #[test]
    fn banner_uses_terminal_escape_sequences() {
        assert!(BANNER.contains("\x1b[1;36m"));
        assert!(BANNER.contains("\x1b[0m"));
        assert!(!BANNER.contains("\\x1b"));
    }
}
