use std::sync::Arc;

use analyticsdb_control::{ClusterNode, NodeRole, NodeStatus};
use analyticsdb_engine::PrototypeEngine;
use analyticsdb_protocol::{serve_flight_sql, serve_postgres_wire};
use anyhow::Result;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::{error, info};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    if let Err(error) = run().await {
        error!("ERROR: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let engine = Arc::new(PrototypeEngine::from_catalog_path(&cli.catalog_path).await?);

    // Register this node in the control plane
    let node_id = cli.node_id.clone();
    engine.control_plane().register_node(ClusterNode {
        id: node_id.clone(),
        role: cli.role,
        endpoint: cli.flight_sql_addr.clone(),
        status: NodeStatus::Ready,
        last_heartbeat_at_epoch_ms: 0, // Control plane will set it properly
    }).await?;
    info!("Registered node '{}' as {:?} in control plane", node_id, cli.role);

    // Start background heartbeat loop
    let cp = Arc::clone(&engine.control_plane());
    let hb_node_id = node_id.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            if let Err(e) = cp.heartbeat(&hb_node_id).await {
                error!("Heartbeat failed for node '{}': {}", hb_node_id, e);
            }
            // Also prune other unhealthy nodes while we're at it (for the prototype)
            let _ = cp.prune_unhealthy_nodes(15000).await;
        }
    });

    let postgres_listener = TcpListener::bind(&cli.postgres_addr).await?;
    let flight_sql_listener = TcpListener::bind(&cli.flight_sql_addr).await?;

    let pg_addr = postgres_listener.local_addr()?;
    let flight_addr = flight_sql_listener.local_addr()?;

    let tls_config = if let (Some(cert_path), Some(key_path)) = (&cli.tls_cert, &cli.tls_key) {
        let cert = std::fs::read(cert_path)?;
        let key = std::fs::read(key_path)?;
        Some((cert, key))
    } else if cli.tls_cert.is_some() || cli.tls_key.is_some() {
        anyhow::bail!("Both --tls-cert and --tls-key must be provided to enable TLS");
    } else {
        None
    };

    print_banner(pg_addr.to_string(), flight_addr.to_string(), tls_config.is_some());

    let pg = tokio::spawn(serve_postgres_wire(postgres_listener, Arc::clone(&engine)));
    let flight = tokio::spawn(serve_flight_sql(
        flight_sql_listener,
        Arc::clone(&engine),
        tls_config,
    ));

    tokio::try_join!(
        async {
            pg.await??;
            Ok::<_, anyhow::Error>(())
        },
        async {
            flight.await??;
            Ok::<_, anyhow::Error>(())
        }
    )?;

    Ok(())
}

fn print_banner(pg_addr: String, flight_addr: String, tls_enabled: bool) {
    let banner = r#"
    _                _       _   _              ____  ____  
   / \   _ __   __ _| |_   _| |_(_) ___ ___    |  _ \| __ ) 
  / _ \ | '_ \ / _` | | | | | __| |/ __/ __|   | | | |  _ \ 
 / ___ \| | | | (_| | | |_| | |_| | (__\__ \_  | |_| | |_) |
/_/   \_\_| |_|\__,_|_|\__, |\__|_|\___|___(_) |____/|____/ 
                       |___/                                
    "#;

    let speed_line = " >>>>> ULTRA-FAST COLUMNAR EXECUTION ENGINE >>>>>";
    
    println!("\x1b[36m{}\x1b[0m", banner);
    println!("\x1b[1;33m{}\x1b[0m\n", speed_line);
    println!(" \x1b[1mStatus:\x1b[0m    \x1b[32mOnline\x1b[0m");
    println!(" \x1b[1mVersion:\x1b[0m   0.1.0-prototype");
    println!(" \x1b[1mListeners:\x1b[0m");
    println!("   \x1b[34mPostgreSQL Wire:\x1b[0m  {}", pg_addr);
    println!("   \x1b[34mArrow Flight SQL:\x1b[0m {}", flight_addr);
    println!(" \x1b[1mSecurity:\x1b[0m");
    println!("   \x1b[34mFlight SQL TLS:\x1b[0m   {}", if tls_enabled { "\x1b[32mEnabled\x1b[0m" } else { "\x1b[31mDisabled (Insecure)\x1b[0m" });
    println!("\n \x1b[1;32mStartup complete. Ready to accept connections.\x1b[0m\n");
}

#[derive(Debug, Parser)]
#[command(name = "analyticsdb-server")]
#[command(about = "Prototype protocol server for AnalyticsDB")]
struct Cli {
    #[arg(long, default_value = "node-1")]
    node_id: String,
    #[arg(long, value_enum, default_value = "control")]
    role: NodeRole,
    #[arg(long, default_value = "127.0.0.1:5432")]
    postgres_addr: String,
    #[arg(long, default_value = "127.0.0.1:50051")]
    flight_sql_addr: String,
    #[arg(long, default_value = "analyticsdb-catalog.json")]
    catalog_path: String,
    #[arg(long)]
    tls_cert: Option<String>,
    #[arg(long)]
    tls_key: Option<String>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, clap::ValueEnum)]
enum NodeRole {
    Control,
    Compute,
    Storage,
    Gateway,
}

impl From<NodeRole> for analyticsdb_control::NodeRole {
    fn from(role: NodeRole) -> Self {
        match role {
            NodeRole::Control => analyticsdb_control::NodeRole::Control,
            NodeRole::Compute => analyticsdb_control::NodeRole::Compute,
            NodeRole::Storage => analyticsdb_control::NodeRole::Storage,
            NodeRole::Gateway => analyticsdb_control::NodeRole::Gateway,
        }
    }
}
