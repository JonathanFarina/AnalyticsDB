use std::sync::Arc;

use analyticsdb_engine::PrototypeEngine;
use analyticsdb_protocol::{serve_flight_sql, serve_postgres_wire};
use anyhow::Result;
use clap::Parser;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("ERROR: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let engine = Arc::new(if let Some(path) = cli.catalog_path.as_deref() {
        PrototypeEngine::from_catalog_path(path)?
    } else {
        PrototypeEngine::new()?
    });

    let postgres_listener = TcpListener::bind(&cli.postgres_addr).await?;
    let flight_sql_listener = TcpListener::bind(&cli.flight_sql_addr).await?;

    println!(
        "PostgreSQL wire listening on {}",
        postgres_listener.local_addr()?
    );
    println!(
        "Flight SQL listening on {}",
        flight_sql_listener.local_addr()?
    );

    let pg = tokio::spawn(serve_postgres_wire(postgres_listener, Arc::clone(&engine)));
    let flight = tokio::spawn(serve_flight_sql(flight_sql_listener, Arc::clone(&engine)));

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

#[derive(Debug, Parser)]
#[command(name = "analyticsdb-server")]
#[command(about = "Prototype protocol server for AnalyticsDB")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:5432")]
    postgres_addr: String,
    #[arg(long, default_value = "127.0.0.1:50051")]
    flight_sql_addr: String,
    #[arg(long)]
    catalog_path: Option<String>,
}
