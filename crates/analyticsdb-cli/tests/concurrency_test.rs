// Concurrency profile benchmark for AnalyticsDB
// Measures tail latency (p50, p95, p99) under increasing client concurrency
// This test is ignored by default as it requires a running AnalyticsDB server.
//
// Usage:
//   cargo test -p analyticsdb-cli --test concurrency_test -- --ignored --nocapture
//
// Environment variables:
//   ANALYTICSDB_HOST     - Server host (default: 127.0.0.1)
//   ANALYTICSDB_PORT     - Server port (default: 5432)
//   ANALYTICSDB_USER     - Database user (default: postgres)
//   ANALYTICSDB_PASSWORD - Password (default: empty)
//   ANALYTICSDB_DB       - Database name (default: postgres)
//   ANALYTICSDB_SCHEMA   - Schema name (default: public)

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio_postgres::{Config, NoTls};

#[tokio::test]
#[ignore]
async fn concurrency_profile_benchmark() {
    let host = std::env::var("ANALYTICSDB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("ANALYTICSDB_PORT")
        .unwrap_or_else(|_| "5432".to_string())
        .parse()
        .expect("Invalid ANALYTICSDB_PORT");
    let user = std::env::var("ANALYTICSDB_USER").unwrap_or_else(|_| "postgres".to_string());
    let password = std::env::var("ANALYTICSDB_PASSWORD").ok();
    let dbname = std::env::var("ANALYTICSDB_DB").unwrap_or_else(|_| "postgres".to_string());
    let schema = std::env::var("ANALYTICSDB_SCHEMA").unwrap_or_else(|_| "public".to_string());

    // Simple query to benchmark
    let query = "SELECT COUNT(*) FROM pg_catalog.pg_tables";

    // Concurrency levels to test
    let concurrency_levels = vec![1, 16, 32, 64];
    let queries_per_client = 10;

    println!("==========================================");
    println!("AnalyticsDB Concurrency Profile Benchmark");
    println!("==========================================");
    println!("Server:   {}:{}", host, port);
    println!("User:     {}", user);
    println!("Database: {}", dbname);
    println!("Schema:   {}", schema);
    println!("Query:    {}", query);
    println!("Queries per client: {}", queries_per_client);
    println!("==========================================");
    println!();

    // Warm-up
    println!("Warming up (1 query)...");
    let _ = run_single_query(
        &host,
        port,
        &user,
        password.as_deref(),
        &dbname,
        &schema,
        query,
    )
    .await;
    println!();

    for &concurrency in &concurrency_levels {
        println!("=== Concurrency Level: {} clients ===", concurrency);

        let start_time = Instant::now();
        let mut handles: Vec<JoinHandle<Vec<u64>>> = Vec::new();

        // Spawn concurrent clients
        for _ in 0..concurrency {
            let host = host.clone();
            let user = user.clone();
            let password = password.clone();
            let dbname = dbname.clone();
            let schema = schema.clone();
            let query = query.to_string();

            let handle: JoinHandle<Vec<u64>> = tokio::spawn(async move {
                let mut latencies = Vec::new();
                for _ in 0..queries_per_client {
                    let lat = run_single_query(
                        &host,
                        port,
                        &user,
                        password.as_deref(),
                        &dbname,
                        &schema,
                        &query,
                    )
                    .await;
                    latencies.push(lat);
                }
                latencies
            });
            handles.push(handle);
        }

        // Collect all latencies
        let mut all_latencies: Vec<u64> = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(latencies) => all_latencies.extend(latencies),
                Err(e) => eprintln!("Client error: {:?}", e),
            }
        }

        let wall_time = start_time.elapsed();
        println!("  Total wall time: {:?}", wall_time);
        println!("  Results:");
        print_stats(&all_latencies);
        println!();
    }

    println!("==========================================");
    println!("Benchmark complete.");
    println!("==========================================");
}

async fn run_single_query(
    host: &str,
    port: u16,
    user: &str,
    password: Option<&str>,
    dbname: &str,
    schema: &str,
    query: &str,
) -> u64 {
    let start = Instant::now();

    let mut config = Config::new()
        .host(host)
        .port(port)
        .user(user)
        .dbname(dbname);

    if let Some(pwd) = password {
        config = config.password(pwd);
    }

    match config.connect(NoTls).await {
        Ok((client, connection)) => {
            let connection_handle = tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("Connection error: {}", e);
                }
            });

            // Set search_path if not public
            if schema != "public" {
                let _ = client
                    .execute(&format!("SET search_path TO {}", schema), &[])
                    .await;
            }

            let _ = client.simple_query(query).await;

            let _ = client.close().await;
            let _ = connection_handle.await;
        }
        Err(e) => {
            eprintln!("Connection failed: {}", e);
        }
    }

    start.elapsed().as_millis() as u64
}

fn print_stats(latencies: &[u64]) {
    if latencies.is_empty() {
        println!("  No successful queries");
        return;
    }

    let count = latencies.len();
    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();

    let min = sorted[0];
    let max = sorted[count - 1];
    let mean: f64 = sorted.iter().map(|&x| x as f64).sum::<f64>() / count as f64;

    // Percentiles
    let p50 = percentile(&sorted, 50.0);
    let p95 = percentile(&sorted, 95.0);
    let p99 = percentile(&sorted, 99.0);

    println!("  Total queries: {}", count);
    println!("  Min latency:  {} ms", min);
    println!("  Max latency:  {} ms", max);
    println!("  Mean latency: {:.2} ms", mean);
    println!("  p50 latency:  {:.2} ms", p50);
    println!("  p95 latency:  {:.2} ms", p95);
    println!("  p99 latency:  {:.2} ms", p99);
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let count = sorted.len();
    let idx = (p / 100.0 * (count - 1) as f64).round() as usize;
    let idx = idx.min(count - 1);
    sorted[idx] as f64
}
