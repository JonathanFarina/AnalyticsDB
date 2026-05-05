use analyticsdb_engine::{PrototypeEngine, QueryRequest};
use analyticsdb_core::{SessionContext, Protocol};
use std::fs;
use std::path::Path;

#[tokio::main]
async fn main() {
    let catalog_path = "repro-insert-catalog.json";
    if Path::new(catalog_path).exists() {
        fs::remove_file(catalog_path).ok();
    }

    let engine = PrototypeEngine::from_catalog_path(catalog_path)
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
        })
        .await
        .unwrap();

    let sql = "INSERT INTO orders (id, customer_name, order_value, date_of_purchase)
SELECT n, 'Customer ' || n, ROUND((10 + random() * 990)::numeric, 2), NOW() - (random() * INTERVAL '5 years')
FROM generate_series(1, 10) AS s(n)";

    let result = engine
        .execute_query(&QueryRequest {
            sql: sql.to_string(),
            session: session.clone(),
        })
        .await;

    match result {
        Ok(_) => println!("Success"),
        Err(e) => println!("Error: {}", e),
    }
}
