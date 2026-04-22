use std::sync::Arc;
use std::thread;
use std::time::Duration;

use analyticsdb_engine::PrototypeEngine;
use analyticsdb_protocol::{serve_flight_sql, serve_postgres_wire};
use assert_cmd::Command;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use uuid::Uuid;

fn temp_catalog_path() -> String {
    let mut path = std::env::temp_dir();
    path.push(format!("analyticsdb-cli-test-{}.json", Uuid::now_v7()));
    path.to_string_lossy().into_owned()
}

fn cleanup_catalog_artifacts(catalog_path: &str) {
    let _ = std::fs::remove_file(catalog_path);
    let mut managed_dir = std::path::PathBuf::from(catalog_path);
    let stem = managed_dir
        .file_stem()
        .and_then(|value| value.to_str())
        .expect("catalog path should have a file stem")
        .to_string();
    managed_dir.set_file_name(format!("{stem}.managed"));
    let _ = std::fs::remove_dir_all(managed_dir);
}

struct BackgroundServer {
    runtime: Runtime,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Drop for BackgroundServer {
    fn drop(&mut self) {
        self.task.abort();
        self.runtime.block_on(async {
            tokio::task::yield_now().await;
        });
    }
}

fn start_postgres_server(catalog_path: &str) -> (BackgroundServer, String) {
    let runtime = Runtime::new().expect("runtime should initialize");
    let listener = runtime
        .block_on(TcpListener::bind("127.0.0.1:0"))
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should exist");
    let engine = Arc::new(
        PrototypeEngine::from_catalog_path(catalog_path).expect("engine should initialize"),
    );
    let task = runtime.spawn(serve_postgres_wire(listener, engine));

    thread::sleep(Duration::from_millis(50));

    (
        BackgroundServer { runtime, task },
        format!("127.0.0.1:{}", addr.port()),
    )
}

fn start_flight_sql_server(catalog_path: &str) -> (BackgroundServer, String) {
    let runtime = Runtime::new().expect("runtime should initialize");
    let listener = runtime
        .block_on(TcpListener::bind("127.0.0.1:0"))
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should exist");
    let engine = Arc::new(
        PrototypeEngine::from_catalog_path(catalog_path).expect("engine should initialize"),
    );
    let task = runtime.spawn(serve_flight_sql(listener, engine));

    thread::sleep(Duration::from_millis(50));

    (
        BackgroundServer { runtime, task },
        format!("http://127.0.0.1:{}", addr.port()),
    )
}

#[test]
fn cli_executes_sql_and_reports_timing() {
    let output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args(["query", "--sql", "SELECT 1 AS one, 2 AS two"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let stdout = String::from_utf8(output).expect("stdout should be valid utf-8");

    assert!(stdout.contains("Query ID: q-"));
    assert!(stdout.contains("Coordinator: control-1"));
    assert!(stdout.contains("Session: user=postgres database=postgres schema=public"));
    assert!(stdout.contains("Message: Query executed successfully."));
    assert!(stdout.contains("Execution Time:"));
    assert!(stdout.contains("| one | two |"));
    assert!(stdout.contains("| 1   | 2   |"));
    assert!(stdout.contains("Rows: 1"));
}

#[test]
fn cli_surfaces_sql_failures() {
    let output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args(["query", "--sql", "SELECT FROM"])
        .assert()
        .failure()
        .get_output()
        .stderr
        .clone();

    let stderr = String::from_utf8(output).expect("stderr should be valid utf-8");

    assert!(stderr.contains("ERROR:"));
}

#[test]
fn cli_rejects_unknown_database_before_query_execution() {
    let output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args(["query", "--database", "missing", "--sql", "SELECT 1 AS one"])
        .assert()
        .failure()
        .get_output()
        .stderr
        .clone();

    let stderr = String::from_utf8(output).expect("stderr should be valid utf-8");

    assert!(stderr.contains("Unknown database 'missing'"));
}

#[test]
fn cli_persists_created_database_across_invocations() {
    let catalog_path = temp_catalog_path();

    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "CREATE DATABASE analytics",
        ])
        .assert()
        .success();

    let output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "SHOW DATABASES",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let stdout = String::from_utf8(output).expect("stdout should be valid utf-8");

    assert!(stdout.contains("database_name"));
    assert!(stdout.contains("analytics"));

    cleanup_catalog_artifacts(&catalog_path);
}

#[test]
fn cli_persists_created_schema_across_invocations() {
    let catalog_path = temp_catalog_path();

    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "CREATE SCHEMA reporting",
        ])
        .assert()
        .success();

    let output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "SHOW SCHEMAS",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let stdout = String::from_utf8(output).expect("stdout should be valid utf-8");

    assert!(stdout.contains("schema_name"));
    assert!(stdout.contains("reporting"));

    cleanup_catalog_artifacts(&catalog_path);
}

#[test]
fn cli_persists_created_view_and_queries_it_across_invocations() {
    let catalog_path = temp_catalog_path();

    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "CREATE VIEW daily_metrics AS SELECT 7 AS metric",
        ])
        .assert()
        .success();

    let views_output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "SHOW VIEWS",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let views_stdout = String::from_utf8(views_output).expect("stdout should be valid utf-8");

    assert!(views_stdout.contains("view_name"));
    assert!(views_stdout.contains("daily_metrics"));

    let query_output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "SELECT * FROM daily_metrics",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let query_stdout = String::from_utf8(query_output).expect("stdout should be valid utf-8");

    assert!(query_stdout.contains("| metric |"));
    assert!(query_stdout.contains("| 7      |"));

    cleanup_catalog_artifacts(&catalog_path);
}

#[test]
fn cli_persists_created_table_and_queries_it_across_invocations() {
    let catalog_path = temp_catalog_path();

    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "CREATE TABLE fact_metrics AS SELECT 11 AS metric, 'ok' AS status",
        ])
        .assert()
        .success();

    let tables_output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "SHOW TABLES",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let tables_stdout = String::from_utf8(tables_output).expect("stdout should be valid utf-8");

    assert!(tables_stdout.contains("table_name"));
    assert!(tables_stdout.contains("fact_metrics"));

    let query_output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "SELECT metric, status FROM fact_metrics",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let query_stdout = String::from_utf8(query_output).expect("stdout should be valid utf-8");

    assert!(query_stdout.contains("| metric | status |"));
    assert!(query_stdout.contains("| 11     | ok     |"));

    cleanup_catalog_artifacts(&catalog_path);
}

#[test]
fn cli_describes_persisted_table_columns_across_invocations() {
    let catalog_path = temp_catalog_path();

    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "CREATE TABLE fact_metrics AS SELECT 11 AS metric, 'ok' AS status",
        ])
        .assert()
        .success();

    let show_columns_output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "SHOW COLUMNS FROM fact_metrics",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let show_columns_stdout =
        String::from_utf8(show_columns_output).expect("stdout should be valid utf-8");

    assert!(show_columns_stdout.contains("column_name"));
    assert!(show_columns_stdout.contains("data_type"));
    assert!(show_columns_stdout.contains("is_nullable"));
    assert!(show_columns_stdout.contains("metric"));
    assert!(show_columns_stdout.contains("status"));

    let describe_output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "DESCRIBE fact_metrics",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let describe_stdout = String::from_utf8(describe_output).expect("stdout should be valid utf-8");

    assert!(describe_stdout.contains("column_name"));
    assert!(describe_stdout.contains("metric"));
    assert!(describe_stdout.contains("Int64"));
    assert!(describe_stdout.contains("Utf8"));

    cleanup_catalog_artifacts(&catalog_path);
}

#[test]
fn cli_creates_defined_table_inserts_rows_and_queries_them_across_invocations() {
    let catalog_path = temp_catalog_path();

    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT, is_hot BOOLEAN)",
        ])
        .assert()
        .success();

    let insert_output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "INSERT INTO fact_metrics VALUES (11, 'ok', true), (12, 'warn', false)",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let insert_stdout = String::from_utf8(insert_output).expect("stdout should be valid utf-8");

    assert!(
        insert_stdout.contains("Message: Inserted 2 row(s) into 'postgres.public.fact_metrics'.")
    );
    assert!(insert_stdout.contains("Execution Time:"));

    let query_output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "SELECT metric, status, is_hot FROM fact_metrics ORDER BY metric",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let query_stdout = String::from_utf8(query_output).expect("stdout should be valid utf-8");

    assert!(query_stdout.contains("| metric | status | is_hot |"));
    assert!(query_stdout.contains("| 11     | ok     | true   |"));
    assert!(query_stdout.contains("| 12     | warn   | false  |"));

    let describe_output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "DESCRIBE fact_metrics",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let describe_stdout = String::from_utf8(describe_output).expect("stdout should be valid utf-8");

    assert!(describe_stdout.contains("metric"));
    assert!(describe_stdout.contains("status"));
    assert!(describe_stdout.contains("is_hot"));
    assert!(describe_stdout.contains("Boolean"));

    cleanup_catalog_artifacts(&catalog_path);
}

#[test]
fn cli_supports_column_list_inserts_and_schema_scoped_show_statements() {
    let catalog_path = temp_catalog_path();

    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "CREATE SCHEMA reporting",
        ])
        .assert()
        .success();

    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "CREATE TABLE reporting.fact_metrics (metric INTEGER NOT NULL, status VARCHAR(20), score DOUBLE PRECISION, active BOOLEAN)",
        ])
        .assert()
        .success();

    let insert_output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--schema",
            "reporting",
            "--sql",
            "INSERT INTO fact_metrics (metric, active, status) VALUES (11, true, 'ok''s')",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let insert_stdout = String::from_utf8(insert_output).expect("stdout should be valid utf-8");

    assert!(insert_stdout.contains("Inserted 1 row(s)"));

    let show_tables_output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "SHOW TABLES FROM reporting",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let show_tables_stdout =
        String::from_utf8(show_tables_output).expect("stdout should be valid utf-8");

    assert!(show_tables_stdout.contains("table_name"));
    assert!(show_tables_stdout.contains("fact_metrics"));

    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "CREATE VIEW reporting.daily_metrics AS SELECT 7 AS metric",
        ])
        .assert()
        .success();

    let show_views_output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "SHOW VIEWS FROM reporting",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let show_views_stdout =
        String::from_utf8(show_views_output).expect("stdout should be valid utf-8");

    assert!(show_views_stdout.contains("view_name"));
    assert!(show_views_stdout.contains("daily_metrics"));

    let query_output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--schema",
            "reporting",
            "--sql",
            "SELECT metric, status, score, active FROM fact_metrics",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let query_stdout = String::from_utf8(query_output).expect("stdout should be valid utf-8");

    assert!(query_stdout.contains("| metric | status | score | active |"));
    assert!(query_stdout.contains("| 11     | ok's   |       | true   |"));

    cleanup_catalog_artifacts(&catalog_path);
}

#[test]
fn cli_rejects_insert_when_not_null_column_is_omitted() {
    let catalog_path = temp_catalog_path();

    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT)",
        ])
        .assert()
        .success();

    let error_output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "INSERT INTO fact_metrics (status) VALUES ('ok')",
        ])
        .assert()
        .failure()
        .get_output()
        .stderr
        .clone();

    let stderr = String::from_utf8(error_output).expect("stderr should be valid utf-8");

    assert!(stderr.contains("Column 'metric' must be provided because it is NOT NULL"));

    cleanup_catalog_artifacts(&catalog_path);
}

#[test]
fn cli_executes_sql_via_postgres_protocol_server() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path);

    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--protocol",
            "postgres",
            "--endpoint",
            &endpoint,
            "--sql",
            "CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT)",
        ])
        .assert()
        .success();

    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--protocol",
            "postgres",
            "--endpoint",
            &endpoint,
            "--sql",
            "INSERT INTO fact_metrics VALUES (11, 'ok')",
        ])
        .assert()
        .success();

    let output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--protocol",
            "postgres",
            "--endpoint",
            &endpoint,
            "--sql",
            "SELECT metric, status FROM fact_metrics",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let stdout = String::from_utf8(output).expect("stdout should be valid utf-8");

    assert!(stdout.contains("Message: PostgreSQL wire query completed."));
    assert!(stdout.contains("| metric | status |"));
    assert!(stdout.contains("| 11     | ok     |"));

    cleanup_catalog_artifacts(&catalog_path);
}

#[test]
fn cli_executes_parameterized_sql_via_postgres_protocol_server() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path);

    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--protocol",
            "postgres",
            "--endpoint",
            &endpoint,
            "--sql",
            "CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT)",
        ])
        .assert()
        .success();

    let insert_output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--protocol",
            "postgres",
            "--endpoint",
            &endpoint,
            "--sql",
            "INSERT INTO fact_metrics VALUES ($1, $2)",
            "--param",
            "11",
            "--param",
            "\"ok\"",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let insert_stdout = String::from_utf8(insert_output).expect("stdout should be valid utf-8");
    assert!(insert_stdout
        .contains("Message: PostgreSQL extended command completed. 1 row(s) affected."));

    let query_output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--protocol",
            "postgres",
            "--endpoint",
            &endpoint,
            "--sql",
            "SELECT metric, status FROM fact_metrics WHERE metric = $1",
            "--param",
            "11",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let query_stdout = String::from_utf8(query_output).expect("stdout should be valid utf-8");

    assert!(query_stdout.contains("Message: PostgreSQL extended query completed."));
    assert!(query_stdout.contains("| metric | status |"));
    assert!(query_stdout.contains("| 11     | ok     |"));

    cleanup_catalog_artifacts(&catalog_path);
}

#[test]
fn cli_executes_sql_via_flight_sql_protocol_server() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_flight_sql_server(&catalog_path);

    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--protocol",
            "flight-sql",
            "--endpoint",
            &endpoint,
            "--sql",
            "CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT)",
        ])
        .assert()
        .success();

    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--protocol",
            "flight-sql",
            "--endpoint",
            &endpoint,
            "--sql",
            "INSERT INTO fact_metrics VALUES (11, 'ok')",
        ])
        .assert()
        .success();

    let output = Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--protocol",
            "flight-sql",
            "--endpoint",
            &endpoint,
            "--sql",
            "SELECT metric, status FROM fact_metrics",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let stdout = String::from_utf8(output).expect("stdout should be valid utf-8");

    assert!(stdout.contains("Message: Flight SQL query completed."));
    assert!(stdout.contains("| metric | status |"));
    assert!(stdout.contains("| 11     | ok     |"));

    cleanup_catalog_artifacts(&catalog_path);
}
