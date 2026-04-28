use std::sync::Arc;
use std::time::Duration;

use analyticsdb_engine::PrototypeEngine;
use analyticsdb_protocol::serve_postgres_wire;
use assert_cmd::Command;
use tokio::net::TcpListener;
use uuid::Uuid;

fn temp_catalog_path() -> String {
    let mut path = std::env::temp_dir();
    path.push(format!("analyticsdb-coverage-test-{}.json", Uuid::now_v7()));
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
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Drop for BackgroundServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn start_postgres_server(catalog_path: &str) -> (BackgroundServer, String) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should exist");
    let engine = Arc::new(
        PrototypeEngine::from_catalog_path(catalog_path)
            .await
            .expect("engine should initialize"),
    );
    let task = tokio::spawn(serve_postgres_wire(listener, engine));

    tokio::time::sleep(Duration::from_millis(50)).await;

    (
        BackgroundServer { task },
        format!("127.0.0.1:{}", addr.port()),
    )
}

fn run_sql(endpoint: &str, sql: &str) -> String {
    let mut command = Command::cargo_bin("analyticsdb").expect("binary should build");
    command.args([
        "query",
        "--protocol",
        "postgres",
        "--endpoint",
        endpoint,
        "--password",
        "postgres",
        "--sql",
        sql,
    ]);

    let output = command.assert().success().get_output().clone();
    String::from_utf8(output.stdout).expect("stdout should be valid utf-8")
}

fn run_sql_failure(endpoint: &str, sql: &str) -> String {
    let mut command = Command::cargo_bin("analyticsdb").expect("binary should build");
    command.args([
        "query",
        "--protocol",
        "postgres",
        "--endpoint",
        endpoint,
        "--password",
        "postgres",
        "--sql",
        sql,
    ]);

    let output = command.assert().failure().get_output().clone();
    format!(
        "{}{}",
        String::from_utf8(output.stdout).expect("stdout should be valid utf-8"),
        String::from_utf8(output.stderr).expect("stderr should be valid utf-8")
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn test_transaction_shims_coverage() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path).await;

    // Test shims: BEGIN, COMMIT, ROLLBACK, ABORT, END, START TRANSACTION
    let commands = vec![
        "BEGIN",
        "COMMIT",
        "BEGIN",
        "ROLLBACK",
        "BEGIN",
        "ABORT",
        "BEGIN",
        "ABORT WORK",
        "BEGIN",
        "ABORT TRANSACTION",
        "BEGIN",
        "END",
        "START TRANSACTION",
        "COMMIT",
    ];

    for sql in commands {
        let stdout = run_sql(&endpoint, sql);
        assert!(stdout.contains("Command completed. 0 row(s) affected."));
    }

    // cleanup_catalog_artifacts(&catalog_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_aggregate_and_collation_coverage() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path).await;

    run_sql(&endpoint, "CREATE AGGREGATE my_agg");
    let agg = run_sql(&endpoint, "ALTER AGGREGATE my_agg RENAME TO new_agg");
    assert!(
        agg.contains("Command completed. 0 row(s) affected."),
        "Actual output: {}",
        agg
    );
    let missing_agg = run_sql_failure(&endpoint, "ALTER AGGREGATE my_agg RENAME TO newer_agg");
    assert!(
        missing_agg.contains("Aggregate 'postgres.public.my_agg' not found"),
        "Actual output: {}",
        missing_agg
    );
    let duplicate_agg = run_sql_failure(&endpoint, "CREATE AGGREGATE new_agg");
    assert!(
        duplicate_agg.contains("Aggregate 'postgres.public.new_agg' already exists"),
        "Actual output: {}",
        duplicate_agg
    );

    run_sql(&endpoint, "CREATE COLLATION my_coll");
    let coll = run_sql(&endpoint, "ALTER COLLATION my_coll RENAME TO new_coll");
    assert!(
        coll.contains("Command completed. 0 row(s) affected."),
        "Actual output: {}",
        coll
    );
    let missing_coll = run_sql_failure(&endpoint, "ALTER COLLATION my_coll RENAME TO newer_coll");
    assert!(
        missing_coll.contains("Collation 'postgres.public.my_coll' not found"),
        "Actual output: {}",
        missing_coll
    );
    let duplicate_coll = run_sql_failure(&endpoint, "CREATE COLLATION new_coll");
    assert!(
        duplicate_coll.contains("Collation 'postgres.public.new_coll' already exists"),
        "Actual output: {}",
        duplicate_coll
    );

    run_sql(&endpoint, "CREATE CONVERSION utf8_to_ascii");
    let conv = run_sql(&endpoint, "ALTER CONVERSION utf8_to_ascii RENAME TO utf8_to_ascii_v2");
    assert!(
        conv.contains("Command completed. 0 row(s) affected."),
        "Actual output: {}",
        conv
    );

    // cleanup_catalog_artifacts(&catalog_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_core_ddl_coverage() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path).await;

    // Test Complete: CREATE DATABASE, CREATE SCHEMA, CREATE TABLE, CREATE VIEW, DROP
    run_sql(&endpoint, "CREATE DATABASE test_db");
    run_sql(&endpoint, "CREATE SCHEMA test_schema");
    run_sql(&endpoint, "CREATE TABLE test_table (id INT, name TEXT)");
    run_sql(
        &endpoint,
        "CREATE VIEW test_view AS SELECT * FROM test_table",
    );

    let show_tables = run_sql(&endpoint, "SHOW TABLES");
    assert!(show_tables.contains("test_table"));

    run_sql(&endpoint, "DROP VIEW test_view");
    run_sql(&endpoint, "DROP TABLE test_table");
    run_sql(&endpoint, "DROP SCHEMA test_schema");
    run_sql(&endpoint, "DROP DATABASE test_db");

    // cleanup_catalog_artifacts(&catalog_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_function_coverage() {
    let catalog_path = "function-catalog.json";
    let _ = std::fs::remove_file(catalog_path);

    let (_server, endpoint) = start_postgres_server(catalog_path).await;

    // Test CREATE FUNCTION
    run_sql(
        &endpoint,
        "CREATE FUNCTION add(a INT, b INT) RETURNS INT AS 'SELECT a + b' LANGUAGE SQL",
    );

    // Test ALTER FUNCTION (Rename)
    run_sql(&endpoint, "ALTER FUNCTION add RENAME TO add_v2");

    // Test ALTER FUNCTION (Owner)
    // Note: Default user is 'postgres'
    run_sql(&endpoint, "ALTER FUNCTION add_v2 OWNER TO postgres");

    // Test DROP FUNCTION
    run_sql(&endpoint, "DROP FUNCTION add_v2");

    // Test DROP FUNCTION IF EXISTS
    run_sql(&endpoint, "DROP FUNCTION IF EXISTS add_v2");

    cleanup_catalog_artifacts(catalog_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_function_advanced_coverage() {
    let catalog_path = "function-advanced-catalog.json";
    let _ = std::fs::remove_file(catalog_path);

    let (_server, endpoint) = start_postgres_server(catalog_path).await;

    // 1. CREATE FUNCTION
    run_sql(
        &endpoint,
        "CREATE FUNCTION test_func() RETURNS INT AS 'SELECT 1' LANGUAGE SQL",
    );

    // 2. CREATE FUNCTION (should fail because it exists)
    let error_msg = run_sql_failure(
        &endpoint,
        "CREATE FUNCTION test_func() RETURNS INT AS 'SELECT 2' LANGUAGE SQL",
    );
    assert!(error_msg.contains("already exists"));

    // 3. CREATE OR REPLACE FUNCTION
    run_sql(
        &endpoint,
        "CREATE OR REPLACE FUNCTION test_func() RETURNS INT AS 'SELECT 3' LANGUAGE SQL",
    );

    // 4. ALTER FUNCTION SET SCHEMA
    run_sql(&endpoint, "CREATE SCHEMA test_schema");
    run_sql(&endpoint, "ALTER FUNCTION test_func SET SCHEMA test_schema");

    // 5. DROP FUNCTION RESTRICT
    run_sql(&endpoint, "DROP FUNCTION test_schema.test_func RESTRICT");

    // 6. DROP FUNCTION IF EXISTS CASCADE
    run_sql(&endpoint, "DROP FUNCTION IF EXISTS test_func CASCADE");

    cleanup_catalog_artifacts(catalog_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_core_dml_coverage() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path).await;

    run_sql(&endpoint, "CREATE TABLE dml_test (id INT, val TEXT)");

    // INSERT, VALUES
    run_sql(
        &endpoint,
        "INSERT INTO dml_test VALUES (1, 'one'), (2, 'two')",
    );

    // SELECT
    let select = run_sql(&endpoint, "SELECT * FROM dml_test ORDER BY id");
    assert!(select.contains("one"));
    assert!(select.contains("two"));

    // UPDATE
    run_sql(
        &endpoint,
        "UPDATE dml_test SET val = 'updated' WHERE id = 1",
    );
    let select_upd = run_sql(&endpoint, "SELECT val FROM dml_test WHERE id = 1");
    assert!(select_upd.contains("updated"));

    // DELETE
    run_sql(&endpoint, "DELETE FROM dml_test WHERE id = 2");
    let select_del = run_sql(&endpoint, "SELECT COUNT(*) FROM dml_test");
    assert!(select_del.contains("1"));

    // TRUNCATE
    run_sql(&endpoint, "TRUNCATE TABLE dml_test");
    let select_trunc = run_sql(&endpoint, "SELECT COUNT(*) FROM dml_test");
    assert!(select_trunc.contains("0"));

    // cleanup_catalog_artifacts(&catalog_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_introspection_coverage() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path).await;

    // EXPLAIN
    let explain = run_sql(&endpoint, "EXPLAIN SELECT 1");
    assert!(explain.contains("plan"));

    // SHOW, SET, RESET - needs same connection
    let (host, port) = endpoint.split_once(':').unwrap();
    let connection_string = format!(
        "host={} port={} user=postgres password=postgres dbname=postgres",
        host, port
    );
    let (client, connection) = tokio_postgres::connect(&connection_string, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .execute("SET extra_float_digits = 3", &[])
        .await
        .unwrap();
    let rows = client.query("SHOW extra_float_digits", &[]).await.unwrap();
    let val: String = rows[0].get(0);
    assert_eq!(val, "3");

    client
        .execute("RESET extra_float_digits", &[])
        .await
        .unwrap();
    let rows = client.query("SHOW extra_float_digits", &[]).await.unwrap();
    let val: String = rows[0].get(0);
    assert_eq!(val, "1"); // Default is 1

    // cleanup_catalog_artifacts(&catalog_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_database_and_shims_coverage() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path).await;

    // Test Complete: ALTER DATABASE RENAME TO
    run_sql(&endpoint, "CREATE DATABASE db_to_rename");
    run_sql(
        &endpoint,
        "CREATE TABLE db_to_rename.public.t1 AS SELECT 1 AS id",
    );

    run_sql(
        &endpoint,
        "ALTER DATABASE db_to_rename RENAME TO renamed_db",
    );

    let show_db = run_sql(&endpoint, "SHOW DATABASES");
    assert!(show_db.contains("renamed_db"));
    assert!(!show_db.contains("db_to_rename"));

    // Verify relation persistence across DB rename
    let select = run_sql(&endpoint, "SELECT id FROM renamed_db.public.t1");
    assert!(select.contains("1"));

    // Test Shims: ALTER CONVERSION
    run_sql(&endpoint, "CREATE CONVERSION my_conv");
    let conv = run_sql(&endpoint, "ALTER CONVERSION my_conv RENAME TO new_conv");
    assert!(
        conv.contains("Command completed"),
        "Actual output: {}",
        conv
    );
    assert!(
        conv.contains("ALTER CONVERSION completed"),
        "Actual output: {}",
        conv
    );

    // cleanup_catalog_artifacts(&catalog_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_all_keywords_coverage() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path).await;

    let keywords = vec![
        "ABORT",
        "ALTER AGGREGATE",
        "ALTER COLLATION",
        "ALTER CONVERSION",
        "ALTER DATABASE",
        "ALTER DEFAULT PRIVILEGES",
        "ALTER DOMAIN",
        "ALTER EVENT TRIGGER",
        "ALTER EXTENSION",
        "ALTER FOREIGN DATA WRAPPER",
        "ALTER FOREIGN TABLE",
        "ALTER FUNCTION",
        "ALTER GROUP",
        "ALTER INDEX",
        "ALTER LANGUAGE",
        "ALTER LARGE OBJECT",
        "ALTER MATERIALIZED VIEW",
        "ALTER OPERATOR",
        "ALTER OPERATOR CLASS",
        "ALTER OPERATOR FAMILY",
        "ALTER POLICY",
        "ALTER PROCEDURE",
        "ALTER PUBLICATION",
        "ALTER ROLE",
        "ALTER ROUTINE",
        "ALTER RULE",
        "ALTER SCHEMA",
        "ALTER SEQUENCE",
        "ALTER SERVER",
        "ALTER STATISTICS",
        "ALTER SUBSCRIPTION",
        "ALTER SYSTEM",
        "ALTER TABLE",
        "ALTER TABLESPACE",
        "ALTER TEXT SEARCH CONFIGURATION",
        "ALTER TEXT SEARCH DICTIONARY",
        "ALTER TEXT SEARCH PARSER",
        "ALTER TEXT SEARCH TEMPLATE",
        "ALTER TRIGGER",
        "ALTER TYPE",
        "ALTER USER",
        "ALTER USER MAPPING",
        "ALTER VIEW",
        "ANALYZE",
        "BEGIN",
        "CALL",
        "CHECKPOINT",
        "CLOSE",
        "CLUSTER",
        "COMMENT",
        "COMMIT",
        "COMMIT PREPARED",
        "COPY",
        "CREATE ACCESS METHOD",
        "CREATE AGGREGATE",
        "CREATE CAST",
        "CREATE COLLATION",
        "CREATE CONVERSION",
        "CREATE DATABASE",
        "CREATE DOMAIN",
        "CREATE EVENT TRIGGER",
        "CREATE EXTENSION",
        "CREATE FOREIGN DATA WRAPPER",
        "CREATE FOREIGN TABLE",
        "CREATE FUNCTION",
        "CREATE GROUP",
        "CREATE INDEX",
        "CREATE LANGUAGE",
        "CREATE MATERIALIZED VIEW",
        "CREATE OPERATOR",
        "CREATE OPERATOR CLASS",
        "CREATE OPERATOR FAMILY",
        "CREATE POLICY",
        "CREATE PROCEDURE",
        "CREATE PUBLICATION",
        "CREATE ROLE",
        "CREATE RULE",
        "CREATE SCHEMA",
        "CREATE SEQUENCE",
        "CREATE SERVER",
        "CREATE STATISTICS",
        "CREATE SUBSCRIPTION",
        "CREATE TABLE",
        "CREATE TABLE AS",
        "CREATE TABLESPACE",
        "CREATE TEXT SEARCH CONFIGURATION",
        "CREATE TEXT SEARCH DICTIONARY",
        "CREATE TEXT SEARCH PARSER",
        "CREATE TEXT SEARCH TEMPLATE",
        "CREATE TRANSFORM",
        "CREATE TRIGGER",
        "CREATE TYPE",
        "CREATE USER",
        "CREATE USER MAPPING",
        "CREATE VIEW",
        "DEALLOCATE",
        "DECLARE",
        "DELETE",
        "DISCARD",
        "DO",
        "DROP ACCESS METHOD",
        "DROP AGGREGATE",
        "DROP CAST",
        "DROP COLLATION",
        "DROP CONVERSION",
        "DROP DATABASE",
        "DROP DOMAIN",
        "DROP EVENT TRIGGER",
        "DROP EXTENSION",
        "DROP FOREIGN DATA WRAPPER",
        "DROP FOREIGN TABLE",
        "DROP FUNCTION",
        "DROP GROUP",
        "DROP INDEX",
        "DROP LANGUAGE",
        "DROP MATERIALIZED VIEW",
        "DROP OPERATOR",
        "DROP OPERATOR CLASS",
        "DROP OPERATOR FAMILY",
        "DROP OWNED",
        "DROP POLICY",
        "DROP PROCEDURE",
        "DROP PUBLICATION",
        "DROP ROLE",
        "DROP ROUTINE",
        "DROP RULE",
        "DROP SCHEMA",
        "DROP SEQUENCE",
        "DROP SERVER",
        "DROP STATISTICS",
        "DROP SUBSCRIPTION",
        "DROP TABLE",
        "DROP TABLESPACE",
        "DROP TEXT SEARCH CONFIGURATION",
        "DROP TEXT SEARCH DICTIONARY",
        "DROP TEXT SEARCH PARSER",
        "DROP TEXT SEARCH TEMPLATE",
        "DROP TRANSFORM",
        "DROP TRIGGER",
        "DROP TYPE",
        "DROP USER",
        "DROP USER MAPPING",
        "DROP VIEW",
        "END",
        "EXECUTE",
        "EXPLAIN",
        "FETCH",
        "GRANT",
        "IMPORT FOREIGN SCHEMA",
        "INSERT",
        "LISTEN",
        "LOAD",
        "LOCK",
        "MERGE",
        "MOVE",
        "NOTIFY",
        "PREPARE",
        "PREPARE TRANSACTION",
        "REASSIGN OWNED",
        "REFRESH MATERIALIZED VIEW",
        "REINDEX",
        "RELEASE SAVEPOINT",
        "RESET",
        "REVOKE",
        "ROLLBACK",
        "ROLLBACK PREPARED",
        "ROLLBACK TO SAVEPOINT",
        "SAVEPOINT",
        "SECURITY LABEL",
        "SELECT",
        "SELECT INTO",
        "SET",
        "SET CONSTRAINTS",
        "SET ROLE",
        "SET SESSION AUTHORIZATION",
        "SET TRANSACTION",
        "SHOW",
        "START TRANSACTION",
        "TRUNCATE",
        "UNLISTEN",
        "UPDATE",
        "VACUUM",
        "VALUES",
    ];

    for kw in keywords {
        // We just want to see that it doesn't crash.
        // For most unsupported keywords, it will fail with a parser error or "not implemented".
        // We don't assert the exact error yet as it's a prototype.
        let mut command = Command::cargo_bin("analyticsdb").expect("binary should build");
        command.args([
            "query",
            "--protocol",
            "postgres",
            "--endpoint",
            &endpoint,
            "--password",
            "postgres",
            "--sql",
            kw,
        ]);

        // We don't use .assert().success() or .failure() because we just want to ensure NO CRASH (no signal)
        let output = command.output().expect("command should run");
        assert!(
            output.status.code().is_some(),
            "Keyword {} caused a crash (signal)",
            kw
        );
    }

    // cleanup_catalog_artifacts(&catalog_path);
}
