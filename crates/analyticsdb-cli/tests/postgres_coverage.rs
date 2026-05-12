use std::path::PathBuf;
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

fn managed_table_storage_dir(
    catalog_path: &str,
    database: &str,
    schema: &str,
    table: &str,
) -> PathBuf {
    let mut managed_dir = PathBuf::from(catalog_path);
    let stem = managed_dir
        .file_stem()
        .and_then(|value| value.to_str())
        .expect("catalog path should have a file stem")
        .to_string();
    managed_dir.set_file_name(format!("{stem}.managed"));
    managed_dir.join(format!("{database}__{schema}__{table}.table.parquet"))
}

fn index_snapshot_root(
    catalog_path: &str,
    database: &str,
    schema: &str,
    table: &str,
    index_name: &str,
) -> PathBuf {
    managed_table_storage_dir(catalog_path, database, schema, table)
        .join(".analyticsdb_indexes")
        .join(index_name)
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
async fn test_select_into() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path).await;

    let output = run_sql(
        &endpoint,
        "SELECT 7 AS metric, 'covered' AS status INTO fact_select_into",
    );
    assert!(
        output.contains("Command completed. 1 row(s) affected."),
        "Actual output: {}",
        output
    );

    let select = run_sql(&endpoint, "SELECT metric, status FROM fact_select_into");
    assert!(select.contains("metric"), "Actual output: {}", select);
    assert!(select.contains("status"), "Actual output: {}", select);
    assert!(select.contains("7"), "Actual output: {}", select);
    assert!(select.contains("covered"), "Actual output: {}", select);

    cleanup_catalog_artifacts(&catalog_path);
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
    let conv = run_sql(
        &endpoint,
        "ALTER CONVERSION utf8_to_ascii RENAME TO utf8_to_ascii_v2",
    );
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

    // cleanup_catalog_artifacts(&catalog_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_table() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path).await;

    // 1. Setup table
    run_sql(
        &endpoint,
        "CREATE TABLE alter_test (id INT PRIMARY KEY, name TEXT)",
    );
    run_sql(&endpoint, "INSERT INTO alter_test VALUES (1, 'initial')");

    // 2. ALTER TABLE alter_test ADD COLUMN age INT DEFAULT 30
    run_sql(
        &endpoint,
        "ALTER TABLE alter_test ADD COLUMN age INT DEFAULT 30",
    );
    let select_add = run_sql(&endpoint, "SELECT age FROM alter_test WHERE id = 1");
    if !select_add.contains("30") {
        panic!(
            "SELECT age output does not contain 30. Output:\n{}",
            select_add
        );
    }

    // 3. ALTER TABLE RENAME COLUMN
    run_sql(
        &endpoint,
        "ALTER TABLE alter_test RENAME COLUMN name TO full_name",
    );
    let select_rename_col = run_sql(&endpoint, "SELECT full_name FROM alter_test WHERE id = 1");
    assert!(select_rename_col.contains("initial"));

    // 4. ALTER TABLE ALTER COLUMN TYPE
    run_sql(
        &endpoint,
        "ALTER TABLE alter_test ALTER COLUMN full_name TYPE VARCHAR(100)",
    );
    // (Type change is mostly metadata in the prototype but we verify it doesn't fail)

    // 5. ALTER TABLE ALTER COLUMN SET/DROP NOT NULL
    run_sql(
        &endpoint,
        "ALTER TABLE alter_test ALTER COLUMN age SET NOT NULL",
    );
    run_sql(
        &endpoint,
        "ALTER TABLE alter_test ALTER COLUMN age DROP NOT NULL",
    );

    // 6. ALTER TABLE ALTER COLUMN SET/DROP DEFAULT
    run_sql(
        &endpoint,
        "ALTER TABLE alter_test ALTER COLUMN age SET DEFAULT 40",
    );
    run_sql(
        &endpoint,
        "INSERT INTO alter_test (id, full_name) VALUES (2, 'second')",
    );
    let select_default = run_sql(&endpoint, "SELECT age FROM alter_test WHERE id = 2");
    assert!(select_default.contains("40"));
    run_sql(
        &endpoint,
        "ALTER TABLE alter_test ALTER COLUMN age DROP DEFAULT",
    );

    // 7. ALTER TABLE ADD/DROP CONSTRAINT
    run_sql(
        &endpoint,
        "ALTER TABLE alter_test ADD CONSTRAINT unique_id UNIQUE (id)",
    );
    run_sql(
        &endpoint,
        "ALTER TABLE alter_test DROP CONSTRAINT unique_id",
    );

    // 8. ALTER TABLE alter_test RENAME TO table_renamed
    run_sql(&endpoint, "ALTER TABLE alter_test RENAME TO table_renamed");
    let show_tables = run_sql(&endpoint, "SHOW TABLES");
    assert!(show_tables.contains("table_renamed"));
    assert!(!show_tables.contains("alter_test"));

    // 9. ALTER TABLE table_renamed DROP COLUMN age
    run_sql(&endpoint, "ALTER TABLE table_renamed DROP COLUMN age");
    let select_drop = run_sql_failure(&endpoint, "SELECT age FROM table_renamed");
    if !select_drop.contains("not found")
        && !select_drop.contains("column")
        && !select_drop.contains("Schema error")
    {
        panic!(
            "SELECT age should have failed with a clear error. Actual output:\n{}",
            select_drop
        );
    }

    cleanup_catalog_artifacts(&catalog_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_index() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path).await;

    run_sql(&endpoint, "CREATE TABLE idx_test (id INT, val TEXT)");
    run_sql(&endpoint, "CREATE INDEX my_idx ON idx_test (val)");

    // Test ALTER INDEX RENAME TO
    let alter = run_sql(&endpoint, "ALTER INDEX my_idx RENAME TO renamed_idx");
    assert!(alter.contains("Command completed"));

    // Verify rename via REINDEX (should work with new name)
    let reindex = run_sql(&endpoint, "REINDEX INDEX renamed_idx");
    assert!(reindex.contains("Command completed"));

    // Old name should fail
    let reindex_fail = run_sql_failure(&endpoint, "REINDEX INDEX my_idx");
    assert!(reindex_fail.contains("not found"));

    cleanup_catalog_artifacts(&catalog_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_schema() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path).await;

    run_sql(&endpoint, "CREATE SCHEMA s1");
    run_sql(&endpoint, "CREATE TABLE s1.t1 (id INT)");

    // Test ALTER SCHEMA RENAME TO
    let alter = run_sql(&endpoint, "ALTER SCHEMA s1 RENAME TO s2");
    assert!(alter.contains("Command completed"));

    let show_schemas = run_sql(&endpoint, "SHOW SCHEMAS");
    assert!(show_schemas.contains("s2"));
    assert!(!show_schemas.contains("s1"));

    // Verify table moved
    let show_tables = run_sql(&endpoint, "SHOW TABLES FROM s2");
    assert!(show_tables.contains("t1"));

    cleanup_catalog_artifacts(&catalog_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_index() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path).await;

    run_sql(&endpoint, "CREATE TABLE idx_test (id INT, val TEXT)");
    run_sql(
        &endpoint,
        "INSERT INTO idx_test VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    );

    // 1. Simple index
    run_sql(&endpoint, "CREATE INDEX simple_idx ON idx_test (val)");
    let select_simple = run_sql(&endpoint, "SELECT id FROM idx_test WHERE val = 'b'");
    assert!(select_simple.contains("2"));
    // (We can't easily verify 'using index' via postgres wire yet without EXPLAIN ANALYZE)

    // 2. Multi-column index
    run_sql(&endpoint, "CREATE INDEX multi_idx ON idx_test (id, val)");
    let select_multi = run_sql(
        &endpoint,
        "SELECT id FROM idx_test WHERE id = 3 AND val = 'c'",
    );
    assert!(select_multi.contains("3"));

    // 3. Unique index
    run_sql(&endpoint, "CREATE UNIQUE INDEX unique_idx ON idx_test (id)");
    let insert_fail = run_sql_failure(&endpoint, "INSERT INTO idx_test VALUES (1, 'dup')");
    assert!(
        insert_fail.contains("unique")
            || insert_fail.contains("violation")
            || insert_fail.contains("exists")
    );

    // 4. Index on missing table
    let create_fail = run_sql_failure(&endpoint, "CREATE INDEX missing_idx ON missing_table (col)");
    assert!(create_fail.contains("not found"));

    cleanup_catalog_artifacts(&catalog_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_index() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path).await;

    run_sql(&endpoint, "CREATE TABLE drop_idx_test (id INT, val TEXT)");
    run_sql(&endpoint, "CREATE INDEX to_drop_idx ON drop_idx_test (val)");

    // Verify index exists (via REINDEX)
    run_sql(&endpoint, "REINDEX INDEX to_drop_idx");

    // 1. DROP INDEX
    run_sql(&endpoint, "DROP INDEX to_drop_idx");

    // Verify index is gone
    let reindex_fail = run_sql_failure(&endpoint, "REINDEX INDEX to_drop_idx");
    assert!(reindex_fail.contains("not found"));

    // 2. DROP INDEX IF EXISTS
    run_sql(&endpoint, "DROP INDEX IF EXISTS to_drop_idx"); // Should not fail

    // 4. Try to drop a constraint-backed index (should fail or be protected)
    run_sql(&endpoint, "CREATE TABLE const_test (id INT PRIMARY KEY)");
    let drop_const_fail = run_sql_failure(&endpoint, "DROP INDEX const_test_id_idx");
    assert!(
        drop_const_fail.contains("constraint")
            || drop_const_fail.contains("protected")
            || drop_const_fail.contains("primary key")
    );

    cleanup_catalog_artifacts(&catalog_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_user_management() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path).await;

    // 1. CREATE USER
    run_sql(&endpoint, "CREATE USER alice PASSWORD 'secret'");

    // 2. ALTER USER (Password rotation)
    run_sql(&endpoint, "ALTER USER alice PASSWORD 'new_secret'");

    // 3. DROP USER
    run_sql(&endpoint, "DROP USER alice");

    // 4. DROP USER IF EXISTS
    run_sql(&endpoint, "DROP USER IF EXISTS alice");

    // 5. CREATE USER without password
    run_sql(&endpoint, "CREATE USER bob");
    run_sql(&endpoint, "DROP USER bob");

    // 6. Errors
    let create_fail = run_sql_failure(&endpoint, "CREATE USER postgres");
    assert!(create_fail.contains("already exists"));

    let drop_fail = run_sql_failure(&endpoint, "DROP USER missing_user");
    assert!(drop_fail.contains("not found"));

    let drop_postgres_fail = run_sql_failure(&endpoint, "DROP USER postgres");
    assert!(drop_postgres_fail.contains("internal user"));

    cleanup_catalog_artifacts(&catalog_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_projection_collisions() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path).await;

    // 1. Duplicate identical expressions (db.oid)
    let sql = "SELECT db.oid, db.oid FROM pg_catalog.pg_database db LIMIT 1";
    let res = run_sql(&endpoint, sql);
    // Should have unique names in header (oid, oid_1)
    assert!(res.contains("oid") && res.contains("oid_1"));

    // 2. Wildcard expansion with overlapping names
    // This query will expand to many columns, including multiple 'oid's
    let sql_wildcard =
        "SELECT db.*, ns.* FROM pg_catalog.pg_database db, pg_catalog.pg_namespace ns LIMIT 1";
    let res_wildcard = run_sql(&endpoint, sql_wildcard);
    // Should not fail planning
    assert!(res_wildcard.contains("datname"));
    assert!(res_wildcard.contains("nspname"));

    // 3. Duplicate unnamed expressions
    let sql_dup = "SELECT count(*), count(*) FROM idx_test";
    run_sql(&endpoint, "CREATE TABLE idx_test (id INT)");
    let res_dup = run_sql(&endpoint, sql_dup);
    assert!(res_dup.contains("count(*)"));

    // 4. Wildcard expansion with explicit column collision (e.g., SELECT *, oid)
    let sql_wc_coll = "SELECT *, oid FROM pg_catalog.pg_database LIMIT 1";
    let res_wc_coll = run_sql(&endpoint, sql_wc_coll);
    assert!(res_wc_coll.contains("oid"));
    assert!(res_wc_coll.contains("oid_1"));

    // 5. Join with same column names (oid) from different tables
    let sql_join =
        "SELECT db.oid, ns.oid FROM pg_catalog.pg_database db, pg_catalog.pg_namespace ns LIMIT 1";
    let res_join = run_sql(&endpoint, sql_join);
    // Should have unique names in header (oid, oid_1)
    assert!(res_join.contains("oid"));
    assert!(res_join.contains("oid_1"));

    cleanup_catalog_artifacts(&catalog_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_group_management() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path).await;

    // 1. CREATE GROUP
    run_sql(&endpoint, "CREATE GROUP analytics_team");

    // 2. ALTER GROUP ADD USER
    run_sql(&endpoint, "CREATE USER carol PASSWORD 'p1'");
    run_sql(&endpoint, "ALTER GROUP analytics_team ADD USER carol");

    // 3. Verify membership (via pg_catalog.pg_user)
    // Actually, membership is in pg_auth_members usually.
    // For prototype, we verify it doesn't fail and we can drop user.

    // 4. ALTER GROUP DROP USER
    run_sql(&endpoint, "ALTER GROUP analytics_team DROP USER carol");

    // 4.1 ALTER GROUP RENAME TO
    run_sql(
        &endpoint,
        "ALTER GROUP analytics_team RENAME TO analytics_group",
    );
    let show_roles = run_sql(&endpoint, "SELECT rolname FROM pg_catalog.pg_roles");
    assert!(show_roles.contains("analytics_group"));
    assert!(!show_roles.contains("analytics_team"));

    // 5. DROP GROUP
    run_sql(&endpoint, "DROP GROUP analytics_group");

    // 6. Errors
    let create_fail = run_sql_failure(&endpoint, "CREATE GROUP postgres");
    assert!(create_fail.contains("already exists"));

    let drop_fail = run_sql_failure(&endpoint, "DROP GROUP missing_group");
    assert!(drop_fail.contains("not found"));

    run_sql(&endpoint, "CREATE GROUP non_empty");
    run_sql(&endpoint, "ALTER GROUP non_empty ADD USER carol");
    let drop_non_empty_fail = run_sql_failure(&endpoint, "DROP GROUP non_empty");
    assert!(drop_non_empty_fail.contains("not empty"));

    cleanup_catalog_artifacts(&catalog_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reindex() {
    let catalog_path = temp_catalog_path();
    let (_server, endpoint) = start_postgres_server(&catalog_path).await;

    for sql in [
        "CREATE TABLE customers (id BIGINT PRIMARY KEY, email TEXT, city TEXT)",
        "INSERT INTO customers VALUES (1, 'one@example.test', 'london'), (2, 'two@example.test', 'paris')",
        "CREATE INDEX customers_email_idx ON customers (email)",
        "CREATE INDEX customers_city_idx ON customers (city)",
    ] {
        let output = run_sql(&endpoint, sql);
        assert!(
            output.contains("Command completed"),
            "setup statement should succeed: {sql}\nactual output: {output}"
        );
    }

    let initial_email_lookup = run_sql(
        &endpoint,
        "SELECT id FROM customers WHERE email = 'two@example.test'",
    );
    assert!(initial_email_lookup.contains("2"));

    let email_index_root = index_snapshot_root(
        &catalog_path,
        "postgres",
        "public",
        "customers",
        "customers_email_idx",
    );
    std::fs::remove_dir_all(&email_index_root).expect("email index snapshot root should exist");

    let email_without_index = run_sql(
        &endpoint,
        "SELECT id FROM customers WHERE email = 'two@example.test'",
    );
    assert!(email_without_index.contains("2"));
    assert!(!email_without_index.contains("using index"));

    let reindex_index = run_sql(&endpoint, "REINDEX INDEX customers_email_idx");
    assert!(
        reindex_index.contains("Command completed"),
        "Actual output: {reindex_index}"
    );
    assert!(
        email_index_root.join("manifest.json").exists(),
        "REINDEX INDEX should recreate the missing manifest"
    );

    let email_with_index_again = run_sql(
        &endpoint,
        "SELECT id FROM customers WHERE email = 'two@example.test'",
    );
    assert!(email_with_index_again.contains("2"));

    let city_index_root = index_snapshot_root(
        &catalog_path,
        "postgres",
        "public",
        "customers",
        "customers_city_idx",
    );
    std::fs::remove_dir_all(&email_index_root).expect("email index snapshot root should exist");
    std::fs::remove_dir_all(&city_index_root).expect("city index snapshot root should exist");

    let city_without_index = run_sql(&endpoint, "SELECT id FROM customers WHERE city = 'paris'");
    assert!(city_without_index.contains("2"));
    assert!(!city_without_index.contains("using index"));

    let reindex_table = run_sql(&endpoint, "REINDEX TABLE customers");
    assert!(
        reindex_table.contains("Command completed"),
        "Actual output: {reindex_table}"
    );
    assert!(email_index_root.join("manifest.json").exists());
    assert!(city_index_root.join("manifest.json").exists());

    let city_with_index_again = run_sql(&endpoint, "SELECT id FROM customers WHERE city = 'paris'");
    assert!(city_with_index_again.contains("2"));

    cleanup_catalog_artifacts(&catalog_path);
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
