use assert_cmd::Command;
use uuid::Uuid;

fn temp_catalog_path() -> String {
    let mut path = std::env::temp_dir();
    path.push(format!("analyticsdb-alter-test-{}.json", Uuid::now_v7()));
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

#[tokio::test(flavor = "multi_thread")]
async fn cli_supports_rename_column_drop_column_and_drop_constraint() {
    let catalog_path = temp_catalog_path();

    // 1. Create table with constraint and some data
    for sql in [
        "CREATE TABLE test_table (id BIGINT PRIMARY KEY, name TEXT, category TEXT)",
        "INSERT INTO test_table VALUES (1, 'item1', 'cat1'), (2, 'item2', 'cat2')",
        "ALTER TABLE test_table ADD CONSTRAINT test_table_unique_name UNIQUE (name)",
    ] {
        Command::cargo_bin("analyticsdb")
            .expect("binary should build")
            .args(["query", "--catalog-path", &catalog_path, "--sql", sql])
            .assert()
            .success();
    }

    // 2. Rename column
    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "ALTER TABLE test_table RENAME COLUMN category TO sub_category",
        ])
        .assert()
        .success();

    // Verify rename
    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "SELECT sub_category FROM test_table WHERE id = 1",
        ])
        .assert()
        .success();

    // 3. Drop column
    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "ALTER TABLE test_table DROP COLUMN sub_category",
        ])
        .assert()
        .success();

    // Verify drop (should fail to select it)
    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "SELECT sub_category FROM test_table",
        ])
        .assert()
        .failure();

    // 4. Drop constraint
    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "ALTER TABLE test_table DROP CONSTRAINT test_table_unique_name",
        ])
        .assert()
        .success();

    // 5. Alter column
    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "ALTER TABLE test_table ALTER COLUMN name TYPE VARCHAR(255)",
        ])
        .assert()
        .success();

    // Verify constraint is gone by inserting a duplicate name
    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "INSERT INTO test_table VALUES (3, 'item1')",
        ])
        .assert()
        .success();

    cleanup_catalog_artifacts(&catalog_path);
}
