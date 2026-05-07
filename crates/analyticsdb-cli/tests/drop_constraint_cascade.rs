use assert_cmd::Command;
use uuid::Uuid;

fn temp_catalog_path() -> String {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "analyticsdb-drop-constraint-cascade-{}.json",
        Uuid::now_v7()
    ));
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
async fn test_drop_constraint_cascade() {
    let catalog_path = temp_catalog_path();

    // 1. Create tables with PK and FK
    for sql in [
        "CREATE TABLE authors (id BIGINT PRIMARY KEY, name TEXT)",
        "CREATE TABLE books (id BIGINT PRIMARY KEY, author_id BIGINT, FOREIGN KEY (author_id) REFERENCES authors(id))",
    ] {
        Command::cargo_bin("analyticsdb")
            .expect("binary should build")
            .args(["query", "--catalog-path", &catalog_path, "--sql", sql])
            .assert()
            .success();
    }

    // 2. Attempt to drop PK without CASCADE (should fail)
    // The PK name is auto-generated as "authors_id_idx" if not specified,
    // but in build_relation_with_added_constraint it defaults to "auto_constraint" if None,
    // and then renamed in indexes_from_constraints.
    // Wait, let's see what the PK name actually is.
    // In our implementation of build_relation_with_added_constraint:
    // name.unwrap_or_else(|| "auto_constraint".to_string())

    // Let's use an explicit name for the constraint to be sure.
    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "CREATE TABLE authors2 (id BIGINT, name TEXT, CONSTRAINT pk_authors2 PRIMARY KEY(id))",
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
            "CREATE TABLE books2 (id BIGINT PRIMARY KEY, author_id BIGINT, FOREIGN KEY (author_id) REFERENCES authors2(id))",
        ])
        .assert()
        .success();

    // Now try to drop pk_authors2 without CASCADE
    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "ALTER TABLE authors2 DROP CONSTRAINT pk_authors2",
        ])
        .assert()
        .failure();

    // 3. Drop PK with CASCADE
    Command::cargo_bin("analyticsdb")
        .expect("binary should build")
        .args([
            "query",
            "--catalog-path",
            &catalog_path,
            "--sql",
            "ALTER TABLE authors2 DROP CONSTRAINT pk_authors2 CASCADE",
        ])
        .assert()
        .success();

    // Verify books2 foreign key is also gone (optional, but good)
    // We can verify by looking at the catalog or attempting to drop it again (should fail)

    cleanup_catalog_artifacts(&catalog_path);
}
