use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let catalog_path = "repro_insert_case.json";
    let _ = std::fs::remove_file(catalog_path);

    // 1. Create the table
    let create_sql = "CREATE TABLE customers (id BIGINT NOT NULL, first_name TEXT NOT NULL)";
    let output = Command::new("cargo")
        .args(["run", "-p", "analyticsdb-cli", "--", "query", "--protocol", "embedded", "--catalog-path", catalog_path, "--sql", create_sql])
        .output()?;

    if !output.status.success() {
        panic!("Create table failed: {}", String::from_utf8_lossy(&output.stderr));
    }

    // 2. Insert with uppercase unquoted column names
    let insert_sql = "INSERT INTO customers (ID, FIRST_NAME) VALUES(9210, 'Jon')";
    let output = Command::new("cargo")
        .args(["run", "-p", "analyticsdb-cli", "--", "query", "--protocol", "embedded", "--catalog-path", catalog_path, "--sql", insert_sql])
        .output()?;

    if !output.status.success() {
        println!("Failure captured:");
        println!("{}", String::from_utf8_lossy(&output.stderr));
    } else {
        println!("Success!");
        println!("{}", String::from_utf8_lossy(&output.stdout));
    }

    let _ = std::fs::remove_file(catalog_path);
    Ok(())
}
