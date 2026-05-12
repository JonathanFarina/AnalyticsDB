use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let catalog_path = "repro_insert_quotes.json";
    let _ = std::fs::remove_file(catalog_path);

    // 1. Create the table
    let create_sql = "CREATE TABLE customers (id BIGINT NOT NULL, first_name TEXT NOT NULL, last_name TEXT, email TEXT, created_at TIMESTAMP)";
    let output = Command::new("cargo")
        .args(["run", "-p", "analyticsdb-cli", "--", "query", "--protocol", "embedded", "--catalog-path", catalog_path, "--sql", create_sql])
        .output()?;

    if !output.status.success() {
        panic!("Create table failed: {}", String::from_utf8_lossy(&output.stderr));
    }

    // 2. Insert with quoted column names
    let insert_sql = "INSERT INTO customers (\"id\", \"first_name\", \"last_name\", \"email\", \"created_at\") VALUES(9210, 'Jon', 'Farina', 'jonathan.farina@gmail.com', '2005-01-01')";
    let output = Command::new("cargo")
        .args(["run", "-p", "analyticsdb-cli", "--", "query", "--protocol", "embedded", "--catalog-path", catalog_path, "--sql", insert_sql])
        .output()?;

    if !output.status.success() {
        println!("Failure captured as expected:");
        println!("{}", String::from_utf8_lossy(&output.stderr));
    } else {
        println!("Success! (Wait, it should have failed if my theory is right)");
        println!("{}", String::from_utf8_lossy(&output.stdout));
    }

    let _ = std::fs::remove_file(catalog_path);
    Ok(())
}
