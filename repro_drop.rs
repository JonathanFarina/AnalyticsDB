use std::process::Command;

fn main() {
    // 1. Create the table
    let create_sql = "CREATE TABLE drop_test (id INT PRIMARY KEY, name TEXT);";
    println!("Creating table...");
    run_query(create_sql);

    // 2. Verify table exists
    let show_sql = "SHOW TABLES;";
    println!("Verifying table exists...");
    let output = get_query_output(show_sql);
    if !output.contains("drop_test") {
        eprintln!("Table drop_test was not created correctly");
        std::process::exit(1);
    }
    println!("Table exists.");

    // 3. Drop the table
    let drop_sql = "DROP TABLE drop_test;";
    println!("Dropping table...");
    run_query(drop_sql);

    // 4. Verify table is gone
    println!("Verifying table is gone...");
    let output = get_query_output(show_sql);
    if output.contains("drop_test") {
        eprintln!("Table drop_test was not dropped correctly");
        std::process::exit(1);
    }
    println!("Table is gone. DROP TABLE works!");
}

fn run_query(sql: &str) {
    let output = Command::new("./target/debug/analyticsdb")
        .args(["query", "--sql", sql, "--protocol", "embedded"])
        .output()
        .expect("Failed to execute query command");

    if !output.status.success() {
        eprintln!("Query failed: {}", String::from_utf8_lossy(&output.stderr));
        std::process::exit(1);
    }

    println!("{}", String::from_utf8_lossy(&output.stdout));
}

fn get_query_output(sql: &str) -> String {
    let output = Command::new("./target/debug/analyticsdb")
        .args(["query", "--sql", sql, "--protocol", "embedded"])
        .output()
        .expect("Failed to execute query command");

    if !output.status.success() {
        eprintln!("Query failed: {}", String::from_utf8_lossy(&output.stderr));
        std::process::exit(1);
    }

    String::from_utf8_lossy(&output.stdout).to_string()
}
