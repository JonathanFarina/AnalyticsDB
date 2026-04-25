use std::process::Command;

fn main() {
    // 1. Create the table
    let create_sql = "CREATE TABLE customers_test_v2 (
    id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);";
    
    println!("Creating table...");
    run_query(create_sql);

    // 2. Insert row omitting created_at
    let insert_sql = "INSERT INTO customers_test_v2 (id, first_name, last_name, email)
VALUES (2, 'Jane', 'Smith', 'jane.smith@example.com');";
    
    println!("Inserting row...");
    run_query(insert_sql);

    // 3. Verify results
    let select_sql = "SELECT id, first_name, last_name, email, created_at FROM customers_test_v2;";
    println!("Verifying results...");
    run_query(select_sql);
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
