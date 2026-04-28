use std::process::Command;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    // 1. Start the server in the background
    let mut server = Command::new("cargo")
        .args([
            "run",
            "-p",
            "analyticsdb-server",
            "--",
            "--catalog-path",
            "repro_catalog.json",
            "--postgres-addr",
            "127.0.0.1:55432",
        ])
        .spawn()
        .expect("failed to start server");

    // Give the server time to start
    sleep(Duration::from_secs(2)).await;

    // 2. Run ABORT command via CLI
    let output = Command::new("cargo")
        .args([
            "run",
            "-p",
            "analyticsdb-cli",
            "--",
            "query",
            "--protocol",
            "postgres",
            "--endpoint",
            "127.0.0.1:55432",
            "--sql",
            "ABORT",
        ])
        .output()
        .expect("failed to run CLI");

    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("ABORT output: {}", stdout);

    if stdout.contains("Command completed. 0 row(s) affected.") {
        println!("ABORT works as expected.");
    } else {
        println!("ABORT FAILED.");
    }

    // 3. Cleanup
    server.kill().expect("failed to kill server");
    std::fs::remove_file("repro_catalog.json").ok();
}
