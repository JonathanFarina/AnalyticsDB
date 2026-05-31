//! pg-wire proxy — forwards SQL to the running AnalyticsDB server so its engine
//! is the single source of truth for execution and catalog mutations.

use anyhow::{Context, Result};
use tokio_postgres::{Config, NoTls, SimpleQueryMessage};

/// The text result of a simple-query execution over the wire.
#[derive(Debug, Default)]
pub struct ProxyResult {
    pub columns: Vec<String>,
    /// Row values as text; `None` represents SQL NULL.
    pub rows: Vec<Vec<Option<String>>>,
    /// Rows affected reported by the final `CommandComplete`, if any.
    pub affected_rows: Option<u64>,
}

/// Splits a `host:port` endpoint, defaulting the port to 5432.
fn parse_endpoint(endpoint: &str) -> (String, u16) {
    match endpoint.rsplit_once(':') {
        Some((host, port)) => (host.to_string(), port.parse().unwrap_or(5432)),
        None => (endpoint.to_string(), 5432),
    }
}

/// Opens a fresh pg-wire connection to the server as `user`, authenticating with
/// `password` (SCRAM-SHA-256 is negotiated transparently). The caller owns the
/// returned client; dropping it closes the connection.
pub async fn connect(
    endpoint: &str,
    user: &str,
    password: &str,
    database: &str,
) -> Result<tokio_postgres::Client> {
    let (host, port) = parse_endpoint(endpoint);
    let mut config = Config::new();
    config
        .host(&host)
        .port(port)
        .user(user)
        .dbname(database)
        .application_name("analyticsdb-gateway");
    if !password.is_empty() {
        config.password(password);
    }

    let (client, connection) = config
        .connect(NoTls)
        .await
        .with_context(|| format!("failed to connect to AnalyticsDB pg-wire at {endpoint}"))?;

    // Drive the connection in the background for the lifetime of the client.
    tokio::spawn(async move {
        let _ = connection.await;
    });

    Ok(client)
}

/// Connects as `user` and executes `sql` via the simple query protocol, which
/// returns all values as text — ideal for a SQL console and DDL alike.
pub async fn execute_sql(
    endpoint: &str,
    user: &str,
    password: &str,
    database: &str,
    sql: &str,
) -> Result<ProxyResult> {
    let client = connect(endpoint, user, password, database).await?;
    let messages = client
        .simple_query(sql)
        .await
        .context("query execution failed")?;

    let mut result = ProxyResult::default();
    for message in messages {
        match message {
            SimpleQueryMessage::Row(row) => {
                if result.columns.is_empty() {
                    result.columns = row
                        .columns()
                        .iter()
                        .map(|column| column.name().to_string())
                        .collect();
                }
                let mut values = Vec::with_capacity(row.columns().len());
                for index in 0..row.columns().len() {
                    values.push(row.get(index).map(|value| value.to_string()));
                }
                result.rows.push(values);
            }
            SimpleQueryMessage::CommandComplete(affected) => {
                result.affected_rows = Some(affected);
            }
            _ => {}
        }
    }
    Ok(result)
}

/// Runs several statements in order over a single connection, stopping at the
/// first error. The server's simple-query handler executes one statement at a
/// time, so multi-statement DDL (e.g. CREATE USER then ALTER GROUP) must be
/// issued as separate queries rather than a semicolon-joined string.
pub async fn execute_statements(
    endpoint: &str,
    user: &str,
    password: &str,
    database: &str,
    statements: &[String],
) -> Result<()> {
    let client = connect(endpoint, user, password, database).await?;
    for statement in statements {
        client
            .simple_query(statement)
            .await
            .with_context(|| format!("statement failed: {statement}"))?;
    }
    Ok(())
}

/// Quotes a string as a SQL single-quoted literal (doubling embedded quotes).
pub fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
