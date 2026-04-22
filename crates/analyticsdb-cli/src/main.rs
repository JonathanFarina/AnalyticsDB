use analyticsdb_core::{Protocol, QueryRequest, QueryResponse, SessionContext};
use analyticsdb_engine::PrototypeEngine;
use anyhow::{anyhow, bail, Context, Result};
use arrow_flight::sql::client::FlightSqlServiceClient;
use clap::{Parser, Subcommand, ValueEnum};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use futures::TryStreamExt;
use std::time::Instant;
use tokio_postgres::types::ToSql;
use tokio_postgres::types::Type;
use tokio_postgres::{NoTls, SimpleQueryMessage};

fn main() {
    if let Err(error) = run() {
        eprintln!("ERROR: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Query(options) => run_query(options),
    }
}

fn run_query(options: QueryOptions) -> Result<()> {
    let session = SessionContext {
        user: options.user,
        database: options.database,
        schema: options.schema,
        protocol: match options.protocol {
            ClientProtocol::Embedded => Protocol::Embedded,
            ClientProtocol::Postgres => Protocol::PostgreSql,
            ClientProtocol::FlightSql => Protocol::ArrowFlightSql,
        },
    };

    let response = match options.protocol {
        ClientProtocol::Embedded => {
            if !options.params.is_empty() {
                bail!("CLI parameters are currently only supported for PostgreSQL protocol mode");
            }
            run_embedded_query(options.sql, session, options.catalog_path)?
        }
        ClientProtocol::Postgres => run_async(run_postgres_query(
            options.sql,
            session,
            options.endpoint,
            options.params,
        ))?,
        ClientProtocol::FlightSql => {
            if !options.params.is_empty() {
                bail!("CLI parameters are not yet supported for Flight SQL mode");
            }
            run_async(run_flight_sql_query(options.sql, session, options.endpoint))?
        }
    };

    render_response(&response, options.format);

    Ok(())
}

fn run_embedded_query(
    sql: String,
    session: SessionContext,
    catalog_path: Option<String>,
) -> Result<QueryResponse> {
    let request = QueryRequest { sql, session };
    let engine = if let Some(path) = catalog_path {
        PrototypeEngine::from_catalog_path(&path)?
    } else {
        PrototypeEngine::new()?
    };

    engine.execute_query(&request)
}

async fn run_postgres_query(
    sql: String,
    session: SessionContext,
    endpoint: Option<String>,
    params: Vec<String>,
) -> Result<QueryResponse> {
    let endpoint = endpoint.unwrap_or_else(|| "127.0.0.1:5432".to_string());
    let (host, port) = parse_postgres_endpoint(&endpoint)?;
    let connection_string = format!(
        "host={host} port={port} user={} dbname={}",
        session.user, session.database
    );

    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .with_context(|| format!("failed to connect to PostgreSQL endpoint '{endpoint}'"))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    if session.schema != "public" {
        client
            .batch_execute(&format!(
                "SET search_path TO {}",
                quote_ident(&session.schema)
            ))
            .await
            .with_context(|| {
                format!(
                    "failed to set PostgreSQL search_path to '{}'",
                    session.schema
                )
            })?;
    }

    let started_at = Instant::now();
    let (columns, rows, _rows_affected, message) = if params.is_empty() {
        let messages = client
            .simple_query(&sql)
            .await
            .with_context(|| "PostgreSQL wire query failed".to_string())?;
        let (columns, rows, rows_affected) = simple_query_to_rows(messages);
        let message = if rows.is_empty() {
            format!("PostgreSQL wire command completed. {rows_affected} row(s) affected.")
        } else {
            "PostgreSQL wire query completed.".to_string()
        };
        (columns, rows, rows_affected, message)
    } else {
        let params = parse_cli_params(&params)?;
        if sql_returns_rows(&sql) {
            let rows = client
                .query_typed(&sql, &params_to_postgres_typed_refs(&params))
                .await
                .with_context(|| "PostgreSQL extended query failed".to_string())?;
            (
                rows.first()
                    .map(|row| {
                        row.columns()
                            .iter()
                            .map(|column| column.name().to_string())
                            .collect()
                    })
                    .unwrap_or_default(),
                rows.into_iter()
                    .map(|row| {
                        row.columns()
                            .iter()
                            .enumerate()
                            .map(|(index, _)| postgres_row_value_to_string(&row, index))
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?,
                0,
                "PostgreSQL extended query completed.".to_string(),
            )
        } else {
            let statement = client
                .prepare_typed(&sql, &params_to_postgres_types(&params))
                .await
                .with_context(|| "PostgreSQL extended prepare failed".to_string())?;
            let param_refs = params_to_postgres_refs(&params);
            let rows_affected = client
                .execute(&statement, &param_refs)
                .await
                .with_context(|| "PostgreSQL extended execution failed".to_string())?;
            (
                Vec::new(),
                Vec::new(),
                rows_affected,
                format!("PostgreSQL extended command completed. {rows_affected} row(s) affected."),
            )
        }
    };

    Ok(QueryResponse {
        query_id: "unavailable-via-postgres-wire".to_string(),
        coordinator_node_id: "unavailable-via-postgres-wire".to_string(),
        session,
        columns,
        rows,
        message,
        execution_time_ms: started_at.elapsed().as_millis(),
    })
}

async fn run_flight_sql_query(
    sql: String,
    session: SessionContext,
    endpoint: Option<String>,
) -> Result<QueryResponse> {
    let endpoint =
        normalize_flight_endpoint(endpoint.as_deref().unwrap_or("http://127.0.0.1:50051"));
    let channel = tonic::transport::Endpoint::new(endpoint.clone())
        .with_context(|| format!("invalid Flight SQL endpoint '{endpoint}'"))?
        .connect()
        .await
        .with_context(|| format!("failed to connect to Flight SQL endpoint '{endpoint}'"))?;

    let mut client = FlightSqlServiceClient::new(channel);
    client.set_header("x-analyticsdb-user", session.user.clone());
    client.set_header("x-analyticsdb-database", session.database.clone());
    client.set_header("x-analyticsdb-schema", session.schema.clone());

    let started_at = Instant::now();
    if sql_returns_rows(&sql) {
        let info = client
            .execute(sql, None)
            .await
            .with_context(|| "Flight SQL statement execution failed".to_string())?;
        let ticket = info
            .endpoint
            .first()
            .and_then(|endpoint| endpoint.ticket.clone())
            .ok_or_else(|| anyhow!("Flight SQL server did not return a ticket"))?;
        let batches = client
            .do_get(ticket)
            .await
            .with_context(|| "Flight SQL do_get failed".to_string())?
            .try_collect::<Vec<_>>()
            .await
            .with_context(|| "Flight SQL result stream could not be collected".to_string())?;

        query_response_from_batches(
            "unavailable-via-flight-sql".to_string(),
            "unavailable-via-flight-sql".to_string(),
            session,
            batches,
            "Flight SQL query completed.".to_string(),
            started_at.elapsed().as_millis(),
        )
    } else {
        let affected = client
            .execute_update(sql, None)
            .await
            .with_context(|| "Flight SQL update execution failed".to_string())?;
        Ok(QueryResponse {
            query_id: "unavailable-via-flight-sql".to_string(),
            coordinator_node_id: "unavailable-via-flight-sql".to_string(),
            session,
            columns: Vec::new(),
            rows: Vec::new(),
            message: format!("Flight SQL command completed. {affected} row(s) affected."),
            execution_time_ms: started_at.elapsed().as_millis(),
        })
    }
}

fn run_async<F, T>(future: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(future)
}

fn query_response_from_batches(
    query_id: String,
    coordinator_node_id: String,
    session: SessionContext,
    batches: Vec<RecordBatch>,
    message: String,
    execution_time_ms: u128,
) -> Result<QueryResponse> {
    let columns = batches
        .first()
        .map(|batch| {
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let mut rows = Vec::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let mut row = Vec::new();
            for column_index in 0..batch.num_columns() {
                let column = batch.column(column_index);
                if column.is_null(row_index) {
                    row.push(String::new());
                } else {
                    row.push(array_value_to_string(column.as_ref(), row_index)?);
                }
            }
            rows.push(row);
        }
    }

    Ok(QueryResponse {
        query_id,
        coordinator_node_id,
        session,
        columns,
        rows,
        message,
        execution_time_ms,
    })
}

fn parse_postgres_endpoint(endpoint: &str) -> Result<(String, u16)> {
    let (host, port) = endpoint
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("PostgreSQL endpoint must be in 'host:port' form"))?;
    let port = port
        .parse::<u16>()
        .with_context(|| format!("invalid PostgreSQL port in endpoint '{endpoint}'"))?;

    Ok((host.to_string(), port))
}

fn simple_query_to_rows(messages: Vec<SimpleQueryMessage>) -> (Vec<String>, Vec<Vec<String>>, u64) {
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    let mut rows_affected = 0_u64;

    for message in messages {
        match message {
            SimpleQueryMessage::RowDescription(description) if columns.is_empty() => {
                columns = description
                    .iter()
                    .map(|column| column.name().to_string())
                    .collect();
            }
            SimpleQueryMessage::RowDescription(_) => {}
            SimpleQueryMessage::Row(row) => {
                if columns.is_empty() {
                    columns = row
                        .columns()
                        .iter()
                        .map(|column| column.name().to_string())
                        .collect();
                }

                rows.push(
                    (0..row.len())
                        .map(|index| row.get(index).unwrap_or_default().to_string())
                        .collect(),
                );
            }
            SimpleQueryMessage::CommandComplete(count) => rows_affected = count,
            _ => {}
        }
    }

    (columns, rows, rows_affected)
}

fn postgres_row_value_to_string(row: &tokio_postgres::Row, index: usize) -> Result<String> {
    let column_type = row.columns()[index].type_();

    if *column_type == Type::BOOL {
        Ok(row
            .try_get::<_, Option<bool>>(index)?
            .map(|v| v.to_string())
            .unwrap_or_default())
    } else if *column_type == Type::INT2 {
        Ok(row
            .try_get::<_, Option<i16>>(index)?
            .map(|v| v.to_string())
            .unwrap_or_default())
    } else if *column_type == Type::INT4 {
        Ok(row
            .try_get::<_, Option<i32>>(index)?
            .map(|v| v.to_string())
            .unwrap_or_default())
    } else if *column_type == Type::INT8 {
        Ok(row
            .try_get::<_, Option<i64>>(index)?
            .map(|v| v.to_string())
            .unwrap_or_default())
    } else if *column_type == Type::FLOAT4 {
        Ok(row
            .try_get::<_, Option<f32>>(index)?
            .map(|v| v.to_string())
            .unwrap_or_default())
    } else if *column_type == Type::FLOAT8 {
        Ok(row
            .try_get::<_, Option<f64>>(index)?
            .map(|v| v.to_string())
            .unwrap_or_default())
    } else {
        Ok(row.try_get::<_, Option<String>>(index)?.unwrap_or_default())
    }
}

fn parse_cli_params(values: &[String]) -> Result<Vec<CliParamValue>> {
    values
        .iter()
        .map(|value| {
            let json = serde_json::from_str::<serde_json::Value>(value).with_context(|| {
                format!(
                    "failed to parse CLI parameter '{value}' as JSON scalar. Use values like 11, true, 3.14, or \"text\"."
                )
            })?;
            CliParamValue::from_json(json)
        })
        .collect()
}

fn params_to_postgres_refs(params: &[CliParamValue]) -> Vec<&(dyn ToSql + Sync)> {
    params.iter().map(CliParamValue::as_to_sql).collect()
}

fn params_to_postgres_types(params: &[CliParamValue]) -> Vec<Type> {
    params.iter().map(CliParamValue::postgres_type).collect()
}

fn params_to_postgres_typed_refs(params: &[CliParamValue]) -> Vec<(&(dyn ToSql + Sync), Type)> {
    params
        .iter()
        .map(|param| (param.as_to_sql(), param.postgres_type()))
        .collect()
}

fn normalize_flight_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn sql_returns_rows(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    let upper = trimmed.to_ascii_uppercase();

    upper.starts_with("SELECT")
        || upper.starts_with("SHOW")
        || upper.starts_with("DESCRIBE")
        || upper.starts_with("WITH")
}

fn render_response(response: &QueryResponse, format: OutputFormat) {
    match format {
        OutputFormat::Table => render_table(response),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(response).expect("response should serialize")
            );
        }
    }
}

fn render_table(response: &QueryResponse) {
    println!("Query ID: {}", response.query_id);
    println!("Coordinator: {}", response.coordinator_node_id);
    println!(
        "Session: user={} database={} schema={}",
        response.session.user, response.session.database, response.session.schema
    );
    println!("Message: {}", response.message);
    println!("Execution Time: {} ms", response.execution_time_ms);

    if response.columns.is_empty() {
        println!("No columns returned.");
        return;
    }

    let mut widths: Vec<usize> = response.columns.iter().map(|column| column.len()).collect();

    for row in &response.rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(value.len());
        }
    }

    let divider = build_divider(&widths);

    println!("{divider}");
    println!("| {} |", format_cells(&response.columns, &widths));
    println!("{divider}");

    for row in &response.rows {
        println!("| {} |", format_cells(row, &widths));
    }

    println!("{divider}");
    println!("Rows: {}", response.row_count());
}

fn build_divider(widths: &[usize]) -> String {
    let mut divider = String::from("+");

    for width in widths {
        divider.push_str(&"-".repeat(*width + 2));
        divider.push('+');
    }

    divider
}

fn format_cells(values: &[String], widths: &[usize]) -> String {
    values
        .iter()
        .zip(widths.iter())
        .map(|(value, width)| format!("{value:<width$}", width = width))
        .collect::<Vec<_>>()
        .join(" | ")
}

#[derive(Debug, Parser)]
#[command(name = "analyticsdb")]
#[command(about = "Prototype CLI for AnalyticsDB")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Query(QueryOptions),
}

#[derive(Debug, Parser)]
struct QueryOptions {
    #[arg(long)]
    sql: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,
    #[arg(long, value_enum, default_value_t = ClientProtocol::Embedded)]
    protocol: ClientProtocol,
    #[arg(long, default_value = "postgres")]
    database: String,
    #[arg(long, default_value = "public")]
    schema: String,
    #[arg(long, default_value = "postgres")]
    user: String,
    #[arg(long)]
    catalog_path: Option<String>,
    #[arg(long)]
    endpoint: Option<String>,
    #[arg(long = "param", action = clap::ArgAction::Append)]
    params: Vec<String>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum OutputFormat {
    Table,
    Json,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum ClientProtocol {
    Embedded,
    Postgres,
    FlightSql,
}

#[derive(Debug)]
enum CliParamValue {
    Bool(bool),
    Float(f64),
    Int(i64),
    String(String),
}

impl CliParamValue {
    fn from_json(value: serde_json::Value) -> Result<Self> {
        match value {
            serde_json::Value::Bool(value) => Ok(Self::Bool(value)),
            serde_json::Value::Number(value) => {
                if let Some(value) = value.as_i64() {
                    Ok(Self::Int(value))
                } else if let Some(value) = value.as_f64() {
                    Ok(Self::Float(value))
                } else {
                    bail!("unsupported numeric CLI parameter '{value}'")
                }
            }
            serde_json::Value::String(value) => Ok(Self::String(value)),
            serde_json::Value::Null => {
                bail!("null CLI parameters are not yet supported for PostgreSQL extended queries")
            }
            _ => bail!("CLI parameters must be JSON scalars, not arrays or objects"),
        }
    }

    fn as_to_sql(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::Bool(value) => value,
            Self::Float(value) => value,
            Self::Int(value) => value,
            Self::String(value) => value,
        }
    }

    fn postgres_type(&self) -> Type {
        match self {
            Self::Bool(_) => Type::BOOL,
            Self::Float(_) => Type::FLOAT8,
            Self::Int(_) => Type::INT8,
            Self::String(_) => Type::TEXT,
        }
    }
}
