use analyticsdb_core::{Protocol, QueryRequest, QueryResponse, SessionContext};
use analyticsdb_engine::PrototypeEngine;
use anyhow::{anyhow, bail, Context, Result};
use arrow_flight::sql::client::FlightSqlServiceClient;
use clap::{Parser, Subcommand, ValueEnum};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use futures::StreamExt;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, KeyPair, SanType,
    PKCS_ECDSA_P256_SHA256,
};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use serde::Deserialize;
use std::path::PathBuf;
use std::time::Instant;
use tokio_postgres::types::ToSql;
use tokio_postgres::types::Type;
use tokio_postgres::{NoTls, SimpleQueryMessage};
use tonic::transport::Certificate;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Endpoint;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    if let Err(error) = run().await {
        eprintln!("ERROR: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Query(options) => run_query(options).await,
        Commands::Interactive(options) => run_interactive(options).await,
        Commands::Ca(opts) => match opts.command {
            CaCommands::Init(init_opts) => run_ca_init(&init_opts),
        },
    }
}

fn run_ca_init(opts: &CaInitOptions) -> Result<()> {
    let output_dir = std::path::Path::new(&opts.output);
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create output directory '{}'", opts.output))?;

    // Build CA cert
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.distinguished_name = DistinguishedName::new();
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "AnalyticsDB Cluster CA");
    ca_params.not_before = time::OffsetDateTime::now_utc();
    ca_params.not_after =
        time::OffsetDateTime::now_utc() + time::Duration::days(opts.validity_days as i64);
    let ca_key =
        KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).context("failed to generate CA key pair")?;
    let ca_cert = ca_params
        .self_signed(&ca_key)
        .context("failed to self-sign CA certificate")?;

    // Build leaf cert
    let mut leaf_params = CertificateParams::default();
    let mut sans = vec![
        SanType::DnsName("localhost".try_into().context("invalid SAN: localhost")?),
        SanType::IpAddress("127.0.0.1".parse().context("invalid SAN: 127.0.0.1")?),
        SanType::DnsName(
            "*.analyticsdb.local"
                .try_into()
                .context("invalid SAN: *.analyticsdb.local")?,
        ),
    ];
    for h in &opts.hostname {
        sans.push(SanType::DnsName(
            h.as_str()
                .try_into()
                .with_context(|| format!("invalid hostname SAN: {h}"))?,
        ));
    }
    leaf_params.subject_alt_names = sans;
    leaf_params.not_before = time::OffsetDateTime::now_utc();
    leaf_params.not_after =
        time::OffsetDateTime::now_utc() + time::Duration::days(opts.validity_days as i64);
    let leaf_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)
        .context("failed to generate leaf key pair")?;
    let leaf_cert = leaf_params
        .signed_by(&leaf_key, &ca_cert, &ca_key)
        .context("failed to sign leaf certificate with CA")?;

    // Write files
    let ca_crt_path = output_dir.join("ca.crt");
    let ca_key_path = output_dir.join("ca.key");
    let server_crt_path = output_dir.join("server.crt");
    let server_key_path = output_dir.join("server.key");

    std::fs::write(&ca_crt_path, ca_cert.pem())
        .with_context(|| format!("failed to write {}", ca_crt_path.display()))?;
    std::fs::write(&ca_key_path, ca_key.serialize_pem())
        .with_context(|| format!("failed to write {}", ca_key_path.display()))?;
    std::fs::write(&server_crt_path, leaf_cert.pem())
        .with_context(|| format!("failed to write {}", server_crt_path.display()))?;
    std::fs::write(&server_key_path, leaf_key.serialize_pem())
        .with_context(|| format!("failed to write {}", server_key_path.display()))?;

    println!("Generated CA and server certificates in '{}':", opts.output);
    println!("  ca.crt     — CA certificate (trust anchor for all cluster nodes)");
    println!("  ca.key     — CA private key (keep secret; used only to issue new certs)");
    println!("  server.crt — Server/client leaf certificate (used by each node)");
    println!("  server.key — Leaf private key");
    println!();
    println!("Add to your cluster-config.json:");
    println!("  \"tls_ca_cert_path\": \"{}\",", ca_crt_path.display());
    println!("  \"tls_cert_path\":    \"{}\",", server_crt_path.display());
    println!("  \"tls_key_path\":     \"{}\"", server_key_path.display());

    Ok(())
}

async fn run_query(options: QueryOptions) -> Result<()> {
    let sql = if let Some(file_path) = &options.file {
        std::fs::read_to_string(file_path)
            .with_context(|| format!("failed to read SQL file '{}'", file_path.display()))?
    } else if let Some(sql) = &options.sql {
        sql.clone()
    } else {
        bail!("Either --sql or --file must be provided");
    };
    let params = options.params.clone();
    let client_options = ClientOptions::from_query(options);
    let started_at = Instant::now();
    let response = execute_sql(sql, &client_options, params).await?;
    let response_received_at = Instant::now();

    let rendered = render_response(&response, client_options.format);
    if let Some(output_path) = &client_options.output_file {
        std::fs::write(output_path, rendered)?;
    } else {
        print!("{rendered}");
    }
    let rendered_at = Instant::now();
    if client_options.timing {
        render_timing(
            &response,
            response_received_at.duration_since(started_at).as_millis(),
            rendered_at.duration_since(response_received_at).as_millis(),
            rendered_at.duration_since(started_at).as_millis(),
            client_options.format,
        );
    }

    Ok(())
}

async fn run_interactive(options: InteractiveOptions) -> Result<()> {
    let mut client_options = ClientOptions::from_interactive(options);
    let mut editor = DefaultEditor::new()?;

    if let Some(path) = history_path() {
        let _ = editor.load_history(&path);
    }

    println!("AnalyticsDB interactive client");
    println!("Enter SQL terminated by ';'. Type \\q to quit or \\? for help.");

    let mut buffer = String::new();
    loop {
        let prompt = if buffer.trim().is_empty() {
            "analyticsdb=> "
        } else {
            "analyticsdb-> "
        };

        match editor.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    let _ = editor.add_history_entry(line.as_str());
                }

                if buffer.trim().is_empty() && trimmed.starts_with('\\') {
                    match handle_meta_command(trimmed, &mut client_options)? {
                        MetaCommandAction::Continue => continue,
                        MetaCommandAction::Quit => break,
                        MetaCommandAction::ExecuteSql(sql) => {
                            buffer.clear();
                            buffer.push_str(&sql);
                            buffer.push('\n');
                        }
                    }
                }

                if trimmed.is_empty() && buffer.trim().is_empty() {
                    continue;
                }

                buffer.push_str(&line);
                buffer.push('\n');

                if sql_statement_is_complete(&buffer) {
                    let sql = buffer.trim().to_string();
                    buffer.clear();

                    client_options.last_sql = Some(sql.clone());

                    let started_at = Instant::now();
                    match execute_sql(sql, &client_options, Vec::new()).await {
                        Ok(response) => {
                            let response_received_at = Instant::now();
                            let rendered = render_response(&response, client_options.format);
                            if let Some(output_path) = &client_options.output_file {
                                std::fs::write(output_path, rendered)?;
                            } else {
                                print!("{rendered}");
                            }
                            let rendered_at = Instant::now();
                            if client_options.timing {
                                render_timing(
                                    &response,
                                    response_received_at.duration_since(started_at).as_millis(),
                                    rendered_at.duration_since(response_received_at).as_millis(),
                                    rendered_at.duration_since(started_at).as_millis(),
                                    client_options.format,
                                );
                            }

                            if let Some(interval) = client_options.watch_interval {
                                loop {
                                    tokio::time::sleep(tokio::time::Duration::from_secs(interval))
                                        .await;
                                    if let Some(ref sql) = client_options.last_sql {
                                        match execute_sql(sql.clone(), &client_options, Vec::new())
                                            .await
                                        {
                                            Ok(response) => {
                                                let response_received_at = Instant::now();
                                                let rendered = render_response(
                                                    &response,
                                                    client_options.format,
                                                );
                                                if let Some(output_path) =
                                                    &client_options.output_file
                                                {
                                                    std::fs::write(output_path, rendered)?;
                                                } else {
                                                    print!("{rendered}");
                                                }
                                                let rendered_at = Instant::now();
                                                if client_options.timing {
                                                    render_timing(
                                                        &response,
                                                        response_received_at
                                                            .duration_since(started_at)
                                                            .as_millis(),
                                                        rendered_at
                                                            .duration_since(response_received_at)
                                                            .as_millis(),
                                                        rendered_at
                                                            .duration_since(started_at)
                                                            .as_millis(),
                                                        client_options.format,
                                                    );
                                                }
                                            }
                                            Err(error) => eprintln!("ERROR: {error:#}"),
                                        }
                                    } else {
                                        eprintln!("No query to watch.");
                                        break;
                                    }
                                }
                            }
                        }
                        Err(error) => eprintln!("ERROR: {error:#}"),
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                if buffer.trim().is_empty() {
                    println!("Use \\q to quit.");
                } else {
                    buffer.clear();
                    println!("Query buffer cleared.");
                }
            }
            Err(ReadlineError::Eof) => break,
            Err(error) => return Err(error.into()),
        }
    }

    if let Some(path) = history_path() {
        let _ = editor.save_history(&path);
    }

    Ok(())
}

async fn execute_sql(
    sql: String,
    options: &ClientOptions,
    params: Vec<String>,
) -> Result<QueryResponse> {
    let protocol = match options.protocol {
        ClientProtocol::Embedded => Protocol::Embedded,
        ClientProtocol::Postgres => Protocol::PostgreSql,
        ClientProtocol::FlightSql => Protocol::ArrowFlightSql,
    };
    let role = options.role.clone().unwrap_or_else(|| options.user.clone());
    let session = SessionContext {
        user: options.user.clone(),
        role,
        database: options.database.clone(),
        schema: options.schema.clone(),
        auth_method: match protocol {
            Protocol::Embedded => "embedded-prototype".to_string(),
            Protocol::PostgreSql => "postgres-wire-startup".to_string(),
            Protocol::ArrowFlightSql => "flight-sql-header".to_string(),
        },
        protocol,
        transaction_status: analyticsdb_core::TransactionStatus::Idle,
        statement_timeout_ms: 0,
        idle_in_transaction_timeout_ms: 0,
    };

    match options.protocol {
        ClientProtocol::Embedded => {
            if !params.is_empty() {
                bail!("CLI parameters are currently only supported for PostgreSQL protocol mode");
            }
            run_embedded_query(sql, session, options.catalog_path.clone()).await
        }
        ClientProtocol::Postgres => {
            run_postgres_query(
                sql,
                session,
                options.endpoint.clone(),
                params,
                options.password.clone(),
            )
            .await
        }
        ClientProtocol::FlightSql => {
            run_flight_sql_query(
                sql,
                session,
                options.endpoint.clone(),
                params,
                options.password.clone(),
                options.tls_ca_cert.clone(),
                options.tls_domain.clone(),
            )
            .await
        }
    }
}

async fn run_embedded_query(
    sql: String,
    session: SessionContext,
    catalog_path: String,
) -> Result<QueryResponse> {
    let request = QueryRequest {
        sql,
        session,
        query_id: None,
    };
    let engine = PrototypeEngine::from_catalog_path(&catalog_path).await?;

    let result = engine.execute_query(&request).await?;
    Ok(result.to_query_response())
}

async fn run_postgres_query(
    sql: String,
    session: SessionContext,
    endpoint: Option<String>,
    params: Vec<String>,
    password: Option<String>,
) -> Result<QueryResponse> {
    let endpoints_str = endpoint.unwrap_or_else(|| "127.0.0.1:5432".to_string());
    let endpoints: Vec<&str> = endpoints_str.split(',').collect();
    let mut last_error = None;

    for endpoint in endpoints {
        let result = async {
            let (host, port) = parse_postgres_endpoint(endpoint)?;
            let mut connection_string = format!(
                "host={host} port={port} user={} dbname={}",
                session.user, session.database
            );
            if let Some(password) = &password {
                connection_string.push_str(&format!(" password={password}"));
            }

            let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
                .await
                .with_context(|| {
                    format!("failed to connect to PostgreSQL endpoint '{endpoint}'")
                })?;
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
                let failure_context = if sql_returns_rows(&sql) {
                    "Remote query execution failed"
                } else {
                    "Remote command execution failed"
                };
                let messages = client
                    .simple_query(&sql)
                    .await
                    .with_context(|| failure_context.to_string())?;
                let (columns, rows, rows_affected) = simple_query_to_rows(messages);
                let message = if columns.is_empty() {
                    command_completed_message(rows_affected)
                } else {
                    query_completed_message()
                };
                (columns, rows, rows_affected, message)
            } else {
                let params_values = parse_cli_params(&params)?;
                if sql_returns_rows(&sql) {
                    let rows = client
                        .query_typed(&sql, &params_to_postgres_typed_refs(&params_values))
                        .await
                        .with_context(|| "Remote query execution failed".to_string())?;
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
                        query_completed_message(),
                    )
                } else {
                    let statement = client
                        .prepare_typed(&sql, &params_to_postgres_types(&params_values))
                        .await
                        .with_context(|| "Remote command preparation failed".to_string())?;
                    let param_refs = params_to_postgres_refs(&params_values);
                    let rows_affected = client
                        .execute(&statement, &param_refs)
                        .await
                        .with_context(|| "Remote command execution failed".to_string())?;
                    (
                        Vec::new(),
                        Vec::new(),
                        rows_affected,
                        command_completed_message(rows_affected),
                    )
                }
            };

            Ok(QueryResponse {
                query_id: "unavailable-via-postgres-wire".to_string(),
                coordinator_node_id: "unavailable-via-postgres-wire".to_string(),
                session: session.clone(),
                columns,
                rows,
                message,
                execution_time_ms: started_at.elapsed().as_millis(),
            })
        }
        .await;

        match result {
            Ok(response) => return Ok(response),
            Err(e) => {
                eprintln!("Warning: Failed to query PostgreSQL endpoint '{endpoint}': {e:#}");
                last_error = Some(e);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("No endpoints provided")))
}

async fn run_flight_sql_query(
    sql: String,
    session: SessionContext,
    endpoint: Option<String>,
    params: Vec<String>,
    password: Option<String>,
    tls_ca_cert: Option<String>,
    tls_domain: Option<String>,
) -> Result<QueryResponse> {
    let endpoints_str = endpoint.unwrap_or_else(|| "http://127.0.0.1:50051".to_string());
    let endpoints: Vec<&str> = endpoints_str.split(',').collect();
    let mut last_error = None;

    for endpoint in endpoints {
        let result = async {
            let normalized_endpoint = normalize_flight_endpoint(endpoint);
            let channel =
                connect_flight_sql_endpoint(&normalized_endpoint, &tls_ca_cert, &tls_domain)
                    .await
                    .with_context(|| {
                        format!("failed to connect to Flight SQL endpoint '{normalized_endpoint}'")
                    })?;

            let inner = arrow_flight::flight_service_client::FlightServiceClient::new(channel)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX);
            let mut client = FlightSqlServiceClient::new_from_inner(inner);
            client.set_header("x-analyticsdb-user", session.user.clone());
            client.set_header("x-analyticsdb-role", session.role.clone());
            client.set_header("x-analyticsdb-database", session.database.clone());
            client.set_header("x-analyticsdb-schema", session.schema.clone());
            client.set_header("x-analyticsdb-auth-method", session.auth_method.clone());

            let handshake_payload = client
                .handshake(&session.user, password.as_deref().unwrap_or(""))
                .await
                .with_context(|| "Flight SQL handshake failed".to_string())?;
            if !handshake_payload.is_empty() {
                if let Ok(decision) =
                    serde_json::from_slice::<HandshakeAuthDecision>(&handshake_payload)
                {
                    client.set_header("x-analyticsdb-user", decision.user);
                    client.set_header("x-analyticsdb-role", decision.role);
                    client.set_header("x-analyticsdb-auth-method", decision.auth_method);
                }
            }

            let started_at = Instant::now();

            if !params.is_empty() {
                bail!("Flight SQL prepared statement binding through CLI is not yet fully integrated - consider using PostgreSQL protocol for parameterized queries");
            }

            if sql_returns_rows(&sql) {
                let info = client
                    .execute(sql.clone(), None)
                    .await
                    .with_context(|| "Remote query execution failed".to_string())?;
                let ticket = info
                    .endpoint
                    .first()
                    .and_then(|endpoint| endpoint.ticket.clone())
                    .ok_or_else(|| anyhow!("Flight SQL server did not return a ticket"))?;
                let fallback_columns = info
                    .try_decode_schema()
                    .with_context(|| "Flight SQL result schema could not be decoded".to_string())?
                    .fields()
                    .iter()
                    .map(|field| field.name().to_string())
                    .collect::<Vec<_>>();
                let stream = client
                    .do_get(ticket)
                    .await
                    .with_context(|| "Remote result retrieval failed".to_string())?;

                query_response_from_batch_stream(
                    "unavailable-via-flight-sql".to_string(),
                    "unavailable-via-flight-sql".to_string(),
                    session.clone(),
                    stream,
                    fallback_columns,
                    query_completed_message(),
                    started_at.elapsed().as_millis(),
                )
                .await
            } else {
                let affected = client
                    .execute_update(sql.clone(), None)
                    .await
                    .with_context(|| "Remote command execution failed".to_string())?;
                Ok(QueryResponse {
                    query_id: "unavailable-via-flight-sql".to_string(),
                    coordinator_node_id: "unavailable-via-flight-sql".to_string(),
                    session: session.clone(),
                    columns: Vec::new(),
                    rows: Vec::new(),
                    message: command_completed_message(affected as u64),
                    execution_time_ms: started_at.elapsed().as_millis(),
                })
            }
        }
        .await;

        match result {
            Ok(response) => return Ok(response),
            Err(e) => {
                eprintln!("Warning: Failed to query Flight SQL endpoint '{endpoint}': {e:#}");
                last_error = Some(e);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("No endpoints provided")))
}

async fn query_response_from_batch_stream<S, E>(
    query_id: String,
    coordinator_node_id: String,
    session: SessionContext,
    mut batches: S,
    fallback_columns: Vec<String>,
    message: String,
    execution_time_ms: u128,
) -> Result<QueryResponse>
where
    S: futures::Stream<Item = std::result::Result<RecordBatch, E>> + Unpin,
    E: std::fmt::Display,
{
    let mut columns = None;
    let mut rows = Vec::new();
    while let Some(batch) = batches.next().await {
        let batch = batch.map_err(|error| anyhow!("Remote result stream failed: {error}"))?;
        if columns.is_none() {
            columns = Some(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().to_string())
                    .collect::<Vec<_>>(),
            );
        }

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
        columns: columns.unwrap_or(fallback_columns),
        rows,
        message,
        execution_time_ms,
    })
}

fn parse_postgres_endpoint(endpoint: &str) -> Result<(String, u16)> {
    // Strip any URL scheme (e.g. https://, postgresql://) — PostgreSQL wire
    // protocol is not HTTP; the scheme is meaningless here.
    let bare = match endpoint.find("://") {
        Some(pos) => &endpoint[pos + 3..],
        None => endpoint,
    };
    let (host, port) = bare.rsplit_once(':').ok_or_else(|| {
        anyhow!("PostgreSQL endpoint must be in 'host:port' form, got: '{endpoint}'")
    })?;
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

async fn connect_flight_sql_endpoint(
    endpoint: &str,
    tls_ca_cert: &Option<String>,
    tls_domain: &Option<String>,
) -> Result<tonic::transport::Channel> {
    let mut endpoint_config = Endpoint::new(endpoint.to_string())
        .with_context(|| format!("invalid Flight SQL endpoint '{endpoint}'"))?;

    if endpoint.starts_with("https://") {
        let mut tls_config = ClientTlsConfig::new();

        if let Some(domain) = tls_domain {
            tls_config = tls_config.domain_name(domain);
        }

        if let Some(cert_path) = tls_ca_cert {
            let pem = std::fs::read(cert_path)
                .with_context(|| format!("failed to read TLS CA certificate '{cert_path}'"))?;
            tls_config = tls_config.ca_certificate(Certificate::from_pem(pem));
        } else {
            tls_config = tls_config.with_enabled_roots();
        }

        endpoint_config = endpoint_config.tls_config(tls_config)?;
    }

    endpoint_config.connect().await.map_err(Into::into)
}

fn query_completed_message() -> String {
    "Query executed successfully.".to_string()
}

fn command_completed_message(rows_affected: u64) -> String {
    format!("Command completed. {rows_affected} row(s) affected.")
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn sql_returns_rows(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    let upper = trimmed.to_ascii_uppercase();

    if select_into_is_command(&upper) {
        return false;
    }

    upper.starts_with("SELECT")
        || upper.starts_with("SHOW")
        || upper.starts_with("DESCRIBE")
        || upper.starts_with("EXPLAIN")
        || upper.starts_with("WITH")
}

fn select_into_is_command(upper_sql: &str) -> bool {
    if !upper_sql.starts_with("SELECT") {
        return false;
    }

    let keywords = top_level_keywords(upper_sql);
    let into_position = keywords
        .iter()
        .position(|keyword| keyword.as_str() == "INTO");
    let from_position = keywords
        .iter()
        .position(|keyword| keyword.as_str() == "FROM");

    match (into_position, from_position) {
        (Some(into), Some(from)) => into < from,
        (Some(_), None) => true,
        _ => false,
    }
}

fn top_level_keywords(sql: &str) -> Vec<String> {
    let mut keywords = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_single_quote {
            if ch == '\'' {
                if chars.peek() == Some(&'\'') {
                    chars.next();
                } else {
                    in_single_quote = false;
                }
            }
            continue;
        }

        if in_double_quote {
            if ch == '"' {
                in_double_quote = false;
            }
            continue;
        }

        match ch {
            '\'' => {
                flush_top_level_keyword(&mut keywords, &mut current, paren_depth);
                in_single_quote = true;
            }
            '"' => {
                flush_top_level_keyword(&mut keywords, &mut current, paren_depth);
                in_double_quote = true;
            }
            '(' => {
                flush_top_level_keyword(&mut keywords, &mut current, paren_depth);
                paren_depth += 1;
            }
            ')' => {
                flush_top_level_keyword(&mut keywords, &mut current, paren_depth);
                paren_depth = paren_depth.saturating_sub(1);
            }
            c if c.is_ascii_alphanumeric() || c == '_' => current.push(c),
            _ => flush_top_level_keyword(&mut keywords, &mut current, paren_depth),
        }
    }

    flush_top_level_keyword(&mut keywords, &mut current, paren_depth);
    keywords
}

fn flush_top_level_keyword(keywords: &mut Vec<String>, current: &mut String, paren_depth: usize) {
    if paren_depth == 0 && !current.is_empty() {
        keywords.push(std::mem::take(current));
    } else {
        current.clear();
    }
}

fn render_response(response: &QueryResponse, format: OutputFormat) -> String {
    match format {
        OutputFormat::Table => render_table(response),
        OutputFormat::Json => {
            serde_json::to_string_pretty(response).expect("response should serialize")
        }
        OutputFormat::Csv => render_csv(response),
    }
}

fn render_csv(response: &QueryResponse) -> String {
    let mut output = String::new();
    if !response.columns.is_empty() {
        output.push_str(&response.columns.join(","));
        output.push('\n');
        for row in &response.rows {
            let escaped: Vec<String> = row
                .iter()
                .map(|v| {
                    if v.contains(',') || v.contains('"') || v.contains('\n') {
                        format!("\"{}\"", v.replace('"', "\"\""))
                    } else {
                        v.clone()
                    }
                })
                .collect();
            output.push_str(&escaped.join(","));
            output.push('\n');
        }
    }
    output
}

fn render_table(response: &QueryResponse) -> String {
    let mut output = String::new();
    output.push_str(&format!("Query ID: {}\n", response.query_id));
    output.push_str(&format!("Coordinator: {}\n", response.coordinator_node_id));
    output.push_str(&format!(
        "Session: user={} database={} schema={}\n",
        response.session.user, response.session.database, response.session.schema
    ));
    output.push_str(&format!("Message: {}\n", response.message));
    output.push_str(&format!(
        "Execution Time: {} ms\n",
        response.execution_time_ms
    ));

    if response.columns.is_empty() {
        output.push_str("No columns returned.\n");
        return output;
    }

    let mut widths: Vec<usize> = response.columns.iter().map(|column| column.len()).collect();
    for row in &response.rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(value.len());
        }
    }

    let divider = build_divider(&widths);
    output.push_str(&format!("{divider}\n"));
    output.push_str(&format!(
        "| {} |\n",
        format_cells(&response.columns, &widths)
    ));
    output.push_str(&format!("{divider}\n"));

    for row in &response.rows {
        output.push_str(&format!("| {} |\n", format_cells(row, &widths)));
    }

    output.push_str(&format!("{divider}\n"));
    output.push_str(&format!("Rows: {}\n", response.row_count()));
    output
}

fn handle_meta_command(command: &str, options: &mut ClientOptions) -> Result<MetaCommandAction> {
    let trimmed = command.trim();
    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    match parts.as_slice() {
        ["\\timing"] => {
            options.timing = !options.timing;
            println!("Timing is {}.", if options.timing { "on" } else { "off" });
            Ok(MetaCommandAction::Continue)
        }
        ["\\timing", value] if value.eq_ignore_ascii_case("on") => {
            options.timing = true;
            println!("Timing is on.");
            Ok(MetaCommandAction::Continue)
        }
        ["\\timing", value] if value.eq_ignore_ascii_case("off") => {
            options.timing = false;
            println!("Timing is off.");
            Ok(MetaCommandAction::Continue)
        }
        ["\\q"] | ["\\quit"] => Ok(MetaCommandAction::Quit),
        ["\\?"] | ["\\help"] => {
            print_interactive_help();
            Ok(MetaCommandAction::Continue)
        }
        ["\\conninfo"] => {
            print_connection_info(options);
            Ok(MetaCommandAction::Continue)
        }
        ["\\d", table] => Ok(MetaCommandAction::ExecuteSql(format!(
            "SHOW COLUMNS FROM {table}"
        ))),
        ["\\dn"] => Ok(MetaCommandAction::ExecuteSql("SHOW SCHEMAS".to_string())),
        ["\\du"] => Ok(MetaCommandAction::ExecuteSql(
            "SELECT rolname, rolsuper, rolcreatedb, rolcanlogin FROM pg_roles ORDER BY rolname"
                .to_string(),
        )),
        ["\\set"] => {
            if options.variables.is_empty() {
                println!("No variables set.");
            } else {
                for (k, v) in &options.variables {
                    println!("{k}={v}");
                }
            }
            Ok(MetaCommandAction::Continue)
        }
        ["\\set", var, value] => {
            options.variables.insert(var.to_string(), value.to_string());
            Ok(MetaCommandAction::Continue)
        }
        ["\\set", var] => {
            options.variables.remove(*var);
            Ok(MetaCommandAction::Continue)
        }
        ["\\watch"] => {
            options.watch_interval = Some(2);
            println!("Watching last query every 2 seconds. Press Ctrl+C to stop.");
            Ok(MetaCommandAction::Continue)
        }
        ["\\watch", seconds_str] => {
            let seconds = seconds_str.parse().unwrap_or(2);
            options.watch_interval = Some(seconds);
            println!("Watching last query every {seconds} seconds. Press Ctrl+C to stop.");
            Ok(MetaCommandAction::Continue)
        }
        _ => {
            bail!(
                "unknown meta command '{}'. Type \\? for available commands.",
                command
            )
        }
    }
}

fn print_interactive_help() {
    println!("Meta commands:");
    println!("  \\q, \\quit      quit");
    println!("  \\?, \\help      show this help");
    println!("  \\conninfo      show current connection/session options");
    println!("  \\timing [on|off] toggle detailed timing output");
    println!("  \\d <table>     describe table (columns, types)");
    println!("  \\dn            list schemas");
    println!("  \\du            list users/roles");
    println!("  \\set [var] [value] set or list variables");
    println!("  \\watch [seconds] execute last query every N seconds (default 2)");
    println!();
    println!("SQL input:");
    println!("  End SQL statements with ';'. Multiline statements are supported.");
}

fn print_connection_info(options: &ClientOptions) {
    println!(
        "protocol={:?} user={} database={} schema={}",
        options.protocol, options.user, options.database, options.schema
    );
    match options.protocol {
        ClientProtocol::Embedded => println!("catalog_path={}", options.catalog_path),
        ClientProtocol::Postgres | ClientProtocol::FlightSql => {
            println!(
                "endpoint={}",
                options.endpoint.as_deref().unwrap_or("<default>")
            );
            if options.protocol == ClientProtocol::FlightSql {
                println!(
                    "tls_ca_cert={}",
                    options.tls_ca_cert.as_deref().unwrap_or("<default roots>")
                );
                println!(
                    "tls_domain={}",
                    options.tls_domain.as_deref().unwrap_or("<endpoint host>")
                );
            }
        }
    }
}

fn history_path() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .map(|home| home.join(".analyticsdb_history"))
}

fn sql_statement_is_complete(sql: &str) -> bool {
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut last_significant = None;
    let chars = sql.char_indices().collect::<Vec<_>>();
    let mut index = 0usize;

    while index < chars.len() {
        let (_, ch) = chars[index];
        if ch == '\'' && !in_double_quote {
            if in_single_quote && index + 1 < chars.len() && chars[index + 1].1 == '\'' {
                index += 1;
            } else {
                in_single_quote = !in_single_quote;
            }
        } else if ch == '"' && !in_single_quote {
            in_double_quote = !in_double_quote;
        }

        if !ch.is_whitespace() && !in_single_quote && !in_double_quote {
            last_significant = Some(ch);
        }

        index += 1;
    }

    last_significant == Some(';') && !in_single_quote && !in_double_quote
}

fn render_timing(
    response: &QueryResponse,
    client_query_ms: u128,
    render_ms: u128,
    end_to_end_ms: u128,
    format: OutputFormat,
) {
    let text = format!(
        "Timing: query/fetch={} ms, client_total={} ms, render={} ms, end_to_end={} ms",
        response.execution_time_ms, client_query_ms, render_ms, end_to_end_ms
    );
    match format {
        OutputFormat::Table => println!("{text}"),
        OutputFormat::Json | OutputFormat::Csv => eprintln!("{text}"),
    }
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
    Interactive(InteractiveOptions),
    Ca(CaOptions),
}

#[derive(Debug, Parser)]
struct CaOptions {
    #[command(subcommand)]
    command: CaCommands,
}

#[derive(Debug, Subcommand)]
enum CaCommands {
    /// Generate a self-signed CA and a leaf certificate signed by that CA.
    Init(CaInitOptions),
}

#[derive(Clone, Debug, Parser)]
struct CaInitOptions {
    /// Directory to write generated certificate files into.
    #[arg(long, default_value = "./certs")]
    output: String,
    /// Certificate validity in days.
    #[arg(long, default_value_t = 365)]
    validity_days: u32,
    /// Additional hostnames to include as Subject Alternative Names (repeatable).
    #[arg(long, default_value = "localhost", action = clap::ArgAction::Append)]
    hostname: Vec<String>,
}

#[derive(Clone, Debug, Parser)]
struct QueryOptions {
    #[arg(long)]
    sql: Option<String>,
    #[arg(long)]
    file: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,
    #[arg(long, value_enum, default_value_t = ClientProtocol::Embedded)]
    protocol: ClientProtocol,
    #[arg(long, default_value = "postgres")]
    database: String,
    #[arg(long)]
    role: Option<String>,
    #[arg(long)]
    password: Option<String>,
    #[arg(long, default_value = "public")]
    schema: String,
    #[arg(long, default_value = "postgres")]
    user: String,
    #[arg(long, default_value = "analyticsdb-catalog.db")]
    catalog_path: String,
    #[arg(long)]
    endpoint: Option<String>,
    #[arg(long)]
    tls_ca_cert: Option<String>,
    #[arg(long)]
    tls_domain: Option<String>,
    #[arg(long = "param", action = clap::ArgAction::Append)]
    params: Vec<String>,
    #[arg(long)]
    timing: bool,
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[derive(Clone, Debug, Parser)]
struct InteractiveOptions {
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,
    #[arg(long, value_enum, default_value_t = ClientProtocol::Embedded)]
    protocol: ClientProtocol,
    #[arg(long, default_value = "postgres")]
    database: String,
    #[arg(long)]
    role: Option<String>,
    #[arg(long)]
    password: Option<String>,
    #[arg(long, default_value = "public")]
    schema: String,
    #[arg(long, default_value = "postgres")]
    user: String,
    #[arg(long, default_value = "analyticsdb-catalog.db")]
    catalog_path: String,
    #[arg(long)]
    endpoint: Option<String>,
    #[arg(long)]
    tls_ca_cert: Option<String>,
    #[arg(long)]
    tls_domain: Option<String>,
    #[arg(long)]
    timing: bool,
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[derive(Clone, Debug)]
struct ClientOptions {
    format: OutputFormat,
    protocol: ClientProtocol,
    database: String,
    role: Option<String>,
    password: Option<String>,
    schema: String,
    user: String,
    catalog_path: String,
    endpoint: Option<String>,
    tls_ca_cert: Option<String>,
    tls_domain: Option<String>,
    timing: bool,
    output_file: Option<PathBuf>,
    variables: std::collections::HashMap<String, String>,
    watch_interval: Option<u64>,
    last_sql: Option<String>,
}

impl ClientOptions {
    fn from_query(options: QueryOptions) -> Self {
        Self {
            format: options.format,
            protocol: options.protocol,
            database: options.database,
            role: options.role,
            password: options.password,
            schema: options.schema,
            user: options.user,
            catalog_path: options.catalog_path,
            endpoint: options.endpoint,
            tls_ca_cert: options.tls_ca_cert,
            tls_domain: options.tls_domain,
            timing: options.timing,
            output_file: options.output,
            variables: std::collections::HashMap::new(),
            watch_interval: None,
            last_sql: None,
        }
    }

    fn from_interactive(options: InteractiveOptions) -> Self {
        Self {
            format: options.format,
            protocol: options.protocol,
            database: options.database,
            role: options.role,
            password: options.password,
            schema: options.schema,
            user: options.user,
            catalog_path: options.catalog_path,
            endpoint: options.endpoint,
            tls_ca_cert: options.tls_ca_cert,
            tls_domain: options.tls_domain,
            timing: options.timing,
            output_file: options.output,
            variables: std::collections::HashMap::new(),
            watch_interval: None,
            last_sql: None,
        }
    }
}

enum MetaCommandAction {
    Continue,
    Quit,
    ExecuteSql(String),
}

#[derive(Debug, Deserialize)]
struct HandshakeAuthDecision {
    user: String,
    role: String,
    auth_method: String,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum OutputFormat {
    Table,
    Json,
    Csv,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn interactive_sql_completion_requires_semicolon_outside_quotes() {
        assert!(sql_statement_is_complete("SELECT 1;"));
        assert!(sql_statement_is_complete("SELECT ';';"));
        assert!(sql_statement_is_complete(
            "INSERT INTO t VALUES ('it''s done');"
        ));
        assert!(!sql_statement_is_complete("SELECT 1"));
        assert!(!sql_statement_is_complete("SELECT ';'"));
        assert!(!sql_statement_is_complete("SELECT 'unterminated;"));
    }

    #[test]
    fn interactive_meta_commands_cover_quit_and_help() {
        let options = ClientOptions {
            format: OutputFormat::Table,
            protocol: ClientProtocol::Embedded,
            database: "postgres".to_string(),
            role: None,
            password: None,
            schema: "public".to_string(),
            user: "postgres".to_string(),
            catalog_path: "analyticsdb-catalog.db".to_string(),
            endpoint: None,
            tls_ca_cert: None,
            tls_domain: None,
            timing: false,
            output_file: None,
            variables: HashMap::new(),
            watch_interval: None,
            last_sql: None,
        };

        assert!(matches!(
            handle_meta_command("\\q", &mut options.clone()).expect("\\q should parse"),
            MetaCommandAction::Quit
        ));
        assert!(matches!(
            handle_meta_command("\\?", &mut options.clone()).expect("\\? should parse"),
            MetaCommandAction::Continue
        ));
        assert!(handle_meta_command("\\missing", &mut options.clone()).is_err());
    }

    #[test]
    fn interactive_timing_meta_command_toggles_output() {
        let mut options = ClientOptions {
            format: OutputFormat::Table,
            protocol: ClientProtocol::Embedded,
            database: "postgres".to_string(),
            role: None,
            password: None,
            schema: "public".to_string(),
            user: "postgres".to_string(),
            catalog_path: "analyticsdb-catalog.db".to_string(),
            endpoint: None,
            tls_ca_cert: None,
            tls_domain: None,
            timing: false,
            output_file: None,
            variables: HashMap::new(),
            watch_interval: None,
            last_sql: None,
        };

        assert!(matches!(
            handle_meta_command("\\timing", &mut options).expect("\\timing should parse"),
            MetaCommandAction::Continue
        ));
        assert!(options.timing);
        handle_meta_command("\\timing off", &mut options).expect("\\timing off should parse");
        assert!(!options.timing);
        handle_meta_command("\\timing on", &mut options).expect("\\timing on should parse");
        assert!(options.timing);
    }

    #[test]
    fn test_output_format_has_csv() {
        let formats = OutputFormat::value_variants();
        assert!(formats.contains(&OutputFormat::Csv));
    }

    #[test]
    fn test_meta_command_d_returns_execute_sql() {
        let mut options = ClientOptions {
            format: OutputFormat::Table,
            protocol: ClientProtocol::Embedded,
            database: "postgres".to_string(),
            role: None,
            password: None,
            schema: "public".to_string(),
            user: "postgres".to_string(),
            catalog_path: "analyticsdb-catalog.db".to_string(),
            endpoint: None,
            tls_ca_cert: None,
            tls_domain: None,
            timing: false,
            output_file: None,
            variables: HashMap::new(),
            watch_interval: None,
            last_sql: None,
        };
        match handle_meta_command("\\d mytable", &mut options).unwrap() {
            MetaCommandAction::ExecuteSql(sql) => {
                assert_eq!(sql, "SHOW COLUMNS FROM mytable");
            }
            _ => panic!("Expected ExecuteSql"),
        }
    }

    #[test]
    fn test_meta_command_dn_returns_show_schemas() {
        let mut options = ClientOptions {
            format: OutputFormat::Table,
            protocol: ClientProtocol::Embedded,
            database: "postgres".to_string(),
            role: None,
            password: None,
            schema: "public".to_string(),
            user: "postgres".to_string(),
            catalog_path: "analyticsdb-catalog.db".to_string(),
            endpoint: None,
            tls_ca_cert: None,
            tls_domain: None,
            timing: false,
            output_file: None,
            variables: HashMap::new(),
            watch_interval: None,
            last_sql: None,
        };
        match handle_meta_command("\\dn", &mut options).unwrap() {
            MetaCommandAction::ExecuteSql(sql) => {
                assert_eq!(sql, "SHOW SCHEMAS");
            }
            _ => panic!("Expected ExecuteSql"),
        }
    }

    #[test]
    fn test_render_csv() {
        let response = QueryResponse {
            query_id: "test".to_string(),
            coordinator_node_id: "test".to_string(),
            session: SessionContext {
                user: "postgres".to_string(),
                role: "postgres".to_string(),
                database: "postgres".to_string(),
                schema: "public".to_string(),
                auth_method: "embedded".to_string(),
                protocol: Protocol::Embedded,
                transaction_status: analyticsdb_core::TransactionStatus::Idle,
                statement_timeout_ms: 0,
                idle_in_transaction_timeout_ms: 0,
            },
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![
                vec!["1".to_string(), "Alice".to_string()],
                vec!["2".to_string(), "Bob".to_string()],
            ],
            message: "Query executed.".to_string(),
            execution_time_ms: 100,
        };
        let csv = render_csv(&response);
        assert!(csv.contains("id,name"));
        assert!(csv.contains("1,Alice"));
        assert!(csv.contains("2,Bob"));
    }
}
