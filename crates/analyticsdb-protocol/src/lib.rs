use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

use analyticsdb_control::CatalogRelationKind;
use analyticsdb_core::{Protocol, QueryRequest, SessionContext};
use analyticsdb_engine::{PrototypeEngine, QueryExecutionResult};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::server::FlightSqlService as ArrowFlightSqlService;
use arrow_flight::sql::server::PeekableFlightDataStream;
use arrow_flight::sql::CommandGetCatalogs;
use arrow_flight::sql::CommandGetDbSchemas;
use arrow_flight::sql::CommandGetSqlInfo;
use arrow_flight::sql::CommandGetTableTypes;
use arrow_flight::sql::CommandGetTables;
use arrow_flight::sql::CommandStatementQuery;
use arrow_flight::sql::CommandStatementUpdate;
use arrow_flight::sql::ProstMessageExt;
use arrow_flight::sql::SqlInfo;
use arrow_flight::sql::TicketStatementQuery;
use arrow_flight::FlightDescriptor;
use arrow_flight::FlightEndpoint;
use arrow_flight::FlightInfo;
use arrow_flight::Ticket;
use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::array::Float32Array;
use datafusion::arrow::array::Float64Array;
use datafusion::arrow::array::Int16Array;
use datafusion::arrow::array::Int32Array;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::array::LargeStringArray;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::array::UInt16Array;
use datafusion::arrow::array::UInt32Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::util::display::array_value_to_string;
use futures::stream;
use futures::Sink;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::StartupHandler;
use pgwire::api::portal::{Format as PgPortalFormat, Portal};
use pgwire::api::query::ExtendedQueryHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::DataRowEncoder;
use pgwire::api::results::DescribePortalResponse;
use pgwire::api::results::DescribeStatementResponse;
use pgwire::api::results::FieldFormat;
use pgwire::api::results::FieldInfo;
use pgwire::api::results::QueryResponse as PgQueryResponse;
use pgwire::api::results::Response as PgResponse;
use pgwire::api::results::Tag;
use pgwire::api::stmt::QueryParser;
use pgwire::api::stmt::StoredStatement;
use pgwire::api::store::PortalStore;
use pgwire::api::ClientInfo;
use pgwire::api::ClientPortalStore;
use pgwire::api::PgWireServerHandlers;
use pgwire::api::Type;
use pgwire::api::METADATA_DATABASE;
use pgwire::api::METADATA_USER;
use pgwire::error::PgWireError;
use pgwire::error::PgWireResult;
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::PgWireFrontendMessage;
use pgwire::tokio::process_socket;
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::task::spawn_blocking;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataMap;
use tonic::transport::Server;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

const FLIGHT_USER_HEADER: &str = "x-analyticsdb-user";
const FLIGHT_DATABASE_HEADER: &str = "x-analyticsdb-database";
const FLIGHT_SCHEMA_HEADER: &str = "x-analyticsdb-schema";

pub async fn serve_postgres_wire(
    listener: TcpListener,
    engine: Arc<PrototypeEngine>,
) -> anyhow::Result<()> {
    let query_parser = Arc::new(AnalyticsQueryParser {
        engine: Arc::clone(&engine),
    });
    let handler = Arc::new(AnalyticsPostgresHandler {
        engine,
        query_parser,
    });
    let factory = Arc::new(AnalyticsPostgresFactory {
        handler: Arc::clone(&handler),
    });

    loop {
        let (socket, _) = listener.accept().await?;
        let factory_ref = Arc::clone(&factory);
        tokio::spawn(async move {
            let _ = process_socket(socket, None, factory_ref).await;
        });
    }
}

pub async fn serve_flight_sql(
    listener: TcpListener,
    engine: Arc<PrototypeEngine>,
) -> anyhow::Result<()> {
    let service = AnalyticsFlightSqlService { engine };

    Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;

    Ok(())
}

struct AnalyticsPostgresFactory {
    handler: Arc<AnalyticsPostgresHandler>,
}

impl PgWireServerHandlers for AnalyticsPostgresFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::clone(&self.handler)
    }
}

struct AnalyticsPostgresHandler {
    engine: Arc<PrototypeEngine>,
    query_parser: Arc<AnalyticsQueryParser>,
}

#[derive(Debug, Clone)]
struct AnalyticsPreparedStatement {
    sql: String,
    parameter_types: Vec<Type>,
    result_schema: Vec<FieldInfo>,
}

struct AnalyticsQueryParser {
    engine: Arc<PrototypeEngine>,
}

#[async_trait]
impl NoopStartupHandler for AnalyticsPostgresHandler {
    async fn post_startup<C>(
        &self,
        client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if !client.metadata().contains_key(METADATA_DATABASE) {
            client
                .metadata_mut()
                .insert(METADATA_DATABASE.to_string(), "postgres".to_string());
        }

        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for AnalyticsPostgresHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<PgResponse>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        let execution = execute_postgres_sql(
            Arc::clone(&self.engine),
            QueryRequest {
                sql: query.to_string(),
                session: postgres_session_from_client(client),
            },
        )
        .await?;

        Ok(vec![query_execution_to_pg_response(
            execution, query, None,
        )?])
    }
}

#[async_trait]
impl QueryParser for AnalyticsQueryParser {
    type Statement = AnalyticsPreparedStatement;

    async fn parse_sql<C>(
        &self,
        client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let parameter_count = referenced_parameter_count(sql);
        let parameter_types = resolved_parameter_types(parameter_count, types);
        let result_schema = if sql_returns_rows(sql) {
            let described_sql = render_sql_with_default_parameters(sql, &parameter_types)?;
            let request = QueryRequest {
                sql: described_sql,
                session: postgres_session_from_client(client),
            };
            let execution = execute_postgres_sql(Arc::clone(&self.engine), request).await?;
            postgres_row_schema_from_arrow(&execution.schema, None)
        } else {
            Vec::new()
        };

        Ok(AnalyticsPreparedStatement {
            sql: sql.to_string(),
            parameter_types,
            result_schema,
        })
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        Ok(stmt.parameter_types.clone())
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        column_format: Option<&PgPortalFormat>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        Ok(stmt
            .result_schema
            .iter()
            .map(|field| {
                FieldInfo::new(
                    field.name().to_string(),
                    None,
                    None,
                    field.datatype().clone(),
                    field_format_from_pg_format(column_format, 0),
                )
            })
            .collect())
    }
}

#[async_trait]
impl ExtendedQueryHandler for AnalyticsPostgresHandler {
    type Statement = AnalyticsPreparedStatement;
    type QueryParser = AnalyticsQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::clone(&self.query_parser)
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<PgResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let rendered_sql = render_sql_with_portal_parameters(portal)?;
        let execution = execute_postgres_sql(
            Arc::clone(&self.engine),
            QueryRequest {
                sql: rendered_sql,
                session: postgres_session_from_client(client),
            },
        )
        .await?;

        let row_schema = if execution.schema.fields().is_empty() {
            None
        } else {
            Some(Arc::new(postgres_row_schema_from_arrow(
                &execution.schema,
                Some(&portal.result_column_format),
            )))
        };

        query_execution_to_pg_response(execution, &portal.statement.statement.sql, row_schema)
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        statement: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(DescribeStatementResponse::new(
            statement.statement.parameter_types.clone(),
            statement.statement.result_schema.clone(),
        ))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(DescribePortalResponse::new(
            portal.statement.statement.result_schema.clone(),
        ))
    }
}

fn postgres_session_from_client<C: ClientInfo>(client: &C) -> SessionContext {
    SessionContext {
        user: client
            .metadata()
            .get(METADATA_USER)
            .cloned()
            .unwrap_or_else(|| "postgres".to_string()),
        database: client
            .metadata()
            .get(METADATA_DATABASE)
            .cloned()
            .unwrap_or_else(|| "postgres".to_string()),
        schema: "public".to_string(),
        protocol: Protocol::PostgreSql,
    }
}

async fn execute_postgres_sql(
    engine: Arc<PrototypeEngine>,
    request: QueryRequest,
) -> PgWireResult<QueryExecutionResult> {
    spawn_blocking(move || engine.execute_query_batches(&request))
        .await
        .map_err(join_error_to_pgwire)?
        .map_err(anyhow_error_to_pgwire)
}

fn query_execution_to_pg_response(
    execution: QueryExecutionResult,
    sql: &str,
    row_schema: Option<Arc<Vec<FieldInfo>>>,
) -> PgWireResult<PgResponse> {
    if execution.schema.fields().is_empty() {
        let rows = rows_affected_from_message(&execution.message);
        return Ok(PgResponse::Execution(
            Tag::new(command_tag_for_sql(sql)).with_rows(rows),
        ));
    }

    let row_schema = row_schema
        .unwrap_or_else(|| Arc::new(postgres_row_schema_from_arrow(&execution.schema, None)));
    let rows = query_execution_to_pg_rows(execution, Arc::clone(&row_schema))?;

    Ok(PgResponse::Query(PgQueryResponse::new(row_schema, rows)))
}

fn query_execution_to_pg_rows(
    execution: QueryExecutionResult,
    row_schema: Arc<Vec<FieldInfo>>,
) -> PgWireResult<impl Stream<Item = PgWireResult<pgwire::messages::data::DataRow>> + Send + 'static>
{
    let mut encoded_rows = Vec::new();

    for batch in execution.batches {
        for row_index in 0..batch.num_rows() {
            let mut encoder = DataRowEncoder::new(Arc::clone(&row_schema));

            for column_index in 0..batch.num_columns() {
                encode_pg_row_value(&mut encoder, batch.column(column_index).as_ref(), row_index)?;
            }

            encoded_rows.push(Ok(encoder.take_row()));
        }
    }

    Ok(stream::iter(encoded_rows))
}

fn encode_pg_row_value(
    encoder: &mut DataRowEncoder,
    column: &dyn datafusion::arrow::array::Array,
    row_index: usize,
) -> PgWireResult<()> {
    if column.is_null(row_index) {
        encoder.encode_field(&None::<String>)?;
        return Ok(());
    }

    match column.data_type() {
        DataType::Boolean => {
            let array = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean array should downcast");
            encoder.encode_field(&array.value(row_index))?;
        }
        DataType::Float32 => {
            let array = column
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("float32 array should downcast");
            encoder.encode_field(&array.value(row_index))?;
        }
        DataType::Float64 => {
            let array = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("float64 array should downcast");
            encoder.encode_field(&array.value(row_index))?;
        }
        DataType::Int16 => {
            let array = column
                .as_any()
                .downcast_ref::<Int16Array>()
                .expect("int16 array should downcast");
            encoder.encode_field(&array.value(row_index))?;
        }
        DataType::Int32 => {
            let array = column
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("int32 array should downcast");
            encoder.encode_field(&array.value(row_index))?;
        }
        DataType::Int64 => {
            let array = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 array should downcast");
            encoder.encode_field(&array.value(row_index))?;
        }
        DataType::LargeUtf8 => {
            let array = column
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("large string array should downcast");
            encoder.encode_field(&array.value(row_index))?;
        }
        DataType::UInt16 => {
            let array = column
                .as_any()
                .downcast_ref::<UInt16Array>()
                .expect("uint16 array should downcast");
            let value = i32::from(array.value(row_index));
            encoder.encode_field(&value)?;
        }
        DataType::UInt32 => {
            let array = column
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("uint32 array should downcast");
            let value = i64::from(array.value(row_index));
            encoder.encode_field(&value)?;
        }
        DataType::UInt64 => {
            let value = array_value_to_string(column, row_index)
                .map_err(|error| anyhow_error_to_pgwire(anyhow::anyhow!(error)))?;
            encoder.encode_field(&value)?;
        }
        _ => {
            if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                encoder.encode_field(&array.value(row_index))?;
            } else {
                let value = array_value_to_string(column, row_index)
                    .map_err(|error| anyhow_error_to_pgwire(anyhow::anyhow!(error)))?;
                encoder.encode_field(&value)?;
            }
        }
    }

    Ok(())
}

fn postgres_row_schema_from_arrow(
    schema: &SchemaRef,
    format: Option<&PgPortalFormat>,
) -> Vec<FieldInfo> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            FieldInfo::new(
                field.name().to_string(),
                None,
                None,
                arrow_data_type_to_pg_type(field.data_type()),
                field_format_from_pg_format(format, index),
            )
        })
        .collect()
}

fn arrow_data_type_to_pg_type(data_type: &DataType) -> Type {
    match data_type {
        DataType::Boolean => Type::BOOL,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Int16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::UInt16 => Type::INT4,
        DataType::UInt32 => Type::INT8,
        DataType::UInt64 => Type::NUMERIC,
        _ => Type::TEXT,
    }
}

fn field_format_from_pg_format(format: Option<&PgPortalFormat>, index: usize) -> FieldFormat {
    match format {
        Some(format) if format.is_binary(index) => FieldFormat::Binary,
        _ => FieldFormat::Text,
    }
}

fn referenced_parameter_count(sql: &str) -> usize {
    let mut count = 0;
    let bytes = sql.as_bytes();
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] == b'$' {
            let start = index + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }

            if end > start {
                if let Ok(value) = sql[start..end].parse::<usize>() {
                    count = count.max(value);
                }
                index = end;
                continue;
            }
        }

        index += 1;
    }

    count
}

fn resolved_parameter_types(parameter_count: usize, types: &[Option<Type>]) -> Vec<Type> {
    (0..parameter_count)
        .map(|index| {
            types
                .get(index)
                .and_then(Clone::clone)
                .unwrap_or(Type::UNKNOWN)
        })
        .collect()
}

fn render_sql_with_default_parameters(sql: &str, parameter_types: &[Type]) -> PgWireResult<String> {
    render_sql_with_literals(sql, parameter_types.len(), |index| {
        default_parameter_literal(
            parameter_types
                .get(index - 1)
                .ok_or_else(|| unsupported_parameter_type_error("missing parameter type"))?,
        )
    })
}

fn render_sql_with_portal_parameters(
    portal: &Portal<AnalyticsPreparedStatement>,
) -> PgWireResult<String> {
    render_sql_with_literals(
        &portal.statement.statement.sql,
        portal.statement.statement.parameter_types.len(),
        |index| {
            parameter_literal_from_portal(
                portal,
                index - 1,
                portal
                    .statement
                    .statement
                    .parameter_types
                    .get(index - 1)
                    .ok_or_else(|| unsupported_parameter_type_error("missing parameter type"))?,
            )
        },
    )
}

fn render_sql_with_literals<F>(
    sql: &str,
    parameter_count: usize,
    mut literal_for_index: F,
) -> PgWireResult<String>
where
    F: FnMut(usize) -> PgWireResult<String>,
{
    let mut rendered = sql.to_string();

    // Replace from the highest placeholder index downward to avoid `$1`
    // accidentally rewriting the prefix of `$10`.
    for index in (1..=parameter_count).rev() {
        rendered = rendered.replace(&format!("${index}"), &literal_for_index(index)?);
    }

    Ok(rendered)
}

fn default_parameter_literal(parameter_type: &Type) -> PgWireResult<String> {
    match *parameter_type {
        Type::BOOL => Ok("FALSE".to_string()),
        Type::INT2 | Type::INT4 | Type::INT8 => Ok("0".to_string()),
        Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC => Ok("0".to_string()),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN => {
            Ok("''".to_string())
        }
        _ => Err(unsupported_parameter_type_error(parameter_type.name())),
    }
}

fn parameter_literal_from_portal(
    portal: &Portal<AnalyticsPreparedStatement>,
    index: usize,
    parameter_type: &Type,
) -> PgWireResult<String> {
    match *parameter_type {
        Type::BOOL => option_to_sql_literal(portal.parameter::<bool>(index, parameter_type)?),
        Type::INT2 => option_to_sql_literal(portal.parameter::<i16>(index, parameter_type)?),
        Type::INT4 => option_to_sql_literal(portal.parameter::<i32>(index, parameter_type)?),
        Type::INT8 => option_to_sql_literal(portal.parameter::<i64>(index, parameter_type)?),
        Type::FLOAT4 => option_to_sql_literal(portal.parameter::<f32>(index, parameter_type)?),
        Type::FLOAT8 | Type::NUMERIC => {
            option_to_sql_literal(portal.parameter::<f64>(index, parameter_type)?)
        }
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN => Ok(
            option_string_to_sql_literal(portal.parameter::<String>(index, parameter_type)?),
        ),
        _ => Err(unsupported_parameter_type_error(parameter_type.name())),
    }
}

fn option_to_sql_literal<T>(value: Option<T>) -> PgWireResult<String>
where
    T: ToString,
{
    Ok(value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "NULL".to_string()))
}

fn option_string_to_sql_literal(value: Option<String>) -> String {
    value
        .map(|value| format!("'{}'", value.replace('\'', "''")))
        .unwrap_or_else(|| "NULL".to_string())
}

fn unsupported_parameter_type_error(name: &str) -> PgWireError {
    PgWireError::ApiError(Box::new(std::io::Error::other(format!(
        "unsupported PostgreSQL parameter type in prototype extended-query path: {name}"
    ))))
}

fn sql_returns_rows(sql: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();

    upper.starts_with("SELECT")
        || upper.starts_with("SHOW")
        || upper.starts_with("DESCRIBE")
        || upper.starts_with("WITH")
}

fn command_tag_for_sql(sql: &str) -> &str {
    let trimmed = sql.trim();
    let upper = trimmed.to_ascii_uppercase();

    if upper.starts_with("CREATE DATABASE") {
        "CREATE DATABASE"
    } else if upper.starts_with("CREATE SCHEMA") {
        "CREATE SCHEMA"
    } else if upper.starts_with("CREATE VIEW") {
        "CREATE VIEW"
    } else if upper.starts_with("CREATE TABLE") {
        "CREATE TABLE"
    } else if upper.starts_with("INSERT") {
        "INSERT"
    } else {
        "OK"
    }
}

fn rows_affected_from_message(message: &str) -> usize {
    let mut digits = String::new();

    for character in message.chars() {
        if character.is_ascii_digit() {
            digits.push(character);
        } else if !digits.is_empty() {
            break;
        }
    }

    digits.parse::<usize>().unwrap_or(0)
}

fn anyhow_error_to_pgwire(error: anyhow::Error) -> PgWireError {
    PgWireError::ApiError(Box::new(std::io::Error::other(error.to_string())))
}

fn join_error_to_pgwire(error: tokio::task::JoinError) -> PgWireError {
    anyhow_error_to_pgwire(anyhow::anyhow!("join error while executing query: {error}"))
}

#[derive(Clone)]
struct AnalyticsFlightSqlService {
    engine: Arc<PrototypeEngine>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatementTicketPayload {
    sql: String,
    session: SessionContext,
}

#[async_trait]
impl ArrowFlightSqlService for AnalyticsFlightSqlService {
    type FlightService = Self;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<arrow_flight::HandshakeRequest>>,
    ) -> Result<
        Response<
            Pin<Box<dyn Stream<Item = Result<arrow_flight::HandshakeResponse, Status>> + Send>>,
        >,
        Status,
    > {
        Err(Status::unimplemented(
            "handshake is not implemented in the current prototype",
        ))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let session = flight_session_from_metadata(request.metadata());
        let execution = self
            .execute_batches(QueryRequest {
                sql: query.query.clone(),
                session: session.clone(),
            })
            .await?;

        let descriptor = request.into_inner();
        let ticket = statement_ticket(query.query, session)?;
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(descriptor)
            .try_with_schema(execution.schema.as_ref())
            .map_err(status_from_error)?;

        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let payload = decode_statement_ticket(ticket.statement_handle)?;
        let execution = self
            .execute_batches(QueryRequest {
                sql: payload.sql,
                session: payload.session,
            })
            .await?;

        let schema = Arc::clone(&execution.schema);
        let batches = if execution.batches.is_empty() {
            vec![RecordBatch::new_empty(Arc::clone(&schema))]
        } else {
            execution.batches
        };

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::iter(
                batches
                    .into_iter()
                    .map(Ok::<_, arrow_flight::error::FlightError>),
            ))
            .map_err(Status::from)
            .boxed();

        Ok(Response::new(stream))
    }

    async fn do_put_statement_update(
        &self,
        command: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let session = flight_session_from_metadata(request.metadata());
        let execution = self
            .execute_batches(QueryRequest {
                sql: command.query,
                session,
            })
            .await?;

        Ok(rows_affected_from_message(&execution.message) as i64)
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        });

        let info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(status_from_error)?
            .with_endpoint(endpoint)
            .with_descriptor(descriptor);

        Ok(Response::new(info))
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let session = flight_session_from_metadata(request.metadata());
        let databases = self
            .execute_blocking(move |engine| engine.list_databases(&session))
            .await?;

        let mut builder = query.into_builder();
        for database in databases {
            builder.append(&database);
        }

        Ok(Response::new(encoded_single_batch(
            builder.schema(),
            builder.build().map_err(status_from_error)?,
        )?))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        });

        let info = FlightInfo::new()
            .try_with_schema(&query.clone().into_builder().schema())
            .map_err(status_from_error)?
            .with_endpoint(endpoint)
            .with_descriptor(descriptor);

        Ok(Response::new(info))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let session = flight_session_from_metadata(request.metadata());
        let databases = if let Some(database) = query.catalog.clone() {
            vec![database]
        } else {
            let list_session = session.clone();
            self.execute_blocking(move |engine| engine.list_databases(&list_session))
                .await?
        };

        let mut builder = query.into_builder();
        for database in databases {
            let schema_session = flight_session_for_database(&session, &database);
            let database_for_list = database.clone();
            let schemas = self
                .execute_blocking(move |engine| {
                    engine.list_schemas(&schema_session, Some(&database_for_list))
                })
                .await?;

            for schema in schemas {
                builder.append(&database, &schema);
            }
        }

        Ok(Response::new(encoded_single_batch(
            builder.schema(),
            builder.build().map_err(status_from_error)?,
        )?))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        });

        let info = FlightInfo::new()
            .try_with_schema(&query.clone().into_builder().schema())
            .map_err(status_from_error)?
            .with_endpoint(endpoint)
            .with_descriptor(descriptor);

        Ok(Response::new(info))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let session = flight_session_from_metadata(request.metadata());
        let databases = if let Some(database) = query.catalog.clone() {
            vec![database]
        } else {
            let list_session = session.clone();
            self.execute_blocking(move |engine| engine.list_databases(&list_session))
                .await?
        };

        let mut builder = query.into_builder();
        for database in databases {
            let db_session = flight_session_for_database(&session, &database);
            let database_for_schemas = database.clone();
            let schemas = self
                .execute_blocking(move |engine| {
                    engine.list_schemas(&db_session, Some(&database_for_schemas))
                })
                .await?;

            for schema_name in schemas {
                let table_session = flight_session_for_database(&session, &database);
                let database_for_tables = database.clone();
                let schema_for_tables = schema_name.clone();
                let tables = self
                    .execute_blocking(move |engine| {
                        engine.list_relations(
                            &table_session,
                            Some(&database_for_tables),
                            Some(&schema_for_tables),
                            CatalogRelationKind::Table,
                        )
                    })
                    .await?;

                for table in tables {
                    let schema = catalog_relation_to_arrow_schema(&table.columns);
                    builder
                        .append(&database, &schema_name, &table.name, "TABLE", &schema)
                        .map_err(status_from_error)?;
                }

                let view_session = flight_session_for_database(&session, &database);
                let database_for_views = database.clone();
                let schema_for_views = schema_name.clone();
                let views = self
                    .execute_blocking(move |engine| {
                        engine.list_relations(
                            &view_session,
                            Some(&database_for_views),
                            Some(&schema_for_views),
                            CatalogRelationKind::View,
                        )
                    })
                    .await?;

                for view in views {
                    let schema = catalog_relation_to_arrow_schema(&view.columns);
                    builder
                        .append(&database, &schema_name, &view.name, "VIEW", &schema)
                        .map_err(status_from_error)?;
                }
            }
        }

        Ok(Response::new(encoded_single_batch(
            builder.schema(),
            builder.build().map_err(status_from_error)?,
        )?))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        });

        let info = FlightInfo::new()
            .try_with_schema(&Schema::new(vec![Field::new(
                "table_type",
                DataType::Utf8,
                false,
            )]))
            .map_err(status_from_error)?
            .with_endpoint(endpoint)
            .with_descriptor(descriptor);

        Ok(Response::new(info))
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let batch = RecordBatch::try_from_iter(vec![(
            "table_type",
            Arc::new(StringArray::from(vec!["TABLE", "VIEW"])) as ArrayRef,
        )])
        .map_err(status_from_error)?;

        Ok(Response::new(encoded_single_batch(batch.schema(), batch)?))
    }

    async fn get_flight_info_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "Flight SQL SqlInfo metadata is not implemented in the current prototype",
        ))
    }

    async fn do_get_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "Flight SQL SqlInfo metadata is not implemented in the current prototype",
        ))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

impl AnalyticsFlightSqlService {
    async fn execute_batches(&self, request: QueryRequest) -> Result<QueryExecutionResult, Status> {
        self.execute_blocking(move |engine| engine.execute_query_batches(&request))
            .await
    }

    async fn execute_blocking<T, F>(&self, f: F) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce(Arc<PrototypeEngine>) -> anyhow::Result<T> + Send + 'static,
    {
        let engine = Arc::clone(&self.engine);
        spawn_blocking(move || f(engine))
            .await
            .map_err(|error| {
                Status::internal(format!("join error while executing request: {error}"))
            })?
            .map_err(status_from_error)
    }
}

fn flight_session_from_metadata(metadata: &MetadataMap) -> SessionContext {
    SessionContext {
        user: metadata_value(metadata, FLIGHT_USER_HEADER)
            .unwrap_or_else(|| "postgres".to_string()),
        database: metadata_value(metadata, FLIGHT_DATABASE_HEADER)
            .unwrap_or_else(|| "postgres".to_string()),
        schema: metadata_value(metadata, FLIGHT_SCHEMA_HEADER)
            .unwrap_or_else(|| "public".to_string()),
        protocol: Protocol::ArrowFlightSql,
    }
}

fn flight_session_for_database(session: &SessionContext, database: &str) -> SessionContext {
    SessionContext {
        user: session.user.clone(),
        database: database.to_string(),
        schema: session.schema.clone(),
        protocol: Protocol::ArrowFlightSql,
    }
}

fn metadata_value(metadata: &MetadataMap, key: &str) -> Option<String> {
    metadata
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn statement_ticket(sql: String, session: SessionContext) -> Result<Ticket, Status> {
    let payload = StatementTicketPayload { sql, session };
    let bytes = serde_json::to_vec(&payload).map_err(status_from_error)?;
    let ticket = TicketStatementQuery {
        statement_handle: bytes.into(),
    };

    Ok(Ticket {
        ticket: ticket.as_any().encode_to_vec().into(),
    })
}

fn decode_statement_ticket(bytes: bytes::Bytes) -> Result<StatementTicketPayload, Status> {
    serde_json::from_slice(&bytes).map_err(status_from_error)
}

fn encoded_single_batch(
    schema: SchemaRef,
    batch: RecordBatch,
) -> Result<<AnalyticsFlightSqlService as FlightService>::DoGetStream, Status> {
    Ok(FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(stream::once(async {
            Ok::<RecordBatch, arrow_flight::error::FlightError>(batch)
        }))
        .map_err(Status::from)
        .boxed())
}

fn catalog_relation_to_arrow_schema(columns: &[analyticsdb_control::CatalogColumn]) -> Schema {
    Schema::new(
        columns
            .iter()
            .map(|column| {
                Field::new(
                    column.name.clone(),
                    catalog_type_to_arrow_data_type(&column.data_type),
                    column.nullable,
                )
            })
            .collect::<Vec<_>>(),
    )
}

fn catalog_type_to_arrow_data_type(data_type: &str) -> DataType {
    match data_type {
        "Boolean" => DataType::Boolean,
        "Float32" => DataType::Float32,
        "Float64" => DataType::Float64,
        "Int32" => DataType::Int32,
        "Int64" => DataType::Int64,
        "UInt32" => DataType::UInt32,
        "UInt64" => DataType::UInt64,
        _ => DataType::Utf8,
    }
}

fn status_from_error(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_flight::sql::client::FlightSqlServiceClient;
    use arrow_flight::sql::CommandGetTables;
    use futures::TryStreamExt;
    use tokio_postgres::types::Type as PgType;
    use tokio_postgres::NoTls;

    use super::*;

    fn temp_catalog_path(label: &str) -> String {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "analyticsdb-protocol-{label}-{}.json",
            uuid::Uuid::now_v7()
        ));
        path.to_string_lossy().into_owned()
    }

    #[tokio::test]
    async fn postgres_wire_executes_simple_queries() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(PrototypeEngine::new().expect("engine should initialize"));

        let server = tokio::spawn(serve_postgres_wire(listener, Arc::clone(&engine)));

        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres dbname=postgres",
                addr.port()
            ),
            NoTls,
        )
        .await
        .expect("postgres client should connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let messages = client
            .simple_query("SELECT 1 AS one, 2 AS two")
            .await
            .expect("query should succeed");

        let row = messages
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .expect("row should exist");

        assert_eq!(row.get("one"), Some("1"));
        assert_eq!(row.get("two"), Some("2"));

        server.abort();
    }

    #[tokio::test]
    async fn postgres_wire_executes_extended_queries_with_parameters() {
        let catalog_path = temp_catalog_path("postgres-extended");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(
            PrototypeEngine::from_catalog_path(&catalog_path).expect("engine should initialize"),
        );

        let server = tokio::spawn(serve_postgres_wire(listener, Arc::clone(&engine)));

        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres dbname=postgres",
                addr.port()
            ),
            NoTls,
        )
        .await
        .expect("postgres client should connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let create = client
            .prepare_typed(
                "CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT)",
                &[],
            )
            .await
            .expect("statement should prepare");
        client
            .execute(&create, &[])
            .await
            .expect("create table should succeed");

        let insert = client
            .prepare_typed(
                "INSERT INTO fact_metrics VALUES ($1, $2)",
                &[PgType::INT8, PgType::TEXT],
            )
            .await
            .expect("insert statement should prepare");
        let inserted = client
            .execute(&insert, &[&11_i64, &"ok"])
            .await
            .expect("insert should succeed");
        assert_eq!(inserted, 1);

        let select = client
            .prepare_typed(
                "SELECT metric, status FROM fact_metrics WHERE metric = $1",
                &[PgType::INT8],
            )
            .await
            .expect("select statement should prepare");
        let row = client
            .query_one(&select, &[&11_i64])
            .await
            .expect("query should succeed");

        assert_eq!(row.get::<_, i64>("metric"), 11);
        assert_eq!(row.get::<_, String>("status"), "ok");

        server.abort();
        let _ = std::fs::remove_file(&catalog_path);
        let _ = std::fs::remove_dir_all(format!(
            "/tmp/{}.managed",
            std::path::Path::new(&catalog_path)
                .file_stem()
                .and_then(|value| value.to_str())
                .expect("catalog path should have stem")
        ));
    }

    #[tokio::test]
    async fn flight_sql_executes_statement_queries_and_updates() {
        let catalog_path = temp_catalog_path("flight");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(
            PrototypeEngine::from_catalog_path(&catalog_path).expect("engine should initialize"),
        );

        let server = tokio::spawn(serve_flight_sql(listener, Arc::clone(&engine)));

        let channel = tonic::transport::Endpoint::new(format!("http://127.0.0.1:{}", addr.port()))
            .expect("endpoint should parse")
            .connect()
            .await
            .expect("channel should connect");
        let mut client = FlightSqlServiceClient::new(channel);
        client.set_header(FLIGHT_USER_HEADER, "postgres");
        client.set_header(FLIGHT_DATABASE_HEADER, "postgres");
        client.set_header(FLIGHT_SCHEMA_HEADER, "public");

        client
            .execute_update(
                "CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT)".to_string(),
                None,
            )
            .await
            .expect("create table should succeed");
        let inserted = client
            .execute_update(
                "INSERT INTO fact_metrics VALUES (11, 'ok')".to_string(),
                None,
            )
            .await
            .expect("insert should succeed");

        assert_eq!(inserted, 1);

        let info = client
            .execute("SELECT metric, status FROM fact_metrics".to_string(), None)
            .await
            .expect("select should succeed");
        let ticket = info
            .endpoint
            .first()
            .and_then(|endpoint| endpoint.ticket.clone())
            .expect("ticket should exist");
        let batches = client
            .do_get(ticket)
            .await
            .expect("do_get should succeed")
            .try_collect::<Vec<_>>()
            .await
            .expect("batches should collect");

        assert_eq!(batches.len(), 1);
        assert_eq!(
            array_value_to_string(batches[0].column(0).as_ref(), 0).expect("value"),
            "11"
        );
        assert_eq!(
            array_value_to_string(batches[0].column(1).as_ref(), 0).expect("value"),
            "ok"
        );

        server.abort();
        let _ = std::fs::remove_file(&catalog_path);
        let _ = std::fs::remove_dir_all(format!(
            "/tmp/{}.managed",
            std::path::Path::new(&catalog_path)
                .file_stem()
                .and_then(|value| value.to_str())
                .expect("catalog path should have stem")
        ));
    }

    #[tokio::test]
    async fn flight_sql_lists_tables() {
        let catalog_path = temp_catalog_path("tables");
        let engine = Arc::new(
            PrototypeEngine::from_catalog_path(&catalog_path).expect("engine should initialize"),
        );

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT)".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("table should be created");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let server = tokio::spawn(serve_flight_sql(listener, Arc::clone(&engine)));

        let channel = tonic::transport::Endpoint::new(format!("http://127.0.0.1:{}", addr.port()))
            .expect("endpoint should parse")
            .connect()
            .await
            .expect("channel should connect");
        let mut client = FlightSqlServiceClient::new(channel);
        client.set_header(FLIGHT_USER_HEADER, "postgres");
        client.set_header(FLIGHT_DATABASE_HEADER, "postgres");
        client.set_header(FLIGHT_SCHEMA_HEADER, "public");

        let info = client
            .get_tables(CommandGetTables {
                catalog: Some("postgres".to_string()),
                db_schema_filter_pattern: Some("public".to_string()),
                table_name_filter_pattern: None,
                table_types: vec!["TABLE".to_string()],
                include_schema: true,
            })
            .await
            .expect("get_tables should succeed");
        let ticket = info
            .endpoint
            .first()
            .and_then(|endpoint| endpoint.ticket.clone())
            .expect("ticket should exist");
        let batches = client
            .do_get(ticket)
            .await
            .expect("do_get should succeed")
            .try_collect::<Vec<_>>()
            .await
            .expect("batches should collect");

        let rendered = array_value_to_string(batches[0].column(2).as_ref(), 0).expect("value");
        assert_eq!(rendered, "fact_metrics");

        server.abort();
        let _ = std::fs::remove_file(&catalog_path);
        let _ = std::fs::remove_dir_all(format!(
            "/tmp/{}.managed",
            std::path::Path::new(&catalog_path)
                .file_stem()
                .and_then(|value| value.to_str())
                .expect("catalog path should have stem")
        ));
    }
}
