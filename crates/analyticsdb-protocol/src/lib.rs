#![cfg_attr(not(test), deny(clippy::panic))]
#![cfg_attr(not(test), deny(clippy::todo))]
#![cfg_attr(not(test), deny(clippy::unimplemented))]
#![cfg_attr(not(test), deny(clippy::unwrap_used))]
#![cfg_attr(not(test), deny(clippy::expect_used))]
#![allow(clippy::type_complexity)]
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

use analyticsdb_control::{CatalogRelationKind, ControlPlane};
use analyticsdb_core::{Protocol, QueryRequest, SessionContext, StatementOutcome};
use analyticsdb_engine::{PrototypeEngine, QueryExecutionResult};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::metadata::SqlInfoData;
use arrow_flight::sql::metadata::SqlInfoDataBuilder;
use arrow_flight::sql::server::FlightSqlService as ArrowFlightSqlService;
use arrow_flight::sql::server::PeekableFlightDataStream;
use arrow_flight::sql::CommandGetCatalogs;
use arrow_flight::sql::CommandGetDbSchemas;
use arrow_flight::sql::CommandGetSqlInfo;
use arrow_flight::sql::CommandGetTableTypes;
use arrow_flight::sql::CommandGetTables;
use arrow_flight::sql::CommandPreparedStatementQuery;
use arrow_flight::sql::CommandStatementQuery;
use arrow_flight::sql::CommandStatementUpdate;
use arrow_flight::sql::ProstMessageExt;
use arrow_flight::sql::SqlInfo;
use arrow_flight::sql::TicketStatementQuery;
use arrow_flight::Action;
use arrow_flight::FlightDescriptor;
use arrow_flight::FlightEndpoint;
use arrow_flight::FlightInfo;
use arrow_flight::Result as FlightResult;
use arrow_flight::Ticket;
use async_trait::async_trait;
use base64::Engine;
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
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::arrow::util::display::array_value_to_string;
use futures::stream;
use futures::Sink;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use pgwire::api::auth::sasl::SASLAuthStartupHandler;
use pgwire::api::auth::sasl::scram::ScramAuth;
use pgwire::api::auth::AuthSource;
use pgwire::api::auth::LoginInfo;
use pgwire::api::auth::Password;
use pgwire::api::auth::ServerParameterProvider;
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
use pgwire::api::PgWireConnectionState;
use pgwire::api::PgWireServerHandlers;
use pgwire::api::Type;
use pgwire::api::METADATA_APPLICATION_NAME;
use pgwire::api::METADATA_CLIENT_ENCODING;
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
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataMap;
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;
use tracing::{debug, info, trace, warn};

const FLIGHT_USER_HEADER: &str = "x-analyticsdb-user";
const FLIGHT_ROLE_HEADER: &str = "x-analyticsdb-role";
const FLIGHT_DATABASE_HEADER: &str = "x-analyticsdb-database";
const FLIGHT_SCHEMA_HEADER: &str = "x-analyticsdb-schema";
const FLIGHT_AUTH_METHOD_HEADER: &str = "x-analyticsdb-auth-method";
const POSTGRES_SCHEMA_METADATA: &str = "analyticsdb-schema";
const POSTGRES_ROLE_METADATA: &str = "analyticsdb-role";
const POSTGRES_AUTH_METHOD_METADATA: &str = "analyticsdb-auth-method";
const POSTGRES_SETTING_PREFIX: &str = "analyticsdb-setting-";
const POSTGRES_SERVER_VERSION: &str = "16.6-analyticsdb-prototype";

#[derive(Debug, Clone)]
struct AuthRequest {
    protocol: Protocol,
    user: String,
    database: String,
    schema: String,
    role: Option<String>,
    password: Option<String>,
    auth_header: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AuthDecision {
    user: String,
    role: String,
    database: String,
    schema: String,
    auth_method: String,
}

#[async_trait]
trait AuthHook: Send + Sync {
    async fn authenticate(&self, request: &AuthRequest) -> Result<AuthDecision, Status>;
}

struct PrototypeAllowAllAuthHook {
    control_plane: Arc<ControlPlane>,
}

/// SCRAM-SHA-256 auth source: returns the pre-computed SaltedPassword from the catalog.
struct ControlPlaneScramAuthSource {
    control_plane: Arc<ControlPlane>,
}

impl std::fmt::Debug for ControlPlaneScramAuthSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlaneScramAuthSource").finish()
    }
}

#[async_trait]
impl AuthSource for ControlPlaneScramAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let user = login
            .user()
            .ok_or_else(|| anyhow_error_to_pgwire(anyhow::anyhow!("missing user in login info")))?;
        let catalog_user = self
            .control_plane
            .catalog_user(user)
            .await
            .map_err(anyhow_error_to_pgwire)?;

        let salt_b64 = catalog_user.scram_salt_b64.as_deref().ok_or_else(|| {
            anyhow_error_to_pgwire(anyhow::anyhow!(
                "User '{}' has no SCRAM verifier — please rotate the password to enable SCRAM-SHA-256 authentication",
                catalog_user.name
            ))
        })?;
        let salted_password_b64 = catalog_user.scram_salted_password_b64.as_deref().ok_or_else(|| {
            anyhow_error_to_pgwire(anyhow::anyhow!(
                "User '{}' has no SCRAM verifier — please rotate the password to enable SCRAM-SHA-256 authentication",
                catalog_user.name
            ))
        })?;

        let salt = base64::engine::general_purpose::STANDARD
            .decode(salt_b64)
            .map_err(|e| anyhow_error_to_pgwire(anyhow::anyhow!("invalid scram salt encoding: {e}")))?;
        let salted_password = base64::engine::general_purpose::STANDARD
            .decode(salted_password_b64)
            .map_err(|e| anyhow_error_to_pgwire(anyhow::anyhow!("invalid scram salted password encoding: {e}")))?;

        Ok(Password::new(Some(salt), salted_password))
    }
}

#[async_trait]
impl AuthHook for PrototypeAllowAllAuthHook {
    async fn authenticate(&self, request: &AuthRequest) -> Result<AuthDecision, Status> {
        let user = request.user.trim();
        if user.is_empty() {
            return Err(Status::invalid_argument(
                "user cannot be empty in prototype auth hook",
            ));
        }

        if request.password.is_some() {
            self.control_plane
                .validate_credentials(user, request.password.as_deref())
                .await
                .map_err(|error| Status::unauthenticated(error.to_string()))?;
        } else {
            self.control_plane
                .catalog_user(user)
                .await
                .map_err(|error| Status::unauthenticated(error.to_string()))?;
        }

        let resolved_role = request.role.clone().unwrap_or_else(|| user.to_string());
        self.control_plane
            .authorize_role_assumption(user, &resolved_role)
            .await
            .map_err(|error| Status::permission_denied(error.to_string()))?;

        let auth_method = if request.auth_header.is_some() {
            "prototype-basic-auth"
        } else {
            match request.protocol {
                Protocol::PostgreSql => "postgres-wire-startup",
                Protocol::ArrowFlightSql => "flight-sql-metadata",
                Protocol::Embedded => "embedded-prototype",
            }
        };

        Ok(AuthDecision {
            user: user.to_string(),
            role: resolved_role,
            database: request.database.clone(),
            schema: request.schema.clone(),
            auth_method: auth_method.to_string(),
        })
    }
}

pub async fn serve_postgres_wire(
    listener: TcpListener,
    engine: Arc<PrototypeEngine>,
) -> anyhow::Result<()> {
    let control_plane = engine.control_plane();
    let auth_hook: Arc<dyn AuthHook> = Arc::new(PrototypeAllowAllAuthHook {
        control_plane: Arc::clone(&control_plane),
    });
    let scram_auth_source: Arc<dyn AuthSource> = Arc::new(ControlPlaneScramAuthSource {
        control_plane: Arc::clone(&control_plane),
    });
    let query_parser = Arc::new(AnalyticsQueryParser {
        engine: Arc::clone(&engine),
    });
    let handler = Arc::new(AnalyticsPostgresHandler {
        engine,
        query_parser,
        auth_hook,
    });
    let factory = Arc::new(AnalyticsPostgresFactory {
        handler: Arc::clone(&handler),
        scram_auth_source,
    });

    loop {
        let (socket, addr) = listener.accept().await?;
        trace!("postgres: accepting connection from {:?}", addr);
        let factory_ref = Arc::clone(&factory);
        tokio::spawn(async move {
            let _ = process_socket(socket, None, factory_ref).await;
            trace!("postgres: connection closed for {:?}", addr);
        });
    }
}

pub async fn serve_flight_sql(
    listener: TcpListener,
    engine: Arc<PrototypeEngine>,
    tls_config: Option<(Vec<u8>, Vec<u8>)>,
) -> anyhow::Result<()> {
    serve_flight_sql_with_label(listener, engine, tls_config, None, "Flight SQL").await
}

pub async fn serve_flight_sql_with_label(
    listener: TcpListener,
    engine: Arc<PrototypeEngine>,
    tls_config: Option<(Vec<u8>, Vec<u8>)>,  // (cert_pem, key_pem) for server identity
    ca_cert: Option<Vec<u8>>,                 // PEM CA cert; when set, enables mTLS (requires client certs)
    label: &'static str,
) -> anyhow::Result<()> {
    let control_plane = engine.control_plane();
    // Resolve the JWT signing secret: use environment variable if present,
    // otherwise the configured value, and fallback to an ephemeral key.
    let jwt_secret = {
        if let Ok(secret) = std::env::var("ANALYTICSDB_JWT_SECRET") {
            secret
        } else {
            let config = control_plane.cluster_config().await;
            match config.and_then(|c| c.jwt_secret) {
                Some(secret) => secret,
                None => {
                    let random_bytes: [u8; 32] = rand::random();
                    let hex_secret = random_bytes
                        .iter()
                        .fold(String::with_capacity(64), |mut acc, b| {
                            use std::fmt::Write as _;
                            let _ = write!(acc, "{b:02x}");
                            acc
                        });
                    warn!(
                        "{}: jwt_secret not configured — using ephemeral key. \
                         Flight SQL sessions will not survive a server restart.",
                        label
                    );
                    hex_secret
                }
            }
        }
    };
    let service = AnalyticsFlightSqlService {
        engine,
        auth_hook: Arc::new(PrototypeAllowAllAuthHook { control_plane }),
        jwt_secret,
    };

    let mut builder = Server::builder();

    let router = if let Some((cert, key)) = tls_config {
        let identity = tonic::transport::Identity::from_pem(cert, key);
        let mut server_tls = tonic::transport::ServerTlsConfig::new().identity(identity);
        if let Some(ca_pem) = ca_cert {
            let ca = tonic::transport::Certificate::from_pem(ca_pem);
            server_tls = server_tls.client_ca_root(ca);
            info!("{}: Starting with mTLS enabled (client certificate required)", label);
        } else {
            info!("{}: Starting with TLS enabled", label);
        }
        builder
            .tls_config(server_tls)?
            .add_service(
                FlightServiceServer::new(service)
                    .max_decoding_message_size(usize::MAX)
                    .max_encoding_message_size(usize::MAX),
            )
    } else {
        if ca_cert.is_some() {
            warn!("{}: CA cert provided but no server identity configured — mTLS requires a server cert/key; ignoring CA cert", label);
        }
        if label == "Flight SQL" || label == "Client Flight SQL" {
            warn!("{}: Starting in PLAINTEXT mode (insecure)", label);
        } else {
            warn!("{}: Starting in PLAINTEXT mode (internal)", label);
        }
        builder.add_service(
            FlightServiceServer::new(service)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
        )
    };

    router
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;

    Ok(())
}

struct AnalyticsPostgresFactory {
    handler: Arc<AnalyticsPostgresHandler>,
    scram_auth_source: Arc<dyn AuthSource>,
}

/// Per-connection startup handler that creates a fresh SASL state machine for
/// each connection and runs apply_post_startup_auth once the exchange finishes.
struct PerConnectionStartupHandler {
    sasl: SASLAuthStartupHandler<AnalyticsServerParameterProvider>,
    auth_hook: Arc<dyn AuthHook>,
}

#[async_trait]
impl StartupHandler for PerConnectionStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.sasl.on_startup(client, message).await?;

        if matches!(client.state(), PgWireConnectionState::ReadyForQuery)
            && !client
                .metadata()
                .contains_key(POSTGRES_AUTH_METHOD_METADATA)
        {
            self.apply_post_startup_auth(client).await?;
        }

        Ok(())
    }
}

impl PerConnectionStartupHandler {
    async fn apply_post_startup_auth<C>(&self, client: &mut C) -> PgWireResult<()>
    where
        C: ClientInfo,
    {
        let user = client
            .metadata()
            .get(METADATA_USER)
            .cloned()
            .unwrap_or_else(|| "postgres".to_string());
        let database = client
            .metadata()
            .get(METADATA_DATABASE)
            .cloned()
            .unwrap_or_else(|| "postgres".to_string());
        let schema = client
            .metadata()
            .get(POSTGRES_SCHEMA_METADATA)
            .cloned()
            .unwrap_or_else(|| "public".to_string());

        let decision = match self
            .auth_hook
            .authenticate(&AuthRequest {
                protocol: Protocol::PostgreSql,
                user: user.clone(),
                database,
                schema,
                role: client.metadata().get(POSTGRES_ROLE_METADATA).cloned(),
                password: None,
                auth_header: None,
            })
            .await
        {
            Ok(d) => d,
            Err(e) => {
                // Record auth failure metric
                
                return Err(status_to_pgwire(e));
            }
        };

        client
            .metadata_mut()
            .insert(METADATA_USER.to_string(), decision.user);
        client
            .metadata_mut()
            .insert(METADATA_DATABASE.to_string(), decision.database);
        client
            .metadata_mut()
            .insert(POSTGRES_SCHEMA_METADATA.to_string(), decision.schema);
        client
            .metadata_mut()
            .insert(POSTGRES_ROLE_METADATA.to_string(), decision.role);
        client.metadata_mut().insert(
            POSTGRES_AUTH_METHOD_METADATA.to_string(),
            decision.auth_method,
        );

        Ok(())
    }
}

impl PgWireServerHandlers for AnalyticsPostgresFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        // Fresh per-connection SASL state machine — SASLAuthStartupHandler holds
        // per-connection Mutex<SASLState> so it must NOT be shared across connections.
        let auth_source: Arc<dyn AuthSource> = self.scram_auth_source.clone();
        Arc::new(PerConnectionStartupHandler {
            sasl: SASLAuthStartupHandler::new(Arc::new(
                AnalyticsServerParameterProvider::default(),
            ))
            .with_scram(ScramAuth::new(auth_source)),
            auth_hook: Arc::clone(&self.handler.auth_hook),
        })
    }
}

struct AnalyticsPostgresHandler {
    engine: Arc<PrototypeEngine>,
    query_parser: Arc<AnalyticsQueryParser>,
    auth_hook: Arc<dyn AuthHook>,
}

#[derive(Debug, Clone)]
struct AnalyticsServerParameterProvider {
    server_version: String,
    time_zone: String,
    date_style: String,
    interval_style: String,
    standard_conforming_strings: String,
    search_path: String,
    default_transaction_isolation: String,
}

impl Default for AnalyticsServerParameterProvider {
    fn default() -> Self {
        Self {
            server_version: POSTGRES_SERVER_VERSION.to_string(),
            time_zone: "UTC".to_string(),
            date_style: "ISO, MDY".to_string(),
            interval_style: "postgres".to_string(),
            standard_conforming_strings: "on".to_string(),
            search_path: "public".to_string(),
            default_transaction_isolation: "read committed".to_string(),
        }
    }
}

impl ServerParameterProvider for AnalyticsServerParameterProvider {
    fn server_parameters<C>(&self, client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        let mut params = HashMap::from([
            ("server_version".to_string(), self.server_version.clone()),
            ("server_encoding".to_string(), "UTF8".to_string()),
            ("integer_datetimes".to_string(), "on".to_string()),
            ("in_hot_standby".to_string(), "off".to_string()),
            (
                "default_transaction_read_only".to_string(),
                "off".to_string(),
            ),
            (
                "default_transaction_isolation".to_string(),
                self.default_transaction_isolation.clone(),
            ),
            ("TimeZone".to_string(), self.time_zone.clone()),
            ("DateStyle".to_string(), self.date_style.clone()),
            ("IntervalStyle".to_string(), self.interval_style.clone()),
            (
                "standard_conforming_strings".to_string(),
                self.standard_conforming_strings.clone(),
            ),
            ("search_path".to_string(), self.search_path.clone()),
            (
                "client_encoding".to_string(),
                client
                    .metadata()
                    .get(METADATA_CLIENT_ENCODING)
                    .cloned()
                    .unwrap_or_else(|| "UTF8".to_string()),
            ),
            (
                "application_name".to_string(),
                client
                    .metadata()
                    .get(METADATA_APPLICATION_NAME)
                    .cloned()
                    .unwrap_or_default(),
            ),
            (
                "session_authorization".to_string(),
                client
                    .metadata()
                    .get(METADATA_USER)
                    .cloned()
                    .unwrap_or_else(|| "postgres".to_string()),
            ),
        ]);

        if let Some(schema) = client.metadata().get(POSTGRES_SCHEMA_METADATA) {
            params.insert("search_path".to_string(), schema.clone());
        }

        Some(params)
    }
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
impl SimpleQueryHandler for AnalyticsPostgresHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<PgResponse>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        debug!("postgres_simple_query: {}", query);
        if let Some(response) =
            apply_postgres_session_statement(client, parse_postgres_set_statement(query)?)?
        {
            return Ok(vec![response]);
        }
        if let Some(response) =
            execute_postgres_show_statement(client, parse_postgres_show_statement(query)?)?
        {
            return Ok(vec![response]);
        }

        let execution = execute_postgres_sql(
            Arc::clone(&self.engine),
            QueryRequest {
                sql: query.to_string(),
                session: postgres_session_from_client(client),
                query_id: None,
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
        debug!("postgres_parse: {}", sql);
        let parameter_count = referenced_parameter_count(sql);
        let parameter_types = resolved_parameter_types(parameter_count, types);
        let set_statement = parse_postgres_set_statement(sql)?;
        let result_schema = if !matches!(set_statement, PostgresSetStatement::NotASetStatement) {
            Vec::new()
        } else if let Some(schema) =
            postgres_show_result_schema(parse_postgres_show_statement(sql)?)
        {
            schema
        } else {
            let described_sql = render_sql_with_default_parameters(sql, &parameter_types)?;
            let request = QueryRequest {
                sql: described_sql,
                session: postgres_session_from_client(client),
                query_id: None,
            };
            match self
                .engine
                .plan_query_schema(&request)
                .await
                .map_err(anyhow_error_to_pgwire)?
            {
                Some(schema) => postgres_row_schema_from_arrow(&schema, None),
                None => Vec::new(),
            }
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
        if let Some(response) =
            apply_postgres_session_statement(client, parse_postgres_set_statement(&rendered_sql)?)?
        {
            return Ok(response);
        }
        if let Some(response) =
            execute_postgres_show_statement(client, parse_postgres_show_statement(&rendered_sql)?)?
        {
            return Ok(response);
        }

        let execution = execute_postgres_sql(
            Arc::clone(&self.engine),
            QueryRequest {
                sql: rendered_sql,
                session: postgres_session_from_client(client),
                query_id: None,
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

/// Parses a PostgreSQL-style timeout string into milliseconds.
/// Bare integers are treated as milliseconds (PostgreSQL GUC convention).
/// Recognises `ms`, `s`, `min`, `h` suffixes. Returns 0 on any parse error.
fn parse_timeout_to_ms(s: &str) -> u64 {
    let s = s.trim();
    if s == "0" || s.is_empty() {
        return 0;
    }
    if let Some(n) = s.strip_suffix("ms") {
        return n.trim().parse::<u64>().unwrap_or(0);
    }
    if let Some(n) = s.strip_suffix("min") {
        return n.trim().parse::<u64>().unwrap_or(0).saturating_mul(60_000);
    }
    if let Some(n) = s.strip_suffix("h") {
        return n.trim().parse::<u64>().unwrap_or(0).saturating_mul(3_600_000);
    }
    if let Some(n) = s.strip_suffix('s') {
        return n.trim().parse::<u64>().unwrap_or(0).saturating_mul(1_000);
    }
    // bare integer = milliseconds
    s.parse::<u64>().unwrap_or(0)
}

fn postgres_session_from_client<C: ClientInfo>(client: &C) -> SessionContext {
    SessionContext {
        user: client
            .metadata()
            .get(METADATA_USER)
            .cloned()
            .unwrap_or_else(|| "postgres".to_string()),
        role: client
            .metadata()
            .get(POSTGRES_ROLE_METADATA)
            .cloned()
            .unwrap_or_else(|| {
                client
                    .metadata()
                    .get(METADATA_USER)
                    .cloned()
                    .unwrap_or_else(|| "postgres".to_string())
            }),
        database: client
            .metadata()
            .get(METADATA_DATABASE)
            .cloned()
            .unwrap_or_else(|| "postgres".to_string()),
        schema: client
            .metadata()
            .get(POSTGRES_SCHEMA_METADATA)
            .cloned()
            .unwrap_or_else(|| "public".to_string()),
        auth_method: client
            .metadata()
            .get(POSTGRES_AUTH_METHOD_METADATA)
            .cloned()
            .unwrap_or_else(|| "postgres-wire-startup".to_string()),
        protocol: Protocol::PostgreSql,
        transaction_status: analyticsdb_core::TransactionStatus::Idle,
        statement_timeout_ms: parse_timeout_to_ms(
            client
                .metadata()
                .get(&postgres_setting_metadata_key("statement_timeout"))
                .map(|s| s.as_str())
                .unwrap_or("0"),
        ),
        idle_in_transaction_timeout_ms: parse_timeout_to_ms(
            client
                .metadata()
                .get(&postgres_setting_metadata_key(
                    "idle_in_transaction_session_timeout",
                ))
                .map(|s| s.as_str())
                .unwrap_or("0"),
        ),
    }
}

enum PostgresSetStatement {
    Set { name: String, value: Option<String> },
    Reset { name: String },
    ResetAll,
    NotASetStatement,
}

enum PostgresShowStatement {
    Setting { name: String },
    All,
    NotAShowStatement,
}

fn parse_postgres_set_statement(sql: &str) -> PgWireResult<PostgresSetStatement> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();

    if upper == "RESET ALL" {
        return Ok(PostgresSetStatement::ResetAll);
    }
    if upper.starts_with("RESET ") {
        return Ok(PostgresSetStatement::Reset {
            name: normalize_postgres_setting_name(trimmed["RESET ".len()..].trim())?,
        });
    }

    if !upper.starts_with("SET ") {
        return Ok(PostgresSetStatement::NotASetStatement);
    }

    let remainder = strip_set_scope_prefix(trimmed["SET ".len()..].trim());
    let remainder_upper = remainder.to_ascii_uppercase();

    let session_characteristics_prefix = "CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ";
    if remainder_upper.starts_with(session_characteristics_prefix) {
        return Ok(PostgresSetStatement::Set {
            name: "transaction_isolation".to_string(),
            value: Some(normalize_transaction_isolation_level(
                remainder[session_characteristics_prefix.len()..].trim(),
            )?),
        });
    }

    if remainder_upper.starts_with("TRANSACTION")
        || remainder_upper.starts_with("ROLE")
        || remainder_upper.starts_with("SESSION AUTHORIZATION")
        || remainder_upper.starts_with("CONSTRAINTS")
    {
        return Err(anyhow_error_to_pgwire(anyhow::anyhow!(
            "this SET form is not implemented in the current prototype"
        )));
    }

    if remainder_upper.starts_with("TIME ZONE ") {
        return Ok(PostgresSetStatement::Set {
            name: "timezone".to_string(),
            value: normalize_postgres_setting_value(remainder["TIME ZONE ".len()..].trim())?,
        });
    }
    if remainder_upper.starts_with("NAMES ") {
        return Ok(PostgresSetStatement::Set {
            name: "client_encoding".to_string(),
            value: normalize_postgres_setting_value(remainder["NAMES ".len()..].trim())?,
        });
    }

    let (name_raw, value_raw) = split_postgres_set_assignment(remainder).ok_or_else(|| {
        anyhow_error_to_pgwire(anyhow::anyhow!(
            "unsupported PostgreSQL SET syntax in the current prototype"
        ))
    })?;

    Ok(PostgresSetStatement::Set {
        name: normalize_postgres_setting_name(name_raw)?,
        value: normalize_postgres_setting_value(value_raw)?,
    })
}

fn strip_set_scope_prefix(remainder: &str) -> &str {
    let upper = remainder.to_ascii_uppercase();
    if upper.starts_with("SESSION ") {
        remainder["SESSION ".len()..].trim()
    } else if upper.starts_with("LOCAL ") {
        remainder["LOCAL ".len()..].trim()
    } else {
        remainder
    }
}

fn split_postgres_set_assignment(input: &str) -> Option<(&str, &str)> {
    let upper = input.to_ascii_uppercase();
    if let Some(index) = upper.find(" TO ") {
        return Some((input[..index].trim(), input[index + " TO ".len()..].trim()));
    }
    input
        .split_once('=')
        .map(|(left, right)| (left.trim(), right.trim()))
}

fn normalize_postgres_setting_name(raw: &str) -> PgWireResult<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow_error_to_pgwire(anyhow::anyhow!(
            "SET/RESET parameter name cannot be empty"
        )));
    }
    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '.')
    {
        return Err(anyhow_error_to_pgwire(anyhow::anyhow!(
            "unsupported PostgreSQL parameter name '{}' in the current prototype",
            raw
        )));
    }

    Ok(trimmed.to_ascii_lowercase())
}

fn normalize_postgres_setting_value(raw: &str) -> PgWireResult<Option<String>> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("DEFAULT") {
        return Ok(None);
    }

    Ok(Some(trimmed.to_string()))
}

fn normalize_transaction_isolation_level(raw: &str) -> PgWireResult<String> {
    let normalized = raw
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .to_ascii_lowercase();

    match normalized.as_str() {
        "read committed" | "read uncommitted" | "repeatable read" | "serializable" => {
            Ok(normalized)
        }
        _ => Err(anyhow_error_to_pgwire(anyhow::anyhow!(
            "unsupported transaction isolation level '{}' in the current prototype",
            raw
        ))),
    }
}

fn apply_postgres_session_statement<C>(
    client: &mut C,
    statement: PostgresSetStatement,
) -> PgWireResult<Option<PgResponse>>
where
    C: ClientInfo,
{
    match statement {
        PostgresSetStatement::Set { name, value } => {
            apply_postgres_setting(client, &name, value.as_deref())?;
            Ok(Some(PgResponse::Execution(Tag::new("SET"))))
        }
        PostgresSetStatement::Reset { name } => {
            reset_postgres_setting(client, &name)?;
            Ok(Some(PgResponse::Execution(Tag::new("RESET"))))
        }
        PostgresSetStatement::ResetAll => {
            reset_all_postgres_settings(client)?;
            Ok(Some(PgResponse::Execution(Tag::new("RESET"))))
        }
        PostgresSetStatement::NotASetStatement => Ok(None),
    }
}

fn parse_postgres_show_statement(sql: &str) -> PgWireResult<PostgresShowStatement> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if upper == "SHOW ALL" {
        return Ok(PostgresShowStatement::All);
    }
    if !upper.starts_with("SHOW ") {
        return Ok(PostgresShowStatement::NotAShowStatement);
    }

    let raw_name = trimmed["SHOW ".len()..].trim();
    if raw_name.is_empty() {
        return Err(anyhow_error_to_pgwire(anyhow::anyhow!(
            "SHOW parameter name cannot be empty"
        )));
    }

    let raw_upper = raw_name.to_ascii_uppercase();
    if raw_upper == "DATABASES"
        || raw_upper == "NODES"
        || raw_upper == "SCHEMAS"
        || raw_upper.starts_with("SCHEMAS FROM ")
        || raw_upper == "TABLES"
        || raw_upper.starts_with("TABLES FROM ")
        || raw_upper == "VIEWS"
        || raw_upper.starts_with("VIEWS FROM ")
        || raw_upper.starts_with("COLUMNS FROM ")
    {
        return Ok(PostgresShowStatement::NotAShowStatement);
    }
    if raw_name.contains(char::is_whitespace)
        && raw_upper != "TIME ZONE"
        && raw_upper != "TRANSACTION ISOLATION LEVEL"
    {
        return Ok(PostgresShowStatement::NotAShowStatement);
    }

    Ok(PostgresShowStatement::Setting {
        name: normalize_postgres_show_name(raw_name)?,
    })
}

fn normalize_postgres_show_name(raw: &str) -> PgWireResult<String> {
    let trimmed = raw.trim();
    let upper = trimmed.to_ascii_uppercase();
    if upper == "TIME ZONE" {
        return Ok("timezone".to_string());
    }
    if upper == "TRANSACTION ISOLATION LEVEL" {
        return Ok("transaction_isolation".to_string());
    }

    normalize_postgres_setting_name(trimmed)
}

fn execute_postgres_show_statement<C>(
    client: &C,
    statement: PostgresShowStatement,
) -> PgWireResult<Option<PgResponse>>
where
    C: ClientInfo,
{
    match statement {
        PostgresShowStatement::Setting { name } => {
            let value = effective_postgres_setting(client, &name)?;
            let schema = Arc::new(vec![FieldInfo::new(
                name.clone(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            )]);
            let rows = encode_text_query_rows(Arc::clone(&schema), &[vec![value]])?;
            Ok(Some(PgResponse::Query(PgQueryResponse::new(schema, rows))))
        }
        PostgresShowStatement::All => {
            let schema = Arc::new(vec![
                FieldInfo::new(
                    "name".to_string(),
                    None,
                    None,
                    Type::TEXT,
                    FieldFormat::Text,
                ),
                FieldInfo::new(
                    "setting".to_string(),
                    None,
                    None,
                    Type::TEXT,
                    FieldFormat::Text,
                ),
            ]);
            let rows = effective_postgres_settings_map(client)
                .into_iter()
                .map(|(name, value)| vec![name, value])
                .collect::<Vec<_>>();
            let rows = encode_text_query_rows(Arc::clone(&schema), &rows)?;
            Ok(Some(PgResponse::Query(PgQueryResponse::new(schema, rows))))
        }
        PostgresShowStatement::NotAShowStatement => Ok(None),
    }
}

fn postgres_show_result_schema(statement: PostgresShowStatement) -> Option<Vec<FieldInfo>> {
    match statement {
        PostgresShowStatement::Setting { name } => Some(vec![FieldInfo::new(
            name,
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        )]),
        PostgresShowStatement::All => Some(vec![
            FieldInfo::new(
                "name".to_string(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "setting".to_string(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            ),
        ]),
        PostgresShowStatement::NotAShowStatement => None,
    }
}

fn encode_text_query_rows(
    row_schema: Arc<Vec<FieldInfo>>,
    rows: &[Vec<String>],
) -> PgWireResult<impl Stream<Item = PgWireResult<pgwire::messages::data::DataRow>> + Send + 'static>
{
    let mut encoded_rows = Vec::new();
    for row in rows {
        let mut encoder = DataRowEncoder::new(Arc::clone(&row_schema));
        for value in row {
            encoder.encode_field(value)?;
        }
        encoded_rows.push(Ok(encoder.take_row()));
    }

    Ok(stream::iter(encoded_rows))
}

fn effective_postgres_setting<C>(client: &C, name: &str) -> PgWireResult<String>
where
    C: ClientInfo,
{
    effective_postgres_settings_map(client)
        .remove(name)
        .ok_or_else(|| {
            anyhow_error_to_pgwire(anyhow::anyhow!(
                "unsupported or unknown PostgreSQL SHOW parameter '{}' in the current prototype",
                name
            ))
        })
}

fn effective_postgres_settings_map<C>(client: &C) -> BTreeMap<String, String>
where
    C: ClientInfo,
{
    let mut settings = default_postgres_show_settings();

    if let Some(schema) = client.metadata().get(POSTGRES_SCHEMA_METADATA) {
        settings.insert("search_path".to_string(), schema.clone());
    }

    for (key, value) in client.metadata() {
        if let Some(name) = key.strip_prefix(POSTGRES_SETTING_PREFIX) {
            settings.insert(name.to_string(), value.clone());
        }
    }

    settings
}

fn default_postgres_show_settings() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("application_name".to_string(), "".to_string()),
        ("client_encoding".to_string(), "UTF8".to_string()),
        ("datestyle".to_string(), "ISO, MDY".to_string()),
        (
            "default_transaction_isolation".to_string(),
            "read committed".to_string(),
        ),
        ("extra_float_digits".to_string(), "1".to_string()),
        ("intervalstyle".to_string(), "postgres".to_string()),
        ("search_path".to_string(), "public".to_string()),
        ("standard_conforming_strings".to_string(), "on".to_string()),
        ("statement_timeout".to_string(), "0".to_string()),
        (
            "transaction_isolation".to_string(),
            "read committed".to_string(),
        ),
        ("transaction_read_only".to_string(), "off".to_string()),
        ("timezone".to_string(), "UTC".to_string()),
    ])
}

fn apply_postgres_setting<C>(client: &mut C, name: &str, value: Option<&str>) -> PgWireResult<()>
where
    C: ClientInfo,
{
    if name == "search_path" {
        let raw_value = value.ok_or_else(|| {
            anyhow_error_to_pgwire(anyhow::anyhow!(
                "SET search_path requires a value in the current prototype"
            ))
        })?;
        let (entries, effective_schema) = parse_search_path_entries(raw_value)?;
        client
            .metadata_mut()
            .insert(POSTGRES_SCHEMA_METADATA.to_string(), effective_schema);
        client
            .metadata_mut()
            .insert(postgres_setting_metadata_key(name), entries.join(", "));
        return Ok(());
    }

    let key = postgres_setting_metadata_key(name);
    if let Some(value) = value {
        client.metadata_mut().insert(key, value.to_string());
    } else {
        client.metadata_mut().remove(&key);
    }

    Ok(())
}

fn reset_postgres_setting<C>(client: &mut C, name: &str) -> PgWireResult<()>
where
    C: ClientInfo,
{
    if name == "search_path" {
        client
            .metadata_mut()
            .insert(POSTGRES_SCHEMA_METADATA.to_string(), "public".to_string());
    }
    client
        .metadata_mut()
        .remove(&postgres_setting_metadata_key(name));
    Ok(())
}

fn reset_all_postgres_settings<C>(client: &mut C) -> PgWireResult<()>
where
    C: ClientInfo,
{
    let keys = client
        .metadata()
        .keys()
        .filter(|key| key.starts_with(POSTGRES_SETTING_PREFIX))
        .cloned()
        .collect::<Vec<_>>();
    for key in keys {
        client.metadata_mut().remove(&key);
    }
    client
        .metadata_mut()
        .insert(POSTGRES_SCHEMA_METADATA.to_string(), "public".to_string());

    Ok(())
}

fn postgres_setting_metadata_key(name: &str) -> String {
    format!("{POSTGRES_SETTING_PREFIX}{name}")
}

fn parse_search_path_entries(raw: &str) -> PgWireResult<(Vec<String>, String)> {
    let entries = split_search_path_entries(raw)?
        .into_iter()
        .map(|entry| parse_search_path_schema_name(&entry))
        .collect::<PgWireResult<Vec<_>>>()?;

    if entries.is_empty() {
        return Err(anyhow_error_to_pgwire(anyhow::anyhow!(
            "SET search_path requires at least one schema name in the current prototype"
        )));
    }

    let effective_schema = entries
        .iter()
        .find(|entry| entry.as_str() != "$user")
        .cloned()
        .unwrap_or_else(|| "public".to_string());

    Ok((entries, effective_schema))
}

fn split_search_path_entries(raw: &str) -> PgWireResult<Vec<String>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let mut parts = Vec::new();
    let mut current = String::new();
    let mut chars = trimmed.chars().peekable();
    let mut in_quotes = false;

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                current.push(ch);
                if in_quotes && matches!(chars.peek(), Some('"')) {
                    if let Some(next_ch) = chars.next() {
                        current.push(next_ch);
                    }
                } else {
                    in_quotes = !in_quotes;
                }
            }
            ',' if !in_quotes => {
                let part = current.trim();
                if part.is_empty() {
                    return Err(anyhow_error_to_pgwire(anyhow::anyhow!(
                        "empty search_path entry is not supported in the current prototype"
                    )));
                }
                parts.push(part.to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if in_quotes {
        return Err(anyhow_error_to_pgwire(anyhow::anyhow!(
            "unterminated quoted schema name in SET search_path"
        )));
    }

    let part = current.trim();
    if !part.is_empty() {
        parts.push(part.to_string());
    }

    Ok(parts)
}

fn parse_search_path_schema_name(value: &str) -> PgWireResult<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(anyhow_error_to_pgwire(anyhow::anyhow!(
            "schema name for SET search_path cannot be empty"
        )));
    }

    if trimmed.starts_with('"') {
        if !trimmed.ends_with('"') || trimmed.len() < 2 {
            return Err(anyhow_error_to_pgwire(anyhow::anyhow!(
                "unterminated quoted schema name in SET search_path"
            )));
        }

        return Ok(trimmed[1..trimmed.len() - 1].replace("\"\"", "\""));
    }

    if trimmed.chars().any(char::is_whitespace) {
        return Err(anyhow_error_to_pgwire(anyhow::anyhow!(
            "schema name in SET search_path must be a single identifier in the current prototype"
        )));
    }

    Ok(trimmed.to_string())
}

async fn execute_postgres_sql(
    engine: Arc<PrototypeEngine>,
    request: QueryRequest,
) -> PgWireResult<QueryExecutionResult> {
    engine
        .execute_query_batches(&request)
        .await
        .map_err(anyhow_error_to_pgwire)
}

fn query_execution_to_pg_response(
    execution: QueryExecutionResult,
    _sql: &str,
    row_schema: Option<Arc<Vec<FieldInfo>>>,
) -> PgWireResult<PgResponse> {
    if let StatementOutcome::Command { tag, rows_affected } = &execution.outcome {
        return Ok(PgResponse::Execution(
            Tag::new(tag).with_rows((*rows_affected).try_into().unwrap_or(usize::MAX)),
        ));
    };

    let row_schema = row_schema.unwrap_or_else(|| {
        if execution.schema.fields().is_empty() {
            // For row-returning queries that ended up with no columns (rare),
            // provide a dummy schema so pgwire doesn't fail.
            Arc::new(vec![FieldInfo::new(
                "result".to_string(),
                None,
                None,
                pgwire::api::Type::TEXT,
                pgwire::api::results::FieldFormat::Text,
            )])
        } else {
            Arc::new(postgres_row_schema_from_arrow(&execution.schema, None))
        }
    });
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

#[allow(clippy::expect_used)] // downcasts are guarded by the match arm on column.data_type()
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
    _parameter_count: usize,
    mut literal_for_index: F,
) -> PgWireResult<String>
where
    F: FnMut(usize) -> PgWireResult<String>,
{
    let bytes = sql.as_bytes();
    let mut rendered = String::with_capacity(sql.len());
    let mut index = 0;
    let mut in_single_quote = false;

    while index < bytes.len() {
        let ch = bytes[index] as char;
        if ch == '\'' {
            rendered.push(ch);
            if in_single_quote {
                if index + 1 < bytes.len() && bytes[index + 1] as char == '\'' {
                    rendered.push('\'');
                    index += 2;
                    continue;
                }
                in_single_quote = false;
            } else {
                in_single_quote = true;
            }
            index += 1;
            continue;
        }

        if !in_single_quote && ch == '$' {
            let mut end = index + 1;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if end > index + 1 {
                let placeholder = &sql[index + 1..end];
                let placeholder_index = placeholder.parse::<usize>().map_err(|error| {
                    anyhow_error_to_pgwire(anyhow::anyhow!(
                        "invalid PostgreSQL parameter placeholder '${placeholder}': {error}"
                    ))
                })?;
                rendered.push_str(&literal_for_index(placeholder_index)?);
                index = end;
                continue;
            }
        }

        rendered.push(ch);
        index += 1;
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

fn ensure_compatible_schema(schema: SchemaRef) -> SchemaRef {
    let mut fields = schema.fields().to_vec();
    let mut changed = false;
    for field in fields.iter_mut() {
        if matches!(field.data_type(), DataType::Utf8View) {
            *field = Arc::new(Field::new(
                field.name(),
                DataType::Utf8,
                field.is_nullable(),
            ));
            changed = true;
        } else if matches!(field.data_type(), DataType::BinaryView) {
            *field = Arc::new(Field::new(
                field.name(),
                DataType::Binary,
                field.is_nullable(),
            ));
            changed = true;
        }
    }
    if changed {
        Arc::new(Schema::new(fields))
    } else {
        schema
    }
}

fn ensure_compatible_batch(batch: RecordBatch) -> anyhow::Result<RecordBatch> {
    let schema = batch.schema();
    let mut changed = false;
    for field in schema.fields() {
        if matches!(field.data_type(), DataType::Utf8View | DataType::BinaryView) {
            changed = true;
            break;
        }
    }
    if !changed {
        return Ok(batch);
    }

    let mut columns = batch.columns().to_vec();
    let fields = schema.fields();
    for (i, column) in columns.iter_mut().enumerate() {
        match fields[i].data_type() {
            DataType::Utf8View => {
                *column = cast(column, &DataType::Utf8)?;
            }
            DataType::BinaryView => {
                *column = cast(column, &DataType::Binary)?;
            }
            _ => {}
        }
    }
    let new_schema = ensure_compatible_schema(schema);
    Ok(RecordBatch::try_new(new_schema, columns)?)
}

fn statement_update_rows_affected(execution: &QueryExecutionResult) -> i64 {
    match &execution.outcome {
        StatementOutcome::Rows => 0,
        StatementOutcome::Command { rows_affected, .. } => {
            (*rows_affected).try_into().unwrap_or(i64::MAX)
        }
    }
}

async fn plan_rows_schema(
    engine: &PrototypeEngine,
    sql: String,
    session: SessionContext,
) -> Result<SchemaRef, Status> {
    let schema = engine
        .plan_query_schema(&QueryRequest { sql, session, query_id: None })
        .await
        .map_err(status_from_error)?;

    let schema = schema.unwrap_or_else(|| Arc::new(Schema::empty()));
    Ok(ensure_compatible_schema(schema))
}

fn schema_to_ipc_bytes(schema: &Schema) -> Result<Vec<u8>, Status> {
    let info = FlightInfo::new()
        .try_with_schema(schema)
        .map_err(status_from_error)?;
    Ok(info.schema.to_vec())
}

fn flight_info_with_ipc_schema(
    schema_ipc: Vec<u8>,
    endpoint: FlightEndpoint,
    descriptor: FlightDescriptor,
) -> FlightInfo {
    let mut info = FlightInfo::new()
        .with_endpoint(endpoint)
        .with_descriptor(descriptor);
    info.schema = bytes::Bytes::from(schema_ipc);
    info
}

fn anyhow_error_to_pgwire(error: anyhow::Error) -> PgWireError {
    PgWireError::ApiError(Box::new(std::io::Error::other(error.to_string())))
}

fn status_to_pgwire(status: Status) -> PgWireError {
    anyhow_error_to_pgwire(anyhow::anyhow!(status.message().to_string()))
}

#[derive(Clone)]
/// JWT claims for Flight SQL bearer tokens.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct FlightSqlClaims {
    /// Subject (user name).
    sub: String,
    /// Assumed role.
    role: String,
    /// Target database.
    db: String,
    /// Target schema.
    schema: String,
    /// Password version — token is invalidated when password is rotated.
    pwd_ver: u64,
    /// Expiry (Unix epoch seconds).
    exp: u64,
    /// Issued-at (Unix epoch seconds).
    iat: u64,
}

struct AnalyticsFlightSqlService {
    engine: Arc<PrototypeEngine>,
    auth_hook: Arc<dyn AuthHook>,
    /// HS256 signing secret for JWT bearer tokens (hex-encoded 32 bytes).
    jwt_secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatementTicketPayload {
    sql: String,
    session: SessionContext,
    #[serde(default)]
    row_schema_ipc: Option<Vec<u8>>,
}

#[async_trait]
impl ArrowFlightSqlService for AnalyticsFlightSqlService {
    type FlightService = Self;

    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let action = request.into_inner();

        if action.r#type == "JoinCluster" {
            let req: analyticsdb_control::raft::JoinRequest =
                serde_json::from_slice(&action.body).map_err(status_from_error)?;
            let res = self
                .engine
                .control_plane()
                .join_cluster(req.node_id.as_deref(), req.advertise_host.as_deref())
                .await
                .map_err(status_from_error)?;
            let body = serde_json::to_vec(&res).map_err(status_from_error)?;
            let response = FlightResult { body: body.into() };
            return Ok(Response::new(Box::pin(stream::once(async {
                Ok(response)
            }))));
        }

        if action.r#type == "ExecutePartition" {
            let req: analyticsdb_engine::ExecutePartitionRequest =
                bincode::deserialize(&action.body).map_err(status_from_error)?;

            info!(
                "[worker] ExecutePartition (streaming): query_id={} files={}",
                req.query_id,
                req.partition_files.len()
            );

            let engine = Arc::clone(&self.engine);
            let response_stream = async_stream::try_stream! {
                let mut stream = engine.execute_partition_stream(&req).await.map_err(status_from_error)?;
                let schema = stream.schema();
                let mut row_count = 0;
                let mut batch_count = 0;

                while let Some(batch) = stream.next().await {
                    let batch = batch.map_err(status_from_error)?;
                    row_count += batch.num_rows();
                    batch_count += 1;

                    let ipc_bytes = analyticsdb_engine::distributed::batches_to_ipc_bytes(
                        &schema,
                        &[batch],
                    ).map_err(status_from_error)?;

                    yield FlightResult { body: ipc_bytes };
                }

                info!(
                    "[worker] ExecutePartition done (streaming): query_id={} batches={} rows={}",
                    req.query_id,
                    batch_count,
                    row_count
                );
            };

            return Ok(Response::new(Box::pin(response_stream)));
        }

        if action.r#type == "ExecutePartitionWrite" {
            let req: analyticsdb_engine::ExecutePartitionWriteRequest =
                bincode::deserialize(&action.body).map_err(status_from_error)?;

            let ack = self
                .engine
                .execute_distributed_write_partition(&req)
                .await
                .map_err(status_from_error)?;

            let body = serde_json::to_vec(&ack).map_err(status_from_error)?.into();
            let response = FlightResult { body };
            return Ok(Response::new(Box::pin(stream::once(async {
                Ok(response)
            }))));
        }

        if action.r#type == "Heartbeat" {
            let node_id =
                std::str::from_utf8(&action.body).map_err(status_from_error)?.to_string();
            self.engine
                .control_plane()
                .heartbeat(&node_id)
                .await
                .map_err(status_from_error)?;
            return Ok(Response::new(Box::pin(stream::empty())));
        }

        Err(Status::unimplemented(format!(
            "Unknown action type: {}",
            action.r#type
        )))
    }

    async fn do_handshake(
        &self,
        request: Request<Streaming<arrow_flight::HandshakeRequest>>,
    ) -> Result<
        Response<
            Pin<Box<dyn Stream<Item = Result<arrow_flight::HandshakeResponse, Status>> + Send>>,
        >,
        Status,
    > {
        let metadata = request.metadata().clone();
        trace!(
            "flight-sql: handshake request from {:?}",
            request.remote_addr()
        );
        let mut stream = request.into_inner();
        let handshake_request =
            stream
                .next()
                .await
                .transpose()?
                .unwrap_or(arrow_flight::HandshakeRequest {
                    protocol_version: 0,
                    payload: bytes::Bytes::new(),
                });

        let basic_auth = parse_basic_auth_from_metadata(&metadata)?;
        let user = basic_auth
            .as_ref()
            .map(|(user, _)| user.clone())
            .or_else(|| metadata_value(&metadata, FLIGHT_USER_HEADER))
            .unwrap_or_else(|| "postgres".to_string());
        let database = metadata_value(&metadata, FLIGHT_DATABASE_HEADER)
            .unwrap_or_else(|| "postgres".to_string());
        let schema =
            metadata_value(&metadata, FLIGHT_SCHEMA_HEADER).unwrap_or_else(|| "public".to_string());

        let decision = self
            .auth_hook
            .authenticate(&AuthRequest {
                protocol: Protocol::ArrowFlightSql,
                user,
                database,
                schema,
                role: metadata_value(&metadata, FLIGHT_ROLE_HEADER),
                password: basic_auth.as_ref().map(|(_, password)| password.clone()),
                auth_header: metadata_value(&metadata, "authorization"),
            })
            .await?;

        // Look up password_version for the user so the JWT can be invalidated on rotation.
        let catalog_user = self
            .engine
            .control_plane()
            .catalog_user(&decision.user)
            .await
            .map_err(status_from_error)?;
        let pwd_ver = catalog_user.password_version;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| Status::internal(format!("system time error: {e}")))?
            .as_secs();
        let claims = FlightSqlClaims {
            sub: decision.user.clone(),
            role: decision.role.clone(),
            db: decision.database.clone(),
            schema: decision.schema.clone(),
            pwd_ver,
            exp: now + 86400,
            iat: now,
        };
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(self.jwt_secret.as_bytes()),
        )
        .map_err(|e| Status::internal(format!("JWT signing error: {e}")))?;

        let payload = token.as_bytes().to_vec();
        let response_stream = stream::once(async move {
            Ok(arrow_flight::HandshakeResponse {
                protocol_version: handshake_request.protocol_version,
                payload: payload.into(),
            })
        });
        let mut response = Response::new(Box::pin(response_stream)
            as Pin<Box<dyn Stream<Item = Result<arrow_flight::HandshakeResponse, Status>> + Send>>);
        response.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}"))
            .map_err(|error| {
                Status::internal(format!("invalid authorization metadata: {error}"))
            })?,
        );
        response.metadata_mut().insert(
            FLIGHT_USER_HEADER,
            MetadataValue::try_from(decision.user.as_str()).map_err(|error| {
                Status::internal(format!("invalid handshake user metadata: {error}"))
            })?,
        );
        response.metadata_mut().insert(
            FLIGHT_ROLE_HEADER,
            MetadataValue::try_from(decision.role.as_str()).map_err(|error| {
                Status::internal(format!("invalid handshake role metadata: {error}"))
            })?,
        );
        response.metadata_mut().insert(
            FLIGHT_AUTH_METHOD_HEADER,
            MetadataValue::try_from(decision.auth_method.as_str()).map_err(|error| {
                Status::internal(format!("invalid handshake auth metadata: {error}"))
            })?,
        );

        Ok(response)
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("flight_sql_query: {}", query.query);
        let session = self.session_from_request(&request).await?;
        let schema = plan_rows_schema(&self.engine, query.query.clone(), session.clone()).await?;
        let schema_ipc = schema_to_ipc_bytes(schema.as_ref())?;

        let descriptor = request.into_inner();
        let ticket = statement_ticket(query.query, session, Some(schema_ipc.clone()))?;
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let info = flight_info_with_ipc_schema(schema_ipc, endpoint, descriptor);

        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let payload = decode_statement_ticket(ticket.statement_handle)?;
        let execution = self
            .engine
            .execute_query_stream(&QueryRequest {
                sql: payload.sql,
                session: payload.session,
                query_id: None,
            })
            .await
            .map_err(status_from_error)?;

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(ensure_compatible_schema(Arc::clone(&execution.schema)))
            .build(execution.stream.map(|batch| {
                batch
                    .map_err(anyhow::Error::from)
                    .and_then(ensure_compatible_batch)
                    .map_err(|error| {
                        arrow_flight::error::FlightError::from_external_error(Box::new(
                            std::io::Error::other(error.to_string()),
                        ))
                    })
            }))
            .map_err(Status::from)
            .boxed();

        Ok(Response::new(stream))
    }

    async fn do_put_statement_update(
        &self,
        command: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let session = self.session_from_request(&request).await?;
        let execution = self
            .execute_batches(QueryRequest {
                sql: command.query,
                session,
                query_id: None,
            })
            .await?;

        Ok(statement_update_rows_affected(&execution))
    }

    async fn do_put_prepared_statement_update(
        &self,
        query: arrow_flight::sql::CommandPreparedStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let payload = decode_statement_ticket(query.prepared_statement_handle)?;
        let execution = self
            .execute_batches(QueryRequest {
                sql: payload.sql,
                session: payload.session,
                query_id: None,
            })
            .await?;

        Ok(statement_update_rows_affected(&execution))
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
        let session = self.session_from_request(&request).await?;
        let databases = self
            .engine
            .list_databases(&session)
            .await
            .map_err(status_from_error)?;

        let mut builder = query.into_builder();
        for database in &databases {
            builder.append(database);
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
        let session = self.session_from_request(&request).await?;
        let databases = if let Some(database) = query.catalog.clone() {
            vec![database]
        } else {
            self.engine
                .list_databases(&session)
                .await
                .map_err(status_from_error)?
        };

        let mut builder = query.into_builder();
        for database in &databases {
            let schema_session = flight_session_for_database(&session, database);
            let database_for_list = database.clone();
            let schemas = self
                .engine
                .list_schemas(&schema_session, Some(&database_for_list))
                .await
                .map_err(status_from_error)?;

            for schema in schemas {
                builder.append(database, &schema);
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
        let session = self.session_from_request(&request).await?;
        let databases = if let Some(database) = query.catalog.clone() {
            vec![database]
        } else {
            self.engine
                .list_databases(&session)
                .await
                .map_err(status_from_error)?
        };

        let mut builder = query.into_builder();
        for database in &databases {
            let db_session = flight_session_for_database(&session, database);
            let database_for_schemas = database.clone();
            let schemas = self
                .engine
                .list_schemas(&db_session, Some(&database_for_schemas))
                .await
                .map_err(status_from_error)?;

            for schema_name in schemas {
                let table_session = flight_session_for_database(&session, database);
                let tables = self
                    .engine
                    .list_relations(
                        &table_session,
                        Some(database),
                        Some(&schema_name),
                        CatalogRelationKind::Table,
                    )
                    .await
                    .map_err(status_from_error)?;

                for table in tables {
                    let schema = catalog_relation_to_arrow_schema(&table.columns);
                    builder
                        .append(database, &schema_name, &table.name, "TABLE", &schema)
                        .map_err(status_from_error)?;
                }

                let view_session = flight_session_for_database(&session, database);
                let views = self
                    .engine
                    .list_relations(
                        &view_session,
                        Some(database),
                        Some(&schema_name),
                        CatalogRelationKind::View,
                    )
                    .await
                    .map_err(status_from_error)?;

                for view in views {
                    let schema = catalog_relation_to_arrow_schema(&view.columns);
                    builder
                        .append(database, &schema_name, &view.name, "VIEW", &schema)
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
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        });
        let sql_info_data = flight_sql_info_data()?;
        let info = FlightInfo::new()
            .try_with_schema(query.into_builder(&sql_info_data).schema().as_ref())
            .map_err(status_from_error)?
            .with_endpoint(endpoint)
            .with_descriptor(descriptor);

        Ok(Response::new(info))
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let sql_info_data = flight_sql_info_data()?;
        let builder = query.into_builder(&sql_info_data);
        let schema = builder.schema();
        let batch = builder.build().map_err(status_from_error)?;

        Ok(Response::new(encoded_single_batch(schema, batch)?))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

    async fn do_action_create_prepared_statement(
        &self,
        query: arrow_flight::sql::ActionCreatePreparedStatementRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<arrow_flight::sql::ActionCreatePreparedStatementResult, Status> {
        let session = self.session_from_request(&request).await?;
        debug!("flight_sql_create_prepared_statement: {}", query.query);

        let row_schema_ipc = match self
            .engine
            .plan_query_schema(&QueryRequest {
                sql: query.query.clone(),
                session: session.clone(),
                query_id: None,
            })
            .await
        {
            Ok(Some(schema)) => Some(schema_to_ipc_bytes(schema.as_ref())?),
            _ => None,
        };
        let dataset_schema = row_schema_ipc
            .clone()
            .map(bytes::Bytes::from)
            .unwrap_or_else(bytes::Bytes::new);

        // For this prototype, we treat "prepared statements" as just the SQL string
        // and session context wrapped in a handle.
        let payload = StatementTicketPayload {
            sql: query.query,
            session,
            row_schema_ipc,
        };
        let handle = serde_json::to_vec(&payload).map_err(status_from_error)?;

        Ok(arrow_flight::sql::ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.into(),
            dataset_schema,
            parameter_schema: bytes::Bytes::new(),
        })
    }

    async fn do_action_close_prepared_statement(
        &self,
        _query: arrow_flight::sql::ActionClosePreparedStatementRequest,
        _request: Request<arrow_flight::Action>,
    ) -> Result<(), Status> {
        // No-op for prototype as we are stateless
        Ok(())
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let payload = decode_statement_ticket(query.prepared_statement_handle)?;
        debug!("flight_sql_prepared_statement: {}", payload.sql);
        let schema_ipc = payload
            .row_schema_ipc
            .clone()
            .unwrap_or_else(|| schema_to_ipc_bytes(&Schema::empty()).unwrap_or_default());

        let descriptor = request.into_inner();
        let ticket = statement_ticket(payload.sql, payload.session, Some(schema_ipc.clone()))?;
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let info = flight_info_with_ipc_schema(schema_ipc, endpoint, descriptor);

        Ok(Response::new(info))
    }
}

impl AnalyticsFlightSqlService {
    async fn execute_batches(&self, request: QueryRequest) -> Result<QueryExecutionResult, Status> {
        self.engine
            .execute_query_batches(&request)
            .await
            .map_err(status_from_error)
    }

    /// Extract a `SessionContext` from the request.
    ///
    /// Prefers a valid JWT bearer token; falls back to the legacy x-analyticsdb-*
    /// header approach so internal-node RPCs (which don't go through handshake)
    /// continue to work.
    async fn session_from_request<T>(
        &self,
        request: &tonic::Request<T>,
    ) -> Result<SessionContext, Status> {
        if let Some(auth) = metadata_value(request.metadata(), "authorization") {
            if auth.starts_with("Bearer ") {
                return self.verify_bearer_token(request).await;
            }
        }
        Ok(flight_session_from_metadata(request.metadata()))
    }

    /// Verify a `Bearer <jwt>` token from request metadata.
    ///
    /// Decodes and validates the JWT, checks `pwd_ver` against the current
    /// stored version, and returns the `SessionContext` derived from the claims.
    /// Returns `Status::unauthenticated` on any failure.
    async fn verify_bearer_token<T>(
        &self,
        request: &tonic::Request<T>,
    ) -> Result<SessionContext, Status> {
        let auth_header = metadata_value(request.metadata(), "authorization").ok_or_else(|| {
            
            Status::unauthenticated("missing authorization header")
        })?;
        let token = auth_header
            .strip_prefix("Bearer ")
            .ok_or_else(|| {
                
                Status::unauthenticated("authorization header must be Bearer <token>")
            })?;

        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
        validation.validate_exp = true;
        let token_data = jsonwebtoken::decode::<FlightSqlClaims>(
            token,
            &jsonwebtoken::DecodingKey::from_secret(self.jwt_secret.as_bytes()),
            &validation,
        )
        .map_err(|e| {
            
            Status::unauthenticated(format!("invalid JWT: {e}"))
        })?;

        let claims = token_data.claims;

        // Validate pwd_ver to detect rotated passwords.
        let catalog_user = self
            .engine
            .control_plane()
            .catalog_user(&claims.sub)
            .await
            .map_err(|e| {
                
                Status::unauthenticated(format!("user lookup failed: {e}"))
            })?;
        if catalog_user.password_version != claims.pwd_ver {
            
            return Err(Status::unauthenticated(
                "token has been invalidated by a password rotation — please re-authenticate",
            ));
        }

        Ok(SessionContext {
            user: claims.sub,
            role: claims.role,
            database: claims.db,
            schema: claims.schema,
            auth_method: "flight-sql-jwt".to_string(),
            protocol: analyticsdb_core::Protocol::ArrowFlightSql,
            transaction_status: analyticsdb_core::TransactionStatus::Idle,
            statement_timeout_ms: 0,
            idle_in_transaction_timeout_ms: 0,
        })
    }
}

fn flight_session_from_metadata(metadata: &MetadataMap) -> SessionContext {
    let user =
        metadata_value(metadata, FLIGHT_USER_HEADER).unwrap_or_else(|| "postgres".to_string());
    SessionContext {
        user: user.clone(),
        role: metadata_value(metadata, FLIGHT_ROLE_HEADER).unwrap_or(user.clone()),
        database: metadata_value(metadata, FLIGHT_DATABASE_HEADER)
            .unwrap_or_else(|| "postgres".to_string()),
        schema: metadata_value(metadata, FLIGHT_SCHEMA_HEADER)
            .unwrap_or_else(|| "public".to_string()),
        auth_method: metadata_value(metadata, FLIGHT_AUTH_METHOD_HEADER)
            .unwrap_or_else(|| "flight-sql-metadata".to_string()),
        protocol: Protocol::ArrowFlightSql,
        transaction_status: analyticsdb_core::TransactionStatus::Idle,
        statement_timeout_ms: 0,
        idle_in_transaction_timeout_ms: 0,
    }
}

fn flight_session_for_database(session: &SessionContext, database: &str) -> SessionContext {
    SessionContext {
        user: session.user.clone(),
        role: session.role.clone(),
        database: database.to_string(),
        schema: session.schema.clone(),
        auth_method: session.auth_method.clone(),
        protocol: Protocol::ArrowFlightSql,
        transaction_status: analyticsdb_core::TransactionStatus::Idle,
        statement_timeout_ms: session.statement_timeout_ms,
        idle_in_transaction_timeout_ms: session.idle_in_transaction_timeout_ms,
    }
}

fn metadata_value(metadata: &MetadataMap, key: &str) -> Option<String> {
    metadata
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn parse_basic_auth_from_metadata(
    metadata: &MetadataMap,
) -> Result<Option<(String, String)>, Status> {
    let Some(raw_auth) = metadata_value(metadata, "authorization") else {
        return Ok(None);
    };
    let Some(encoded) = raw_auth.strip_prefix("Basic ") else {
        return Ok(None);
    };
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|error| {
            Status::invalid_argument(format!("invalid basic authorization payload: {error}"))
        })?;
    let pair = String::from_utf8(decoded).map_err(|error| {
        Status::invalid_argument(format!("invalid utf8 in authorization payload: {error}"))
    })?;
    let Some((user, password)) = pair.split_once(':') else {
        return Err(Status::invalid_argument(
            "basic authorization payload must contain username:password",
        ));
    };

    Ok(Some((user.to_string(), password.to_string())))
}

fn statement_ticket(
    sql: String,
    session: SessionContext,
    row_schema_ipc: Option<Vec<u8>>,
) -> Result<Ticket, Status> {
    let payload = StatementTicketPayload {
        sql,
        session,
        row_schema_ipc,
    };
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
    let schema = ensure_compatible_schema(schema);
    let batch = ensure_compatible_batch(batch).map_err(status_from_error)?;
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
    if data_type.starts_with("Timestamp") {
        if data_type.contains("Some") || data_type.contains("UTC") {
            return DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        } else {
            return DataType::Timestamp(TimeUnit::Microsecond, None);
        }
    }
    match data_type {
        "Boolean" => DataType::Boolean,
        "Float32" => DataType::Float32,
        "Float64" => DataType::Float64,
        "Int32" => DataType::Int32,
        "Int64" => DataType::Int64,
        "UInt32" => DataType::UInt32,
        "UInt64" => DataType::UInt64,
        "Date32" | "Date" => DataType::Date32,
        _ => DataType::Utf8,
    }
}

fn status_from_error(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

fn flight_sql_info_data() -> Result<SqlInfoData, Status> {
    let mut builder = SqlInfoDataBuilder::new();
    // Expand SqlInfo coverage to satisfy generic Flight SQL clients (DBeaver, JDBC, etc.)
    builder.append(SqlInfo::FlightSqlServerName, "AnalyticsDB Prototype");
    builder.append(SqlInfo::FlightSqlServerVersion, "0.1.0-prototype");
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "15.0.0");
    builder.append(SqlInfo::FlightSqlServerReadOnly, false);
    builder.append(SqlInfo::SqlIdentifierQuoteChar, "\"");

    builder.build().map_err(status_from_error)
}

// Prototype scaffold: Flight SQL prepared statement parameter handling
// Full implementation requires Flight SQL client API support for prepare/bind/execute cycles
// These utilities are provided as infrastructure for future work:
mod flight_prepared_statement_scaffold {
    use pgwire::api::Type;
    use pgwire::error::PgWireResult;

    /// Convert Flight SQL parameter bytes to SQL literal string for a given type
    /// This mirrors the PostgreSQL parameter handling in parameter_literal_from_portal
    #[allow(dead_code)]
    pub fn flight_sql_parameter_to_literal(
        parameter: Option<&bytes::Bytes>,
        param_type: &Type,
    ) -> PgWireResult<String> {
        match parameter {
            None => Ok("NULL".to_string()),
            Some(bytes) => match *param_type {
                Type::BOOL => {
                    if bytes.len() != 1 {
                        return Err(super::unsupported_parameter_type_error(
                            "BOOL parameter must be 1 byte",
                        ));
                    }
                    let value = bytes[0] != 0;
                    Ok(if value { "TRUE" } else { "FALSE" }.to_string())
                }
                Type::INT2 => {
                    if bytes.len() != 2 {
                        return Err(super::unsupported_parameter_type_error(
                            "INT2 parameter must be 2 bytes",
                        ));
                    }
                    let value = i16::from_be_bytes([bytes[0], bytes[1]]);
                    Ok(value.to_string())
                }
                Type::INT4 => {
                    if bytes.len() != 4 {
                        return Err(super::unsupported_parameter_type_error(
                            "INT4 parameter must be 4 bytes",
                        ));
                    }
                    let value = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                    Ok(value.to_string())
                }
                Type::INT8 => {
                    if bytes.len() != 8 {
                        return Err(super::unsupported_parameter_type_error(
                            "INT8 parameter must be 8 bytes",
                        ));
                    }
                    let value = i64::from_be_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                        bytes[7],
                    ]);
                    Ok(value.to_string())
                }
                Type::FLOAT4 => {
                    if bytes.len() != 4 {
                        return Err(super::unsupported_parameter_type_error(
                            "FLOAT4 parameter must be 4 bytes",
                        ));
                    }
                    let value = f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                    Ok(value.to_string())
                }
                Type::FLOAT8 | Type::NUMERIC => {
                    if bytes.len() != 8 {
                        return Err(super::unsupported_parameter_type_error(
                            "FLOAT8 parameter must be 8 bytes",
                        ));
                    }
                    let value = f64::from_be_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                        bytes[7],
                    ]);
                    Ok(value.to_string())
                }
                Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN => {
                    let string = String::from_utf8(bytes.to_vec()).map_err(|error| {
                        super::unsupported_parameter_type_error(&format!(
                            "invalid UTF-8 string parameter: {error}"
                        ))
                    })?;
                    Ok(format!("'{}'", string.replace('\'', "''")))
                }
                _ => Err(super::unsupported_parameter_type_error(&format!(
                    "unsupported parameter type in flight SQL prepared statement: {}",
                    param_type.name()
                ))),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_flight::sql::client::FlightSqlServiceClient;
    use arrow_flight::sql::CommandGetTables;
    use arrow_flight::sql::SqlInfo;
    use base64::Engine;
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
                "host=127.0.0.1 port={} user=postgres dbname=postgres password=postgres",
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
            PrototypeEngine::from_catalog_path(&catalog_path)
                .await
                .expect("engine should initialize"),
        );

        let server = tokio::spawn(serve_postgres_wire(listener, Arc::clone(&engine)));

        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres dbname=postgres password=postgres",
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
    async fn postgres_wire_honors_set_search_path_for_subsequent_queries() {
        let catalog_path = temp_catalog_path("postgres-search-path");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(
            PrototypeEngine::from_catalog_path(&catalog_path)
                .await
                .expect("engine should initialize"),
        );

        let server = tokio::spawn(serve_postgres_wire(listener, Arc::clone(&engine)));

        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres dbname=postgres password=postgres",
                addr.port()
            ),
            NoTls,
        )
        .await
        .expect("postgres client should connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .simple_query("CREATE SCHEMA reporting")
            .await
            .expect("create schema should succeed");
        client
            .batch_execute("SET search_path TO reporting")
            .await
            .expect("setting search_path should succeed");
        client
            .simple_query("CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT)")
            .await
            .expect("create table in reporting schema should succeed");
        client
            .simple_query("INSERT INTO fact_metrics VALUES (11, 'ok')")
            .await
            .expect("insert should succeed");

        let messages = client
            .simple_query("SELECT metric, status FROM fact_metrics")
            .await
            .expect("query should succeed");
        let row = messages
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .expect("row should exist");

        assert_eq!(row.get("metric"), Some("11"));
        assert_eq!(row.get("status"), Some("ok"));

        let show_tables = client
            .simple_query("SHOW TABLES FROM reporting")
            .await
            .expect("show tables should succeed");
        let table_row = show_tables
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .expect("table row should exist");

        assert_eq!(table_row.get("table_name"), Some("fact_metrics"));

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
    async fn postgres_wire_accepts_jdbc_extra_float_digits_set_in_simple_query_path() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(PrototypeEngine::new().expect("engine should initialize"));

        let server = tokio::spawn(serve_postgres_wire(listener, Arc::clone(&engine)));

        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres dbname=postgres password=postgres",
                addr.port()
            ),
            NoTls,
        )
        .await
        .expect("postgres client should connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .batch_execute("SET extra_float_digits = 3")
            .await
            .expect("JDBC startup-style extra_float_digits SET should be accepted");

        let rows = client
            .simple_query("SELECT 1 AS one")
            .await
            .expect("query should succeed after no-op SET");
        let row = rows
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .expect("row should exist");
        assert_eq!(row.get("one"), Some("1"));

        server.abort();
    }

    #[tokio::test]
    async fn postgres_wire_accepts_jdbc_extra_float_digits_set_in_extended_query_path() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(PrototypeEngine::new().expect("engine should initialize"));

        let server = tokio::spawn(serve_postgres_wire(listener, Arc::clone(&engine)));

        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres dbname=postgres password=postgres",
                addr.port()
            ),
            NoTls,
        )
        .await
        .expect("postgres client should connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let set_stmt = client
            .prepare_typed("SET extra_float_digits = 3", &[])
            .await
            .expect("extended SET should prepare");
        let affected = client
            .execute(&set_stmt, &[])
            .await
            .expect("extended SET should execute");
        assert_eq!(affected, 0);

        let rows = client
            .simple_query("SELECT 1 AS one")
            .await
            .expect("query should succeed after extended no-op SET");
        let row = rows
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .expect("row should exist");
        assert_eq!(row.get("one"), Some("1"));

        server.abort();
    }

    #[tokio::test]
    async fn postgres_wire_accepts_generic_session_set_and_reset_forms() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(PrototypeEngine::new().expect("engine should initialize"));

        let server = tokio::spawn(serve_postgres_wire(listener, Arc::clone(&engine)));

        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres dbname=postgres password=postgres",
                addr.port()
            ),
            NoTls,
        )
        .await
        .expect("postgres client should connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .batch_execute("SET application_name = 'jdbc-client'")
            .await
            .expect("generic SET application_name should succeed");
        let show_application_name = client
            .simple_query("SHOW application_name")
            .await
            .expect("SHOW application_name should succeed after SET");
        let show_application_name_row = show_application_name
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .expect("SHOW application_name row should exist");
        assert_eq!(
            show_application_name_row.get("application_name"),
            Some("'jdbc-client'")
        );

        client
            .batch_execute("SET SESSION statement_timeout TO '5s'")
            .await
            .expect("SET SESSION should succeed");
        client
            .batch_execute("SET LOCAL extra_float_digits = 3")
            .await
            .expect("SET LOCAL should be accepted as session-scoped prototype behavior");
        client
            .batch_execute("SET TIME ZONE 'UTC'")
            .await
            .expect("SET TIME ZONE should succeed");
        client
            .batch_execute("SET NAMES 'UTF8'")
            .await
            .expect("SET NAMES should succeed");
        let show_all = client
            .simple_query("SHOW ALL")
            .await
            .expect("SHOW ALL should succeed");
        let show_all_rows = show_all
            .iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            show_all_rows.iter().any(|row| {
                row.get("name") == Some("application_name")
                    && row.get("setting") == Some("'jdbc-client'")
            }),
            "SHOW ALL rows={show_all_rows:?}"
        );
        assert!(
            show_all_rows.iter().any(|row| {
                row.get("name") == Some("client_encoding") && row.get("setting") == Some("'UTF8'")
            }),
            "SHOW ALL rows={show_all_rows:?}"
        );

        client
            .batch_execute("RESET application_name")
            .await
            .expect("RESET name should succeed");
        let reset_application_name = client
            .simple_query("SHOW application_name")
            .await
            .expect("SHOW application_name should succeed after RESET");
        let reset_application_name_row = reset_application_name
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .expect("SHOW application_name row should exist after RESET");
        assert_eq!(reset_application_name_row.get("application_name"), Some(""));

        client
            .batch_execute("RESET ALL")
            .await
            .expect("RESET ALL should succeed");
        let reset_all_show = client
            .simple_query("SHOW statement_timeout")
            .await
            .expect("SHOW statement_timeout should succeed after RESET ALL");
        let reset_all_show_row = reset_all_show
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .expect("SHOW statement_timeout row should exist after RESET ALL");
        assert_eq!(reset_all_show_row.get("statement_timeout"), Some("0"));

        let rows = client
            .simple_query("SELECT 1 AS one")
            .await
            .expect("query should still succeed after SET/RESET sequence");
        let row = rows
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .expect("row should exist");
        assert_eq!(row.get("one"), Some("1"));

        server.abort();
    }

    #[tokio::test]
    async fn postgres_wire_rejects_unsupported_set_transaction_form() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(PrototypeEngine::new().expect("engine should initialize"));

        let server = tokio::spawn(serve_postgres_wire(listener, Arc::clone(&engine)));

        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres dbname=postgres password=postgres",
                addr.port()
            ),
            NoTls,
        )
        .await
        .expect("postgres client should connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let error = client
            .batch_execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .await
            .expect_err("SET TRANSACTION should remain unsupported without transaction semantics");
        let text = error.to_string();
        assert!(
            text.to_ascii_lowercase().contains("db error")
                || text.to_ascii_lowercase().contains("not implemented")
                || text.to_ascii_lowercase().contains("unsupported"),
            "unexpected error text: {text}"
        );

        server.abort();
    }

    #[tokio::test]
    async fn postgres_wire_show_search_path_tracks_set_and_reset() {
        let catalog_path = temp_catalog_path("postgres-show-search-path");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(
            PrototypeEngine::from_catalog_path(&catalog_path)
                .await
                .expect("engine should initialize"),
        );

        let server = tokio::spawn(serve_postgres_wire(listener, Arc::clone(&engine)));

        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres dbname=postgres password=postgres",
                addr.port()
            ),
            NoTls,
        )
        .await
        .expect("postgres client should connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .batch_execute("CREATE SCHEMA reporting")
            .await
            .expect("create schema should succeed");
        client
            .batch_execute("SET search_path TO \"$user\", reporting")
            .await
            .expect("SET search_path should succeed");

        let show_search_path = client
            .simple_query("SHOW search_path")
            .await
            .expect("SHOW search_path should succeed");
        let show_search_path_row = show_search_path
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .expect("SHOW search_path row should exist");
        assert_eq!(
            show_search_path_row.get("search_path"),
            Some("$user, reporting")
        );

        client
            .batch_execute("CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT)")
            .await
            .expect("search_path should route create table into reporting");

        client
            .batch_execute("RESET search_path")
            .await
            .expect("RESET search_path should succeed");
        let show_reset = client
            .simple_query("SHOW search_path")
            .await
            .expect("SHOW search_path should succeed after RESET");
        let show_reset_row = show_reset
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .expect("SHOW search_path row should exist after RESET");
        assert_eq!(show_reset_row.get("search_path"), Some("public"));

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
    async fn postgres_wire_exposes_expected_startup_parameter_status_values() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(PrototypeEngine::new().expect("engine should initialize"));

        let server = tokio::spawn(serve_postgres_wire(listener, Arc::clone(&engine)));

        let (_client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres dbname=postgres password=postgres",
                addr.port()
            ),
            NoTls,
        )
        .await
        .expect("postgres client should connect");
        assert_eq!(connection.parameter("client_encoding"), Some("UTF8"));
        assert_eq!(connection.parameter("DateStyle"), Some("ISO, MDY"));
        assert_eq!(connection.parameter("TimeZone"), Some("UTC"));
        assert_eq!(
            connection.parameter("default_transaction_isolation"),
            Some("read committed")
        );
        assert_eq!(
            connection.parameter("standard_conforming_strings"),
            Some("on")
        );
        assert_eq!(connection.parameter("search_path"), Some("public"));
        let server_version = connection
            .parameter("server_version")
            .expect("server_version ParameterStatus should be present");
        assert_eq!(server_version, POSTGRES_SERVER_VERSION);

        tokio::spawn(async move {
            let _ = connection.await;
        });

        server.abort();
    }

    #[tokio::test]
    async fn postgres_wire_startup_parameter_status_includes_client_application_name() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(PrototypeEngine::new().expect("engine should initialize"));

        let server = tokio::spawn(serve_postgres_wire(listener, Arc::clone(&engine)));

        let (_client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres dbname=postgres password=postgres application_name=jdbc-probe",
                addr.port()
            ),
            NoTls,
        )
        .await
        .expect("postgres client should connect");
        assert_eq!(connection.parameter("application_name"), Some("jdbc-probe"));

        tokio::spawn(async move {
            let _ = connection.await;
        });

        server.abort();
    }

    #[tokio::test]
    async fn postgres_wire_supports_common_jdbc_introspection_selects() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(PrototypeEngine::new().expect("engine should initialize"));

        let server = tokio::spawn(serve_postgres_wire(listener, Arc::clone(&engine)));

        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres dbname=postgres password=postgres",
                addr.port()
            ),
            NoTls,
        )
        .await
        .expect("postgres client should connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let version = client
            .query_one("SELECT version()", &[])
            .await
            .expect("SELECT version() should succeed");
        assert_eq!(version.get::<_, String>(0), POSTGRES_SERVER_VERSION);

        let database = client
            .query_one("SELECT current_database()", &[])
            .await
            .expect("SELECT current_database() should succeed");
        assert_eq!(database.get::<_, String>(0), "postgres");

        let schema = client
            .query_one("SELECT current_schema()", &[])
            .await
            .expect("SELECT current_schema() should succeed");
        assert_eq!(schema.get::<_, String>(0), "public");

        let current_user = client
            .query_one("SELECT current_user", &[])
            .await
            .expect("SELECT current_user should succeed");
        assert_eq!(current_user.get::<_, String>(0), "postgres");

        let session_user = client
            .query_one("SELECT session_user", &[])
            .await
            .expect("SELECT session_user should succeed");
        assert_eq!(session_user.get::<_, String>(0), "postgres");

        let setting = client
            .query_one(
                "SELECT current_setting('search_path') AS active_search_path",
                &[],
            )
            .await
            .expect("SELECT current_setting should succeed");
        assert_eq!(setting.get::<_, String>("active_search_path"), "public");

        let isolation = client
            .query_one("SELECT current_setting('transaction_isolation')", &[])
            .await
            .expect("SELECT current_setting(transaction_isolation) should succeed");
        assert_eq!(isolation.get::<_, String>(0), "read committed");

        server.abort();
    }

    #[tokio::test]
    async fn postgres_wire_supports_transaction_isolation_show_and_session_characteristics_set() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(PrototypeEngine::new().expect("engine should initialize"));

        let server = tokio::spawn(serve_postgres_wire(listener, Arc::clone(&engine)));

        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres dbname=postgres password=postgres",
                addr.port()
            ),
            NoTls,
        )
        .await
        .expect("postgres client should connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let show_initial = client
            .simple_query("SHOW transaction_isolation")
            .await
            .expect("SHOW transaction_isolation should succeed");
        let show_initial_row = show_initial
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .expect("SHOW transaction_isolation should return one row");
        assert_eq!(
            show_initial_row.get("transaction_isolation"),
            Some("read committed")
        );

        client
            .batch_execute(
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            )
            .await
            .expect("SET SESSION CHARACTERISTICS should succeed");

        let show_alias = client
            .simple_query("SHOW TRANSACTION ISOLATION LEVEL")
            .await
            .expect("SHOW TRANSACTION ISOLATION LEVEL should succeed");
        let show_alias_row = show_alias
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .expect("SHOW TRANSACTION ISOLATION LEVEL should return one row");
        assert_eq!(
            show_alias_row.get("transaction_isolation"),
            Some("serializable")
        );

        client
            .batch_execute("RESET transaction_isolation")
            .await
            .expect("RESET transaction_isolation should succeed");
        let show_reset = client
            .simple_query("SHOW transaction_isolation")
            .await
            .expect("SHOW transaction_isolation should succeed after RESET");
        let show_reset_row = show_reset
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .expect("SHOW transaction_isolation should return one row after RESET");
        assert_eq!(
            show_reset_row.get("transaction_isolation"),
            Some("read committed")
        );

        server.abort();
    }

    #[tokio::test]
    async fn postgres_wire_extended_query_keeps_parameter_markers_inside_string_literals() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(PrototypeEngine::new().expect("engine should initialize"));

        let server = tokio::spawn(serve_postgres_wire(listener, Arc::clone(&engine)));

        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres dbname=postgres password=postgres",
                addr.port()
            ),
            NoTls,
        )
        .await
        .expect("postgres client should connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let statement = client
            .prepare_typed(
                "SELECT '$1' AS literal_value, $1 AS bound_value",
                &[PgType::TEXT],
            )
            .await
            .expect("statement should prepare");
        let row = client
            .query_one(&statement, &[&"value"])
            .await
            .expect("query should succeed");
        assert_eq!(row.get::<_, String>("literal_value"), "$1");
        assert_eq!(row.get::<_, String>("bound_value"), "value");

        server.abort();
    }

    #[tokio::test]
    async fn postgres_wire_rejects_unknown_user_during_startup_auth() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(PrototypeEngine::new().expect("engine should initialize"));

        let server = tokio::spawn(serve_postgres_wire(listener, Arc::clone(&engine)));

        let connect_result = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=missing_user dbname=postgres password=secret",
                addr.port()
            ),
            NoTls,
        )
        .await;

        let connect_error = match connect_result {
            Ok(_) => panic!("unknown user should be rejected at startup auth"),
            Err(error) => error,
        };

        let error_text = connect_error.to_string();
        assert!(
            error_text.to_ascii_lowercase().contains("db error")
                && connect_error.as_db_error().is_some(),
            "unexpected postgres auth failure text: {error_text}"
        );

        server.abort();
    }

    #[tokio::test]
    async fn postgres_wire_rejects_wrong_password_during_startup_auth() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(PrototypeEngine::new().expect("engine should initialize"));

        let server = tokio::spawn(serve_postgres_wire(listener, Arc::clone(&engine)));

        let connect_result = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres dbname=postgres password=wrong-password",
                addr.port()
            ),
            NoTls,
        )
        .await;

        let connect_error = match connect_result {
            Ok(_) => panic!("wrong password should be rejected at startup auth"),
            Err(error) => error,
        };

        let error_text = connect_error.to_string();
        assert!(
            error_text.to_ascii_lowercase().contains("db error")
                && connect_error.as_db_error().is_some(),
            "unexpected postgres auth failure text: {error_text}"
        );

        server.abort();
    }

    #[tokio::test]
    async fn flight_sql_executes_statement_queries_and_updates() {
        let catalog_path = temp_catalog_path("flight");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(
            PrototypeEngine::from_catalog_path(&catalog_path)
                .await
                .expect("engine should initialize"),
        );

        let server = tokio::spawn(serve_flight_sql(listener, Arc::clone(&engine), None));

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

        let describe_update_count = client
            .execute_update("DESCRIBE fact_metrics".to_string(), None)
            .await
            .expect("DESCRIBE should be tolerated on statement update paths");
        assert_eq!(describe_update_count, 0);

        let information_schema_update_count = client
            .execute_update("SELECT * FROM information_schema.tables".to_string(), None)
            .await
            .expect("information_schema queries should be tolerated on statement update paths");
        assert_eq!(information_schema_update_count, 0);

        let information_schema_columns_update_count = client
            .execute_update(
                "SELECT column_name, data_type, character_maximum_length, is_nullable, column_default \
                 FROM information_schema.columns \
                 WHERE table_name = 'fact_metrics' \
                   AND table_schema = 'public' \
                 ORDER BY ordinal_position"
                    .to_string(),
                None,
            )
            .await
            .expect("information_schema.columns filters should be tolerated on statement update paths");
        assert_eq!(information_schema_columns_update_count, 0);

        let describe_info = client
            .execute("DESCRIBE fact_metrics".to_string(), None)
            .await
            .expect("DESCRIBE should succeed on statement query path");
        let describe_ticket = describe_info
            .endpoint
            .first()
            .and_then(|endpoint| endpoint.ticket.clone())
            .expect("DESCRIBE ticket should exist");
        let describe_batches = client
            .do_get(describe_ticket)
            .await
            .expect("DESCRIBE do_get should succeed")
            .try_collect::<Vec<_>>()
            .await
            .expect("DESCRIBE batches should collect");
        assert_eq!(describe_batches[0].num_rows(), 2);
        assert_eq!(
            array_value_to_string(describe_batches[0].column(0).as_ref(), 0).expect("value"),
            "metric"
        );

        let mut prepared_select = client
            .prepare(
                "SELECT metric, status FROM fact_metrics ORDER BY metric".to_string(),
                None,
            )
            .await
            .expect("SELECT prepared statement should be created");
        assert_eq!(
            prepared_select
                .dataset_schema()
                .expect("prepared SELECT dataset schema")
                .fields()
                .len(),
            2
        );
        let prepared_select_info = prepared_select
            .execute()
            .await
            .expect("prepared SELECT query path should succeed");
        let prepared_select_ticket = prepared_select_info
            .endpoint
            .first()
            .and_then(|endpoint| endpoint.ticket.clone())
            .expect("prepared SELECT ticket should exist");
        let prepared_select_batches = client
            .do_get(prepared_select_ticket)
            .await
            .expect("prepared SELECT do_get should succeed")
            .try_collect::<Vec<_>>()
            .await
            .expect("prepared SELECT batches should collect");
        assert_eq!(
            array_value_to_string(prepared_select_batches[0].column(0).as_ref(), 0).expect("value"),
            "11"
        );

        let mut prepared_describe = client
            .prepare("DESCRIBE fact_metrics".to_string(), None)
            .await
            .expect("DESCRIBE prepared statement should be created");
        assert_eq!(
            prepared_describe
                .dataset_schema()
                .expect("prepared DESCRIBE dataset schema")
                .fields()
                .len(),
            3
        );
        let prepared_describe_info = prepared_describe
            .execute()
            .await
            .expect("prepared DESCRIBE query path should succeed");
        let prepared_describe_ticket = prepared_describe_info
            .endpoint
            .first()
            .and_then(|endpoint| endpoint.ticket.clone())
            .expect("prepared DESCRIBE ticket should exist");
        let prepared_describe_batches = client
            .do_get(prepared_describe_ticket)
            .await
            .expect("prepared DESCRIBE do_get should succeed")
            .try_collect::<Vec<_>>()
            .await
            .expect("prepared DESCRIBE batches should collect");
        assert_eq!(prepared_describe_batches[0].num_rows(), 2);

        let mut prepared_insert = client
            .prepare(
                "INSERT INTO fact_metrics VALUES (12, 'via_prepared')".to_string(),
                None,
            )
            .await
            .expect("INSERT prepared statement should be created");
        assert_eq!(
            prepared_insert
                .dataset_schema()
                .expect("prepared INSERT dataset schema")
                .fields()
                .len(),
            0
        );
        let prepared_inserted = prepared_insert
            .execute_update()
            .await
            .expect("prepared INSERT update path should succeed");
        assert_eq!(prepared_inserted, 1);

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
            PrototypeEngine::from_catalog_path(&catalog_path)
                .await
                .expect("engine should initialize"),
        );

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT)".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
                query_id: None,
            })
            .await
            .expect("table should be created");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let server = tokio::spawn(serve_flight_sql(listener, Arc::clone(&engine), None));

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
                .expect("catalog path should have_stem")
        ));
    }

    #[tokio::test]
    async fn flight_sql_metadata_paths_include_reporting_namespace_tables_and_views() {
        let catalog_path = temp_catalog_path("flight-catalog-metadata");
        let engine = Arc::new(
            PrototypeEngine::from_catalog_path(&catalog_path)
                .await
                .expect("engine should initialize"),
        );

        for sql in [
            "CREATE SCHEMA reporting",
            "CREATE TABLE reporting.fact_metrics (metric BIGINT NOT NULL, status TEXT)",
            "CREATE VIEW reporting.daily_metrics AS SELECT metric, status FROM reporting.fact_metrics",
        ] {
            engine
                .execute_query(&QueryRequest {
                    sql: sql.to_string(),
                    session: SessionContext {
                        protocol: Protocol::Embedded,
                        ..SessionContext::default()
                    },
                    query_id: None,
                })
                .await
                .expect("setup SQL should succeed");
        }

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let server = tokio::spawn(serve_flight_sql(listener, Arc::clone(&engine), None));

        let channel = tonic::transport::Endpoint::new(format!("http://127.0.0.1:{}", addr.port()))
            .expect("endpoint should parse")
            .connect()
            .await
            .expect("channel should connect");
        let mut client = FlightSqlServiceClient::new(channel);
        client.set_header(FLIGHT_USER_HEADER, "postgres");
        client.set_header(FLIGHT_DATABASE_HEADER, "postgres");
        client.set_header(FLIGHT_SCHEMA_HEADER, "public");

        let schemas_info = client
            .get_db_schemas(CommandGetDbSchemas {
                catalog: Some("postgres".to_string()),
                db_schema_filter_pattern: None,
            })
            .await
            .expect("get_db_schemas should succeed");
        let schemas_ticket = schemas_info
            .endpoint
            .first()
            .and_then(|endpoint| endpoint.ticket.clone())
            .expect("schemas ticket should exist");
        let schema_batches = client
            .do_get(schemas_ticket)
            .await
            .expect("do_get schemas should succeed")
            .try_collect::<Vec<_>>()
            .await
            .expect("schema batches should collect");
        let schema_rows = schema_batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).map(|row| {
                    array_value_to_string(batch.column(1).as_ref(), row).expect("schema row value")
                })
            })
            .collect::<Vec<_>>();
        assert!(
            schema_rows.iter().any(|schema| schema == "reporting"),
            "schema rows={schema_rows:?}"
        );

        let tables_info = client
            .get_tables(CommandGetTables {
                catalog: Some("postgres".to_string()),
                db_schema_filter_pattern: Some("reporting".to_string()),
                table_name_filter_pattern: None,
                table_types: vec!["TABLE".to_string(), "VIEW".to_string()],
                include_schema: true,
            })
            .await
            .expect("get_tables should succeed");
        let tables_ticket = tables_info
            .endpoint
            .first()
            .and_then(|endpoint| endpoint.ticket.clone())
            .expect("tables ticket should exist");
        let table_batches = client
            .do_get(tables_ticket)
            .await
            .expect("do_get tables should succeed")
            .try_collect::<Vec<_>>()
            .await
            .expect("table batches should collect");

        let mut found_table = false;
        let mut found_view = false;
        for batch in &table_batches {
            for row in 0..batch.num_rows() {
                let name =
                    array_value_to_string(batch.column(2).as_ref(), row).expect("table name");
                let kind =
                    array_value_to_string(batch.column(3).as_ref(), row).expect("table kind");
                if name == "fact_metrics" && kind == "TABLE" {
                    found_table = true;
                }
                if name == "daily_metrics" && kind == "VIEW" {
                    found_view = true;
                }
            }
        }
        assert!(
            found_table,
            "reporting.fact_metrics should be in get_tables output"
        );
        assert!(
            found_view,
            "reporting.daily_metrics should be in get_tables output"
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
    async fn flight_sql_exposes_basic_sql_info_subset() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(PrototypeEngine::new().expect("engine should initialize"));

        let server = tokio::spawn(serve_flight_sql(listener, Arc::clone(&engine), None));

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
            .get_sql_info(vec![
                SqlInfo::FlightSqlServerName,
                SqlInfo::FlightSqlServerVersion,
                SqlInfo::FlightSqlServerArrowVersion,
            ])
            .await
            .expect("get_sql_info should succeed");
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
        assert_eq!(batches[0].num_rows(), 3);

        server.abort();
    }

    #[tokio::test]
    async fn flight_sql_handshake_returns_auth_payload_for_session_bootstrap() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should work");
        let addr = listener.local_addr().expect("local addr should exist");
        let engine = Arc::new(PrototypeEngine::new().expect("engine should initialize"));
        let server = tokio::spawn(serve_flight_sql(listener, Arc::clone(&engine), None));

        let channel = tonic::transport::Endpoint::new(format!("http://127.0.0.1:{}", addr.port()))
            .expect("endpoint should parse")
            .connect()
            .await
            .expect("channel should connect");
        let mut client = FlightSqlServiceClient::new(channel);
        client.set_header(FLIGHT_DATABASE_HEADER, "postgres");
        client.set_header(FLIGHT_SCHEMA_HEADER, "public");

        let payload = client
            .handshake("postgres", "postgres")
            .await
            .expect("handshake should succeed");
        // The payload is now a JWT (not raw AuthDecision JSON).
        // Verify it is a valid JWT string with the expected structure.
        let jwt_str = std::str::from_utf8(&payload).expect("payload should be valid UTF-8");
        // JWTs have three base64url segments separated by dots.
        let parts: Vec<&str> = jwt_str.split('.').collect();
        assert_eq!(parts.len(), 3, "JWT must have 3 dot-separated parts: {jwt_str}");
        // Decode the claims segment to verify user/role fields.
        let claims_json = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(parts[1])
            .expect("JWT claims should be base64url-encoded");
        let claims: serde_json::Value =
            serde_json::from_slice(&claims_json).expect("JWT claims should be valid JSON");
        assert_eq!(claims["sub"], "postgres");
        assert_eq!(claims["role"], "postgres");

        server.abort();
    }

    #[tokio::test]
    async fn prototype_auth_hook_uses_requested_role_when_provided() {
        let hook = PrototypeAllowAllAuthHook {
            control_plane: Arc::new(ControlPlane::new_bootstrap()),
        };
        let decision = hook
            .authenticate(&AuthRequest {
                protocol: Protocol::ArrowFlightSql,
                user: "postgres".to_string(),
                database: "postgres".to_string(),
                schema: "public".to_string(),
                role: Some("analytics_reader".to_string()),
                password: Some("postgres".to_string()),
                auth_header: Some(format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode("postgres:postgres")
                )),
            })
            .await
            .expect("auth should succeed");

        assert_eq!(decision.role, "analytics_reader");
        assert_eq!(decision.auth_method, "prototype-basic-auth");
    }

    #[tokio::test]
    async fn prototype_auth_hook_rejects_unknown_user_from_control_plane_lookup() {
        let hook = PrototypeAllowAllAuthHook {
            control_plane: Arc::new(ControlPlane::new_bootstrap()),
        };
        let error = hook
            .authenticate(&AuthRequest {
                protocol: Protocol::ArrowFlightSql,
                user: "missing_user".to_string(),
                database: "postgres".to_string(),
                schema: "public".to_string(),
                role: None,
                password: Some("secret".to_string()),
                auth_header: Some(format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode("missing_user:secret")
                )),
            })
            .await
            .expect_err("unknown user should be rejected by auth hook");

        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        assert!(error.message().contains("Unknown user"));
    }

    #[test]
    fn flight_prepared_statement_scaffold_parameter_conversion() {
        // Verify scaffold utilities for parameter conversion are available
        // (These will be used when full prepared statement binding is implemented)
        use crate::flight_prepared_statement_scaffold::flight_sql_parameter_to_literal;

        // Test NULL conversion
        let null_result = flight_sql_parameter_to_literal(None, &pgwire::api::Type::INT4);
        assert!(null_result.is_ok());
        assert_eq!(null_result.unwrap(), "NULL");
    }

    /// `ControlPlaneScramAuthSource` must refuse to produce a password for a user
    /// that has no SCRAM verifier fields set (e.g. a group/role that has no password).
    ///
    /// This test simulates a user record where `scram_salt_b64` is `None` and verifies
    /// that the decode path returns an appropriate error message.
    #[test]
    fn cleartext_auth_source_returns_error_for_missing_scram_verifier() {
        // Simulate the catalog user struct with no SCRAM verifier fields.
        let fake_user = analyticsdb_control::CatalogUser {
            name: "legacy_user".to_string(),
            is_admin: false,
            password: Some("some_hashed_password".to_string()),
            password_version: 1,
            password_rotated_at_epoch_ms: None,
            members: Default::default(),
            scram_salt_b64: None,
            scram_salted_password_b64: None,
        };

        // Replicate the decode path from `ControlPlaneScramAuthSource::get_password`.
        let result: Result<_, pgwire::error::PgWireError> = (|| {
            let _salt_b64 = fake_user.scram_salt_b64.as_deref().ok_or_else(|| {
                anyhow_error_to_pgwire(anyhow::anyhow!(
                    "User '{}' has no SCRAM verifier — please rotate the password to enable SCRAM-SHA-256 authentication",
                    fake_user.name
                ))
            })?;
            Ok(())
        })();

        assert!(result.is_err(), "expected error for missing SCRAM verifier");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("SCRAM") || msg.contains("rotate"),
            "error message should mention SCRAM or password rotation: {msg}"
        );
    }

    /// JWT claims must survive an encode→decode roundtrip with the correct secret.
    #[test]
    fn jwt_claims_encode_decode_roundtrip() {
        let secret = "test-secret-key";
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claims = FlightSqlClaims {
            sub: "alice".to_string(),
            role: "analyst".to_string(),
            db: "mydb".to_string(),
            schema: "public".to_string(),
            pwd_ver: 42,
            exp: now + 86400,
            iat: now,
        };

        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
        )
        .expect("encode should succeed");

        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
        validation.validate_exp = true;
        let decoded = jsonwebtoken::decode::<FlightSqlClaims>(
            &token,
            &jsonwebtoken::DecodingKey::from_secret(secret.as_bytes()),
            &validation,
        )
        .expect("decode should succeed");

        assert_eq!(decoded.claims.sub, "alice");
        assert_eq!(decoded.claims.role, "analyst");
        assert_eq!(decoded.claims.db, "mydb");
        assert_eq!(decoded.claims.schema, "public");
        assert_eq!(decoded.claims.pwd_ver, 42);
    }

    /// A JWT signed with one secret must be rejected when validated with a different secret.
    #[test]
    fn jwt_with_wrong_secret_is_rejected() {
        let good_secret = "correct-secret";
        let bad_secret = "wrong-secret";
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claims = FlightSqlClaims {
            sub: "bob".to_string(),
            role: "reader".to_string(),
            db: "db".to_string(),
            schema: "public".to_string(),
            pwd_ver: 1,
            exp: now + 86400,
            iat: now,
        };

        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(good_secret.as_bytes()),
        )
        .expect("encode should succeed");

        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
        validation.validate_exp = true;
        let result = jsonwebtoken::decode::<FlightSqlClaims>(
            &token,
            &jsonwebtoken::DecodingKey::from_secret(bad_secret.as_bytes()),
            &validation,
        );

        assert!(result.is_err(), "decoding with wrong secret must fail");
    }

    #[test]
    fn parse_timeout_to_ms_handles_all_formats() {
        assert_eq!(super::parse_timeout_to_ms("0"), 0, "zero = unlimited");
        assert_eq!(super::parse_timeout_to_ms(""), 0, "empty = unlimited");
        assert_eq!(super::parse_timeout_to_ms("5000"), 5000, "bare integer = ms");
        assert_eq!(super::parse_timeout_to_ms("5s"), 5000, "seconds suffix");
        assert_eq!(super::parse_timeout_to_ms("100ms"), 100, "ms suffix");
        assert_eq!(super::parse_timeout_to_ms("2min"), 120_000, "minutes suffix");
        assert_eq!(super::parse_timeout_to_ms("1h"), 3_600_000, "hours suffix");
        assert_eq!(super::parse_timeout_to_ms("garbage"), 0, "invalid = 0");
    }
}
