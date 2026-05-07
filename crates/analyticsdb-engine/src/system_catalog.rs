use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{BooleanArray, Int16Array, Int32Array, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, SchemaProvider, Session};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    project_schema, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};

use analyticsdb_control::{CatalogTableConstraintKind, ControlPlane};
use analyticsdb_core::SessionContext;

pub struct AnalyticsCatalogProvider {
    control_plane: Arc<ControlPlane>,
    session: SessionContext,
    inner: Arc<MemoryCatalogProvider>,
}

impl AnalyticsCatalogProvider {
    pub fn new(control_plane: Arc<ControlPlane>, session: SessionContext) -> Self {
        let inner = Arc::new(MemoryCatalogProvider::new());
        // For AnalyticsDB, we provide a dynamic schema provider for all schemas in the database
        Self {
            control_plane,
            session,
            inner,
        }
    }
}

impl std::fmt::Debug for AnalyticsCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnalyticsCatalogProvider")
            .field("session", &self.session)
            .finish()
    }
}

#[async_trait]
impl CatalogProvider for AnalyticsCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        // We should probably return all schemas from control plane here.
        // But for simplicity and to match DataFusion's expectation, we'll return at least what's in inner + public
        let mut names = self.inner.schema_names();
        if !names.contains(&"public".to_string()) {
            names.push("public".to_string());
        }
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if let Some(s) = self.inner.schema(name) {
            return Some(s);
        }

        // Return a dynamic schema provider that loads from control plane
        Some(Arc::new(AnalyticsSchemaProvider::new(
            Arc::clone(&self.control_plane),
            self.session.clone(),
            name.to_string(),
        )))
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> DataFusionResult<Option<Arc<dyn SchemaProvider>>> {
        self.inner.register_schema(name, schema)
    }
}

#[derive(Debug)]
pub struct AnalyticsSchemaProvider {
    control_plane: Arc<ControlPlane>,
    session: SessionContext,
    schema_name: String,
}

impl AnalyticsSchemaProvider {
    pub fn new(
        control_plane: Arc<ControlPlane>,
        session: SessionContext,
        schema_name: String,
    ) -> Self {
        Self {
            control_plane,
            session,
            schema_name,
        }
    }
}

#[async_trait]
impl SchemaProvider for AnalyticsSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // This is tricky as it's async in control plane.
        // For the prototype, we'll block or just return names from a snapshot.
        let cluster = futures::executor::block_on(self.control_plane.cluster_snapshot());
        cluster
            .relations
            .iter()
            .filter(|r| r.database == self.session.database && r.schema == self.schema_name)
            .map(|r| r.name.clone())
            .collect()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let relation_result = self
            .control_plane
            .find_relation(
                &self.session,
                Some(&self.session.database),
                Some(&self.schema_name),
                name,
            )
            .await;

        let relation = match relation_result {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        if relation.kind == analyticsdb_control::CatalogRelationKind::Table {
            if let Some(storage_path) = &relation.storage_path {
                let schema = catalog_relation_to_schema(&relation);

                let table_path = ListingTableUrl::parse(storage_path)?;
                let config = ListingTableConfig::new(table_path)
                    .with_listing_options(ListingOptions::new(Arc::new(ParquetFormat::default())))
                    .with_schema(schema);

                let table = ListingTable::try_new(config)?;
                return Ok(Some(Arc::new(table)));
            }
        }

        Ok(None)
    }

    fn table_exist(&self, name: &str) -> bool {
        let cluster = futures::executor::block_on(self.control_plane.cluster_snapshot());
        cluster.relations.iter().any(|r| {
            r.database == self.session.database && r.schema == self.schema_name && r.name == name
        })
    }
}

fn catalog_relation_to_schema(relation: &analyticsdb_control::CatalogRelation) -> SchemaRef {
    let mut fields: Vec<Field> = relation
        .columns
        .iter()
        .map(|c| {
            let lower = c.data_type.to_lowercase();
            let dt = if lower.starts_with("numeric") || lower.starts_with("decimal") {
                DataType::Decimal128(38, 10)
            } else {
                match lower.as_str() {
                    "bool" | "boolean" => DataType::Boolean,
                    "int2" | "smallint" => DataType::Int16,
                    "int4" | "integer" | "int" | "int32" => DataType::Int32,
                    "int8" | "bigint" | "int64" => DataType::Int64,
                    "float4" | "real" | "float32" => DataType::Float32,
                    "float8" | "double precision" | "double" | "float64" => DataType::Float64,
                    "text" | "varchar" | "char" | "utf8" => DataType::Utf8,
                    "date" => DataType::Date32,
                    _ => {
                        if lower.starts_with("timestamp") {
                            DataType::Timestamp(
                                datafusion::arrow::datatypes::TimeUnit::Nanosecond,
                                None,
                            )
                        } else {
                            DataType::Utf8 // Default to text for prototype
                        }
                    }
                }
            };
            Field::new(&c.name, dt, c.nullable)
        })
        .collect();

    if !fields.iter().any(|f| f.name() == "_row_id") {
        fields.push(Field::new("_row_id", DataType::Utf8, true));
    }

    Arc::new(Schema::new(fields))
}

pub struct PgCatalogSchemaProvider {
    _control_plane: Arc<ControlPlane>,
    tables: BTreeMap<String, Arc<dyn TableProvider>>,
}

impl PgCatalogSchemaProvider {
    pub fn new(control_plane: Arc<ControlPlane>) -> Self {
        let mut tables: BTreeMap<String, Arc<dyn TableProvider>> = BTreeMap::new();

        tables.insert(
            "pg_tables".to_string(),
            Arc::new(PgTablesTable::new(Arc::clone(&control_plane))),
        );
        tables.insert(
            "pg_views".to_string(),
            Arc::new(PgViewsTable::new(Arc::clone(&control_plane))),
        );
        tables.insert(
            "pg_namespace".to_string(),
            Arc::new(PgNamespaceTable::new(Arc::clone(&control_plane))),
        );
        tables.insert(
            "pg_database".to_string(),
            Arc::new(PgDatabaseTable::new(Arc::clone(&control_plane))),
        );
        tables.insert(
            "pg_roles".to_string(),
            Arc::new(PgRolesTable::new(Arc::clone(&control_plane))),
        );
        tables.insert(
            "pg_type".to_string(),
            Arc::new(PgTypeTable::new(Arc::clone(&control_plane))),
        );
        tables.insert(
            "pg_class".to_string(),
            Arc::new(PgClassTable::new(Arc::clone(&control_plane))),
        );
        tables.insert(
            "pg_attribute".to_string(),
            Arc::new(PgAttributeTable::new(Arc::clone(&control_plane))),
        );
        tables.insert(
            "pg_index".to_string(),
            Arc::new(PgIndexTable::new(Arc::clone(&control_plane))),
        );
        tables.insert(
            "pg_description".to_string(),
            Arc::new(PgDescriptionTable::new(Arc::clone(&control_plane))),
        );
        tables.insert(
            "pg_attrdef".to_string(),
            Arc::new(PgAttrdefTable::new(Arc::clone(&control_plane))),
        );
        tables.insert(
            "pg_depend".to_string(),
            Arc::new(PgDependTable::new(Arc::clone(&control_plane))),
        );
        tables.insert(
            "pg_constraint".to_string(),
            Arc::new(PgConstraintTable::new(Arc::clone(&control_plane))),
        );

        Self {
            _control_plane: control_plane,
            tables,
        }
    }
}

impl std::fmt::Debug for PgCatalogSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgCatalogSchemaProvider")
            .field("table_names", &self.table_names())
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for PgCatalogSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.get(name).cloned())
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

#[derive(Debug)]
struct PgTablesTable {
    control_plane: Arc<ControlPlane>,
    schema: SchemaRef,
}

impl PgTablesTable {
    fn new(control_plane: Arc<ControlPlane>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("schemaname", DataType::Utf8, false),
            Field::new("tablename", DataType::Utf8, false),
            Field::new("tableowner", DataType::Utf8, false),
            Field::new("tablespace", DataType::Utf8, true),
            Field::new("hasindexes", DataType::Utf8, false),
            Field::new("hasrules", DataType::Utf8, false),
            Field::new("hastriggers", DataType::Utf8, false),
            Field::new("rowsecurity", DataType::Utf8, false),
        ]));
        Self {
            control_plane,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for PgTablesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let session = postgres_session_from_state(state);
        let cluster = self.control_plane.cluster_snapshot().await;

        let mut schemaname = Vec::new();
        let mut tablename = Vec::new();
        let mut tableowner = Vec::new();
        let mut tablespace = Vec::new();
        let mut hasindexes = Vec::new();
        let mut hasrules = Vec::new();
        let mut hastriggers = Vec::new();
        let mut rowsecurity = Vec::new();

        for rel in &cluster.relations {
            if rel.kind == analyticsdb_control::CatalogRelationKind::Table
                && rel.database == session.database
            {
                schemaname.push(rel.schema.clone());
                tablename.push(rel.name.clone());
                tableowner.push("postgres".to_string());
                tablespace.push(None::<String>);
                hasindexes.push((!rel.indexes.is_empty()).to_string());
                hasrules.push("false".to_string());
                hastriggers.push("false".to_string());
                rowsecurity.push("false".to_string());
            }
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(StringArray::from(schemaname)),
                Arc::new(StringArray::from(tablename)),
                Arc::new(StringArray::from(tableowner)),
                Arc::new(StringArray::from(tablespace)),
                Arc::new(StringArray::from(hasindexes)),
                Arc::new(StringArray::from(hasrules)),
                Arc::new(StringArray::from(hastriggers)),
                Arc::new(StringArray::from(rowsecurity)),
            ],
        )?;

        Ok(Arc::new(SystemCatalogExec::new(batch, projection)))
    }
}

#[derive(Debug)]
struct PgViewsTable {
    control_plane: Arc<ControlPlane>,
    schema: SchemaRef,
}

impl PgViewsTable {
    fn new(control_plane: Arc<ControlPlane>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("schemaname", DataType::Utf8, false),
            Field::new("viewname", DataType::Utf8, false),
            Field::new("viewowner", DataType::Utf8, false),
            Field::new("definition", DataType::Utf8, false),
        ]));
        Self {
            control_plane,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for PgViewsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let session = postgres_session_from_state(state);
        let cluster = self.control_plane.cluster_snapshot().await;

        let mut schemaname = Vec::new();
        let mut viewname = Vec::new();
        let mut viewowner = Vec::new();
        let mut definition = Vec::new();

        for rel in &cluster.relations {
            if rel.kind == analyticsdb_control::CatalogRelationKind::View
                && rel.database == session.database
            {
                schemaname.push(rel.schema.clone());
                viewname.push(rel.name.clone());
                viewowner.push("postgres".to_string());
                definition.push(rel.definition_sql.clone().unwrap_or_default());
            }
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(StringArray::from(schemaname)),
                Arc::new(StringArray::from(viewname)),
                Arc::new(StringArray::from(viewowner)),
                Arc::new(StringArray::from(definition)),
            ],
        )?;

        Ok(Arc::new(SystemCatalogExec::new(batch, projection)))
    }
}

#[derive(Debug)]
struct PgNamespaceTable {
    control_plane: Arc<ControlPlane>,
    schema: SchemaRef,
}

impl PgNamespaceTable {
    fn new(control_plane: Arc<ControlPlane>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("nspname", DataType::Utf8, false),
            Field::new("nspowner", DataType::Utf8, false),
            Field::new("nspacl", DataType::Utf8, true),
        ]));
        Self {
            control_plane,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for PgNamespaceTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let session = postgres_session_from_state(state);
        let schemas = self
            .control_plane
            .list_schemas(&session, Some(&session.database))
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        let mut oid = Vec::with_capacity(schemas.len() + 1);
        let mut nspname = Vec::with_capacity(schemas.len() + 1);
        let mut nspowner = Vec::with_capacity(schemas.len() + 1);
        let mut nspacl = Vec::with_capacity(schemas.len() + 1);

        // Standard pg_catalog
        oid.push(11);
        nspname.push("pg_catalog".to_string());
        nspowner.push("postgres".to_string());
        nspacl.push(None::<String>);

        for schema in schemas {
            oid.push(synthetic_namespace_oid(&session.database, &schema));
            nspname.push(schema);
            nspowner.push("postgres".to_string());
            nspacl.push(None::<String>);
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(UInt32Array::from(oid)),
                Arc::new(StringArray::from(nspname)),
                Arc::new(StringArray::from(nspowner)),
                Arc::new(StringArray::from(nspacl)),
            ],
        )?;

        Ok(Arc::new(SystemCatalogExec::new(batch, projection)))
    }
}

#[derive(Debug)]
struct PgDatabaseTable {
    control_plane: Arc<ControlPlane>,
    schema: SchemaRef,
}

impl PgDatabaseTable {
    fn new(control_plane: Arc<ControlPlane>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("datname", DataType::Utf8, false),
            Field::new("datdba", DataType::UInt32, false),
            Field::new("encoding", DataType::Int32, false),
            Field::new("datcollate", DataType::Utf8, false),
            Field::new("datctype", DataType::Utf8, false),
            Field::new("datistemplate", DataType::Boolean, false),
            Field::new("datallowconn", DataType::Boolean, false),
            Field::new("datconnlimit", DataType::Int32, false),
            Field::new("datlastsysoid", DataType::UInt32, false),
            Field::new("datfrozenxid", DataType::UInt32, false),
            Field::new("datminmxid", DataType::UInt32, false),
            Field::new("dattablespace", DataType::UInt32, false),
            Field::new("datacl", DataType::Utf8, true),
        ]));
        Self {
            control_plane,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for PgDatabaseTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let session = postgres_session_from_state(state);
        let databases = self
            .control_plane
            .list_databases(&session)
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        let mut oid = Vec::with_capacity(databases.len());
        let mut datname = Vec::with_capacity(databases.len());
        let mut datdba = Vec::with_capacity(databases.len());
        let mut encoding = Vec::with_capacity(databases.len());
        let mut datcollate = Vec::with_capacity(databases.len());
        let mut datctype = Vec::with_capacity(databases.len());
        let mut datistemplate = Vec::with_capacity(databases.len());
        let mut datallowconn = Vec::with_capacity(databases.len());
        let mut datconnlimit = Vec::with_capacity(databases.len());
        let mut datlastsysoid = Vec::with_capacity(databases.len());
        let mut datfrozenxid = Vec::with_capacity(databases.len());
        let mut datminmxid = Vec::with_capacity(databases.len());
        let mut dattablespace = Vec::with_capacity(databases.len());
        let mut datacl = Vec::with_capacity(databases.len());

        for db in databases {
            oid.push(synthetic_database_oid(&db));
            datname.push(db);
            datdba.push(10_u32);
            encoding.push(6_i32);
            datcollate.push("C".to_string());
            datctype.push("C".to_string());
            datistemplate.push(false);
            datallowconn.push(true);
            datconnlimit.push(-1_i32);
            datlastsysoid.push(0_u32);
            datfrozenxid.push(0_u32);
            datminmxid.push(1_u32);
            dattablespace.push(1663_u32);
            datacl.push(None::<String>);
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(UInt32Array::from(oid)),
                Arc::new(StringArray::from(datname)),
                Arc::new(datafusion::arrow::array::UInt32Array::from(datdba)),
                Arc::new(datafusion::arrow::array::Int32Array::from(encoding)),
                Arc::new(StringArray::from(datcollate)),
                Arc::new(StringArray::from(datctype)),
                Arc::new(datafusion::arrow::array::BooleanArray::from(datistemplate)),
                Arc::new(datafusion::arrow::array::BooleanArray::from(datallowconn)),
                Arc::new(datafusion::arrow::array::Int32Array::from(datconnlimit)),
                Arc::new(datafusion::arrow::array::UInt32Array::from(datlastsysoid)),
                Arc::new(datafusion::arrow::array::UInt32Array::from(datfrozenxid)),
                Arc::new(datafusion::arrow::array::UInt32Array::from(datminmxid)),
                Arc::new(datafusion::arrow::array::UInt32Array::from(dattablespace)),
                Arc::new(StringArray::from(datacl)),
            ],
        )?;

        Ok(Arc::new(SystemCatalogExec::new(batch, projection)))
    }
}

#[derive(Debug)]
struct PgRolesTable {
    control_plane: Arc<ControlPlane>,
    schema: SchemaRef,
}

impl PgRolesTable {
    fn new(control_plane: Arc<ControlPlane>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("rolname", DataType::Utf8, false),
            Field::new("rolsuper", DataType::Boolean, false),
            Field::new("rolinherit", DataType::Boolean, false),
            Field::new("rolcreaterole", DataType::Boolean, false),
            Field::new("rolcreatedb", DataType::Boolean, false),
            Field::new("rolcanlogin", DataType::Boolean, false),
            Field::new("rolreplication", DataType::Boolean, false),
            Field::new("rolbypassrls", DataType::Boolean, false),
            Field::new("rolconnlimit", DataType::Int32, false),
            Field::new("rolpassword", DataType::Utf8, true),
            Field::new("rolvaliduntil", DataType::Utf8, true),
        ]));
        Self {
            control_plane,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for PgRolesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mut users = self.control_plane.cluster_snapshot().await.users;
        users.sort_by(|left, right| left.name.cmp(&right.name));

        let mut oid = Vec::with_capacity(users.len());
        let mut rolname = Vec::with_capacity(users.len());
        let mut rolsuper = Vec::with_capacity(users.len());
        let mut rolinherit = Vec::with_capacity(users.len());
        let mut rolcreaterole = Vec::with_capacity(users.len());
        let mut rolcreatedb = Vec::with_capacity(users.len());
        let mut rolcanlogin = Vec::with_capacity(users.len());
        let mut rolreplication = Vec::with_capacity(users.len());
        let mut rolbypassrls = Vec::with_capacity(users.len());
        let mut rolconnlimit = Vec::with_capacity(users.len());
        let mut rolpassword = Vec::with_capacity(users.len());
        let mut rolvaliduntil = Vec::with_capacity(users.len());

        for user in users {
            oid.push(synthetic_role_oid(&user.name));
            rolname.push(user.name);
            rolsuper.push(user.is_admin);
            rolinherit.push(true);
            rolcreaterole.push(user.is_admin);
            rolcreatedb.push(user.is_admin);
            rolcanlogin.push(true);
            rolreplication.push(false);
            rolbypassrls.push(false);
            rolconnlimit.push(-1_i32);
            rolpassword.push(None::<String>);
            rolvaliduntil.push(None::<String>);
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(UInt32Array::from(oid)),
                Arc::new(StringArray::from(rolname)),
                Arc::new(datafusion::arrow::array::BooleanArray::from(rolsuper)),
                Arc::new(datafusion::arrow::array::BooleanArray::from(rolinherit)),
                Arc::new(datafusion::arrow::array::BooleanArray::from(rolcreaterole)),
                Arc::new(datafusion::arrow::array::BooleanArray::from(rolcreatedb)),
                Arc::new(datafusion::arrow::array::BooleanArray::from(rolcanlogin)),
                Arc::new(datafusion::arrow::array::BooleanArray::from(rolreplication)),
                Arc::new(datafusion::arrow::array::BooleanArray::from(rolbypassrls)),
                Arc::new(datafusion::arrow::array::Int32Array::from(rolconnlimit)),
                Arc::new(StringArray::from(rolpassword)),
                Arc::new(StringArray::from(rolvaliduntil)),
            ],
        )?;

        Ok(Arc::new(SystemCatalogExec::new(batch, projection)))
    }
}

#[derive(Debug)]
struct PgTypeTable {
    _control_plane: Arc<ControlPlane>,
    schema: SchemaRef,
}

impl PgTypeTable {
    fn new(control_plane: Arc<ControlPlane>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("typname", DataType::Utf8, false),
            Field::new("typnamespace", DataType::UInt32, false),
            Field::new("typowner", DataType::UInt32, false),
            Field::new("typlen", DataType::Int16, false),
            Field::new("typbyval", DataType::Boolean, false),
            Field::new("typtype", DataType::Utf8, false),
            Field::new("typcategory", DataType::Utf8, false),
            Field::new("typispreferred", DataType::Boolean, false),
            Field::new("typisdefined", DataType::Boolean, false),
            Field::new("typdelim", DataType::Utf8, false),
            Field::new("typrelid", DataType::UInt32, false),
            Field::new("typelem", DataType::UInt32, false),
            Field::new("typarray", DataType::UInt32, false),
            Field::new("typinput", DataType::Utf8, false),
            Field::new("typbasetype", DataType::UInt32, false),
            Field::new("typtypmod", DataType::Int32, false),
            Field::new("typndims", DataType::Int32, false),
            Field::new("typcollation", DataType::UInt32, false),
        ]));
        Self {
            _control_plane: control_plane,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for PgTypeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Minimal types for JDBC/DBeaver
        let types = vec![
            (16, "bool", 1, true, "b", "B"),
            (18, "char", 1, true, "b", "S"),
            (20, "int8", 8, true, "b", "N"),
            (21, "int2", 2, true, "b", "N"),
            (23, "int4", 4, true, "b", "N"),
            (25, "text", -1, false, "b", "S"),
            (700, "float4", 4, true, "b", "N"),
            (701, "float8", 8, true, "b", "N"),
            (1114, "timestamp", 8, true, "b", "D"),
            (1184, "timestamptz", 8, true, "b", "D"),
        ];

        let mut oid = Vec::new();
        let mut typname = Vec::new();
        let mut typnamespace = Vec::new();
        let mut typowner = Vec::new();
        let mut typlen = Vec::new();
        let mut typbyval = Vec::new();
        let mut typtype = Vec::new();
        let mut typcategory = Vec::new();
        let mut typispreferred = Vec::new();
        let mut typisdefined = Vec::new();
        let mut typdelim = Vec::new();
        let mut typrelid = Vec::new();
        let mut typelem = Vec::new();
        let mut typarray = Vec::new();
        let mut typinput = Vec::new();
        let mut typbasetype = Vec::new();
        let mut typtypmod = Vec::new();
        let mut typndims = Vec::new();
        let mut typcollation = Vec::new();

        for (o, name, len, byval, t, cat) in types {
            oid.push(o as u32);
            typname.push(name.to_string());
            typnamespace.push(11_u32); // pg_catalog
            typowner.push(10_u32);
            typlen.push(len as i16);
            typbyval.push(byval);
            typtype.push(t.to_string());
            typcategory.push(cat.to_string());
            typispreferred.push(false);
            typisdefined.push(true);
            typdelim.push(",".to_string());
            typrelid.push(0_u32);
            typelem.push(0_u32);
            typarray.push(0_u32);
            typinput.push("-".to_string());
            typbasetype.push(0_u32);
            typtypmod.push(-1_i32);
            typndims.push(0_i32);
            typcollation.push(0_u32);
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(UInt32Array::from(oid)),
                Arc::new(StringArray::from(typname)),
                Arc::new(UInt32Array::from(typnamespace)),
                Arc::new(UInt32Array::from(typowner)),
                Arc::new(Int16Array::from(typlen)),
                Arc::new(BooleanArray::from(typbyval)),
                Arc::new(StringArray::from(typtype)),
                Arc::new(StringArray::from(typcategory)),
                Arc::new(BooleanArray::from(typispreferred)),
                Arc::new(BooleanArray::from(typisdefined)),
                Arc::new(StringArray::from(typdelim)),
                Arc::new(UInt32Array::from(typrelid)),
                Arc::new(UInt32Array::from(typelem)),
                Arc::new(UInt32Array::from(typarray)),
                Arc::new(StringArray::from(typinput)),
                Arc::new(UInt32Array::from(typbasetype)),
                Arc::new(Int32Array::from(typtypmod)),
                Arc::new(Int32Array::from(typndims)),
                Arc::new(UInt32Array::from(typcollation)),
            ],
        )?;

        Ok(Arc::new(SystemCatalogExec::new(batch, projection)))
    }
}

#[derive(Debug)]
struct PgClassTable {
    control_plane: Arc<ControlPlane>,
    schema: SchemaRef,
}

impl PgClassTable {
    fn new(control_plane: Arc<ControlPlane>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("relname", DataType::Utf8, false),
            Field::new("relnamespace", DataType::UInt32, false),
            Field::new("reltype", DataType::UInt32, false),
            Field::new("reloftype", DataType::UInt32, false),
            Field::new("relowner", DataType::UInt32, false),
            Field::new("relam", DataType::UInt32, false),
            Field::new("relfilenode", DataType::UInt32, false),
            Field::new("reltablespace", DataType::UInt32, false),
            Field::new("relpages", DataType::Int32, false),
            Field::new("reltuples", DataType::Float32, false),
            Field::new("relallvisible", DataType::Int32, false),
            Field::new("reltoastrelid", DataType::UInt32, false),
            Field::new("relhasindex", DataType::Boolean, false),
            Field::new("relisshared", DataType::Boolean, false),
            Field::new("relpersistence", DataType::Utf8, false),
            Field::new("relkind", DataType::Utf8, false),
            Field::new("relnatts", DataType::Int16, false),
            Field::new("relchecks", DataType::Int16, false),
            Field::new("relhasrules", DataType::Boolean, false),
            Field::new("relhastriggers", DataType::Boolean, false),
            Field::new("relhassubclass", DataType::Boolean, false),
            Field::new("relrowsecurity", DataType::Boolean, false),
            Field::new("relforcerowsecurity", DataType::Boolean, false),
            Field::new("relispartition", DataType::Boolean, false),
            Field::new("relrewrite", DataType::UInt32, false),
            Field::new("relfrozenxid", DataType::UInt32, false),
            Field::new("relminmxid", DataType::UInt32, false),
            Field::new("relacl", DataType::Utf8, true),
            Field::new("reloptions", DataType::Utf8, true),
            Field::new("relpartbound", DataType::Utf8, true),
        ]));
        Self {
            control_plane,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for PgClassTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let session = postgres_session_from_state(state);
        let cluster = self.control_plane.cluster_snapshot().await;

        let mut oid = Vec::new();
        let mut relname = Vec::new();
        let mut relnamespace = Vec::new();
        let mut reltype = Vec::new();
        let mut reloftype = Vec::new();
        let mut relowner = Vec::new();
        let mut relam = Vec::new();
        let mut relfilenode = Vec::new();
        let mut reltablespace = Vec::new();
        let mut relpages = Vec::new();
        let mut reltuples = Vec::new();
        let mut relallvisible = Vec::new();
        let mut reltoastrelid = Vec::new();
        let mut relhasindex = Vec::new();
        let mut relisshared = Vec::new();
        let mut relpersistence = Vec::new();
        let mut relkind = Vec::new();
        let mut relnatts = Vec::new();
        let mut relchecks = Vec::new();
        let mut relhasrules = Vec::new();
        let mut relhastriggers = Vec::new();
        let mut relhassubclass = Vec::new();
        let mut relrowsecurity = Vec::new();
        let mut relforcerowsecurity = Vec::new();
        let mut relispartition = Vec::new();
        let mut relrewrite = Vec::new();
        let mut relfrozenxid = Vec::new();
        let mut relminmxid = Vec::new();
        let mut relacl = Vec::new();
        let mut reloptions = Vec::new();
        let mut relpartbound = Vec::new();

        for rel in &cluster.relations {
            if rel.database == session.database {
                oid.push(synthetic_relation_oid(
                    &rel.database,
                    &rel.schema,
                    &rel.name,
                ));
                relname.push(rel.name.clone());
                relnamespace.push(synthetic_namespace_oid(&rel.database, &rel.schema));
                reltype.push(0_u32);
                reloftype.push(0_u32);
                relowner.push(10_u32);
                relam.push(0_u32);
                relfilenode.push(0_u32);
                reltablespace.push(0_u32);
                relpages.push(0_i32);
                reltuples.push(0.0_f32);
                relallvisible.push(0_i32);
                reltoastrelid.push(0_u32);
                relhasindex.push(!rel.indexes.is_empty());
                relisshared.push(false);
                relpersistence.push("p".to_string());
                relkind.push(match rel.kind {
                    analyticsdb_control::CatalogRelationKind::Table => "r".to_string(),
                    analyticsdb_control::CatalogRelationKind::View => "v".to_string(),
                });
                let natts = rel.columns.iter().filter(|c| c.name != "_row_id").count();
                relnatts.push(natts as i16);
                relchecks.push(0_i16);
                relhasrules.push(false);
                relhastriggers.push(false);
                relhassubclass.push(false);
                relrowsecurity.push(false);
                relforcerowsecurity.push(false);
                relispartition.push(false);
                relrewrite.push(0_u32);
                relfrozenxid.push(0_u32);
                relminmxid.push(0_u32);
                relacl.push(None::<String>);
                reloptions.push(None::<String>);
                relpartbound.push(None::<String>);

                // Add indexes
                for index in &rel.indexes {
                    oid.push(synthetic_relation_oid(
                        &rel.database,
                        &rel.schema,
                        &index.name,
                    ));
                    relname.push(index.name.clone());
                    relnamespace.push(synthetic_namespace_oid(&rel.database, &rel.schema));
                    reltype.push(0_u32);
                    reloftype.push(0_u32);
                    relowner.push(10_u32);
                    relam.push(0_u32);
                    relfilenode.push(0_u32);
                    reltablespace.push(0_u32);
                    relpages.push(0_i32);
                    reltuples.push(0.0_f32);
                    relallvisible.push(0_i32);
                    reltoastrelid.push(0_u32);
                    relhasindex.push(false);
                    relisshared.push(false);
                    relpersistence.push("p".to_string());
                    relkind.push("i".to_string());
                    relnatts.push(index.columns.len() as i16);
                    relchecks.push(0_i16);
                    relhasrules.push(false);
                    relhastriggers.push(false);
                    relhassubclass.push(false);
                    relrowsecurity.push(false);
                    relforcerowsecurity.push(false);
                    relispartition.push(false);
                    relrewrite.push(0_u32);
                    relfrozenxid.push(0_u32);
                    relminmxid.push(0_u32);
                    relacl.push(None::<String>);
                    reloptions.push(None::<String>);
                    relpartbound.push(None::<String>);
                }
            }
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(UInt32Array::from(oid)),
                Arc::new(StringArray::from(relname)),
                Arc::new(UInt32Array::from(relnamespace)),
                Arc::new(UInt32Array::from(reltype)),
                Arc::new(UInt32Array::from(reloftype)),
                Arc::new(UInt32Array::from(relowner)),
                Arc::new(UInt32Array::from(relam)),
                Arc::new(UInt32Array::from(relfilenode)),
                Arc::new(UInt32Array::from(reltablespace)),
                Arc::new(datafusion::arrow::array::Int32Array::from(relpages)),
                Arc::new(datafusion::arrow::array::Float32Array::from(reltuples)),
                Arc::new(datafusion::arrow::array::Int32Array::from(relallvisible)),
                Arc::new(UInt32Array::from(reltoastrelid)),
                Arc::new(BooleanArray::from(relhasindex)),
                Arc::new(BooleanArray::from(relisshared)),
                Arc::new(StringArray::from(relpersistence)),
                Arc::new(StringArray::from(relkind)),
                Arc::new(Int16Array::from(relnatts)),
                Arc::new(Int16Array::from(relchecks)),
                Arc::new(BooleanArray::from(relhasrules)),
                Arc::new(BooleanArray::from(relhastriggers)),
                Arc::new(BooleanArray::from(relhassubclass)),
                Arc::new(BooleanArray::from(relrowsecurity)),
                Arc::new(BooleanArray::from(relforcerowsecurity)),
                Arc::new(BooleanArray::from(relispartition)),
                Arc::new(UInt32Array::from(relrewrite)),
                Arc::new(UInt32Array::from(relfrozenxid)),
                Arc::new(UInt32Array::from(relminmxid)),
                Arc::new(StringArray::from(relacl)),
                Arc::new(StringArray::from(reloptions)),
                Arc::new(StringArray::from(relpartbound)),
            ],
        )?;

        Ok(Arc::new(SystemCatalogExec::new(batch, projection)))
    }
}

#[derive(Debug)]
struct PgAttributeTable {
    control_plane: Arc<ControlPlane>,
    schema: SchemaRef,
}

impl PgAttributeTable {
    fn new(control_plane: Arc<ControlPlane>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("attrelid", DataType::UInt32, false),
            Field::new("attname", DataType::Utf8, false),
            Field::new("atttypid", DataType::UInt32, false),
            Field::new("attnum", DataType::Int16, false),
            Field::new("attnotnull", DataType::Boolean, false),
            Field::new("atttypmod", DataType::Int32, false),
            Field::new("attndims", DataType::Int32, false),
            Field::new("atthasdef", DataType::Boolean, false),
            Field::new("attidentity", DataType::Utf8, false),
            Field::new("attgenerated", DataType::Utf8, false),
            Field::new("attisdropped", DataType::Boolean, false),
            Field::new("attislocal", DataType::Boolean, false),
            Field::new("attinhcount", DataType::Int32, false),
            Field::new("attcollation", DataType::UInt32, false),
        ]));
        Self {
            control_plane,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for PgAttributeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let session = postgres_session_from_state(state);
        let cluster = self.control_plane.cluster_snapshot().await;

        let mut attrelid = Vec::new();
        let mut attname = Vec::new();
        let mut atttypid = Vec::new();
        let mut attnum = Vec::new();
        let mut attnotnull = Vec::new();
        let mut atttypmod = Vec::new();
        let mut attndims = Vec::new();
        let mut atthasdef = Vec::new();
        let mut attidentity = Vec::new();
        let mut attgenerated = Vec::new();
        let mut attisdropped = Vec::new();
        let mut attislocal = Vec::new();
        let mut attinhcount = Vec::new();
        let mut attcollation = Vec::new();

        for rel in &cluster.relations {
            if rel.database == session.database {
                let rel_oid = synthetic_relation_oid(&rel.database, &rel.schema, &rel.name);
                let mut att_num = 1;
                for col in &rel.columns {
                    if col.name == "_row_id" {
                        continue;
                    }
                    attrelid.push(rel_oid);
                    attname.push(col.name.clone());
                    atttypid.push(match col.data_type.to_lowercase().as_str() {
                        "bool" | "boolean" => 16,
                        "int2" | "smallint" => 21,
                        "int4" | "integer" | "int" => 23,
                        "int8" | "bigint" => 20,
                        "float4" | "real" => 700,
                        "float8" | "double precision" | "double" => 701,
                        _ => 25, // text
                    });
                    attnum.push(att_num as i16);
                    attnotnull.push(!col.nullable);
                    atttypmod.push(-1_i32);
                    attndims.push(0_i32);
                    atthasdef.push(col.default_value.is_some());
                    attidentity.push(" ".to_string());
                    attgenerated.push(" ".to_string());
                    attisdropped.push(false);
                    attislocal.push(true);
                    attinhcount.push(0_i32);
                    attcollation.push(0_u32);
                    att_num += 1;
                }
            }
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(UInt32Array::from(attrelid)),
                Arc::new(StringArray::from(attname)),
                Arc::new(UInt32Array::from(atttypid)),
                Arc::new(Int16Array::from(attnum)),
                Arc::new(BooleanArray::from(attnotnull)),
                Arc::new(Int32Array::from(atttypmod)),
                Arc::new(Int32Array::from(attndims)),
                Arc::new(BooleanArray::from(atthasdef)),
                Arc::new(StringArray::from(attidentity)),
                Arc::new(StringArray::from(attgenerated)),
                Arc::new(BooleanArray::from(attisdropped)),
                Arc::new(BooleanArray::from(attislocal)),
                Arc::new(Int32Array::from(attinhcount)),
                Arc::new(UInt32Array::from(attcollation)),
            ],
        )?;

        Ok(Arc::new(SystemCatalogExec::new(batch, projection)))
    }
}

#[derive(Debug)]
struct SystemCatalogExec {
    batch: RecordBatch,
    projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
}

impl SystemCatalogExec {
    fn new(batch: RecordBatch, projection: Option<&Vec<usize>>) -> Self {
        let schema = batch.schema();
        let projected_schema = project_schema(&schema, projection).unwrap();
        let cache = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            batch,
            projection: projection.cloned(),
            properties: Arc::new(cache),
        }
    }
}

impl DisplayAs for SystemCatalogExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SystemCatalogExec")
    }
}

impl ExecutionPlan for SystemCatalogExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "SystemCatalogExec"
    }

    fn schema(&self) -> SchemaRef {
        self.properties.equivalence_properties().schema().clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::context::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let batch = if let Some(projection) = &self.projection {
            self.batch
                .project(projection)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?
        } else {
            self.batch.clone()
        };

        Ok(Box::pin(MemoryStream::try_new(
            vec![batch],
            self.schema(),
            None,
        )?))
    }
}

#[derive(Debug)]
struct PgIndexTable {
    control_plane: Arc<ControlPlane>,
    schema: SchemaRef,
}

impl PgIndexTable {
    fn new(control_plane: Arc<ControlPlane>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("indexrelid", DataType::UInt32, false),
            Field::new("indrelid", DataType::UInt32, false),
            Field::new("indnatts", DataType::Int16, false),
            Field::new("indisunique", DataType::Boolean, false),
            Field::new("indisprimary", DataType::Boolean, false),
            Field::new("indisvalid", DataType::Boolean, false),
        ]));
        Self {
            control_plane,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for PgIndexTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let session = postgres_session_from_state(state);
        let cluster = self.control_plane.cluster_snapshot().await;

        let mut indexrelid = Vec::new();
        let mut indrelid = Vec::new();
        let mut indnatts = Vec::new();
        let mut indisunique = Vec::new();
        let mut indisprimary = Vec::new();
        let mut indisvalid = Vec::new();

        for rel in &cluster.relations {
            if rel.database == session.database {
                let table_oid = synthetic_relation_oid(&rel.database, &rel.schema, &rel.name);
                for index in &rel.indexes {
                    indexrelid.push(synthetic_relation_oid(
                        &rel.database,
                        &rel.schema,
                        &index.name,
                    ));
                    indrelid.push(table_oid);
                    indnatts.push(index.columns.len() as i16);
                    indisunique.push(index.is_unique);
                    indisprimary.push(index.is_primary);
                    indisvalid.push(true);
                }
            }
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(UInt32Array::from(indexrelid)),
                Arc::new(UInt32Array::from(indrelid)),
                Arc::new(Int16Array::from(indnatts)),
                Arc::new(BooleanArray::from(indisunique)),
                Arc::new(BooleanArray::from(indisprimary)),
                Arc::new(BooleanArray::from(indisvalid)),
            ],
        )?;

        Ok(Arc::new(SystemCatalogExec::new(batch, projection)))
    }
}

#[derive(Debug)]
struct PgDescriptionTable {
    _control_plane: Arc<ControlPlane>,
    schema: SchemaRef,
}

impl PgDescriptionTable {
    fn new(control_plane: Arc<ControlPlane>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("objoid", DataType::UInt32, false),
            Field::new("classoid", DataType::UInt32, false),
            Field::new("objsubid", DataType::Int32, false),
            Field::new("description", DataType::Utf8, false),
        ]));
        Self {
            _control_plane: control_plane,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for PgDescriptionTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Return empty for now
        let batch = RecordBatch::new_empty(Arc::clone(&self.schema));
        Ok(Arc::new(SystemCatalogExec::new(batch, projection)))
    }
}

#[derive(Debug)]
struct PgAttrdefTable {
    control_plane: Arc<ControlPlane>,
    schema: SchemaRef,
}

impl PgAttrdefTable {
    fn new(control_plane: Arc<ControlPlane>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("adrelid", DataType::UInt32, false),
            Field::new("adnum", DataType::Int16, false),
            Field::new("adbin", DataType::Utf8, true),
        ]));
        Self {
            control_plane,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for PgAttrdefTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let session = postgres_session_from_state(state);
        let cluster = self.control_plane.cluster_snapshot().await;

        let mut oid = Vec::new();
        let mut adrelid = Vec::new();
        let mut adnum = Vec::new();
        let mut adbin = Vec::new();

        for rel in &cluster.relations {
            if rel.database == session.database {
                let rel_oid = synthetic_relation_oid(&rel.database, &rel.schema, &rel.name);
                for (idx, col) in rel.columns.iter().enumerate() {
                    if let Some(default_val) = &col.default_value {
                        oid.push(synthetic_attrdef_oid(rel_oid, (idx + 1) as i16));
                        adrelid.push(rel_oid);
                        adnum.push((idx + 1) as i16);
                        adbin.push(Some(default_val.clone()));
                    }
                }
            }
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(UInt32Array::from(oid)),
                Arc::new(UInt32Array::from(adrelid)),
                Arc::new(Int16Array::from(adnum)),
                Arc::new(StringArray::from(adbin)),
            ],
        )?;

        Ok(Arc::new(SystemCatalogExec::new(batch, projection)))
    }
}

#[derive(Debug)]
struct PgDependTable {
    _control_plane: Arc<ControlPlane>,
    schema: SchemaRef,
}

impl PgDependTable {
    fn new(control_plane: Arc<ControlPlane>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("classid", DataType::UInt32, false),
            Field::new("objid", DataType::UInt32, false),
            Field::new("objsubid", DataType::Int32, false),
            Field::new("refclassid", DataType::UInt32, false),
            Field::new("refobjid", DataType::UInt32, false),
            Field::new("refobjsubid", DataType::Int32, false),
            Field::new("deptype", DataType::Utf8, false),
        ]));
        Self {
            _control_plane: control_plane,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for PgDependTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Return empty for now to satisfy joins
        let batch = RecordBatch::new_empty(Arc::clone(&self.schema));
        Ok(Arc::new(SystemCatalogExec::new(batch, projection)))
    }
}

#[derive(Debug)]
struct PgConstraintTable {
    control_plane: Arc<ControlPlane>,
    schema: SchemaRef,
}

impl PgConstraintTable {
    fn new(control_plane: Arc<ControlPlane>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("conname", DataType::Utf8, false),
            Field::new("connamespace", DataType::UInt32, false),
            Field::new("contype", DataType::Utf8, false),
            Field::new("condeferrable", DataType::Boolean, false),
            Field::new("condeferred", DataType::Boolean, false),
            Field::new("convalidated", DataType::Boolean, false),
            Field::new("conrelid", DataType::UInt32, false),
            Field::new("contypid", DataType::UInt32, false),
            Field::new("conindid", DataType::UInt32, false),
            Field::new("confrelid", DataType::UInt32, false),
            Field::new("confupdtype", DataType::Utf8, false),
            Field::new("confdeltype", DataType::Utf8, false),
            Field::new("confmatchtype", DataType::Utf8, false),
            Field::new("conislocal", DataType::Boolean, false),
            Field::new("coninhcount", DataType::Int32, false),
            Field::new("connoinherit", DataType::Boolean, false),
            // Standard PostgreSQL uses int2[] and int2[] for these.
            // We provide them as NULL for now to satisfy basic JDBC discovery.
            Field::new("conkey", DataType::Utf8, true),
            Field::new("confkey", DataType::Utf8, true),
        ]));
        Self {
            control_plane,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for PgConstraintTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let session = postgres_session_from_state(state);
        let cluster = self.control_plane.cluster_snapshot().await;

        let mut oid = Vec::new();
        let mut conname = Vec::new();
        let mut connamespace = Vec::new();
        let mut contype = Vec::new();
        let mut condeferrable = Vec::new();
        let mut condeferred = Vec::new();
        let mut convalidated = Vec::new();
        let mut conrelid = Vec::new();
        let mut contypid = Vec::new();
        let mut conindid = Vec::new();
        let mut confrelid = Vec::new();
        let mut confupdtype = Vec::new();
        let mut confdeltype = Vec::new();
        let mut confmatchtype = Vec::new();
        let mut conislocal = Vec::new();
        let mut coninhcount = Vec::new();
        let mut connoinherit = Vec::new();
        let mut conkey: Vec<Option<String>> = Vec::new();
        let mut confkey: Vec<Option<String>> = Vec::new();

        for rel in &cluster.relations {
            if rel.database == session.database {
                let rel_oid = synthetic_relation_oid(&rel.database, &rel.schema, &rel.name);
                let nsp_oid = synthetic_namespace_oid(&rel.database, &rel.schema);

                for constraint in &rel.constraints {
                    oid.push(synthetic_constraint_oid(rel_oid, &constraint.name));
                    conname.push(constraint.name.clone());
                    connamespace.push(nsp_oid);
                    contype.push(match constraint.kind {
                        CatalogTableConstraintKind::PrimaryKey => "p".to_string(),
                        CatalogTableConstraintKind::ForeignKey => "f".to_string(),
                        CatalogTableConstraintKind::Unique => "u".to_string(),
                    });
                    condeferrable.push(false);
                    condeferred.push(false);
                    convalidated.push(true);
                    conrelid.push(rel_oid);
                    contypid.push(0_u32);
                    conindid.push(0_u32);

                    let frel_oid = if let Some(ref_table) = &constraint.referenced_table {
                        let ref_db = constraint
                            .referenced_database
                            .as_ref()
                            .unwrap_or(&rel.database);
                        let ref_sch = constraint.referenced_schema.as_ref().unwrap_or(&rel.schema);
                        synthetic_relation_oid(ref_db, ref_sch, ref_table)
                    } else {
                        0_u32
                    };
                    confrelid.push(frel_oid);

                    confupdtype.push("a".to_string());
                    confdeltype.push("a".to_string());
                    confmatchtype.push("s".to_string());
                    conislocal.push(true);
                    coninhcount.push(0_i32);
                    connoinherit.push(true);

                    // Prototype simplification: arrays not provided yet
                    conkey.push(None);
                    confkey.push(None);
                }
            }
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(UInt32Array::from(oid)),
                Arc::new(StringArray::from(conname)),
                Arc::new(UInt32Array::from(connamespace)),
                Arc::new(StringArray::from(contype)),
                Arc::new(BooleanArray::from(condeferrable)),
                Arc::new(BooleanArray::from(condeferred)),
                Arc::new(BooleanArray::from(convalidated)),
                Arc::new(UInt32Array::from(conrelid)),
                Arc::new(UInt32Array::from(contypid)),
                Arc::new(UInt32Array::from(conindid)),
                Arc::new(UInt32Array::from(confrelid)),
                Arc::new(StringArray::from(confupdtype)),
                Arc::new(StringArray::from(confdeltype)),
                Arc::new(StringArray::from(confmatchtype)),
                Arc::new(BooleanArray::from(conislocal)),
                Arc::new(Int32Array::from(coninhcount)),
                Arc::new(BooleanArray::from(connoinherit)),
                Arc::new(StringArray::from(conkey)),
                Arc::new(StringArray::from(confkey)),
            ],
        )?;

        Ok(Arc::new(SystemCatalogExec::new(batch, projection)))
    }
}

fn postgres_session_from_state(state: &dyn Session) -> analyticsdb_core::SessionContext {
    state
        .config()
        .get_extension::<analyticsdb_core::SessionContext>()
        .map(|v| v.as_ref().clone())
        .unwrap_or_else(|| analyticsdb_core::SessionContext {
            user: "postgres".to_string(),
            role: "postgres".to_string(),
            database: "postgres".to_string(),
            schema: "public".to_string(),
            auth_method: "postgres-wire-startup".to_string(),
            protocol: analyticsdb_core::Protocol::PostgreSql,
            transaction_status: analyticsdb_core::TransactionStatus::Idle,
        })
}

fn synthetic_namespace_oid(database: &str, schema: &str) -> u32 {
    let mut hash = 2166136261_u32;
    for byte in database.bytes().chain([b'.']).chain(schema.bytes()) {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }
    if hash < 16384 {
        hash + 16384
    } else {
        hash
    }
}

fn synthetic_database_oid(database: &str) -> u32 {
    let mut hash = 2166136261_u32;
    for byte in database.bytes() {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }
    if hash < 16384 {
        hash + 16384
    } else {
        hash
    }
}

fn synthetic_role_oid(role: &str) -> u32 {
    let mut hash = 2166136261_u32;
    for byte in role.bytes() {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }
    if hash < 16384 {
        hash + 16384
    } else {
        hash
    }
}

fn synthetic_relation_oid(database: &str, schema: &str, name: &str) -> u32 {
    let mut hash = 2166136261_u32;
    for byte in database
        .bytes()
        .chain([b'.'])
        .chain(schema.bytes())
        .chain([b'.'])
        .chain(name.bytes())
    {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }
    if hash < 16384 {
        hash + 16384
    } else {
        hash
    }
}

fn synthetic_attrdef_oid(rel_oid: u32, attnum: i16) -> u32 {
    let mut hash = 2166136261_u32;
    for byte in rel_oid
        .to_le_bytes()
        .iter()
        .copied()
        .chain(attnum.to_le_bytes().iter().copied())
    {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }
    if hash < 16384 {
        hash + 16384
    } else {
        hash
    }
}

fn synthetic_constraint_oid(rel_oid: u32, conname: &str) -> u32 {
    let mut hash = 2166136261_u32;
    for byte in rel_oid.to_le_bytes().iter().copied().chain(conname.bytes()) {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }
    if hash < 16384 {
        hash + 16384
    } else {
        hash
    }
}
