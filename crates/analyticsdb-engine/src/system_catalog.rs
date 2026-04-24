use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{BooleanArray, Int16Array, Int32Array, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{SchemaProvider, Session};
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

use analyticsdb_control::ControlPlane;

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
                hasindexes.push("false".to_string());
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
            Field::new("typlen", DataType::Int16, false),
            Field::new("typbyval", DataType::Boolean, false),
            Field::new("typtype", DataType::Utf8, false),
            Field::new("typcategory", DataType::Utf8, false),
            Field::new("typrelid", DataType::UInt32, false),
            Field::new("typelem", DataType::UInt32, false),
            Field::new("typinput", DataType::Utf8, false),
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
        let mut typlen = Vec::new();
        let mut typbyval = Vec::new();
        let mut typtype = Vec::new();
        let mut typcategory = Vec::new();
        let mut typrelid = Vec::new();
        let mut typelem = Vec::new();
        let mut typinput = Vec::new();

        for (o, name, len, byval, t, cat) in types {
            oid.push(o as u32);
            typname.push(name.to_string());
            typnamespace.push(11_u32); // pg_catalog
            typlen.push(len as i16);
            typbyval.push(byval);
            typtype.push(t.to_string());
            typcategory.push(cat.to_string());
            typrelid.push(0_u32);
            typelem.push(0_u32);
            typinput.push("-".to_string());
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(UInt32Array::from(oid)),
                Arc::new(StringArray::from(typname)),
                Arc::new(UInt32Array::from(typnamespace)),
                Arc::new(Int16Array::from(typlen)),
                Arc::new(BooleanArray::from(typbyval)),
                Arc::new(StringArray::from(typtype)),
                Arc::new(StringArray::from(typcategory)),
                Arc::new(UInt32Array::from(typrelid)),
                Arc::new(UInt32Array::from(typelem)),
                Arc::new(StringArray::from(typinput)),
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
            Field::new("relkind", DataType::Utf8, false),
            Field::new("relowner", DataType::UInt32, false),
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
        let mut relkind = Vec::new();
        let mut relowner = Vec::new();

        for rel in &cluster.relations {
            if rel.database == session.database {
                oid.push(synthetic_relation_oid(&rel.database, &rel.schema, &rel.name));
                relname.push(rel.name.clone());
                relnamespace.push(synthetic_namespace_oid(&rel.database, &rel.schema));
                relkind.push(match rel.kind {
                    analyticsdb_control::CatalogRelationKind::Table => "r".to_string(),
                    analyticsdb_control::CatalogRelationKind::View => "v".to_string(),
                });
                relowner.push(10_u32);
            }
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(UInt32Array::from(oid)),
                Arc::new(StringArray::from(relname)),
                Arc::new(UInt32Array::from(relnamespace)),
                Arc::new(StringArray::from(relkind)),
                Arc::new(UInt32Array::from(relowner)),
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

        for rel in &cluster.relations {
            if rel.database == session.database {
                let rel_oid = synthetic_relation_oid(&rel.database, &rel.schema, &rel.name);
                for (idx, col) in rel.columns.iter().enumerate() {
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
                    attnum.push((idx + 1) as i16);
                    attnotnull.push(!col.nullable);
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
