use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use analyticsdb_control::{
    parse_metadata_statement, CatalogColumn, CatalogRelationKind, ControlPlane, MetadataStatement,
    TableColumnDefinition,
};
use analyticsdb_core::{QueryRequest, QueryResponse};
use anyhow::Result;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    LargeStringArray, NullArray, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedTableFile {
    storage_layout: PersistedStorageLayout,
    schema: Vec<PersistedField>,
    columns: Vec<PersistedColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedField {
    name: String,
    data_type: PersistedDataType,
    nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum PersistedStorageLayout {
    Columnar,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedColumn {
    values: Vec<PersistedScalarValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum PersistedDataType {
    Boolean,
    Float32,
    Float64,
    Int32,
    Int64,
    Null,
    UInt32,
    UInt64,
    Utf8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum PersistedScalarValue {
    Boolean(bool),
    Float32(f32),
    Float64(f64),
    Int32(i32),
    Int64(i64),
    Null,
    UInt32(u32),
    UInt64(u64),
    Utf8(String),
}

pub struct PrototypeEngine {
    control_plane: Arc<ControlPlane>,
    runtime: Runtime,
}

pub struct QueryExecutionResult {
    pub query_id: String,
    pub coordinator_node_id: String,
    pub session: analyticsdb_core::SessionContext,
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub message: String,
    pub execution_time_ms: u128,
}

impl QueryExecutionResult {
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    pub fn columns(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect()
    }
}

impl PrototypeEngine {
    pub fn new() -> Result<Self> {
        Self::with_control_plane(Arc::new(ControlPlane::new_bootstrap()))
    }

    pub fn from_catalog_path(path: &str) -> Result<Self> {
        Self::with_control_plane(Arc::new(ControlPlane::from_catalog_path(path)?))
    }

    pub fn with_control_plane(control_plane: Arc<ControlPlane>) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        Ok(Self {
            control_plane,
            runtime,
        })
    }

    pub fn execute_query(&self, request: &QueryRequest) -> Result<QueryResponse> {
        let execution = self.execute_query_batches(request)?;
        let columns = execution.columns();
        let rows = batches_to_rows(&execution.batches)?;

        Ok(QueryResponse {
            query_id: execution.query_id,
            coordinator_node_id: execution.coordinator_node_id,
            session: execution.session,
            columns,
            rows,
            message: execution.message,
            execution_time_ms: execution.execution_time_ms,
        })
    }

    pub fn execute_query_batches(&self, request: &QueryRequest) -> Result<QueryExecutionResult> {
        let started = Instant::now();
        let admission = self.control_plane.admit_query(&request.session)?;
        if let Some(statement) = parse_metadata_statement(&request.sql) {
            return self.execute_metadata_query(request, statement, admission, started);
        }

        let control_plane = Arc::clone(&self.control_plane);
        let sql = request.sql.clone();
        let session = request.session.clone();

        self.runtime.block_on(async move {
            let context = SessionContext::new();
            register_persisted_tables(&context, &control_plane, &session).await?;
            register_persisted_views(&context, &control_plane, &session).await?;
            let dataframe = context.sql(&sql).await?;
            let schema = Arc::new(dataframe.schema().as_arrow().as_ref().clone());
            let batches = dataframe.collect().await?;
            let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();

            Ok(QueryExecutionResult {
                query_id: admission.query_id,
                coordinator_node_id: admission.coordinator_node_id,
                session,
                schema,
                batches,
                message: format!("Query executed successfully. {row_count} row(s) returned."),
                execution_time_ms: started.elapsed().as_millis(),
            })
        })
    }

    fn execute_metadata_query(
        &self,
        request: &QueryRequest,
        statement: MetadataStatement,
        admission: analyticsdb_control::QueryAdmission,
        started: Instant,
    ) -> Result<QueryExecutionResult> {
        let session = request.session.clone();

        let (schema, batches, message) = match statement {
            MetadataStatement::CreateDatabase { .. }
            | MetadataStatement::CreateSchema { .. }
            | MetadataStatement::CreateView { .. } => {
                let message = self
                    .control_plane
                    .execute_metadata_statement(&request.session, &statement)?;
                (Arc::new(Schema::empty()), Vec::new(), message)
            }
            MetadataStatement::CreateTableAs {
                database,
                schema,
                name,
                query_sql,
            } => {
                let storage_path = self.control_plane.managed_table_storage_path(
                    &request.session,
                    database.as_deref(),
                    schema.as_deref(),
                    &name,
                )?;

                let control_plane = Arc::clone(&self.control_plane);
                let session = request.session.clone();
                let storage_path_for_write = storage_path.clone();
                let (row_count, columns_metadata) = self.runtime.block_on(async move {
                    let context = SessionContext::new();
                    register_persisted_tables(&context, &control_plane, &session).await?;
                    register_persisted_views(&context, &control_plane, &session).await?;
                    let dataframe = context.sql(&query_sql).await?;
                    let arrow_schema = dataframe.schema().as_arrow().clone();
                    let batches = dataframe.collect().await?;
                    let columns_metadata = catalog_columns_from_schema(&arrow_schema);

                    let row_count =
                        persist_table_snapshot(&storage_path_for_write, &arrow_schema, &batches)?;

                    Ok::<_, anyhow::Error>((row_count, columns_metadata))
                })?;

                let created_message = self.control_plane.register_managed_table(
                    &request.session,
                    database.as_deref(),
                    schema.as_deref(),
                    &name,
                    &storage_path,
                    columns_metadata,
                )?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!("{created_message} {row_count} row(s) materialized."),
                )
            }
            MetadataStatement::CreateTable {
                database,
                schema,
                name,
                columns,
            } => {
                let storage_path = self.control_plane.managed_table_storage_path(
                    &request.session,
                    database.as_deref(),
                    schema.as_deref(),
                    &name,
                )?;
                let arrow_schema = build_arrow_schema_from_definitions(&columns)?;

                persist_empty_table_snapshot(&storage_path, &arrow_schema)?;

                let created_message = self.control_plane.register_managed_table(
                    &request.session,
                    database.as_deref(),
                    schema.as_deref(),
                    &name,
                    &storage_path,
                    catalog_columns_from_schema(&arrow_schema),
                )?;

                (Arc::new(Schema::empty()), Vec::new(), created_message)
            }
            MetadataStatement::InsertInto {
                database,
                schema,
                name,
                columns,
                rows,
            } => {
                let relation = self.control_plane.table_relation(
                    &request.session,
                    database.as_deref(),
                    schema.as_deref(),
                    &name,
                )?;
                let storage_path = relation.storage_path.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Managed table '{}.{}.{}' is missing a storage path",
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })?;
                let inserted_row_count = append_rows_to_table_snapshot(
                    Path::new(storage_path),
                    columns.as_deref(),
                    &rows,
                )?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!(
                        "Inserted {inserted_row_count} row(s) into '{}.{}.{}'.",
                        relation.database, relation.schema, relation.name
                    ),
                )
            }
            MetadataStatement::ShowDatabases => {
                let rows = self
                    .control_plane
                    .list_databases(&request.session)?
                    .into_iter()
                    .map(|database| vec![database])
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(&["database_name"], &rows)?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} database(s) listed successfully."),
                )
            }
            MetadataStatement::ShowSchemas { database } => {
                let rows = self
                    .control_plane
                    .list_schemas(&request.session, database.as_deref())?
                    .into_iter()
                    .map(|schema| vec![schema])
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(&["schema_name"], &rows)?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} schema(s) listed successfully."),
                )
            }
            MetadataStatement::ShowTables { database, schema } => {
                let rows = self
                    .control_plane
                    .list_relations(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        CatalogRelationKind::Table,
                    )?
                    .into_iter()
                    .map(|relation| vec![relation.name])
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(&["table_name"], &rows)?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} table(s) listed successfully."),
                )
            }
            MetadataStatement::ShowViews { database, schema } => {
                let rows = self
                    .control_plane
                    .list_relations(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        CatalogRelationKind::View,
                    )?
                    .into_iter()
                    .map(|relation| vec![relation.name])
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(&["view_name"], &rows)?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} view(s) listed successfully."),
                )
            }
            MetadataStatement::ShowColumns {
                database,
                schema,
                name,
            }
            | MetadataStatement::DescribeRelation {
                database,
                schema,
                name,
            } => {
                let rows = self
                    .control_plane
                    .relation_columns(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )?
                    .into_iter()
                    .map(|column| {
                        vec![
                            column.name,
                            column.data_type,
                            if column.nullable {
                                "YES".to_string()
                            } else {
                                "NO".to_string()
                            },
                        ]
                    })
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(&["column_name", "data_type", "is_nullable"], &rows)?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} column(s) described successfully."),
                )
            }
        };

        Ok(QueryExecutionResult {
            query_id: admission.query_id,
            coordinator_node_id: admission.coordinator_node_id,
            session,
            schema,
            batches,
            message,
            execution_time_ms: started.elapsed().as_millis(),
        })
    }

    pub fn list_databases(
        &self,
        session: &analyticsdb_core::SessionContext,
    ) -> Result<Vec<String>> {
        self.control_plane.list_databases(session)
    }

    pub fn list_schemas(
        &self,
        session: &analyticsdb_core::SessionContext,
        database: Option<&str>,
    ) -> Result<Vec<String>> {
        self.control_plane.list_schemas(session, database)
    }

    pub fn list_relations(
        &self,
        session: &analyticsdb_core::SessionContext,
        database: Option<&str>,
        schema: Option<&str>,
        kind: CatalogRelationKind,
    ) -> Result<Vec<analyticsdb_control::CatalogRelation>> {
        self.control_plane
            .list_relations(session, database, schema, kind)
    }
}

impl PersistedField {
    fn from_arrow(field: &Field) -> Result<Self> {
        Ok(Self {
            name: field.name().to_string(),
            data_type: PersistedDataType::from_arrow(field.data_type())?,
            nullable: field.is_nullable(),
        })
    }

    fn to_arrow_field(&self) -> Field {
        Field::new(
            self.name.clone(),
            self.data_type.to_arrow_data_type(),
            self.nullable,
        )
    }
}

fn catalog_columns_from_schema(schema: &Schema) -> Vec<CatalogColumn> {
    schema
        .fields()
        .iter()
        .map(|field| CatalogColumn {
            name: field.name().to_string(),
            data_type: field.data_type().to_string(),
            nullable: field.is_nullable(),
        })
        .collect()
}

fn build_arrow_schema_from_definitions(columns: &[TableColumnDefinition]) -> Result<Schema> {
    if columns.is_empty() {
        anyhow::bail!("Managed tables must define at least one column");
    }

    let mut seen = BTreeSet::new();
    let fields = columns
        .iter()
        .map(|column| {
            let normalized_name = column.name.to_ascii_lowercase();
            if !seen.insert(normalized_name) {
                anyhow::bail!(
                    "Duplicate column '{}' in managed table definition",
                    column.name
                );
            }

            Ok(Field::new(
                column.name.clone(),
                sql_type_to_arrow_data_type(&column.data_type)?,
                column.nullable,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Schema::new(fields))
}

fn sql_type_to_arrow_data_type(raw: &str) -> Result<DataType> {
    let normalized = raw.trim().to_ascii_uppercase();

    let data_type = if normalized.starts_with("VARCHAR(")
        || normalized.starts_with("CHAR(")
        || normalized.starts_with("CHARACTER VARYING(")
    {
        DataType::Utf8
    } else {
        match normalized.as_str() {
            "BOOL" | "BOOLEAN" => DataType::Boolean,
            "FLOAT" | "FLOAT4" | "REAL" => DataType::Float32,
            "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => DataType::Float64,
            "SMALLINT" | "INT2" | "INT" | "INTEGER" | "INT4" => DataType::Int32,
            "BIGINT" | "INT8" => DataType::Int64,
            "STRING" | "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER" | "CHARACTER VARYING" => {
                DataType::Utf8
            }
            unsupported => {
                anyhow::bail!(
                    "Unsupported SQL column type '{}' in the current prototype",
                    unsupported
                )
            }
        }
    };

    Ok(data_type)
}

impl PersistedDataType {
    fn from_arrow(data_type: &DataType) -> Result<Self> {
        Ok(match data_type {
            DataType::Boolean => Self::Boolean,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::Null => Self::Null,
            DataType::UInt32 => Self::UInt32,
            DataType::UInt64 => Self::UInt64,
            DataType::Utf8 | DataType::LargeUtf8 => Self::Utf8,
            unsupported => {
                anyhow::bail!(
                    "Unsupported managed table data type in current prototype: {unsupported:?}"
                )
            }
        })
    }

    fn to_arrow_data_type(&self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::Null => DataType::Null,
            Self::UInt32 => DataType::UInt32,
            Self::UInt64 => DataType::UInt64,
            Self::Utf8 => DataType::Utf8,
        }
    }
}

impl PersistedScalarValue {
    fn from_array(array: &ArrayRef, row_index: usize) -> Result<Self> {
        if array.is_null(row_index) {
            return Ok(Self::Null);
        }

        let value = match array.data_type() {
            DataType::Boolean => Self::Boolean(
                array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("boolean array downcast should succeed")
                    .value(row_index),
            ),
            DataType::Float32 => Self::Float32(
                array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .expect("float32 array downcast should succeed")
                    .value(row_index),
            ),
            DataType::Float64 => Self::Float64(
                array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("float64 array downcast should succeed")
                    .value(row_index),
            ),
            DataType::Int32 => Self::Int32(
                array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("int32 array downcast should succeed")
                    .value(row_index),
            ),
            DataType::Int64 => Self::Int64(
                array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64 array downcast should succeed")
                    .value(row_index),
            ),
            DataType::UInt32 => Self::UInt32(
                array
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .expect("uint32 array downcast should succeed")
                    .value(row_index),
            ),
            DataType::UInt64 => Self::UInt64(
                array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("uint64 array downcast should succeed")
                    .value(row_index),
            ),
            DataType::Utf8 => Self::Utf8(
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8 array downcast should succeed")
                    .value(row_index)
                    .to_string(),
            ),
            DataType::LargeUtf8 => Self::Utf8(
                array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .expect("large utf8 array downcast should succeed")
                    .value(row_index)
                    .to_string(),
            ),
            DataType::Null => Self::Null,
            unsupported => {
                anyhow::bail!(
                    "Unsupported managed table value type in current prototype: {unsupported:?}"
                )
            }
        };

        Ok(value)
    }

    fn from_sql_literal(raw: &str, data_type: &PersistedDataType, nullable: bool) -> Result<Self> {
        let trimmed = raw.trim();

        if trimmed.eq_ignore_ascii_case("NULL") {
            if !nullable {
                anyhow::bail!("NULL is not allowed for a NOT NULL column");
            }

            return Ok(Self::Null);
        }

        let value = match data_type {
            PersistedDataType::Boolean => {
                if trimmed.eq_ignore_ascii_case("TRUE") {
                    Self::Boolean(true)
                } else if trimmed.eq_ignore_ascii_case("FALSE") {
                    Self::Boolean(false)
                } else {
                    anyhow::bail!("Expected BOOLEAN literal, found '{}'", trimmed);
                }
            }
            PersistedDataType::Float32 => Self::Float32(trimmed.parse::<f32>()?),
            PersistedDataType::Float64 => Self::Float64(trimmed.parse::<f64>()?),
            PersistedDataType::Int32 => Self::Int32(trimmed.parse::<i32>()?),
            PersistedDataType::Int64 => Self::Int64(trimmed.parse::<i64>()?),
            PersistedDataType::Null => {
                anyhow::bail!("NULL-only columns are not writable in the current prototype")
            }
            PersistedDataType::UInt32 => Self::UInt32(trimmed.parse::<u32>()?),
            PersistedDataType::UInt64 => Self::UInt64(trimmed.parse::<u64>()?),
            PersistedDataType::Utf8 => Self::Utf8(parse_sql_string_literal(trimmed)?),
        };

        Ok(value)
    }
}

impl PersistedTableFile {
    fn empty_from_schema(schema: &Schema) -> Result<Self> {
        Ok(Self {
            storage_layout: PersistedStorageLayout::Columnar,
            schema: schema
                .fields()
                .iter()
                .map(|field| PersistedField::from_arrow(field.as_ref()))
                .collect::<Result<Vec<_>>>()?,
            columns: schema
                .fields()
                .iter()
                .map(|_| PersistedColumn { values: Vec::new() })
                .collect(),
        })
    }

    fn from_batches(schema: &Schema, batches: &[RecordBatch]) -> Result<Self> {
        let persisted_schema = schema
            .fields()
            .iter()
            .map(|field| PersistedField::from_arrow(field.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        let mut columns = Vec::with_capacity(schema.fields().len());

        for column_index in 0..schema.fields().len() {
            let mut values = Vec::new();

            for batch in batches {
                for row_index in 0..batch.num_rows() {
                    let value =
                        PersistedScalarValue::from_array(batch.column(column_index), row_index)?;
                    values.push(value);
                }
            }

            columns.push(PersistedColumn { values });
        }

        Ok(Self {
            storage_layout: PersistedStorageLayout::Columnar,
            schema: persisted_schema,
            columns,
        })
    }

    fn to_record_batch(&self) -> Result<RecordBatch> {
        if self.storage_layout != PersistedStorageLayout::Columnar {
            anyhow::bail!("Unsupported managed table storage layout");
        }

        let schema = Arc::new(Schema::new(
            self.schema
                .iter()
                .map(PersistedField::to_arrow_field)
                .collect::<Vec<_>>(),
        ));
        let arrays = self
            .schema
            .iter()
            .enumerate()
            .map(|(column_index, field)| self.build_array(column_index, field))
            .collect::<Result<Vec<_>>>()?;

        Ok(RecordBatch::try_new(schema, arrays)?)
    }

    fn build_array(&self, column_index: usize, field: &PersistedField) -> Result<ArrayRef> {
        let column = self
            .columns
            .get(column_index)
            .ok_or_else(|| anyhow::anyhow!("Missing column at column index {column_index}"))?;

        let array: ArrayRef = match field.data_type {
            PersistedDataType::Boolean => Arc::new(BooleanArray::from(self.collect_optional(
                column,
                |value| match value {
                    PersistedScalarValue::Boolean(value) => Ok(Some(*value)),
                    PersistedScalarValue::Null => Ok(None),
                    other => anyhow::bail!("Expected boolean value, found {other:?}"),
                },
            )?)),
            PersistedDataType::Float32 => Arc::new(Float32Array::from(self.collect_optional(
                column,
                |value| match value {
                    PersistedScalarValue::Float32(value) => Ok(Some(*value)),
                    PersistedScalarValue::Null => Ok(None),
                    other => anyhow::bail!("Expected float32 value, found {other:?}"),
                },
            )?)),
            PersistedDataType::Float64 => Arc::new(Float64Array::from(self.collect_optional(
                column,
                |value| match value {
                    PersistedScalarValue::Float64(value) => Ok(Some(*value)),
                    PersistedScalarValue::Null => Ok(None),
                    other => anyhow::bail!("Expected float64 value, found {other:?}"),
                },
            )?)),
            PersistedDataType::Int32 => Arc::new(Int32Array::from(self.collect_optional(
                column,
                |value| match value {
                    PersistedScalarValue::Int32(value) => Ok(Some(*value)),
                    PersistedScalarValue::Null => Ok(None),
                    other => anyhow::bail!("Expected int32 value, found {other:?}"),
                },
            )?)),
            PersistedDataType::Int64 => Arc::new(Int64Array::from(self.collect_optional(
                column,
                |value| match value {
                    PersistedScalarValue::Int64(value) => Ok(Some(*value)),
                    PersistedScalarValue::Null => Ok(None),
                    other => anyhow::bail!("Expected int64 value, found {other:?}"),
                },
            )?)),
            PersistedDataType::UInt32 => Arc::new(UInt32Array::from(self.collect_optional(
                column,
                |value| match value {
                    PersistedScalarValue::UInt32(value) => Ok(Some(*value)),
                    PersistedScalarValue::Null => Ok(None),
                    other => anyhow::bail!("Expected uint32 value, found {other:?}"),
                },
            )?)),
            PersistedDataType::UInt64 => Arc::new(UInt64Array::from(self.collect_optional(
                column,
                |value| match value {
                    PersistedScalarValue::UInt64(value) => Ok(Some(*value)),
                    PersistedScalarValue::Null => Ok(None),
                    other => anyhow::bail!("Expected uint64 value, found {other:?}"),
                },
            )?)),
            PersistedDataType::Utf8 => Arc::new(StringArray::from(self.collect_optional(
                column,
                |value| match value {
                    PersistedScalarValue::Utf8(value) => Ok(Some(value.as_str())),
                    PersistedScalarValue::Null => Ok(None),
                    other => anyhow::bail!("Expected utf8 value, found {other:?}"),
                },
            )?)),
            PersistedDataType::Null => Arc::new(NullArray::new(column.values.len())),
        };

        Ok(array)
    }

    fn collect_optional<'a, T>(
        &'a self,
        column: &'a PersistedColumn,
        mapper: impl Fn(&'a PersistedScalarValue) -> Result<Option<T>>,
    ) -> Result<Vec<Option<T>>> {
        column.values.iter().map(mapper).collect()
    }

    fn row_count(&self) -> usize {
        self.columns.first().map_or(0, |column| column.values.len())
    }

    fn append_rows(
        &mut self,
        selected_columns: Option<&[String]>,
        rows: &[Vec<String>],
    ) -> Result<usize> {
        let mut column_order = Vec::new();

        match selected_columns {
            Some(selected_columns) => {
                let mut seen = BTreeSet::new();

                for selected_column in selected_columns {
                    let normalized_name = selected_column.to_ascii_lowercase();
                    if !seen.insert(normalized_name) {
                        anyhow::bail!("Duplicate column '{}' in INSERT target", selected_column);
                    }

                    let column_index = self
                        .schema
                        .iter()
                        .position(|field| field.name.eq_ignore_ascii_case(selected_column))
                        .ok_or_else(|| anyhow::anyhow!("Unknown column '{}'", selected_column))?;
                    column_order.push(column_index);
                }
            }
            None => column_order.extend(0..self.schema.len()),
        }

        for row in rows {
            if row.len() != column_order.len() {
                anyhow::bail!(
                    "Expected {} value(s) per row, found {}",
                    column_order.len(),
                    row.len()
                );
            }

            let mut row_values = vec![PersistedScalarValue::Null; self.schema.len()];
            let mut assigned_columns = vec![false; self.schema.len()];

            for (value_index, raw_value) in row.iter().enumerate() {
                let column_index = column_order[value_index];
                let field = self
                    .schema
                    .get(column_index)
                    .ok_or_else(|| anyhow::anyhow!("Missing field at index {column_index}"))?;
                let value = PersistedScalarValue::from_sql_literal(
                    raw_value,
                    &field.data_type,
                    field.nullable,
                )?;
                row_values[column_index] = value;
                assigned_columns[column_index] = true;
            }

            for (column_index, field) in self.schema.iter().enumerate() {
                if !assigned_columns[column_index] && !field.nullable {
                    anyhow::bail!(
                        "Column '{}' must be provided because it is NOT NULL",
                        field.name
                    );
                }

                self.columns
                    .get_mut(column_index)
                    .ok_or_else(|| anyhow::anyhow!("Missing column at index {column_index}"))?
                    .values
                    .push(row_values[column_index].clone());
            }
        }

        Ok(rows.len())
    }
}

fn persist_table_snapshot(path: &Path, schema: &Schema, batches: &[RecordBatch]) -> Result<usize> {
    let snapshot = PersistedTableFile::from_batches(schema, batches)?;
    write_table_snapshot(path, &snapshot)?;

    Ok(snapshot.row_count())
}

fn persist_empty_table_snapshot(path: &Path, schema: &Schema) -> Result<()> {
    let snapshot = PersistedTableFile::empty_from_schema(schema)?;
    write_table_snapshot(path, &snapshot)
}

fn write_table_snapshot(path: &Path, snapshot: &PersistedTableFile) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::write(path, serde_json::to_string_pretty(snapshot)?)?;

    Ok(())
}

fn append_rows_to_table_snapshot(
    path: &Path,
    selected_columns: Option<&[String]>,
    rows: &[Vec<String>],
) -> Result<usize> {
    let mut snapshot = load_persisted_table_file(path)?;
    let inserted_row_count = snapshot.append_rows(selected_columns, rows)?;
    write_table_snapshot(path, &snapshot)?;

    Ok(inserted_row_count)
}

fn load_persisted_table_file(path: &Path) -> Result<PersistedTableFile> {
    let raw = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&raw)?)
}

fn load_persisted_table_snapshot(path: &Path) -> Result<RecordBatch> {
    let snapshot = load_persisted_table_file(path)?;
    snapshot.to_record_batch()
}

fn parse_sql_string_literal(raw: &str) -> Result<String> {
    if !(raw.starts_with('\'') && raw.ends_with('\'')) {
        anyhow::bail!("Expected quoted string literal, found '{}'", raw);
    }

    Ok(raw[1..raw.len() - 1].replace("''", "'"))
}

async fn register_persisted_tables(
    context: &SessionContext,
    control_plane: &ControlPlane,
    session: &analyticsdb_core::SessionContext,
) -> Result<()> {
    for table in control_plane.list_tables_for_session(session)? {
        let Some(storage_path) = table.storage_path.as_deref() else {
            anyhow::bail!(
                "Managed table '{}.{}.{}' is missing a storage path",
                table.database,
                table.schema,
                table.name
            );
        };

        let batch = load_persisted_table_snapshot(Path::new(storage_path))?;
        let provider = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        context.register_table(table.name, Arc::new(provider))?;
    }

    Ok(())
}

async fn register_persisted_views(
    context: &SessionContext,
    control_plane: &ControlPlane,
    session: &analyticsdb_core::SessionContext,
) -> Result<()> {
    for view in control_plane.list_views_for_session(session)? {
        if let Some(definition_sql) = view.definition_sql.as_deref() {
            let statement = format!("CREATE VIEW {} AS {}", view.name, definition_sql);
            context.sql(&statement).await?.collect().await?;
        }
    }

    Ok(())
}

fn batches_to_rows(batches: &[RecordBatch]) -> Result<Vec<Vec<String>>> {
    let mut rows = Vec::new();

    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());

            for column_index in 0..batch.num_columns() {
                let value = array_value_to_string(batch.column(column_index).as_ref(), row_index)?;
                row.push(value);
            }

            rows.push(row);
        }
    }

    Ok(rows)
}

fn utf8_record_batch(columns: &[&str], rows: &[Vec<String>]) -> Result<RecordBatch> {
    let arrays = columns
        .iter()
        .enumerate()
        .map(|(column_index, column_name)| {
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                let value = row.get(column_index).ok_or_else(|| {
                    anyhow::anyhow!(
                        "Row had {} column(s), expected at least {}",
                        row.len(),
                        column_index + 1
                    )
                })?;
                values.push(Some(value.as_str()));
            }

            Ok((
                (*column_name).to_string(),
                Arc::new(StringArray::from(values)) as ArrayRef,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(RecordBatch::try_from_iter(arrays)?)
}

#[cfg(test)]
mod tests {
    use analyticsdb_core::{Protocol, QueryRequest, SessionContext};
    use serde_json::Value;

    use super::{PersistedStorageLayout, PrototypeEngine};

    #[test]
    fn executes_scalar_select() {
        let engine = PrototypeEngine::new().expect("engine should initialize");
        let response = engine
            .execute_query(&QueryRequest {
                sql: "SELECT 42 AS answer".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("query should execute");

        assert!(response.query_id.starts_with("q-"));
        assert_eq!(response.coordinator_node_id, "control-1");
        assert_eq!(response.session.database, "postgres");
        assert_eq!(response.columns, vec!["answer"]);
        assert_eq!(response.rows, vec![vec!["42".to_string()]]);
        assert!(response.message.contains("1 row(s)"));
    }

    #[test]
    fn rejects_unknown_schema_before_execution() {
        let engine = PrototypeEngine::new().expect("engine should initialize");
        let error = engine
            .execute_query(&QueryRequest {
                sql: "SELECT 1".to_string(),
                session: SessionContext {
                    schema: "missing".to_string(),
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect_err("unknown schema should fail");

        assert!(error.to_string().contains("Unknown schema"));
    }

    #[test]
    fn executes_metadata_show_databases() {
        let engine = PrototypeEngine::new().expect("engine should initialize");
        let response = engine
            .execute_query(&QueryRequest {
                sql: "SHOW DATABASES".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("metadata query should execute");

        assert_eq!(response.columns, vec!["database_name"]);
        assert!(response
            .rows
            .iter()
            .any(|row| row == &vec!["postgres".to_string()]));
        assert!(response.message.contains("database(s) listed"));
    }

    #[test]
    fn executes_persisted_view_query() {
        let catalog_path = {
            let mut path = std::env::temp_dir();
            path.push(format!(
                "analyticsdb-engine-view-{}.json",
                uuid::Uuid::now_v7()
            ));
            path
        };

        let engine = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE VIEW daily_metrics AS SELECT 7 AS metric".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("view creation should succeed");

        let reloaded = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should reload");

        let response = reloaded
            .execute_query(&QueryRequest {
                sql: "SELECT * FROM daily_metrics".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("persisted view should be queryable");

        assert_eq!(response.columns, vec!["metric"]);
        assert_eq!(response.rows, vec![vec!["7".to_string()]]);

        let _ = std::fs::remove_file(catalog_path);
    }

    #[test]
    fn executes_persisted_table_query() {
        let catalog_path = {
            let mut path = std::env::temp_dir();
            path.push(format!(
                "analyticsdb-engine-table-{}.json",
                uuid::Uuid::now_v7()
            ));
            path
        };

        let engine = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics AS SELECT 11 AS metric, 'ok' AS status".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("table creation should succeed");

        let reloaded = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should reload");

        let response = reloaded
            .execute_query(&QueryRequest {
                sql: "SELECT metric, status FROM fact_metrics".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("persisted table should be queryable");

        assert_eq!(response.columns, vec!["metric", "status"]);
        assert_eq!(
            response.rows,
            vec![vec!["11".to_string(), "ok".to_string()]]
        );

        let _ = std::fs::remove_file(&catalog_path);
        let managed_dir = catalog_path.with_file_name(format!(
            "{}.managed",
            catalog_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("catalog file stem should be present")
        ));
        let _ = std::fs::remove_dir_all(managed_dir);
    }

    #[test]
    fn creates_explicit_table_inserts_rows_and_queries_it() {
        let catalog_path = {
            let mut path = std::env::temp_dir();
            path.push(format!(
                "analyticsdb-engine-insert-{}.json",
                uuid::Uuid::now_v7()
            ));
            path
        };

        let engine = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT, is_hot BOOLEAN)"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("table definition should succeed");

        engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO fact_metrics VALUES (11, 'ok', true), (12, 'warn', false)"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("insert should succeed");

        let reloaded = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should reload");

        let response = reloaded
            .execute_query(&QueryRequest {
                sql: "SELECT metric, status, is_hot FROM fact_metrics ORDER BY metric".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("persisted inserted rows should be queryable");

        assert_eq!(response.columns, vec!["metric", "status", "is_hot"]);
        assert_eq!(
            response.rows,
            vec![
                vec!["11".to_string(), "ok".to_string(), "true".to_string()],
                vec!["12".to_string(), "warn".to_string(), "false".to_string()]
            ]
        );

        let describe = reloaded
            .execute_query(&QueryRequest {
                sql: "DESCRIBE fact_metrics".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("describe should succeed");

        assert!(describe.rows.iter().any(|row| {
            row == &vec!["metric".to_string(), "Int64".to_string(), "NO".to_string()]
        }));
        assert!(describe.rows.iter().any(|row| {
            row == &vec!["status".to_string(), "Utf8".to_string(), "YES".to_string()]
        }));
        assert!(describe.rows.iter().any(|row| {
            row == &vec![
                "is_hot".to_string(),
                "Boolean".to_string(),
                "YES".to_string(),
            ]
        }));

        let _ = std::fs::remove_file(&catalog_path);
        let managed_dir = catalog_path.with_file_name(format!(
            "{}.managed",
            catalog_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("catalog file stem should be present")
        ));
        let _ = std::fs::remove_dir_all(managed_dir);
    }

    #[test]
    fn inserts_with_column_list_and_omitted_nullable_columns() {
        let catalog_path = {
            let mut path = std::env::temp_dir();
            path.push(format!(
                "analyticsdb-engine-insert-columns-{}.json",
                uuid::Uuid::now_v7()
            ));
            path
        };

        let engine = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics (metric INTEGER NOT NULL, status VARCHAR(20), score DOUBLE PRECISION, active BOOLEAN)"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("table definition should succeed");

        engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO fact_metrics (metric, active, status) VALUES (11, true, 'ok''s')"
                    .to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("insert should succeed");

        let response = engine
            .execute_query(&QueryRequest {
                sql: "SELECT metric, status, score, active FROM fact_metrics".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("query should succeed");

        assert_eq!(
            response.columns,
            vec!["metric", "status", "score", "active"]
        );
        assert_eq!(
            response.rows,
            vec![vec![
                "11".to_string(),
                "ok's".to_string(),
                "".to_string(),
                "true".to_string()
            ]]
        );

        let describe = engine
            .execute_query(&QueryRequest {
                sql: "DESCRIBE fact_metrics".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("describe should succeed");

        assert!(describe.rows.iter().any(|row| {
            row == &vec!["metric".to_string(), "Int32".to_string(), "NO".to_string()]
        }));
        assert!(describe.rows.iter().any(|row| {
            row == &vec![
                "score".to_string(),
                "Float64".to_string(),
                "YES".to_string(),
            ]
        }));

        let _ = std::fs::remove_file(&catalog_path);
        let managed_dir = catalog_path.with_file_name(format!(
            "{}.managed",
            catalog_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("catalog file stem should be present")
        ));
        let _ = std::fs::remove_dir_all(managed_dir);
    }

    #[test]
    fn rejects_insert_with_wrong_column_count() {
        let catalog_path = {
            let mut path = std::env::temp_dir();
            path.push(format!(
                "analyticsdb-engine-insert-error-{}.json",
                uuid::Uuid::now_v7()
            ));
            path
        };

        let engine = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT)".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("table definition should succeed");

        let error = engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO fact_metrics VALUES (11)".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect_err("insert should fail");

        assert!(error
            .to_string()
            .contains("Expected 2 value(s) per row, found 1"));

        let _ = std::fs::remove_file(&catalog_path);
        let managed_dir = catalog_path.with_file_name(format!(
            "{}.managed",
            catalog_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("catalog file stem should be present")
        ));
        let _ = std::fs::remove_dir_all(managed_dir);
    }

    #[test]
    fn rejects_insert_when_not_null_column_is_omitted() {
        let catalog_path = {
            let mut path = std::env::temp_dir();
            path.push(format!(
                "analyticsdb-engine-insert-not-null-{}.json",
                uuid::Uuid::now_v7()
            ));
            path
        };

        let engine = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics (metric BIGINT NOT NULL, status TEXT)".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("table definition should succeed");

        let error = engine
            .execute_query(&QueryRequest {
                sql: "INSERT INTO fact_metrics (status) VALUES ('ok')".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect_err("insert should fail");

        assert!(error
            .to_string()
            .contains("Column 'metric' must be provided because it is NOT NULL"));

        let _ = std::fs::remove_file(&catalog_path);
        let managed_dir = catalog_path.with_file_name(format!(
            "{}.managed",
            catalog_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("catalog file stem should be present")
        ));
        let _ = std::fs::remove_dir_all(managed_dir);
    }

    #[test]
    fn persists_managed_table_in_columnar_layout() {
        let catalog_path = {
            let mut path = std::env::temp_dir();
            path.push(format!(
                "analyticsdb-engine-columnar-{}.json",
                uuid::Uuid::now_v7()
            ));
            path
        };

        let engine = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics AS SELECT 11 AS metric, 'ok' AS status".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("table creation should succeed");

        let managed_dir = catalog_path.with_file_name(format!(
            "{}.managed",
            catalog_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("catalog file stem should be present")
        ));
        let table_file = managed_dir.join("postgres__public__fact_metrics.table.json");
        let raw = std::fs::read_to_string(&table_file).expect("table snapshot should exist");
        let parsed: Value = serde_json::from_str(&raw).expect("table snapshot should be json");

        assert_eq!(
            parsed
                .get("storage_layout")
                .and_then(Value::as_str)
                .expect("storage layout should be present"),
            match PersistedStorageLayout::Columnar {
                PersistedStorageLayout::Columnar => "Columnar",
            }
        );
        assert!(parsed.get("columns").and_then(Value::as_array).is_some());
        assert!(parsed.get("rows").is_none());

        let _ = std::fs::remove_file(&catalog_path);
        let _ = std::fs::remove_dir_all(managed_dir);
    }

    #[test]
    fn describes_persisted_table_columns() {
        let catalog_path = {
            let mut path = std::env::temp_dir();
            path.push(format!(
                "analyticsdb-engine-describe-{}.json",
                uuid::Uuid::now_v7()
            ));
            path
        };

        let engine = PrototypeEngine::from_catalog_path(
            catalog_path
                .to_str()
                .expect("temp catalog path should be valid utf-8"),
        )
        .expect("engine should initialize");

        engine
            .execute_query(&QueryRequest {
                sql: "CREATE TABLE fact_metrics AS SELECT 11 AS metric, 'ok' AS status".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("table creation should succeed");

        let response = engine
            .execute_query(&QueryRequest {
                sql: "DESCRIBE fact_metrics".to_string(),
                session: SessionContext {
                    protocol: Protocol::Embedded,
                    ..SessionContext::default()
                },
            })
            .expect("describe should succeed");

        assert_eq!(
            response.columns,
            vec!["column_name", "data_type", "is_nullable"]
        );
        assert!(response.rows.iter().any(|row| {
            row == &vec!["metric".to_string(), "Int64".to_string(), "NO".to_string()]
        }));
        assert!(response.rows.iter().any(|row| {
            row == &vec!["status".to_string(), "Utf8".to_string(), "NO".to_string()]
        }));

        let _ = std::fs::remove_file(&catalog_path);
        let managed_dir = catalog_path.with_file_name(format!(
            "{}.managed",
            catalog_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("catalog file stem should be present")
        ));
        let _ = std::fs::remove_dir_all(managed_dir);
    }
}
