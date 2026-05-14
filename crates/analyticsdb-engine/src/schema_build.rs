use super::*;

pub(crate) fn utf8_schema(columns: &[&str]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|column| Field::new(*column, DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ))
}

pub(crate) fn build_arrow_schema_from_definitions(
    columns: &[TableColumnDefinition],
    _is_view: bool,
) -> Result<SchemaRef> {
    let mut fields = Vec::new();
    for col in columns {
        let dt = catalog_data_type(&col.data_type);
        fields.push(Field::new(&col.name, dt, col.nullable));
    }

    if !fields.iter().any(|f| f.name() == "_row_id") {
        fields.push(Field::new("_row_id", DataType::Utf8, true));
    }

    Ok(Arc::new(Schema::new(fields)))
}

pub(crate) fn build_arrow_schema_from_catalog_columns(columns: &[CatalogColumn]) -> Result<SchemaRef> {
    let definitions = columns
        .iter()
        .map(|c| TableColumnDefinition {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            nullable: c.nullable,
            default_value: c.default_value.clone(),
        })
        .collect::<Vec<_>>();
    build_arrow_schema_from_definitions(&definitions, false)
}

pub(crate) async fn build_partition_read_schema(
    ctx: &DfSessionContext,
    paths: Vec<&str>,
    columns: &[CatalogColumn],
) -> Result<SchemaRef> {
    let mut schema = build_arrow_schema_from_catalog_columns(columns)?;
    if !schema
        .fields()
        .iter()
        .any(|field| matches!(field.data_type(), DataType::Utf8))
    {
        return Ok(schema);
    }

    let sample_df = ctx
        .read_parquet(paths, ParquetReadOptions::default())
        .await
        .map_err(sanitize_error)?;
    let inferred_schema = Arc::new(sample_df.schema().as_arrow().as_ref().clone());
    let sample_batches = sample_df
        .limit(0, Some(1024))
        .map_err(sanitize_error)?
        .collect()
        .await
        .map_err(sanitize_error)?;

    schema = refine_utf8_partition_schema_from_sample(&schema, &inferred_schema, &sample_batches);
    Ok(schema)
}

pub(crate) fn refine_utf8_partition_schema_from_sample(
    schema: &SchemaRef,
    inferred_schema: &SchemaRef,
    sample_batches: &[RecordBatch],
) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            if !matches!(field.data_type(), DataType::Utf8) || field.name() == "_row_id" {
                return Arc::clone(field);
            }

            if let Ok(inferred_idx) = inferred_schema.index_of(field.name()) {
                let inferred_field = inferred_schema.field(inferred_idx);
                if !matches!(inferred_field.data_type(), DataType::Utf8) {
                    return Arc::new(Field::new(
                        field.name(),
                        inferred_field.data_type().clone(),
                        field.is_nullable(),
                    ));
                }
            }

            match infer_utf8_partition_column_type(field.name(), sample_batches) {
                Some(data_type) => {
                    Arc::new(Field::new(field.name(), data_type, field.is_nullable()))
                }
                None => Arc::clone(field),
            }
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

pub(crate) fn infer_utf8_partition_column_type(
    column_name: &str,
    sample_batches: &[RecordBatch],
) -> Option<DataType> {
    let mut saw_value = false;
    let mut all_numeric = true;
    let mut all_date = true;

    for batch in sample_batches {
        let Ok(idx) = batch.schema().index_of(column_name) else {
            return None;
        };
        let values = batch.column(idx).as_any().downcast_ref::<StringArray>()?;

        for row in 0..values.len() {
            if values.is_null(row) {
                continue;
            }
            let value = values.value(row).trim();
            if value.is_empty() {
                all_numeric = false;
                all_date = false;
                continue;
            }
            saw_value = true;
            if value.parse::<f64>().is_err() {
                all_numeric = false;
            }
            if chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_err() {
                all_date = false;
            }
        }
    }

    if !saw_value {
        return None;
    }
    if all_date {
        Some(DataType::Date32)
    } else if all_numeric {
        Some(DataType::Float64)
    } else {
        None
    }
}

pub(crate) fn catalog_columns_from_schema(schema: &SchemaRef) -> Vec<CatalogColumn> {
    schema
        .fields()
        .iter()
        .map(|f| CatalogColumn {
            name: f.name().clone(),
            data_type: format!("{:?}", f.data_type()),
            nullable: f.is_nullable(),
            default_value: None,
        })
        .collect()
}

pub(crate) fn catalog_constraints_from_definitions(
    _table: &str,
    _database: Option<&str>,
    _schema: Option<&str>,
    _session: &SessionContext,
    defs: &[TableConstraintDefinition],
) -> Result<Vec<CatalogTableConstraint>> {
    let mut out = Vec::new();
    for def in defs {
        let (kind, ref_db, ref_sch, ref_tab, ref_cols) = match def {
            TableConstraintDefinition::PrimaryKey { .. } => (
                CatalogTableConstraintKind::PrimaryKey,
                None,
                None,
                None,
                Vec::new(),
            ),
            TableConstraintDefinition::ForeignKey {
                referenced_database,
                referenced_schema,
                referenced_table,
                referenced_columns,
                ..
            } => (
                CatalogTableConstraintKind::ForeignKey,
                referenced_database.clone(),
                referenced_schema.clone(),
                Some(referenced_table.clone()),
                referenced_columns.clone(),
            ),
            TableConstraintDefinition::Unique { .. } => (
                CatalogTableConstraintKind::Unique,
                None,
                None,
                None,
                Vec::new(),
            ),
        };
        let name_opt = match def {
            TableConstraintDefinition::PrimaryKey { name, .. } => name.clone(),
            TableConstraintDefinition::ForeignKey { name, .. } => name.clone(),
            TableConstraintDefinition::Unique { name, .. } => name.clone(),
        };
        let columns = match def {
            TableConstraintDefinition::PrimaryKey { columns, .. } => columns.clone(),
            TableConstraintDefinition::ForeignKey { columns, .. } => columns.clone(),
            TableConstraintDefinition::Unique { columns, .. } => columns.clone(),
        };
        out.push(CatalogTableConstraint {
            name: name_opt.unwrap_or_else(|| "auto_constraint".to_string()),
            columns,
            kind,
            referenced_database: ref_db,
            referenced_schema: ref_sch,
            referenced_table: ref_tab,
            referenced_columns: ref_cols,
        });
    }
    Ok(out)
}

pub(crate) fn catalog_data_type(data_type: &str) -> DataType {
    let upper = data_type.to_ascii_uppercase();
    if upper.starts_with("NUMERIC") || upper.starts_with("DECIMAL") {
        // Default to Decimal128(38, 10) for prototype if no precision/scale specified
        // or parse them if we want to be more precise
        return DataType::Decimal128(38, 10);
    }
    if upper.starts_with("TIMESTAMP") {
        if upper.contains("WITH TIME ZONE") || upper.contains("TZ") || upper.contains("UTC") {
            return DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        } else {
            return DataType::Timestamp(TimeUnit::Microsecond, None);
        }
    }
    match upper.as_str() {
        "INT" | "INTEGER" | "INT4" | "INT32" => DataType::Int32,
        "BIGINT" | "INT8" | "INT64" => DataType::Int64,
        "TEXT" | "VARCHAR" | "STRING" | "UTF8" => DataType::Utf8,
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "FLOAT4" | "REAL" | "FLOAT32" => DataType::Float32,
        "FLOAT8" | "DOUBLE PRECISION" | "FLOAT64" => DataType::Float64,
        "DATE" | "DATE32" => DataType::Date32,
        _ => DataType::Utf8,
    }
}
