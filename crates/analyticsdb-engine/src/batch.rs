use super::*;

pub(crate) fn build_record_batch_from_rows(
    schema: &SchemaRef,
    catalog_columns: &[CatalogColumn],
    target_columns: Option<Vec<String>>,
    rows: &[Vec<String>],
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (i, field) in schema.fields().iter().enumerate() {
        let mut values = Vec::new();

        if field.name() == "_row_id" {
            for _ in 0..rows.len() {
                values.push(Some(uuid::Uuid::now_v7().to_string()));
            }
        } else {
            let target_idx = if let Some(ref names) = target_columns {
                names.iter().position(|n| {
                    if n.starts_with('"') && n.ends_with('"') {
                        &n[1..n.len() - 1] == field.name()
                    } else {
                        n.eq_ignore_ascii_case(field.name())
                    }
                })
            } else {
                Some(i)
            };

            for row in rows {
                if let Some(idx) = target_idx {
                    if idx < row.len() {
                        if row[idx].trim().eq_ignore_ascii_case("NULL") {
                            if !field.is_nullable() {
                                bail!(
                                    "Column '{}' must be provided because it is NOT NULL",
                                    field.name()
                                );
                            }
                            values.push(None);
                        } else {
                            values.push(Some(normalize_insert_value(&row[idx], field.data_type())));
                        }
                    } else if let Some(default_value) =
                        default_value_for_column(catalog_columns, field.name(), field.data_type())
                    {
                        values.push(Some(default_value));
                    } else if !field.is_nullable() {
                        bail!(
                            "Column '{}' must be provided because it is NOT NULL",
                            field.name()
                        );
                    } else {
                        values.push(None);
                    }
                } else if let Some(default_value) =
                    default_value_for_column(catalog_columns, field.name(), field.data_type())
                {
                    values.push(Some(default_value));
                } else if !field.is_nullable() {
                    bail!(
                        "Column '{}' must be provided because it is NOT NULL",
                        field.name()
                    );
                } else {
                    values.push(None);
                }
            }
        }

        let array: ArrayRef = Arc::new(StringArray::from(values));
        // Cast to actual type
        let casted = datafusion::arrow::compute::cast(&array, field.data_type())?;
        columns.push(casted);
    }
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

pub(crate) fn default_value_for_column(
    catalog_columns: &[CatalogColumn],
    column_name: &str,
    data_type: &DataType,
) -> Option<String> {
    let raw = catalog_columns
        .iter()
        .find(|column| column.name == column_name)?
        .default_value
        .as_deref()
        .unwrap_or_else(|| {
            if column_name.eq_ignore_ascii_case("created_at") {
                "CURRENT_TIMESTAMP"
            } else {
                ""
            }
        })
        .trim();
    if raw.is_empty() {
        return None;
    }

    if raw.eq_ignore_ascii_case("CURRENT_TIMESTAMP()")
        || raw.eq_ignore_ascii_case("CURRENT_TIMESTAMP")
        || raw.eq_ignore_ascii_case("NOW()")
    {
        return Some(chrono::Utc::now().to_rfc3339());
    }

    Some(normalize_insert_value(raw, data_type))
}

pub(crate) fn normalize_insert_value(raw: &str, _data_type: &DataType) -> String {
    let trimmed = raw.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        return trimmed[1..trimmed.len() - 1].replace("''", "'");
    }
    trimmed.to_string()
}

pub(crate) async fn write_dataframe_to_table_snapshot(
    df: datafusion::dataframe::DataFrame,
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
) -> Result<usize> {
    let mut stream = df.execute_stream().await.map_err(sanitize_error)?;
    let mut row_count = 0;
    let mut prepared_batches = Vec::new();

    use futures::StreamExt;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(sanitize_error)?;
        let (batch_row_count, prepared_batch) = prepare_batch_for_storage(batch)?;
        row_count += batch_row_count;
        prepared_batches.push(prepared_batch);
    }

    storage::clear_parquet_files(store, prefix).await?;

    let mut manifest_entries = Vec::new();
    for prepared_batch in prepared_batches {
        let filename = format!("{}.parquet", uuid::Uuid::now_v7());
        let key = prefix.clone().join(filename.as_str());
        let bytes = storage::encode_parquet_batches(
            prepared_batch.schema(),
            std::slice::from_ref(&prepared_batch),
        )?;
        let size = bytes.len() as u64;
        let entry_row_count = prepared_batch.num_rows() as i64;
        store.put(&key, bytes.into()).await?;
        manifest_entries.push(crate::manifest::ManifestEntry {
            path: filename,
            size,
            row_count: entry_row_count,
        });
    }
    crate::manifest::replace_manifest(store, prefix, manifest_entries).await?;
    Ok(row_count)
}

pub(crate) fn prepare_batch_for_storage(batch: RecordBatch) -> Result<(usize, RecordBatch)> {
    let num_rows = batch.num_rows();
    let has_row_id = batch
        .schema()
        .fields()
        .iter()
        .any(|f| f.name() == "_row_id");

    if has_row_id {
        let mut columns = batch.columns().to_vec();
        let row_id_col_idx = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == "_row_id")
            .ok_or_else(|| anyhow::anyhow!("_row_id column not found in batch schema"))?;
        let row_id_array = batch.column(row_id_col_idx);

        // Check if it's the AUTO_UUID placeholder
        let mut needs_replace = false;
        if let Some(strings) = row_id_array.as_any().downcast_ref::<StringArray>() {
            if strings.len() > 0 && strings.value(0) == "AUTO_UUID" {
                needs_replace = true;
            }
        }

        if needs_replace {
            let row_ids: Vec<String> = (0..num_rows)
                .map(|_| uuid::Uuid::now_v7().to_string())
                .collect();
            columns[row_id_col_idx] = Arc::new(StringArray::from(row_ids));
            Ok((num_rows, RecordBatch::try_new(batch.schema(), columns)?))
        } else {
            Ok((num_rows, batch))
        }
    } else {
        // We need to inject _row_id
        let mut columns = batch.columns().to_vec();
        let row_ids: Vec<String> = (0..num_rows)
            .map(|_| uuid::Uuid::now_v7().to_string())
            .collect();
        columns.push(Arc::new(StringArray::from(row_ids)));

        let mut fields = batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("_row_id", DataType::Utf8, true)));
        let new_schema = Arc::new(Schema::new(fields));

        Ok((num_rows, RecordBatch::try_new(new_schema, columns)?))
    }
}

pub(crate) async fn persist_empty_table_snapshot(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
    schema: &SchemaRef,
) -> Result<()> {
    storage::clear_parquet_files(store, prefix).await?;
    let key = prefix.clone().join("empty.parquet");
    storage::write_empty_parquet(store, &key, schema).await?;
    crate::manifest::replace_manifest(store, prefix, Vec::new()).await
}

pub(crate) fn utf8_record_batch(columns: &[&str], rows: &[Vec<String>]) -> Result<RecordBatch> {
    let schema = utf8_schema(columns);
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for i in 0..columns.len() {
        let values: Vec<Option<String>> = rows.iter().map(|r| Some(r[i].clone())).collect();
        arrays.push(Arc::new(StringArray::from(values)));
    }
    Ok(RecordBatch::try_new(schema, arrays)?)
}

pub(crate) fn record_batch_rows(batch: &RecordBatch) -> Result<Vec<Vec<String>>> {
    let mut rows = Vec::new();
    for row_idx in 0..batch.num_rows() {
        let mut row = Vec::new();
        for col_idx in 0..batch.num_columns() {
            row.push(array_value_to_string(batch.column(col_idx), row_idx).unwrap_or_default());
        }
        rows.push(row);
    }
    Ok(rows)
}
