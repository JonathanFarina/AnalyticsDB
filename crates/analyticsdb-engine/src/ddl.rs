use super::*;

impl PrototypeEngine {
    pub(super) async fn execute_metadata_query(
        &self,
        request: &QueryRequest,
        statement: MetadataStatement,
        admission: QueryAdmission,
        started: Instant,
        _probe: &query_log::QueryProbe,
    ) -> Result<QueryExecutionResult> {
        let (schema, batches, message, outcome, new_session) = match statement {
            MetadataStatement::CreateDatabase { .. }
            | MetadataStatement::CreateAggregate { .. }
            | MetadataStatement::CreateCollation { .. }
            | MetadataStatement::CreateConversion { .. }
            | MetadataStatement::CreateFunction { .. }
            | MetadataStatement::AlterFunction { .. }
            | MetadataStatement::DropFunction { .. }
            | MetadataStatement::CreateSchema { .. }
            | MetadataStatement::Begin
            | MetadataStatement::Commit
            | MetadataStatement::Rollback
            | MetadataStatement::InformationSchemaSchemata { .. }
            | MetadataStatement::InformationSchemaTables { .. }
            | MetadataStatement::InformationSchemaColumns { .. }
            | MetadataStatement::InformationSchemaViews { .. }
            | MetadataStatement::InformationSchemaTableConstraints { .. }
            | MetadataStatement::InformationSchemaKeyColumnUsage { .. }
            | MetadataStatement::InformationSchemaConstraintColumnUsage { .. }
            | MetadataStatement::InformationSchemaConstraintTableUsage { .. }
            | MetadataStatement::InformationSchemaReferentialConstraints { .. }
            | MetadataStatement::CreateUser { .. }
            | MetadataStatement::DropUser { .. }
            | MetadataStatement::CreateGroup { .. }
            | MetadataStatement::AlterGroup { .. }
            | MetadataStatement::DropGroup { .. }
            | MetadataStatement::AlterUserPassword { .. } => match statement {
                MetadataStatement::InformationSchemaSchemata { sql } => {
                    let columns = [
                        "catalog_name",
                        "schema_name",
                        "schema_owner",
                        "default_character_set_catalog",
                        "default_character_set_schema",
                        "default_character_set_name",
                        "sql_path",
                    ];
                    let rows = self
                        .information_schema_schemata_rows(&request.session)
                        .await?;
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "information_schema.schemata",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!(
                            "{row_count} information_schema.schemata row(s) listed successfully."
                        ),
                        rows_outcome(),
                        request.session.clone(),
                    )
                }
                MetadataStatement::InformationSchemaTables { sql } => {
                    let columns = [
                        "table_catalog",
                        "table_schema",
                        "table_name",
                        "table_type",
                        "self_referencing_column_name",
                        "reference_generation",
                        "user_defined_type_catalog",
                        "user_defined_type_schema",
                        "user_defined_type_name",
                        "is_insertable_into",
                        "is_typed",
                        "commit_action",
                    ];
                    let rows = self
                        .information_schema_tables_rows(&request.session)
                        .await?;
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "information_schema.tables",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!(
                            "{row_count} information_schema.tables row(s) listed successfully."
                        ),
                        rows_outcome(),
                        request.session.clone(),
                    )
                }
                MetadataStatement::InformationSchemaColumns { sql } => {
                    let columns = [
                        "table_catalog",
                        "table_schema",
                        "table_name",
                        "column_name",
                        "ordinal_position",
                        "column_default",
                        "is_nullable",
                        "data_type",
                        "character_maximum_length",
                        "character_octet_length",
                        "numeric_precision",
                        "numeric_precision_radix",
                        "numeric_scale",
                        "datetime_precision",
                    ];
                    let rows = self
                        .information_schema_columns_rows(&request.session)
                        .await?;
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "information_schema.columns",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!(
                            "{row_count} information_schema.columns row(s) listed successfully."
                        ),
                        rows_outcome(),
                        request.session.clone(),
                    )
                }
                MetadataStatement::InformationSchemaViews { sql } => {
                    let columns = [
                        "table_catalog",
                        "table_schema",
                        "table_name",
                        "view_definition",
                        "check_option",
                        "is_updatable",
                        "is_insertable_into",
                        "is_trigger_updatable",
                        "is_trigger_deletable",
                        "is_trigger_insertable_into",
                    ];
                    let rows = self.information_schema_views_rows(&request.session).await?;
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "information_schema.views",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!("{row_count} information_schema.views row(s) listed successfully."),
                        rows_outcome(),
                        request.session.clone(),
                    )
                }
                MetadataStatement::InformationSchemaTableConstraints { sql } => {
                    let columns = [
                        "constraint_catalog",
                        "constraint_schema",
                        "constraint_name",
                        "table_catalog",
                        "table_schema",
                        "table_name",
                        "constraint_type",
                        "is_deferrable",
                        "initially_deferred",
                        "enforced",
                        "nulls_distinct",
                    ];
                    let rows = self
                        .information_schema_table_constraints_rows(&request.session)
                        .await?;
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "information_schema.table_constraints",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!(
                            "{row_count} information_schema.table_constraints row(s) listed successfully."
                        ),
                        rows_outcome(),
                        request.session.clone(),
                    )
                }
                MetadataStatement::InformationSchemaKeyColumnUsage { sql } => {
                    let columns = [
                        "constraint_catalog",
                        "constraint_schema",
                        "constraint_name",
                        "table_catalog",
                        "table_schema",
                        "table_name",
                        "column_name",
                        "ordinal_position",
                        "position_in_unique_constraint",
                    ];
                    let rows = self
                        .information_schema_key_column_usage_rows(&request.session)
                        .await?;
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "information_schema.key_column_usage",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!(
                            "{row_count} information_schema.key_column_usage row(s) listed successfully."
                        ),
                        rows_outcome(),
                        request.session.clone(),
                    )
                }
                MetadataStatement::InformationSchemaConstraintColumnUsage { sql } => {
                    let columns = [
                        "table_catalog",
                        "table_schema",
                        "table_name",
                        "column_name",
                        "constraint_catalog",
                        "constraint_schema",
                        "constraint_name",
                    ];
                    let rows = self
                        .information_schema_constraint_column_usage_rows(&request.session)
                        .await?;
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "information_schema.constraint_column_usage",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!(
                            "{row_count} information_schema.constraint_column_usage row(s) listed successfully."
                        ),
                        rows_outcome(),
                        request.session.clone(),
                    )
                }
                MetadataStatement::InformationSchemaConstraintTableUsage { sql } => {
                    let columns = [
                        "table_catalog",
                        "table_schema",
                        "table_name",
                        "constraint_catalog",
                        "constraint_schema",
                        "constraint_name",
                    ];
                    let rows = self
                        .information_schema_constraint_table_usage_rows(&request.session)
                        .await?;
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "information_schema.constraint_table_usage",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!(
                            "{row_count} information_schema.constraint_table_usage row(s) listed successfully."
                        ),
                        rows_outcome(),
                        request.session.clone(),
                    )
                }
                MetadataStatement::InformationSchemaReferentialConstraints { sql } => {
                    let columns = [
                        "constraint_catalog",
                        "constraint_schema",
                        "constraint_name",
                        "unique_constraint_catalog",
                        "unique_constraint_schema",
                        "unique_constraint_name",
                        "match_option",
                        "update_rule",
                        "delete_rule",
                    ];
                    let rows = self
                        .information_schema_referential_constraints_rows(&request.session)
                        .await?;
                    let (batch, row_count) = execute_pg_catalog_select(
                        &sql,
                        "information_schema.referential_constraints",
                        &columns,
                        &rows,
                    )?;
                    (
                        batch.schema(),
                        vec![batch],
                        format!(
                            "{row_count} information_schema.referential_constraints row(s) listed successfully."
                        ),
                        rows_outcome(),
                        request.session.clone(),
                    )
                }
                _ => {
                    let (message, new_session) = self
                        .control_plane
                        .execute_metadata_statement(&request.session, &statement)
                        .await?;
                    (
                        Arc::new(Schema::empty()),
                        Vec::new(),
                        message,
                        command_outcome("OK", 0),
                        new_session,
                    )
                }
            },
            MetadataStatement::CreateIndex {
                ref database,
                ref schema,
                ref table,
                ref name,
                ref columns,
                unique,
                concurrently: _,
            } => {
                let preview_relation = self
                    .control_plane
                    .preview_create_index(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        table,
                        name,
                        columns,
                        unique,
                    )
                    .await?;
                let relation_lock = self.relation_lock(&preview_relation).await?;
                let _write_guard = relation_lock.write().await;
                let (idx_store, idx_prefix) = table_store_prefix(&preview_relation)?;
                let version = uuid::Uuid::now_v7().to_string();
                let snapshot = self
                    .build_index_snapshot_for_relation(
                        &request.session,
                        &preview_relation,
                        name,
                        &version,
                    )
                    .await?;
                write_index_snapshot(&idx_store, &idx_prefix, &snapshot, &version).await?;

                let (message, _new_session) = match self
                    .control_plane
                    .execute_metadata_statement(&request.session, &statement)
                    .await
                {
                    Ok(result) => result,
                    Err(error) => {
                        let _ = remove_index_snapshot(&idx_store, &idx_prefix, name).await;
                        return Err(error);
                    }
                };

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    message,
                    command_outcome("CREATE INDEX", 0),
                    request.session.clone(),
                )
            }
            MetadataStatement::AlterIndex {
                ref database,
                ref schema,
                ref name,
                ref operation,
            } => {
                let relation = self
                    .control_plane
                    .index_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        name,
                    )
                    .await?;
                let relation_lock = self.relation_lock(&relation).await?;
                let _write_guard = relation_lock.write().await;

                let new_name = match operation {
                    AlterObjectOperation::Rename { new_name } => new_name.clone(),
                    _ => anyhow::bail!("Unsupported index operation"),
                };
                let mut preview_relation = relation.clone();
                let index = preview_relation
                    .indexes
                    .iter_mut()
                    .find(|index| index.name == *name)
                    .ok_or_else(|| anyhow::anyhow!("Index '{}' not found", name))?;
                index.name = new_name.clone();
                let (idx_store, idx_prefix) = table_store_prefix(&preview_relation)?;
                let version = uuid::Uuid::now_v7().to_string();
                let snapshot = self
                    .build_index_snapshot_for_relation(
                        &request.session,
                        &preview_relation,
                        &new_name,
                        &version,
                    )
                    .await?;
                write_index_snapshot(&idx_store, &idx_prefix, &snapshot, &version).await?;

                let message = match self
                    .control_plane
                    .rename_index(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &relation.name,
                        name,
                        &new_name,
                    )
                    .await
                {
                    Ok(msg) => msg,
                    Err(error) => {
                        let _ = remove_index_snapshot(&idx_store, &idx_prefix, &new_name).await;
                        return Err(error);
                    }
                };
                let _ = remove_index_snapshot(&idx_store, &idx_prefix, name).await;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    message,
                    command_outcome("ALTER INDEX", 0),
                    request.session.clone(),
                )
            }
            MetadataStatement::DropIndex {
                ref database,
                ref schema,
                ref name,
                if_exists,
                cascade: _,
            } => {
                let relation = match self
                    .control_plane
                    .index_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        name,
                    )
                    .await
                {
                    Ok(relation) => Some(relation),
                    Err(_) if if_exists => None,
                    Err(error) => return Err(error),
                };
                if let Some(relation) = &relation {
                    let relation_lock = self.relation_lock(relation).await?;
                    let _write_guard = relation_lock.write().await;

                    let (message, _new_session) = self
                        .control_plane
                        .execute_metadata_statement(&request.session, &statement)
                        .await?;

                    if let Ok((idx_store, idx_prefix)) = table_store_prefix(relation) {
                        let _ = remove_index_snapshot(&idx_store, &idx_prefix, name).await;
                    }

                    (
                        Arc::new(Schema::empty()),
                        Vec::new(),
                        message,
                        command_outcome("DROP INDEX", 0),
                        request.session.clone(),
                    )
                } else {
                    let (message, _new_session) = self
                        .control_plane
                        .execute_metadata_statement(&request.session, &statement)
                        .await?;
                    (
                        Arc::new(Schema::empty()),
                        Vec::new(),
                        message,
                        command_outcome("DROP INDEX", 0),
                        request.session.clone(),
                    )
                }
            }
            MetadataStatement::Reindex { ref target } => match target {
                ReindexTarget::Index {
                    database,
                    schema,
                    name,
                    concurrently: _,
                } => {
                    let relation = self
                        .control_plane
                        .index_relation(
                            &request.session,
                            database.as_deref(),
                            schema.as_deref(),
                            name,
                        )
                        .await?;
                    let relation_lock = self.relation_lock(&relation).await?;
                    let _write_guard = relation_lock.write().await;

                    self.invalidate_session_contexts().await;
                    let (idx_store, idx_prefix) = table_store_prefix(&relation)?;
                    let version = uuid::Uuid::now_v7().to_string();
                    let snapshot = self
                        .build_index_snapshot_for_relation(
                            &request.session,
                            &relation,
                            name,
                            &version,
                        )
                        .await?;
                    write_index_snapshot(&idx_store, &idx_prefix, &snapshot, &version).await?;

                    (
                        Arc::new(Schema::empty()),
                        Vec::new(),
                        format!("Index '{}' reindexed successfully.", name),
                        command_outcome("REINDEX", 0),
                        request.session.clone(),
                    )
                }
                ReindexTarget::Table {
                    database,
                    schema,
                    name,
                    concurrently: _,
                } => {
                    let relation = self
                        .control_plane
                        .table_relation(
                            &request.session,
                            database.as_deref(),
                            schema.as_deref(),
                            name,
                        )
                        .await?;
                    let relation_lock = self.relation_lock(&relation).await?;
                    let _write_guard = relation_lock.write().await;

                    self.rebuild_all_index_snapshots(&request.session, &relation)
                        .await?;

                    (
                        Arc::new(Schema::empty()),
                        Vec::new(),
                        format!(
                            "Reindexed {} index(es) on '{}.{}.{}'.",
                            relation.indexes.len(),
                            relation.database,
                            relation.schema,
                            relation.name
                        ),
                        command_outcome("REINDEX", 0),
                        request.session.clone(),
                    )
                }
            },
            MetadataStatement::CreateView {
                database,
                schema,
                name,
                definition_sql,
            } => {
                // Determine schema of the view query
                let session = SessionContext {
                    database: database.clone().unwrap_or(request.session.database.clone()),
                    schema: schema.clone().unwrap_or(request.session.schema.clone()),
                    ..request.session.clone()
                };
                let query_sql = definition_sql.clone();
                let target_schema_opt = schema.clone();
                let columns = async move {
                    // Use a context where the default schema is the target schema
                    // This ensures unqualified names in the view SQL resolve correctly.
                    let context = self.create_session_context(&session).await?;
                    let rewritten_query_sql = sql_rewriter::rewrite_sql_for_postgres_compatibility(
                        &query_sql,
                        &self.control_plane,
                        &session,
                    )
                    .await?;
                    let dataframe = context
                        .sql(&rewritten_query_sql)
                        .await
                        .map_err(sanitize_error)?;
                    let arrow_schema = Arc::new(dataframe.schema().as_arrow().clone());
                    Ok::<_, anyhow::Error>(catalog_columns_from_schema(&arrow_schema))
                }
                .await?;

                let message = self
                    .control_plane
                    .register_view(
                        &request.session,
                        database.as_deref(),
                        target_schema_opt.as_deref(),
                        &name,
                        &definition_sql,
                        columns,
                    )
                    .await?;
                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    message,
                    command_outcome("CREATE VIEW", 0),
                    request.session.clone(),
                )
            }

            MetadataStatement::CreateExternalTable {
                database,
                schema,
                name,
                format,
                location,
            } => {
                let context = DfSessionContext::new_with_config(base_session_config());
                let table_path =
                    datafusion::datasource::listing::ListingTableUrl::parse(&location)?;
                let config = datafusion::datasource::listing::ListingTableConfig::new(table_path)
                    .with_listing_options(datafusion::datasource::listing::ListingOptions::new(
                        Arc::new(
                            datafusion::datasource::file_format::parquet::ParquetFormat::default(),
                        ),
                    ))
                    .infer_schema(&context.state())
                    .await?;
                let arrow_schema = config
                    .file_schema
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("Failed to infer schema for external table"))?;
                let columns = catalog_columns_from_schema(&arrow_schema);

                let message = self
                    .control_plane
                    .register_external_table(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                        &location,
                        format,
                        columns,
                    )
                    .await?;
                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    message,
                    command_outcome("CREATE TABLE", 0),
                    request.session.clone(),
                )
            }

            MetadataStatement::CreateTableAs {
                database,
                schema,
                name,
                query_sql,
            } => {
                let location_str = self
                    .control_plane
                    .managed_table_storage_location(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;
                let (store, prefix) = storage::store_for_location(&location_str)?;

                let context = self.create_session_context(&request.session).await?;
                let rewritten_query_sql = sql_rewriter::rewrite_sql_for_postgres_compatibility(
                    &query_sql,
                    &self.control_plane,
                    &request.session,
                )
                .await?;
                let dataframe = context
                    .sql(&rewritten_query_sql)
                    .await
                    .map_err(sanitize_error)?;
                let arrow_schema = Arc::new(dataframe.schema().as_arrow().clone());
                let columns_metadata = catalog_columns_from_schema(&arrow_schema);
                let row_count =
                    write_dataframe_to_table_snapshot(dataframe, &store, &prefix).await?;

                let created_message = self
                    .control_plane
                    .register_managed_table(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                        &location_str,
                        columns_metadata,
                        Vec::new(),
                    )
                    .await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!("{created_message} {row_count} row(s) materialized."),
                    command_outcome("CREATE TABLE", 0),
                    request.session.clone(),
                )
            }
            MetadataStatement::SelectInto {
                database,
                schema,
                name,
                query_sql,
            } => {
                let location_str = self
                    .control_plane
                    .managed_table_storage_location(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;
                let (store, prefix) = storage::store_for_location(&location_str)?;

                let context = self.create_session_context(&request.session).await?;
                let rewritten_query_sql = sql_rewriter::rewrite_sql_for_postgres_compatibility(
                    &query_sql,
                    &self.control_plane,
                    &request.session,
                )
                .await?;
                let dataframe = context
                    .sql(&rewritten_query_sql)
                    .await
                    .map_err(sanitize_error)?;
                let arrow_schema = Arc::new(dataframe.schema().as_arrow().clone());
                let columns_metadata = catalog_columns_from_schema(&arrow_schema);
                let row_count =
                    write_dataframe_to_table_snapshot(dataframe, &store, &prefix).await?;

                let created_message = self
                    .control_plane
                    .register_managed_table(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                        &location_str,
                        columns_metadata,
                        Vec::new(),
                    )
                    .await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!("{created_message} {row_count} row(s) materialized by SELECT INTO."),
                    command_outcome("SELECT INTO", row_count as u64),
                    request.session.clone(),
                )
            }
            MetadataStatement::CreateTable {
                database,
                schema,
                name,
                columns,
                constraints,
            } => {
                let location_str = self
                    .control_plane
                    .managed_table_storage_location(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;
                let (store, prefix) = storage::store_for_location(&location_str)?;
                let arrow_schema = build_arrow_schema_from_definitions(&columns, false)?;

                persist_empty_table_snapshot(&store, &prefix, &arrow_schema).await?;

                let created_message = self
                    .control_plane
                    .register_managed_table(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                        &location_str,
                        catalog_columns_from_schema(&arrow_schema),
                        catalog_constraints_from_definitions(
                            &name,
                            database.as_deref(),
                            schema.as_deref(),
                            &request.session,
                            &constraints,
                        )?,
                    )
                    .await?;
                let relation = self
                    .control_plane
                    .table_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;
                self.rebuild_all_index_snapshots(&request.session, &relation)
                    .await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    created_message,
                    command_outcome("CREATE TABLE", 0),
                    request.session.clone(),
                )
            }
            MetadataStatement::InsertInto {
                database,
                schema,
                name,
                columns,
                rows,
            } => {
                let relation = self
                    .control_plane
                    .table_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;
                let storage_location = relation.storage_path.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Managed table '{}.{}.{}' is missing a storage path",
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })?;
                let (store, prefix) = storage::store_for_location(storage_location)?;
                let relation_lock = self.relation_lock(&relation).await?;
                let _write_guard = relation_lock.write().await;

                let column_definitions: Vec<TableColumnDefinition> = relation
                    .columns
                    .iter()
                    .map(|column| TableColumnDefinition {
                        name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        nullable: column.nullable,
                        default_value: column.default_value.clone(),
                    })
                    .collect();
                let arrow_schema = build_arrow_schema_from_definitions(&column_definitions, false)?;
                let expected_values = columns.as_ref().map_or_else(
                    || {
                        relation
                            .columns
                            .iter()
                            .filter(|c| c.name != "_row_id")
                            .count()
                    },
                    Vec::len,
                );
                for row in &rows {
                    if row.len() != expected_values {
                        bail!(
                            "Expected {expected_values} value(s) per row, found {}",
                            row.len()
                        );
                    }
                }
                let batch =
                    build_record_batch_from_rows(&arrow_schema, &relation.columns, columns, &rows)?;
                let (row_count, prepared_batch) = prepare_batch_for_storage(batch)?;

                self.validate_batch_against_table_uniqueness(
                    &request.session,
                    &relation,
                    &prepared_batch,
                )
                .await?;

                crate::manifest::append_batch(&store, &prefix, prepared_batch).await?;
                self.refresh_index_snapshots_after_mutation(&request.session, &relation)
                    .await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!(
                        "Inserted {row_count} row(s) into '{}.{}.{}'.",
                        relation.database, relation.schema, relation.name
                    ),
                    command_outcome("INSERT", row_count as u64),
                    request.session.clone(),
                )
            }
            MetadataStatement::Update {
                database,
                schema,
                name,
                assignments,
                selection_sql,
            } => {
                let relation = self
                    .control_plane
                    .table_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;
                let storage_location = relation.storage_path.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Managed table '{}.{}.{}' is missing a storage path",
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })?;
                let (store, prefix) = storage::store_for_location(storage_location)?;
                let relation_lock = self.relation_lock(&relation).await?;
                let _write_guard = relation_lock.write().await;

                let context = self.create_session_context(&request.session).await?;
                let full_table_name = format!(
                    "\"{}\".\"{}\".\"{}\"",
                    relation.database, relation.schema, relation.name
                );

                let filter_clause = selection_sql
                    .as_ref()
                    .map(|sql| format!("WHERE {sql}"))
                    .unwrap_or_default();
                let mut update_expressions = Vec::new();
                for column in &relation.columns {
                    if let Some((_, value_sql)) = assignments.iter().find(|(name, _)| {
                        if name.starts_with('"') && name.ends_with('"') {
                            name[1..name.len() - 1] == column.name
                        } else {
                            name.eq_ignore_ascii_case(&column.name)
                        }
                    }) {
                        update_expressions.push(format!("{value_sql} AS \"{}\"", column.name));
                    } else {
                        update_expressions.push(format!("\"{}\"", column.name));
                    }
                }

                let update_sql = format!(
                    "SELECT {} FROM {full_table_name} {filter_clause}",
                    update_expressions.join(", ")
                );
                let rewritten_update_sql = sql_rewriter::rewrite_sql_for_postgres_compatibility(
                    &update_sql,
                    &self.control_plane,
                    &request.session,
                )
                .await?;
                let updated_dataframe = context
                    .sql(&rewritten_update_sql)
                    .await
                    .map_err(sanitize_error)?;
                let updated_batches = updated_dataframe.clone().collect().await?;
                let mut updated_rows = Vec::new();
                for batch in &updated_batches {
                    updated_rows.extend(record_batch_rows(batch)?);
                }
                self.validate_unique_indexes_for_rows(&relation, &updated_rows)?;

                let row_count =
                    write_dataframe_to_table_snapshot(updated_dataframe, &store, &prefix).await?;
                self.refresh_index_snapshots_after_mutation(&request.session, &relation)
                    .await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!(
                        "Updated {row_count} row(s) in '{}.{}.{}'.",
                        relation.database, relation.schema, relation.name
                    ),
                    command_outcome("UPDATE", row_count as u64),
                    request.session.clone(),
                )
            }
            MetadataStatement::Delete {
                database,
                schema,
                name,
                selection_sql,
            } => {
                let relation = self
                    .control_plane
                    .table_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;
                let storage_location = relation.storage_path.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Managed table '{}.{}.{}' is missing a storage path",
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })?;
                let (store, prefix) = storage::store_for_location(storage_location)?;
                let relation_lock = self.relation_lock(&relation).await?;
                let _write_guard = relation_lock.write().await;

                let context = self.create_session_context(&request.session).await?;
                let full_table_name = format!(
                    "\"{}\".\"{}\".\"{}\"",
                    relation.database, relation.schema, relation.name
                );

                let filter_clause = selection_sql
                    .as_ref()
                    .map(|sql| format!("WHERE NOT ({sql})"))
                    .unwrap_or_default();
                let delete_sql = format!("SELECT * FROM {full_table_name} {filter_clause}");
                let rewritten_delete_sql = sql_rewriter::rewrite_sql_for_postgres_compatibility(
                    &delete_sql,
                    &self.control_plane,
                    &request.session,
                )
                .await?;
                let remaining_dataframe = context
                    .sql(&rewritten_delete_sql)
                    .await
                    .map_err(sanitize_error)?;
                let row_count =
                    write_dataframe_to_table_snapshot(remaining_dataframe, &store, &prefix).await?;
                self.refresh_index_snapshots_after_mutation(&request.session, &relation)
                    .await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!(
                        "DELETE completed for '{}.{}.{}'.",
                        relation.database, relation.schema, relation.name
                    ),
                    command_outcome("DELETE", row_count as u64),
                    request.session.clone(),
                )
            }
            MetadataStatement::Truncate {
                database,
                schema,
                name,
            } => {
                let relation = self
                    .control_plane
                    .table_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;
                let storage_location = relation.storage_path.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Managed table '{}.{}.{}' is missing a storage path",
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })?;
                let (store, prefix) = storage::store_for_location(storage_location)?;
                let relation_lock = self.relation_lock(&relation).await?;
                let _write_guard = relation_lock.write().await;

                let column_definitions: Vec<TableColumnDefinition> = relation
                    .columns
                    .iter()
                    .map(|column| TableColumnDefinition {
                        name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        nullable: column.nullable,
                        default_value: column.default_value.clone(),
                    })
                    .collect();
                let arrow_schema = build_arrow_schema_from_definitions(&column_definitions, false)?;

                persist_empty_table_snapshot(&store, &prefix, &arrow_schema).await?;
                self.refresh_index_snapshots_after_mutation(&request.session, &relation)
                    .await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!(
                        "TRUNCATE completed for '{}.{}.{}'.",
                        relation.database, relation.schema, relation.name
                    ),
                    command_outcome("TRUNCATE", 0),
                    request.session.clone(),
                )
            }
            MetadataStatement::VacuumTable {
                database,
                schema,
                name,
            } => {
                let relation = self
                    .control_plane
                    .table_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;
                let storage_location = relation.storage_path.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Managed table '{}.{}.{}' is missing a storage path",
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })?;
                let (store, prefix) = storage::store_for_location(storage_location)?;
                let relation_lock = self.relation_lock(&relation).await?;
                let _write_guard = relation_lock.write().await;

                const TARGET_FILE_BYTES: u64 = 128 * 1024 * 1024; // 128 MiB
                const MIN_FILES_TO_COMPACT: usize = 2;
                let files_written = crate::manifest::compact_table(
                    &store,
                    &prefix,
                    TARGET_FILE_BYTES,
                    MIN_FILES_TO_COMPACT,
                )
                .await?;

                tracing::info!(
                    "VACUUM compacted '{}.{}.{}': {} file(s) written",
                    relation.database,
                    relation.schema,
                    relation.name,
                    files_written,
                );
                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!(
                        "VACUUM completed for '{}.{}.{}'.",
                        relation.database, relation.schema, relation.name
                    ),
                    command_outcome("VACUUM", 0),
                    request.session.clone(),
                )
            }
            MetadataStatement::AlterTable {
                database,
                schema,
                name,
                operation,
            } => {
                let relation = self
                    .control_plane
                    .table_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await?;

                match operation {
                    AlterTableOperation::AddColumn { column } => {
                        let result = self
                            .control_plane
                            .add_column(
                                &request.session,
                                database.as_deref(),
                                schema.as_deref(),
                                &name,
                                CatalogColumn {
                                    name: column.name.clone(),
                                    data_type: column.data_type.clone(),
                                    nullable: column.nullable,
                                    default_value: column.default_value.clone(),
                                },
                            )
                            .await;

                        if result.is_ok() {
                            let updated_relation = self
                                .control_plane
                                .find_relation(
                                    &request.session,
                                    database.as_deref(),
                                    schema.as_deref(),
                                    &name,
                                )
                                .await?;
                            let _ = self
                                .physical_migrate_relation(&request.session, &updated_relation)
                                .await;
                        }

                        let _ = result?;

                        (
                            Arc::new(Schema::empty()),
                            Vec::new(),
                            format!(
                                "ALTER TABLE completed. Column '{}' added to '{}.{}.{}'.",
                                column.name, relation.database, relation.schema, relation.name
                            ),
                            command_outcome("ALTER TABLE", 0),
                            request.session.clone(),
                        )
                    }
                    AlterTableOperation::AddConstraint { constraint } => {
                        // ...
                        let relation_lock = self.relation_lock(&relation).await?;
                        let _write_guard = relation_lock.write().await;
                        let catalog_constraints = catalog_constraints_from_definitions(
                            &name,
                            database.as_deref(),
                            schema.as_deref(),
                            &request.session,
                            &[constraint],
                        )?;

                        let mut message = String::new();
                        for cat_con in catalog_constraints {
                            let preview_relation = self
                                .control_plane
                                .preview_add_constraint(
                                    &request.session,
                                    database.as_deref(),
                                    schema.as_deref(),
                                    &name,
                                    &cat_con,
                                )
                                .await?;
                            let staged_index_names = preview_relation
                                .indexes
                                .iter()
                                .filter(|index| {
                                    relation
                                        .indexes
                                        .iter()
                                        .all(|existing| existing.name != index.name)
                                })
                                .map(|index| index.name.clone())
                                .collect::<Vec<_>>();

                            let (idx_store, idx_prefix) = table_store_prefix(&preview_relation)?;
                            for index_name in &staged_index_names {
                                let version = uuid::Uuid::now_v7().to_string();
                                let snapshot = self
                                    .build_index_snapshot_for_relation(
                                        &request.session,
                                        &preview_relation,
                                        index_name,
                                        &version,
                                    )
                                    .await?;
                                write_index_snapshot(&idx_store, &idx_prefix, &snapshot, &version)
                                    .await?;
                            }

                            message = match self
                                .control_plane
                                .add_constraint(
                                    &request.session,
                                    database.as_deref(),
                                    schema.as_deref(),
                                    &name,
                                    cat_con.clone(),
                                )
                                .await
                            {
                                Ok(message) => message,
                                Err(error) => {
                                    for index_name in &staged_index_names {
                                        let _ = remove_index_snapshot(
                                            &idx_store,
                                            &idx_prefix,
                                            index_name,
                                        )
                                        .await;
                                    }
                                    return Err(error);
                                }
                            };
                        }

                        (
                            Arc::new(Schema::empty()),
                            Vec::new(),
                            message,
                            command_outcome("ALTER TABLE", 0),
                            request.session.clone(),
                        )
                    }
                    AlterTableOperation::RenameTable { new_name } => {
                        let relation_lock = self.relation_lock(&relation).await?;
                        let _write_guard = relation_lock.write().await;
                        // 1. Rename catalog metadata
                        self.control_plane
                            .rename_relation(
                                &request.session,
                                database.as_deref(),
                                schema.as_deref(),
                                &name,
                                &new_name,
                            )
                            .await?;

                        // 2. Physically rename managed directory if it exists
                        if let Some(storage_path_str) = &relation.storage_path {
                            let (store, old_prefix) =
                                storage::store_for_location(storage_path_str)?;
                            // Calculate new storage location by replacing the table name part.
                            // Managed tables use names like <db>__<schema>__<table>.table.parquet
                            let old_suffix = format!("{}.table.parquet", name);
                            let new_suffix = format!("{}.table.parquet", new_name);
                            let new_location_str =
                                storage_path_str.replace(&old_suffix, &new_suffix);
                            let (_, new_prefix) = storage::store_for_location(&new_location_str)?;
                            storage::rename_prefix(&store, &old_prefix, &new_prefix).await?;

                            // 3. Update the storage path in catalog after physical rename
                            self.control_plane
                                .update_relation_storage_path(
                                    &request.session,
                                    database.as_deref(),
                                    schema.as_deref(),
                                    &new_name,
                                    &new_location_str,
                                )
                                .await?;
                        }

                        (
                            Arc::new(Schema::empty()),
                            Vec::new(),
                            format!(
                                "ALTER TABLE completed. Relation '{}.{}.{}' renamed to '{}'.",
                                relation.database, relation.schema, relation.name, new_name
                            ),
                            command_outcome("ALTER TABLE", 0),
                            request.session.clone(),
                        )
                    }
                    AlterTableOperation::DropColumn {
                        column_name,
                        if_exists,
                        cascade: _,
                    } => {
                        let result = self
                            .control_plane
                            .drop_column(
                                &request.session,
                                database.as_deref(),
                                schema.as_deref(),
                                &name,
                                &column_name,
                                if_exists,
                            )
                            .await;

                        if result.is_ok() {
                            let updated_relation = self
                                .control_plane
                                .find_relation(
                                    &request.session,
                                    database.as_deref(),
                                    schema.as_deref(),
                                    &name,
                                )
                                .await?;
                            let _ = self
                                .physical_migrate_relation(&request.session, &updated_relation)
                                .await;
                        }

                        let message = result?;

                        (
                            Arc::new(Schema::empty()),
                            Vec::new(),
                            message,
                            command_outcome("ALTER TABLE", 0),
                            request.session.clone(),
                        )
                    }
                    AlterTableOperation::RenameColumn { old_name, new_name } => {
                        let relation_lock = self.relation_lock(&relation).await?;
                        let _write_guard = relation_lock.write().await;

                        let message = self
                            .control_plane
                            .rename_column(
                                &request.session,
                                database.as_deref(),
                                schema.as_deref(),
                                &name,
                                &old_name,
                                &new_name,
                            )
                            .await?;

                        // Physically rewrite the table to apply the rename
                        let updated_relation = self
                            .control_plane
                            .find_relation(
                                &request.session,
                                database.as_deref(),
                                schema.as_deref(),
                                &name,
                            )
                            .await?;
                        let _ = self
                            .physical_migrate_rename_column(
                                &request.session,
                                &updated_relation,
                                &old_name,
                                &new_name,
                            )
                            .await;

                        (
                            Arc::new(Schema::empty()),
                            Vec::new(),
                            message,
                            command_outcome("ALTER TABLE", 0),
                            request.session.clone(),
                        )
                    }
                    AlterTableOperation::DropConstraint {
                        name: constraint_name,
                        if_exists,
                        cascade,
                    } => {
                        // ...
                        let preview_result = self
                            .control_plane
                            .preview_drop_constraint(
                                &request.session,
                                database.as_deref(),
                                schema.as_deref(),
                                &name,
                                &constraint_name,
                                cascade,
                            )
                            .await;

                        match preview_result {
                            Ok(preview_relation) => {
                                let dropped_index_names = relation
                                    .indexes
                                    .iter()
                                    .filter(|existing| {
                                        !preview_relation
                                            .indexes
                                            .iter()
                                            .any(|preview| preview.name == existing.name)
                                    })
                                    .map(|i| i.name.clone())
                                    .collect::<Vec<_>>();

                                // 2. Perform the drop in catalog
                                let message = self
                                    .control_plane
                                    .drop_constraint(
                                        &request.session,
                                        database.as_deref(),
                                        schema.as_deref(),
                                        &name,
                                        &constraint_name,
                                        if_exists,
                                        cascade,
                                    )
                                    .await?;

                                // 3. Physically remove dropped index snapshots
                                if let Ok((idx_store, idx_prefix)) = table_store_prefix(&relation) {
                                    for index_name in dropped_index_names {
                                        let _ = remove_index_snapshot(
                                            &idx_store,
                                            &idx_prefix,
                                            &index_name,
                                        )
                                        .await;
                                    }
                                }

                                (
                                    Arc::new(Schema::empty()),
                                    Vec::new(),
                                    message,
                                    command_outcome("ALTER TABLE", 0),
                                    request.session.clone(),
                                )
                            }
                            Err(e) => {
                                if if_exists && e.to_string().contains("not found") {
                                    (
                                        Arc::new(Schema::empty()),
                                        Vec::new(),
                                        format!(
                                            "Constraint '{}' does not exist, skipping.",
                                            constraint_name
                                        ),
                                        command_outcome("ALTER TABLE", 0),
                                        request.session.clone(),
                                    )
                                } else {
                                    return Err(e);
                                }
                            }
                        }
                    }
                    AlterTableOperation::AlterColumn {
                        column_name,
                        operation,
                    } => {
                        let result = self
                            .control_plane
                            .alter_column(
                                &request.session,
                                database.as_deref(),
                                schema.as_deref(),
                                &name,
                                &column_name,
                                operation,
                            )
                            .await;

                        if result.is_ok() {
                            let updated_relation = self
                                .control_plane
                                .find_relation(
                                    &request.session,
                                    database.as_deref(),
                                    schema.as_deref(),
                                    &name,
                                )
                                .await?;
                            let _ = self
                                .physical_migrate_relation(&request.session, &updated_relation)
                                .await;
                        }

                        let message = result?;

                        (
                            Arc::new(Schema::empty()),
                            Vec::new(),
                            message,
                            command_outcome("ALTER TABLE", 0),
                            request.session.clone(),
                        )
                    }
                }
            }
            MetadataStatement::AlterSchema {
                database,
                name,
                new_name,
            } => {
                // 1. Get all relations in this schema to update their physical paths if managed
                let relations = self
                    .control_plane
                    .list_relations(
                        &request.session,
                        database.as_deref(),
                        Some(&name),
                        CatalogRelationKind::Table,
                    )
                    .await?;

                // 2. Rename schema in catalog
                self.control_plane
                    .rename_schema(&request.session, database.as_deref(), &name, &new_name)
                    .await?;

                // 3. Physically rename managed directories and update metadata
                let database_name = database.as_deref().unwrap_or(&request.session.database);
                for relation in relations {
                    if let Some(storage_path_str) = &relation.storage_path {
                        let (store, old_obj_prefix) =
                            storage::store_for_location(storage_path_str)?;
                        let old_part = format!("{}__{}__", database_name, name);
                        let new_part = format!("{}__{}__", database_name, new_name);
                        let new_location_str = storage_path_str.replace(&old_part, &new_part);
                        let (_, new_obj_prefix) = storage::store_for_location(&new_location_str)?;
                        storage::rename_prefix(&store, &old_obj_prefix, &new_obj_prefix).await?;
                        self.control_plane
                            .update_relation_storage_path(
                                &request.session,
                                Some(database_name),
                                Some(&new_name),
                                &relation.name,
                                &new_location_str,
                            )
                            .await?;
                    }
                }

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    format!(
                        "ALTER SCHEMA completed. Schema '{}.{}' renamed to '{}'.",
                        database_name, name, new_name
                    ),
                    command_outcome("ALTER SCHEMA", 0),
                    request.session.clone(),
                )
            }
            MetadataStatement::AlterDatabase { name, operation } => match &operation {
                AlterDatabaseOperation::Rename { new_name } => {
                    // 1. Get all tables in this database to update their physical paths
                    let relations = self
                        .control_plane
                        .list_relations_for_database(
                            &request.session,
                            &name,
                            CatalogRelationKind::Table,
                        )
                        .await?;

                    // 2. Rename database in catalog
                    let (msg, new_session) = self
                        .control_plane
                        .execute_metadata_statement(
                            &request.session,
                            &MetadataStatement::AlterDatabase {
                                name: name.clone(),
                                operation: AlterDatabaseOperation::Rename {
                                    new_name: new_name.clone(),
                                },
                            },
                        )
                        .await?;

                    // 3. Physically rename managed directories
                    for relation in relations {
                        if let Some(storage_path_str) = &relation.storage_path {
                            let (store, old_obj_prefix) =
                                storage::store_for_location(storage_path_str)?;
                            let old_part = format!("{}__{}__", name, relation.schema);
                            let new_part = format!("{}__{}__", new_name, relation.schema);
                            let new_location_str = storage_path_str.replace(&old_part, &new_part);
                            let (_, new_obj_prefix) =
                                storage::store_for_location(&new_location_str)?;
                            storage::rename_prefix(&store, &old_obj_prefix, &new_obj_prefix)
                                .await?;
                            self.control_plane
                                .update_relation_storage_path(
                                    &request.session,
                                    Some(new_name),
                                    Some(&relation.schema),
                                    &relation.name,
                                    &new_location_str,
                                )
                                .await?;
                        }
                    }

                    (
                        Arc::new(Schema::empty()),
                        Vec::new(),
                        msg,
                        command_outcome("ALTER DATABASE", 0),
                        new_session,
                    )
                }
                _ => {
                    let (message, new_session) = self
                        .control_plane
                        .execute_metadata_statement(
                            &request.session,
                            &MetadataStatement::AlterDatabase {
                                name: name.clone(),
                                operation: operation.clone(),
                            },
                        )
                        .await?;
                    (
                        Arc::new(Schema::empty()),
                        Vec::new(),
                        message,
                        command_outcome("ALTER DATABASE", 0),
                        new_session,
                    )
                }
            },
            MetadataStatement::AlterAggregate {
                database,
                schema,
                name,
                operation,
            } => {
                let (msg, new_session) = self
                    .control_plane
                    .execute_metadata_statement(
                        &request.session,
                        &MetadataStatement::AlterAggregate {
                            database,
                            schema,
                            name,
                            operation,
                        },
                    )
                    .await?;
                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    msg,
                    command_outcome("ALTER AGGREGATE", 0),
                    new_session,
                )
            }
            MetadataStatement::AlterCollation {
                database,
                schema,
                name,
                operation,
            } => {
                let (msg, new_session) = self
                    .control_plane
                    .execute_metadata_statement(
                        &request.session,
                        &MetadataStatement::AlterCollation {
                            database,
                            schema,
                            name,
                            operation,
                        },
                    )
                    .await?;
                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    msg,
                    command_outcome("ALTER COLLATION", 0),
                    new_session,
                )
            }
            MetadataStatement::AlterConversion {
                database,
                schema,
                name,
                operation,
            } => {
                let (msg, new_session) = self
                    .control_plane
                    .execute_metadata_statement(
                        &request.session,
                        &MetadataStatement::AlterConversion {
                            database,
                            schema,
                            name,
                            operation,
                        },
                    )
                    .await?;
                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    msg,
                    command_outcome("ALTER CONVERSION", 0),
                    new_session,
                )
            }
            MetadataStatement::ShowDatabases => {
                let rows = self
                    .control_plane
                    .list_databases(&request.session)
                    .await?
                    .into_iter()
                    .map(|database| vec![database])
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(&["database_name"], &rows)?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} database(s) listed successfully."),
                    rows_outcome(),
                    request.session.clone(),
                )
            }
            MetadataStatement::ShowSchemas { database } => {
                let rows = self
                    .control_plane
                    .list_schemas(&request.session, database.as_deref())
                    .await?
                    .into_iter()
                    .map(|schema| vec![schema])
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(&["schema_name"], &rows)?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} schema(s) listed successfully."),
                    rows_outcome(),
                    request.session.clone(),
                )
            }
            MetadataStatement::ShowNodes => {
                let nodes = self.control_plane.list_nodes().await?;
                let rows = nodes
                    .into_iter()
                    .map(|node| {
                        vec![
                            node.id,
                            format!("{:?}", node.role),
                            format!("{:?}", node.status),
                            node.last_heartbeat_at_epoch_ms.to_string(),
                        ]
                    })
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch =
                    utf8_record_batch(&["node_id", "kind", "status", "last_heartbeat_ms"], &rows)?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} node(s) listed successfully."),
                    rows_outcome(),
                    request.session.clone(),
                )
            }
            MetadataStatement::ShowTables { database, schema } => {
                let relations = self
                    .control_plane
                    .list_relations(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        CatalogRelationKind::Table,
                    )
                    .await?;
                let rows = relations
                    .into_iter()
                    .map(|rel| vec![rel.name])
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(&["table_name"], &rows)?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} table(s) listed successfully."),
                    rows_outcome(),
                    request.session.clone(),
                )
            }
            MetadataStatement::ShowViews { database, schema } => {
                let relations = self
                    .control_plane
                    .list_relations(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        CatalogRelationKind::View,
                    )
                    .await?;
                let rows = relations
                    .into_iter()
                    .map(|rel| vec![rel.name])
                    .collect::<Vec<_>>();
                let row_count = rows.len();
                let batch = utf8_record_batch(&["view_name"], &rows)?;
                (
                    batch.schema(),
                    vec![batch],
                    format!("{row_count} view(s) listed successfully."),
                    rows_outcome(),
                    request.session.clone(),
                )
            }
            MetadataStatement::ShowColumns {
                database,
                schema,
                table,
            } => {
                let relation = self
                    .control_plane
                    .table_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &table,
                    )
                    .await?;
                let rows = relation
                    .columns
                    .into_iter()
                    .filter(|col| col.name != "_row_id")
                    .map(|col| {
                        vec![
                            col.name,
                            col.data_type,
                            if col.nullable {
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
                    format!("{row_count} column(s) listed successfully."),
                    rows_outcome(),
                    request.session.clone(),
                )
            }
            MetadataStatement::DropTable {
                database,
                schema,
                name,
                if_exists,
                cascade: _,
            } => {
                let relation = match self
                    .control_plane
                    .find_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                    )
                    .await
                {
                    Ok(rel) => {
                        if rel.kind != CatalogRelationKind::Table {
                            anyhow::bail!("Relation '{}' is not a table", name);
                        }
                        Some(rel)
                    }
                    Err(_) => {
                        if if_exists {
                            None
                        } else {
                            anyhow::bail!("Table '{}' not found", name);
                        }
                    }
                };

                if let Some(rel) = relation {
                    let relation_lock = self.relation_lock(&rel).await?;
                    let _write_guard = relation_lock.write().await;
                    // For managed tables, delete all storage objects
                    if rel.external_format.is_none() {
                        if let Some(path_str) = &rel.storage_path {
                            let (store, prefix) = storage::store_for_location(path_str)?;
                            storage::delete_prefix(&store, &prefix).await?;
                        }
                    }
                }

                let message = self
                    .control_plane
                    .drop_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                        CatalogRelationKind::Table,
                        if_exists,
                    )
                    .await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    message,
                    command_outcome("DROP TABLE", 0),
                    request.session.clone(),
                )
            }
            MetadataStatement::DropView {
                database,
                schema,
                name,
                if_exists,
                cascade: _,
            } => {
                let message = self
                    .control_plane
                    .drop_relation(
                        &request.session,
                        database.as_deref(),
                        schema.as_deref(),
                        &name,
                        CatalogRelationKind::View,
                        if_exists,
                    )
                    .await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    message,
                    command_outcome("DROP VIEW", 0),
                    request.session.clone(),
                )
            }
            MetadataStatement::DropDatabase { name, if_exists } => {
                let message = self
                    .control_plane
                    .drop_database(&request.session, &name, if_exists)
                    .await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    message,
                    command_outcome("DROP DATABASE", 0),
                    request.session.clone(),
                )
            }
            MetadataStatement::DropSchema {
                database,
                name,
                if_exists,
                cascade,
            } => {
                let message = self
                    .control_plane
                    .drop_schema(
                        &request.session,
                        database.as_deref(),
                        &name,
                        if_exists,
                        cascade,
                    )
                    .await?;

                (
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    message,
                    command_outcome("DROP SCHEMA", 0),
                    request.session.clone(),
                )
            }
            MetadataStatement::KillQuery { query_id } => {
                if let Some(entry) = self.active_queries.get(&query_id) {
                    entry.value().cancel();
                    (
                        Arc::new(Schema::empty()),
                        Vec::new(),
                        format!("Query '{}' cancelled.", query_id),
                        command_outcome("KILL", 1),
                        request.session.clone(),
                    )
                } else {
                    return Err(anyhow::anyhow!(
                        "No active query with id '{}'",
                        query_id
                    ));
                }
            }
        };

        Ok(QueryExecutionResult {
            query_id: admission.query_id,
            coordinator_node_id: admission.coordinator_node_id,
            session: new_session,
            schema,
            batches,
            message,
            outcome,
            execution_time_ms: started.elapsed().as_millis(),
        })
    }
}
