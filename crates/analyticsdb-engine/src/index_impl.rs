use super::*;

impl PrototypeEngine {
    pub(super) async fn rebuild_all_index_snapshots(
        &self,
        session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
    ) -> Result<()> {
        self.invalidate_session_contexts().await;
        let (store, table_prefix) = table_store_prefix(relation)?;
        for index in &relation.indexes {
            println!("rebuilding index snapshot for: {}", index.name);
            let version = uuid::Uuid::now_v7().to_string();
            let snapshot = self
                .build_index_snapshot_for_relation(session, relation, &index.name, &version)
                .await?;
            println!("writing index snapshot for: {}", index.name);
            write_index_snapshot(&store, &table_prefix, &snapshot, &version).await?;
        }
        Ok(())
    }

    pub(super) async fn physical_migrate_relation(
        &self,
        _session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
    ) -> Result<()> {
        let Some(storage_path) = &relation.storage_path else {
            return Ok(());
        };

        let (store, prefix) = storage::store_for_location(storage_path)?;
        let files = crate::manifest::list_files(&store, &prefix).await?;

        for file_path in files {
            let ctx = DfSessionContext::new_with_config(base_session_config());
            let df = ctx
                .read_parquet(vec![file_path.as_str()], ParquetReadOptions::default())
                .await
                .map_err(sanitize_error)?;

            // When reading with default options, DataFusion infers the schema from the file.
            // We want to write it back with the NEW schema from the catalog.
            let full_schema = build_arrow_schema_from_catalog_columns(&relation.columns)?;

            // Collect existing data and write it back with the new schema.
            // DataFusion will handle missing columns by filling with NULLs.
            let batches = df.collect().await.map_err(sanitize_error)?;
            let bytes = storage::encode_parquet_batches(full_schema, &batches)?;
            let key = OPath::parse(file_path.trim_start_matches('/'))?;
            store.put(&key, bytes.into()).await?;
        }

        Ok(())
    }

    pub(super) async fn physical_migrate_rename_column(
        &self,
        _session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        let Some(storage_path) = &relation.storage_path else {
            return Ok(());
        };

        let (store, prefix) = storage::store_for_location(storage_path)?;
        let files = crate::manifest::list_files(&store, &prefix).await?;

        for file_path in files {
            let ctx = DfSessionContext::new_with_config(base_session_config());
            let df = ctx
                .read_parquet(vec![file_path.as_str()], ParquetReadOptions::default())
                .await
                .map_err(sanitize_error)?;

            let mut projection = Vec::new();
            for field in df.schema().fields() {
                if field.name() == old_name {
                    projection.push(datafusion::prelude::col(field.name()).alias(new_name));
                } else {
                    projection.push(datafusion::prelude::col(field.name()));
                }
            }
            let renamed_df = df.select(projection).map_err(sanitize_error)?;
            let schema = Arc::new(renamed_df.schema().as_arrow().as_ref().clone());
            let batches = renamed_df.collect().await.map_err(sanitize_error)?;

            let bytes = storage::encode_parquet_batches(schema, &batches)?;
            let key = OPath::parse(file_path.trim_start_matches('/'))?;
            store.put(&key, bytes.into()).await?;
        }

        Ok(())
    }

    pub(super) async fn build_index_snapshot_for_relation(
        &self,
        session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
        index_name: &str,
        version: &str,
    ) -> Result<IndexSnapshot> {
        let Some(index) = relation.indexes.iter().find(|idx| idx.name == index_name) else {
            bail!(
                "Index '{}' not found on relation '{}.{}.{}'",
                index_name,
                relation.database,
                relation.schema,
                relation.name
            );
        };

        let df_context = self.create_session_context(session).await?;
        let table_name = format!(
            "\"{}\".\"{}\".\"{}\"",
            relation.database, relation.schema, relation.name
        );
        let index_cols = index
            .columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {}, \"_row_id\" FROM {} ORDER BY {}",
            index_cols, table_name, index_cols
        );
        let df = df_context.sql(&sql).await.map_err(sanitize_error)?;

        // Validate uniqueness if needed
        if index.is_unique || index.is_primary {
            let check_sql = format!(
                "SELECT COUNT(*) as count FROM (SELECT {} FROM {} GROUP BY {} HAVING COUNT(*) > 1)",
                index_cols, table_name, index_cols
            );
            let check_df = df_context.sql(&check_sql).await.map_err(sanitize_error)?;
            let results = check_df.collect().await.map_err(sanitize_error)?;
            let mut count_val: i64 = 0;
            for b in &results {
                if b.num_rows() > 0 {
                    let arr = b
                        .column(0)
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::Int64Array>()
                        .ok_or_else(|| anyhow::anyhow!("COUNT(*) column is not Int64"))?;
                    count_val += arr.value(0);
                }
            }
            if count_val > 0 {
                anyhow::bail!(
                    "Unique index '{}' on '{}.{}.{}' would contain duplicate keys",
                    index.name,
                    relation.database,
                    relation.schema,
                    relation.name
                );
            }
        }

        let (store, table_prefix) = table_store_prefix(relation)?;
        let data_key = index_data_key(&table_prefix, index_name, version);

        let sort_exprs = index
            .columns
            .iter()
            .map(|c| col(c).sort(true, true))
            .collect::<Vec<_>>();

        let sorted_batches = df
            .clone()
            .sort(sort_exprs)
            .map_err(sanitize_error)?
            .collect()
            .await
            .map_err(sanitize_error)?;

        let schema = if let Some(first) = sorted_batches.first() {
            first.schema()
        } else {
            Arc::new(df.schema().as_arrow().clone())
        };
        storage::write_parquet_batches(&store, &data_key, schema, &sorted_batches).await?;

        let row_count_df = df.aggregate(vec![], vec![count(col("_row_id")).alias("count")])?;
        let row_count_results = row_count_df.collect().await.map_err(sanitize_error)?;
        let mut row_count: usize = 0;
        for b in &row_count_results {
            if b.num_rows() > 0 {
                let arr = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .ok_or_else(|| anyhow::anyhow!("row count column is not Int64"))?;
                row_count += arr.value(0) as usize;
            }
        }

        Ok(IndexSnapshot {
            database: relation.database.clone(),
            schema: relation.schema.clone(),
            table: relation.name.clone(),
            index: index.name.clone(),
            columns: index.columns.clone(),
            unique: index.is_unique,
            primary: index.is_primary,
            entries_object: version.to_string(),
            row_count,
        })
    }

    pub(super) async fn refresh_index_snapshots_after_mutation(
        &self,
        session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
    ) -> Result<()> {
        if relation.indexes.is_empty() {
            return Ok(());
        }

        self.rebuild_all_index_snapshots(session, relation).await
    }

    pub(super) fn validate_unique_indexes_for_rows(
        &self,
        relation: &analyticsdb_control::CatalogRelation,
        rows: &[Vec<String>],
    ) -> Result<()> {
        for index in &relation.indexes {
            if index.is_unique || index.is_primary {
                validate_unique_index_rows(relation, index, rows)?;
            }
        }
        Ok(())
    }

    pub(super) async fn try_execute_indexed_select(
        &self,
        request: &QueryRequest,
        statement: IndexedSelectStatement,
        admission: &QueryAdmission,
        started: Instant,
        _probe: &query_log::QueryProbe,
    ) -> Result<Option<QueryExecutionResult>> {
        let relation = match self
            .control_plane
            .table_relation(
                &request.session,
                statement.database.as_deref(),
                statement.schema.as_deref(),
                &statement.table,
            )
            .await
        {
            Ok(relation) => relation,
            Err(_) => return Ok(None),
        };

        let relation_lock = self.relation_lock(&relation).await?;
        let _read_guard = relation_lock.read().await;

        let Some((index, row_ids)) = self
            .best_index_match(&request.session, &relation, &statement)
            .await?
        else {
            return Ok(None);
        };

        if row_ids.is_empty() {
            let schema = build_arrow_schema_from_catalog_columns(&relation.columns)?;
            let user_schema = Arc::new(Schema::new(
                schema
                    .fields()
                    .iter()
                    .filter(|f| f.name() != "_row_id")
                    .cloned()
                    .collect::<Vec<_>>(),
            ));
            return Ok(Some(QueryExecutionResult {
                query_id: admission.query_id.clone(),
                coordinator_node_id: admission.coordinator_node_id.clone(),
                session: request.session.clone(),
                schema: user_schema,
                batches: vec![],
                message: format!(
                    "Query executed successfully using index '{}'. 0 row(s) returned.",
                    index.name
                ),
                outcome: StatementOutcome::Rows,
                execution_time_ms: started.elapsed().as_millis(),
            }));
        }

        // Use a clean DataFusion SessionContext to filter by _row_id
        let context = DfSessionContext::new_with_config(base_session_config());

        let storage_path = relation
            .storage_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Missing storage path"))?;
        let full_schema = build_arrow_schema_from_catalog_columns(&relation.columns)?;

        let (store, prefix) = crate::storage::store_for_location(storage_path)?;
        let committed_files = crate::manifest::list_files(&store, &prefix)
            .await
            .unwrap_or_default();

        let listing_opts = datafusion::datasource::listing::ListingOptions::new(Arc::new(
            datafusion::datasource::file_format::parquet::ParquetFormat::default(),
        ));
        let config = if committed_files.is_empty() {
            let table_path = listing_table_url_for_storage_location(storage_path)?;
            datafusion::datasource::listing::ListingTableConfig::new(table_path)
                .with_listing_options(listing_opts)
                .with_schema(full_schema)
        } else {
            let urls: Vec<datafusion::datasource::listing::ListingTableUrl> = committed_files
                .iter()
                .filter_map(|f| datafusion::datasource::listing::ListingTableUrl::parse(f).ok())
                .collect();
            datafusion::datasource::listing::ListingTableConfig::new_with_multi_paths(urls)
                .with_listing_options(listing_opts)
                .with_schema(full_schema)
        };
        let table = datafusion::datasource::listing::ListingTable::try_new(config)?;
        context.register_table("indexed_table", Arc::new(table))?;

        let projection_sql = match &statement.projection {
            Some(cols) => cols
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", "),
            None => relation
                .columns
                .iter()
                .filter(|c| c.name != "_row_id")
                .map(|c| format!("\"{}\"", c.name))
                .collect::<Vec<_>>()
                .join(", "),
        };

        let row_ids_literal = row_ids
            .iter()
            .map(|id| format!("'{}'", id))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {} FROM indexed_table WHERE \"_row_id\" IN ({})",
            projection_sql, row_ids_literal
        );

        let df = context.sql(&sql).await.map_err(sanitize_error)?;
        let schema = df.schema().as_arrow().clone().into();
        let batches = df.collect().await?;
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        Ok(Some(QueryExecutionResult {
            query_id: admission.query_id.clone(),
            coordinator_node_id: admission.coordinator_node_id.clone(),
            session: request.session.clone(),
            schema,
            batches,
            message: format!(
                "Query executed successfully using index '{}'. {row_count} row(s) returned.",
                index.name
            ),
            outcome: StatementOutcome::Rows,
            execution_time_ms: started.elapsed().as_millis(),
        }))
    }

    pub(super) async fn validate_batch_against_table_uniqueness(
        &self,
        _session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
        batch: &RecordBatch,
    ) -> Result<()> {
        if !relation
            .indexes
            .iter()
            .any(|index| index.is_unique || index.is_primary)
        {
            return Ok(());
        }

        for index in &relation.indexes {
            if !index.is_unique && !index.is_primary {
                continue;
            }

            let (store, table_prefix) = table_store_prefix(relation)?;
            let Some(snapshot) = read_index_snapshot(&store, &table_prefix, &index.name).await?
            else {
                continue;
            };

            // Use a fresh context for internal validation to avoid catalog registration issues
            let df_context = DfSessionContext::new_with_config(base_session_config());
            let new_batch_df = df_context.read_batch(batch.clone())?;

            let data_key = index_data_key(&table_prefix, &index.name, &snapshot.entries_object);
            if !storage::object_exists(&store, &data_key).await? {
                continue;
            }

            let data_local_path = format!("/{}", data_key.as_ref());
            let snapshot_df = df_context
                .read_parquet(&data_local_path, Default::default())
                .await
                .map_err(sanitize_error)?;

            let index_cols = index.columns.iter().map(col).collect::<Vec<_>>();

            // 1. Check for duplicates within the new batch
            let batch_dup_df = new_batch_df
                .clone()
                .aggregate(index_cols.clone(), vec![count(lit(1)).alias("count")])?
                .filter(col("count").gt(lit(1)))?;
            let batch_dup_results = batch_dup_df.collect().await.map_err(sanitize_error)?;
            if batch_dup_results.iter().any(|b| b.num_rows() > 0) {
                anyhow::bail!(
                    "Unique index '{}' on '{}.{}.{}' would contain duplicate keys within the new batch",
                    index.name,
                    relation.database,
                    relation.schema,
                    relation.name
                );
            }

            // 2. Check for duplicates against existing data (the index snapshot)
            let join_on_cols = index.columns.iter().map(|c| c.as_str()).collect::<Vec<_>>();
            let join_df = new_batch_df.join(
                snapshot_df,
                datafusion::prelude::JoinType::LeftSemi,
                &join_on_cols,
                &join_on_cols,
                None,
            )?;

            let join_results = join_df.collect().await.map_err(sanitize_error)?;
            let count = join_results.iter().map(|b| b.num_rows()).sum::<usize>();
            println!("validate uniqueness join count: {}", count);
            if count > 0 {
                anyhow::bail!(
                    "Unique index '{}' on '{}.{}.{}' would contain duplicate keys (violation against existing data)",
                    index.name,
                    relation.database,
                    relation.schema,
                    relation.name
                );
            }
        }

        Ok(())
    }

    /// After a distributed insert completes, validates that the union of newly
    /// written Parquet files does not violate any unique/primary index — both
    /// against itself (cross-partition duplicates) and against existing data
    /// captured in the published index snapshots.
    pub(super) async fn validate_uniqueness_across_new_files(
        &self,
        relation: &analyticsdb_control::CatalogRelation,
        new_files: &[String],
    ) -> Result<()> {
        if new_files.is_empty() {
            return Ok(());
        }
        if !relation
            .indexes
            .iter()
            .any(|idx| idx.is_unique || idx.is_primary)
        {
            return Ok(());
        }

        let (store, table_prefix) = table_store_prefix(relation)?;

        for index in &relation.indexes {
            if !index.is_unique && !index.is_primary {
                continue;
            }

            let df_context = DfSessionContext::new_with_config(base_session_config());
            register_postgres_functions(&df_context);

            let paths: Vec<&str> = new_files.iter().map(|s| s.as_str()).collect();
            let new_df = df_context
                .read_parquet(paths, ParquetReadOptions::default())
                .await
                .map_err(sanitize_error)?;

            let index_cols: Vec<_> = index.columns.iter().map(col).collect();

            // 1. Cross-partition duplicates within the newly written files.
            let dup_df = new_df
                .clone()
                .aggregate(index_cols.clone(), vec![count(lit(1)).alias("count")])?
                .filter(col("count").gt(lit(1)))?;
            let dup_results = dup_df.collect().await.map_err(sanitize_error)?;
            if dup_results.iter().any(|b| b.num_rows() > 0) {
                anyhow::bail!(
                    "Unique index '{}' on '{}.{}.{}' would contain duplicate keys across distributed partitions",
                    index.name,
                    relation.database,
                    relation.schema,
                    relation.name
                );
            }

            // 2. Conflicts against existing data captured in the index snapshot.
            let Some(snapshot) = read_index_snapshot(&store, &table_prefix, &index.name).await?
            else {
                continue;
            };
            let data_key = index_data_key(&table_prefix, &index.name, &snapshot.entries_object);
            if !storage::object_exists(&store, &data_key).await? {
                continue;
            }
            let data_local_path = format!("/{}", data_key.as_ref());
            let snapshot_df = df_context
                .read_parquet(&data_local_path, ParquetReadOptions::default())
                .await
                .map_err(sanitize_error)?;

            let join_on_cols: Vec<&str> = index.columns.iter().map(|c| c.as_str()).collect();
            let join_df = new_df.clone().join(
                snapshot_df,
                datafusion::prelude::JoinType::LeftSemi,
                &join_on_cols,
                &join_on_cols,
                None,
            )?;
            let join_results = join_df.collect().await.map_err(sanitize_error)?;
            if join_results.iter().any(|b| b.num_rows() > 0) {
                anyhow::bail!(
                    "Unique index '{}' on '{}.{}.{}' would conflict with existing rows",
                    index.name,
                    relation.database,
                    relation.schema,
                    relation.name
                );
            }
        }

        Ok(())
    }

    pub(super) async fn best_index_match(
        &self,
        session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
        statement: &IndexedSelectStatement,
    ) -> Result<Option<(analyticsdb_control::CatalogIndex, Vec<String>)>> {
        let mut best_match: Option<(analyticsdb_control::CatalogIndex, Vec<String>, usize, bool)> =
            None;

        let (idx_store, idx_prefix) = table_store_prefix(relation)?;
        for index in &relation.indexes {
            let Some(snapshot) = read_index_snapshot(&idx_store, &idx_prefix, &index.name).await?
            else {
                continue;
            };
            let Some((score, has_range, row_ids)) = self
                .candidate_row_ids_from_snapshot(
                    session,
                    relation,
                    index,
                    &snapshot,
                    &statement.predicates,
                )
                .await?
            else {
                continue;
            };

            let replace = match &best_match {
                None => true,
                Some((best_index, best_row_ids, best_score, best_has_range)) => {
                    score > *best_score
                        || (score == *best_score && !has_range && *best_has_range)
                        || (score == *best_score
                            && has_range == *best_has_range
                            && index.is_unique
                            && !best_index.is_unique)
                        || (score == *best_score
                            && has_range == *best_has_range
                            && index.is_unique == best_index.is_unique
                            && row_ids.len() < best_row_ids.len())
                }
            };

            if replace {
                best_match = Some((index.clone(), row_ids, score, has_range));
            }
        }

        Ok(best_match.map(|(index, row_ids, _, _)| (index, row_ids)))
    }

    pub(super) async fn candidate_row_ids_from_snapshot(
        &self,
        _session: &SessionContext,
        relation: &analyticsdb_control::CatalogRelation,
        index: &analyticsdb_control::CatalogIndex,
        snapshot: &IndexSnapshot,
        predicates: &BTreeMap<String, IndexPredicate>,
    ) -> Result<Option<(usize, bool, Vec<String>)>> {
        let mut matched_prefix_len = 0usize;
        let mut has_range = false;
        let mut covered_predicate_columns = 0usize;

        for column in &index.columns {
            let Some(predicate) = find_index_predicate(predicates, column) else {
                break;
            };
            covered_predicate_columns += 1;
            match predicate {
                IndexPredicate::Eq(_) | IndexPredicate::In(_) => {
                    matched_prefix_len += 1;
                }
                IndexPredicate::Range { .. } => {
                    has_range = true;
                    break;
                }
            }
        }

        if matched_prefix_len == 0 && !has_range {
            return Ok(None);
        }
        if covered_predicate_columns != predicates.len() {
            return Ok(None);
        }

        // Use a fresh context for internal index lookups to avoid catalog registration issues
        let df_context = DfSessionContext::new_with_config(base_session_config());
        let (store, table_prefix) = table_store_prefix(relation)?;
        let data_key = index_data_key(&table_prefix, &index.name, &snapshot.entries_object);
        if !storage::object_exists(&store, &data_key).await? {
            return Ok(None);
        }

        let data_local_path = format!("/{}", data_key.as_ref());
        let mut df = df_context
            .read_parquet(&data_local_path, Default::default())
            .await
            .map_err(sanitize_error)?;

        for (col_name, predicate) in predicates {
            let col_expr = col(col_name);
            let filter_expr = match predicate {
                IndexPredicate::Eq(val) => col_expr.eq(lit(val.clone())),
                IndexPredicate::In(vals) => {
                    col_expr.in_list(vals.iter().map(|v| lit(v.clone())).collect(), false)
                }
                IndexPredicate::Range { lower, upper } => {
                    let mut expr = lit(true);
                    if let Some((val, inclusive)) = lower {
                        let lower_expr = if *inclusive {
                            col_expr.clone().gt_eq(lit(val.clone()))
                        } else {
                            col_expr.clone().gt(lit(val.clone()))
                        };
                        expr = expr.and(lower_expr);
                    }
                    if let Some((val, inclusive)) = upper {
                        let upper_expr = if *inclusive {
                            col_expr.clone().lt_eq(lit(val.clone()))
                        } else {
                            col_expr.clone().lt(lit(val.clone()))
                        };
                        expr = expr.and(upper_expr);
                    }
                    expr
                }
            };
            df = df.filter(filter_expr).map_err(sanitize_error)?;
        }

        let batches = df
            .select(vec![col("_row_id")])
            .map_err(sanitize_error)?
            .collect()
            .await
            .map_err(sanitize_error)?;

        let mut row_ids = Vec::new();
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                if let Ok(val) = array_value_to_string(batch.column(0), row_idx) {
                    row_ids.push(val);
                }
            }
        }

        Ok(Some((matched_prefix_len, has_range, row_ids)))
    }
}
