use super::*;

impl PrototypeEngine {
    /// Attempts to execute `request.sql` as a distributed scatter-gather query stream.
    pub(super) async fn try_execute_distributed_select_stream(
        &self,
        request: &QueryRequest,
        admission: &QueryAdmission,
        started: Instant,
        _probe: &query_log::QueryProbe,
    ) -> Result<Option<QueryExecutionStream>> {
        // Only attempt distribution for plain SELECT statements.
        let Some((db, schema_name, table_name)) = parse_plain_select_table(&request.sql) else {
            return Ok(None);
        };

        // Resolve the managed relation; fall through if not found.
        let relation = match self
            .control_plane
            .table_relation(
                &request.session,
                db.as_deref(),
                schema_name.as_deref(),
                &table_name,
            )
            .await
        {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        let Some(storage_location) = relation.storage_path.as_deref() else {
            return Ok(None);
        };

        // Enumerate the Parquet files backing the table.
        let (store, prefix) = storage::store_for_location(storage_location)?;
        let table_key = format!(
            "{}.{}.{}",
            relation.database, relation.schema, relation.name
        );
        let files = self
            .file_list_cache
            .get_or_list(&table_key, &store, &prefix)
            .await?;

        // Select the distributed plan to use (in priority order).
        let aggregate_plan: Option<(String, String)> = {
            if let Some(plan) = distributed_aggregate_plan(&request.sql, &table_name) {
                Some(plan)
            } else if let Some(plan) = distributed_distinct_plan(&request.sql, &table_name) {
                Some(plan)
            } else {
                distributed_order_limit_plan(&request.sql, &table_name)
            }
        };
        // Block distribution for window functions or other unsupported function patterns.
        if aggregate_plan.is_none()
            && (has_window_functions(&request.sql)
                || select_projection_contains_function(&request.sql))
        {
            return Ok(None);
        }

        // Calculate optimal worker count based on data size and file count.
        let total_size: u64 = files.iter().map(|(_, size, _)| *size).sum();
        let _total_rows: i64 = files.iter().map(|(_, _, rows)| *rows).sum();
        let file_count = files.len();
        let partition_client = Arc::clone(&self.partition_client);

        let mut attempts = 0;
        const MAX_ATTEMPTS: usize = 3;

        while attempts < MAX_ATTEMPTS {
            attempts += 1;

            // Discover available Compute nodes.
            let mut compute_nodes = partition_client.list_compute_nodes().await?;
            if compute_nodes.is_empty() {
                warn!("[coordinator] No compute nodes available for distributed query; falling back to local execution.");
                return Ok(None);
            }

            let optimal_worker_count = distributed::calculate_optimal_worker_count(
                total_size,
                file_count,
                compute_nodes.len(),
            );

            // If heuristic says 1 node and it's just the coordinator, might as well run locally
            // unless we want to use the partition executor anyway. For now, if N=1 and we have
            // workers, we'll still pick one worker to keep the distributed path exercised.

            // Select a subset of nodes (round-robin selection could be added here,
            // but for now we just take the first N).
            compute_nodes.truncate(optimal_worker_count);

            if files.is_empty() {
                let schema = build_arrow_schema_from_catalog_columns(&relation.columns)?;
                return self
                    .execute_coordinator_select_over_partition_batches(
                        request,
                        admission,
                        started,
                        &table_name,
                        schema,
                        Vec::new(),
                        compute_nodes.len(),
                        None,
                    )
                    .await
                    .map(Some);
            }

            // Partition files across available workers (greedy size-aware).
            let chunks =
                distributed::partition_files_for_workers(files.clone(), compute_nodes.len());

            let node_list: Vec<&str> = compute_nodes
                .iter()
                .map(distributed::PartitionClient::node_channel_endpoint)
                .collect();
            info!(
                "[coordinator] Distributed SELECT on '{}' (attempt {}): {} file(s) across {} worker(s) [{} of {} available]: [{}]",
                table_name,
                attempts,
                files.len(),
                compute_nodes.len(),
                optimal_worker_count,
                partition_client.list_compute_nodes().await?.len(),
                node_list.join(", ")
            );

            let worker_sql = aggregate_plan
                .as_ref()
                .map(|(worker_sql, _)| worker_sql.clone())
                .unwrap_or_else(|| {
                    rewrite_sql_for_partition(&request.sql, &table_name)
                        .unwrap_or_else(|| "SELECT * FROM __partition__".to_string())
                });

            // Build tasks.
            let worker_tasks: Vec<_> = chunks
                .into_iter()
                .zip(compute_nodes.iter())
                .map(|(chunk_files, node)| {
                    let req = distributed::ExecutePartitionRequest {
                        query_id: admission.query_id.clone(),
                        initial_query_id: admission.query_id.clone(),
                        coordinator_node_id: admission.coordinator_node_id.clone(),
                        sql: worker_sql.clone(),
                        session: request.session.clone(),
                        partition_files: chunk_files,
                        source_columns: relation.columns.clone(),
                    };
                    (node, req)
                })
                .collect();

            // Resolve the cancellation token for this query so worker legs can be
            // aborted promptly when KILL QUERY is issued.
            let cancel = self
                .active_queries
                .get(&admission.query_id)
                .map(|entry| entry.value().clone())
                .unwrap_or_default();

            // Dispatch all concurrently.
            let mut dispatch_futures = Vec::new();
            for (node, req) in &worker_tasks {
                let endpoint =
                    distributed::PartitionClient::node_channel_endpoint(node).to_string();
                let pc = Arc::clone(&partition_client);
                let node_id = node.id.clone();
                let cancel = cancel.clone();
                dispatch_futures.push(async move {
                    match pc.execute_on_node(&endpoint, req, cancel).await {
                        Ok(stream) => Ok((node_id, stream)),
                        Err(e) => Err((node_id, e)),
                    }
                });
            }

            let dispatch_results = futures::future::join_all(dispatch_futures).await;
            let mut worker_streams = Vec::new();
            let mut failed_node_id = None;

            for res in dispatch_results {
                match res {
                    Ok((node_id, stream)) => {
                        // Wrap stream to catch mid-flight failures and identify the node.
                        let node_id_inner = node_id.clone();
                        worker_streams.push(stream.map(move |batch_res| {
                            batch_res.map_err(|e| (node_id_inner.clone(), e))
                        }));
                    }
                    Err((node_id, e)) => {
                        warn!(
                            "[coordinator] Failed to dispatch to node '{}': {}",
                            node_id, e
                        );
                        failed_node_id = Some(node_id);
                        break;
                    }
                }
            }

            if let Some(node_id) = failed_node_id {
                info!(
                    "[coordinator] Marking node '{}' as unavailable and retrying query...",
                    node_id
                );
                let _ = partition_client.mark_node_unavailable(&node_id).await;
                continue;
            }

            let ctx = DfSessionContext::new_with_config(base_session_config());
            let all_file_paths: Vec<&str> = files.iter().map(|(p, _, _)| p.as_str()).collect();
            let base_schema =
                build_partition_read_schema(&ctx, all_file_paths, &relation.columns).await?;

            if aggregate_plan.is_some() {
                // For aggregates, materialize all partial results then finalize on the coordinator.
                let mut merged_stream = futures::stream::select_all(worker_streams);
                let mut all_batches = Vec::new();
                let mut stream_failed_node_id = None;

                while let Some(batch_res) = merged_stream.next().await {
                    match batch_res {
                        Ok(batch) => all_batches.push(batch),
                        Err((node_id, e)) => {
                            warn!(
                                "[coordinator] Node '{}' failed during streaming: {}",
                                node_id, e
                            );
                            stream_failed_node_id = Some(node_id);
                            break;
                        }
                    }
                }

                if let Some(node_id) = stream_failed_node_id {
                    info!(
                        "[coordinator] Marking node '{}' as unavailable and retrying query...",
                        node_id
                    );
                    let _ = partition_client.mark_node_unavailable(&node_id).await;
                    continue;
                }

                // Success!
                let schema = all_batches
                    .first()
                    .map(|batch| batch.schema())
                    .unwrap_or(Arc::clone(&base_schema));

                return self
                    .execute_coordinator_select_over_partition_batches(
                        request,
                        admission,
                        started,
                        &table_name,
                        schema,
                        all_batches,
                        compute_nodes.len(),
                        aggregate_plan.map(|(_, final_sql)| final_sql),
                    )
                    .await
                    .map(Some);
            } else {
                // For plain SELECTs (large results), use the TRUE STREAMING path.
                // Each worker stream is bridged through a bounded mpsc channel so that a
                // slow consumer creates backpressure all the way to the gRPC transport,
                // preventing unbounded memory growth on the coordinator.
                const PARTITION_BUFFER: usize = 16;
                let mut rx_streams = Vec::new();
                for stream in worker_streams {
                    let (tx, rx) = tokio::sync::mpsc::channel(PARTITION_BUFFER);
                    tokio::spawn(async move {
                        tokio::pin!(stream);
                        while let Some(item) = stream.next().await {
                            if tx.send(item).await.is_err() {
                                break;
                            }
                        }
                    });
                    rx_streams
                        .push(tokio_stream::wrappers::ReceiverStream::new(rx));
                }
                let merged_stream = futures::stream::select_all(rx_streams);

                let df_stream = merged_stream.map(|res| match res {
                    Ok(batch) => Ok(batch),
                    Err((node_id, e)) => Err(DataFusionError::Execution(format!(
                        "Node {node_id} failed: {e}"
                    ))),
                });

                let partition_stream: SendableRecordBatchStream = Box::pin(
                    RecordBatchStreamAdapter::new(Arc::clone(&base_schema), df_stream),
                );

                return self
                    .execute_coordinator_select_over_partition_stream(
                        request,
                        admission,
                        started,
                        &table_name,
                        base_schema,
                        partition_stream,
                        compute_nodes.len(),
                        None,
                    )
                    .await
                    .map(Some);
            }
        }

        warn!("[coordinator] Distributed query failed after {} attempts; falling back to local execution.", MAX_ATTEMPTS);
        Ok(None)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute_coordinator_select_over_partition_stream(
        &self,
        request: &QueryRequest,
        admission: &QueryAdmission,
        started: Instant,
        table_name: &str,
        partition_schema: SchemaRef,
        partition_stream: SendableRecordBatchStream,
        worker_count: usize,
        final_sql_override: Option<String>,
    ) -> Result<QueryExecutionStream> {
        let final_sql = final_sql_override.unwrap_or_else(|| {
            rewrite_sql_for_partition(&request.sql, table_name)
                .unwrap_or_else(|| "SELECT * FROM __partition__".to_string())
        });
        let context = DfSessionContext::new_with_config(base_session_config());
        register_postgres_functions(&context);

        let table = StreamingTableProvider {
            schema: Arc::clone(&partition_schema),
            stream: Arc::new(tokio::sync::Mutex::new(Some(partition_stream))),
        };
        context.register_table("__partition__", Arc::new(table))?;

        let dataframe = context.sql(&final_sql).await.map_err(sanitize_error)?;
        let schema = Arc::new(dataframe.schema().as_arrow().as_ref().clone());
        let stream = dataframe.execute_stream().await.map_err(sanitize_error)?;

        Ok(QueryExecutionStream {
            query_id: admission.query_id.clone(),
            coordinator_node_id: admission.coordinator_node_id.clone(),
            session: request.session.clone(),
            schema,
            stream,
            message: format!(
                "Distributed query: coordinator streaming result from {worker_count} node(s)."
            ),
            outcome: StatementOutcome::Rows,
            execution_time_ms: started.elapsed().as_millis(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute_coordinator_select_over_partition_batches(
        &self,
        request: &QueryRequest,
        admission: &QueryAdmission,
        started: Instant,
        table_name: &str,
        partition_schema: SchemaRef,
        partition_batches: Vec<RecordBatch>,
        worker_count: usize,
        final_sql_override: Option<String>,
    ) -> Result<QueryExecutionStream> {
        let final_sql = final_sql_override.unwrap_or_else(|| {
            rewrite_sql_for_partition(&request.sql, table_name)
                .unwrap_or_else(|| "SELECT * FROM __partition__".to_string())
        });
        let context = DfSessionContext::new_with_config(base_session_config());
        register_postgres_functions(&context);
        let partitions = if partition_batches.is_empty() {
            vec![vec![RecordBatch::new_empty(Arc::clone(&partition_schema))]]
        } else {
            vec![partition_batches]
        };
        let table = MemTable::try_new(Arc::clone(&partition_schema), partitions)?;
        context.register_table("__partition__", Arc::new(table))?;

        let dataframe = context.sql(&final_sql).await.map_err(sanitize_error)?;
        let schema = Arc::new(dataframe.schema().as_arrow().as_ref().clone());
        let stream = dataframe.execute_stream().await.map_err(sanitize_error)?;

        Ok(QueryExecutionStream {
            query_id: admission.query_id.clone(),
            coordinator_node_id: admission.coordinator_node_id.clone(),
            session: request.session.clone(),
            schema,
            stream,
            message: format!(
                "Distributed query: coordinator finalized result from {worker_count} node(s)."
            ),
            outcome: StatementOutcome::Rows,
            execution_time_ms: started.elapsed().as_millis(),
        })
    }

    /// Attempts to execute `request.sql` as a distributed scatter-gather query.
    ///
    /// Returns `None` when:
    /// - the SQL is not a plain `SELECT … FROM <table>` targeting a managed table, or
    /// - there are no Ready Compute nodes available (falls through to local execution).
    ///
    /// When Compute nodes are available the query is rewritten to use
    /// `read_parquet([…])` syntax so workers don't need catalog access, then
    /// dispatched concurrently.  RecordBatches from all workers are concatenated.
    pub(super) async fn try_execute_distributed_select(
        &self,
        request: &QueryRequest,
        admission: &QueryAdmission,
        started: Instant,
        _probe: &query_log::QueryProbe,
    ) -> Result<Option<QueryExecutionResult>> {
        let Some(exec_stream) = self
            .try_execute_distributed_select_stream(request, admission, started, _probe)
            .await?
        else {
            return Ok(None);
        };

        let schema = exec_stream.schema;
        let batches = datafusion::physical_plan::common::collect(exec_stream.stream)
            .await
            .map_err(sanitize_error)?;

        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        Ok(Some(QueryExecutionResult {
            query_id: exec_stream.query_id,
            coordinator_node_id: exec_stream.coordinator_node_id,
            session: exec_stream.session,
            schema,
            batches,
            message: format!(
                "Distributed query: {row_count} row(s) returned from {} nodes.",
                // We don't have the node count easily here without re-calculating,
                // but we can just use the message from exec_stream or a generic one.
                "multiple"
            ),
            outcome: exec_stream.outcome,
            execution_time_ms: started.elapsed().as_millis(),
        }))
    }

    /// Attempts to execute `INSERT INTO target SELECT … FROM source` in a distributed fashion.
    ///
    /// Returns `None` (fall through to local single-node execution) when:
    /// - the SELECT source is not a plain managed table, or
    /// - the target table has unique/primary-key indexes (cross-partition duplicate
    ///   checking is not yet implemented), or
    /// - there are no Ready Compute nodes.
    ///
    /// When distribution is possible, each worker receives a subset of the source
    /// Parquet files and writes its output directly to the target table's shared
    /// object store prefix.  The Coordinator then updates index sidecars.
    pub(super) async fn try_execute_distributed_insert_select(
        &self,
        request: &QueryRequest,
        statement: &InsertSelectStatement,
        admission: &QueryAdmission,
        started: Instant,
        _probe: &query_log::QueryProbe,
    ) -> Result<Option<QueryExecutionResult>> {
        // Resolve the target relation first so we can describe it in logs.
        let target_relation = match self
            .control_plane
            .table_relation(
                &request.session,
                statement.database.as_deref(),
                statement.schema.as_deref(),
                &statement.name,
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                info!(
                    "[coordinator] Distributed insert skipped: target table lookup failed: {}",
                    e
                );
                return Ok(None);
            }
        };

        let Some(target_storage) = target_relation.storage_path.as_deref() else {
            info!(
                "[coordinator] Distributed insert skipped: target '{}.{}.{}' has no storage path",
                target_relation.database, target_relation.schema, target_relation.name
            );
            return Ok(None);
        };

        // Try generate_series first — it doesn't need a managed source table.
        if let Some(plan) = parse_generate_series_select(&statement.query_sql) {
            return self
                .try_execute_distributed_generate_series_insert(
                    request,
                    statement,
                    &target_relation,
                    target_storage,
                    plan,
                    admission,
                    started,
                )
                .await;
        }

        // Otherwise the SELECT source must be a plain managed table.
        let Some((src_db, src_schema, src_table)) = parse_plain_select_table(&statement.query_sql)
        else {
            info!(
                "[coordinator] Distributed insert skipped: SELECT source is not a plain table reference (and not generate_series). Falling back to single-node execution."
            );
            return Ok(None);
        };

        // Resolve the source relation and its Parquet files.
        let source_relation = match self
            .control_plane
            .table_relation(
                &request.session,
                src_db.as_deref(),
                src_schema.as_deref(),
                &src_table,
            )
            .await
        {
            Ok(r) => r,
            Err(_) => {
                info!(
                    "[coordinator] Distributed insert skipped: source '{}' is not a managed table. Falling back to single-node execution.",
                    src_table
                );
                return Ok(None);
            }
        };

        let Some(source_storage) = source_relation.storage_path.as_deref() else {
            info!(
                "[coordinator] Distributed insert skipped: source '{}.{}.{}' has no storage path",
                source_relation.database, source_relation.schema, source_relation.name
            );
            return Ok(None);
        };

        let (src_store, src_prefix) = storage::store_for_location(source_storage)?;
        let src_key = format!(
            "{}.{}.{}",
            source_relation.database, source_relation.schema, source_relation.name
        );
        let source_files = self
            .file_list_cache
            .get_or_list(&src_key, &src_store, &src_prefix)
            .await?;

        if source_files.is_empty() {
            // Nothing to insert.
            return Ok(Some(QueryExecutionResult {
                query_id: admission.query_id.clone(),
                coordinator_node_id: admission.coordinator_node_id.clone(),
                session: request.session.clone(),
                schema: Arc::new(Schema::empty()),
                batches: vec![],
                message: format!(
                    "Inserted 0 row(s) into '{}.{}.{}' (source table is empty).",
                    target_relation.database, target_relation.schema, target_relation.name
                ),
                outcome: StatementOutcome::Command {
                    tag: "INSERT".to_string(),
                    rows_affected: 0,
                },
                execution_time_ms: started.elapsed().as_millis(),
            }));
        }

        let total_size: u64 = source_files.iter().map(|(_, size, _)| *size).sum();
        let file_count = source_files.len();

        // Acquire the write lock on the target relation once for the duration
        // of all retry attempts; otherwise other writers could slip in between
        // an aborted attempt and a retry.
        let relation_lock = self.relation_lock(&target_relation).await?;
        let _write_guard = relation_lock.write().await;

        let make_tasks = |compute_nodes: &[analyticsdb_control::ClusterNode]| -> Vec<(
            analyticsdb_control::ClusterNode,
            distributed::ExecutePartitionWriteRequest,
        )> {
            let chunks =
                distributed::partition_files_for_workers(source_files.clone(), compute_nodes.len());
            chunks
                .into_iter()
                .zip(compute_nodes.iter().cloned())
                .map(|(chunk_files, node)| {
                    let req = distributed::ExecutePartitionWriteRequest {
                        query_id: admission.query_id.clone(),
                        initial_query_id: admission.query_id.clone(),
                        coordinator_node_id: admission.coordinator_node_id.clone(),
                        sql: format!("SELECT * FROM partition ({} files)", chunk_files.len()),
                        session: request.session.clone(),
                        partition_files: chunk_files,
                        source_columns: source_relation.columns.clone(),
                        write_prefix: target_storage.to_string(),
                        attempt_id: String::new(), // overwritten by run_distributed_insert
                    };
                    (node, req)
                })
                .collect()
        };

        self.run_distributed_insert(
            request,
            &target_relation,
            target_storage,
            total_size,
            file_count,
            make_tasks,
            admission,
            started,
        )
        .await
    }

    /// Driver shared by the file-based and generate_series distributed-insert
    /// paths.  Handles retry-with-cleanup, post-merge uniqueness validation,
    /// commit (index snapshot refresh + file-list cache invalidation), and the
    /// final success/failure result.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_distributed_insert<F>(
        &self,
        request: &QueryRequest,
        target_relation: &analyticsdb_control::CatalogRelation,
        target_storage: &str,
        total_size: u64,
        file_count: usize,
        make_tasks: F,
        admission: &QueryAdmission,
        started: Instant,
    ) -> Result<Option<QueryExecutionResult>>
    where
        F: Fn(
            &[analyticsdb_control::ClusterNode],
        ) -> Vec<(
            analyticsdb_control::ClusterNode,
            distributed::ExecutePartitionWriteRequest,
        )>,
    {
        let partition_client = Arc::clone(&self.partition_client);
        let (target_store, _) = storage::store_for_location(target_storage)?;

        let mut attempts = 0;
        const MAX_ATTEMPTS: usize = 3;

        while attempts < MAX_ATTEMPTS {
            attempts += 1;

            // Discover Ready Compute nodes.
            let mut compute_nodes = partition_client.list_compute_nodes().await?;
            if compute_nodes.is_empty() {
                warn!("[coordinator] No compute nodes available for distributed insert; falling back to local execution.");
                return Ok(None);
            }

            let optimal_worker_count = distributed::calculate_optimal_worker_count(
                total_size,
                file_count,
                compute_nodes.len(),
            );
            compute_nodes.truncate(optimal_worker_count);

            let mut worker_tasks = make_tasks(&compute_nodes);
            if worker_tasks.is_empty() {
                info!("[coordinator] Distributed insert: no work to dispatch.");
                return Ok(None);
            }

            // Tag every request with a per-attempt ID so workers embed it in
            // output filenames, enabling recovery cleanup by prefix scan.
            let attempt_id = format!("{}_a{}", admission.query_id, attempts);
            for (_, req) in &mut worker_tasks {
                req.attempt_id = attempt_id.clone();
            }

            // Dispatch all concurrently.
            let mut dispatch_futures = Vec::new();
            for (node, req) in &worker_tasks {
                let endpoint =
                    distributed::PartitionClient::node_channel_endpoint(node).to_string();
                let pc = Arc::clone(&partition_client);
                let node_id = node.id.clone();
                let req = req.clone();
                dispatch_futures.push(async move {
                    match pc.write_on_node(&endpoint, &req).await {
                        Ok(ack) => Ok((node_id, ack)),
                        Err(e) => Err((node_id, e)),
                    }
                });
            }

            let dispatch_results = futures::future::join_all(dispatch_futures).await;

            // Collect every ack we received, plus any failures.  We must inspect
            // every result so we can clean up files written by successful workers
            // before retrying.
            let mut all_acks = Vec::new();
            let mut failed_node_id: Option<String> = None;
            for res in dispatch_results {
                match res {
                    Ok((_, ack)) => all_acks.push(ack),
                    Err((node_id, e)) => {
                        warn!(
                            "[coordinator] Node '{}' failed during distributed write: {}",
                            node_id, e
                        );
                        if failed_node_id.is_none() {
                            failed_node_id = Some(node_id);
                        }
                    }
                }
            }

            if let Some(node_id) = failed_node_id {
                // Clean up files written by workers that did succeed, otherwise
                // a retry would double-insert their rows.
                let already_written: Vec<String> = all_acks
                    .iter()
                    .flat_map(|a| a.written_files.iter().cloned())
                    .collect();
                if let Err(cleanup_err) =
                    delete_written_files(&target_store, &already_written).await
                {
                    warn!(
                        "[coordinator] Failed to clean up partial writes after node failure: {}",
                        cleanup_err
                    );
                }
                info!(
                    "[coordinator] Marking node '{}' as unavailable and retrying distributed insert...",
                    node_id
                );
                let _ = partition_client.mark_node_unavailable(&node_id).await;
                continue;
            }

            // All workers succeeded.  Before committing, validate uniqueness
            // across new files + existing data if the target has unique/PK
            // indexes — individual workers cannot check this themselves.
            let written_files: Vec<String> = all_acks
                .iter()
                .flat_map(|a| a.written_files.iter().cloned())
                .collect();

            let needs_unique_check = target_relation
                .indexes
                .iter()
                .any(|idx| idx.is_unique || idx.is_primary);
            if needs_unique_check {
                if let Err(e) = self
                    .validate_uniqueness_across_new_files(target_relation, &written_files)
                    .await
                {
                    warn!(
                        "[coordinator] Distributed insert failed uniqueness validation: {}",
                        e
                    );
                    if let Err(cleanup_err) =
                        delete_written_files(&target_store, &written_files).await
                    {
                        warn!(
                            "[coordinator] Failed to clean up written files after validation error: {}",
                            cleanup_err
                        );
                    }
                    return Err(e);
                }
            }

            let total_rows: usize = all_acks.iter().map(|a| a.row_count).sum();

            // Commit: refresh index sidecars now that new Parquet files are visible.
            self.refresh_index_snapshots_after_mutation(&request.session, target_relation)
                .await?;

            let table_key = format!(
                "{}.{}.{}",
                target_relation.database, target_relation.schema, target_relation.name
            );
            self.file_list_cache.invalidate(&table_key);

            return Ok(Some(QueryExecutionResult {
                query_id: admission.query_id.clone(),
                coordinator_node_id: admission.coordinator_node_id.clone(),
                session: request.session.clone(),
                schema: Arc::new(Schema::empty()),
                batches: vec![],
                message: format!(
                    "Distributed insert: {total_rows} row(s) inserted into '{}.{}.{}' via {} node(s) (attempt {}).",
                    target_relation.database,
                    target_relation.schema,
                    target_relation.name,
                    compute_nodes.len(),
                    attempts,
                ),
                outcome: StatementOutcome::Command {
                    tag: "INSERT".to_string(),
                    rows_affected: total_rows as u64,
                },
                execution_time_ms: started.elapsed().as_millis(),
            }));
        }

        warn!("[coordinator] Distributed insert failed after {} attempts; falling back to local execution.", MAX_ATTEMPTS);
        Ok(None)
    }

    /// Distributed insert path for `INSERT … SELECT … FROM generate_series(start, end) AS s(n)`.
    ///
    /// Slices the integer range across workers and asks each worker to execute
    /// its own `SELECT` with the assigned subrange, writing results directly to
    /// the target's storage prefix.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn try_execute_distributed_generate_series_insert(
        &self,
        request: &QueryRequest,
        statement: &InsertSelectStatement,
        target_relation: &analyticsdb_control::CatalogRelation,
        target_storage: &str,
        plan: GenerateSeriesPlan,
        admission: &QueryAdmission,
        started: Instant,
    ) -> Result<Option<QueryExecutionResult>> {
        let total_rows_estimate =
            plan.end.saturating_sub(plan.start).saturating_add(1).max(1) as u64;

        // Use a synthetic "file count" of one per worker-worth of rows so the
        // worker-count heuristic spreads across nodes.  We treat each ~1M rows
        // as roughly 128MB (a coarse but useful approximation for size-based
        // planning).
        let approx_bytes_per_row: u64 = 128;
        let total_size = total_rows_estimate.saturating_mul(approx_bytes_per_row);
        let synthetic_file_count = total_rows_estimate.max(1) as usize;

        // Determine the column names workers should emit so future reads of
        // the target table find the expected schema.  Prefer the explicit
        // column list on the INSERT; otherwise fall back to the catalog order.
        let target_columns: Vec<String> = match &statement.columns {
            Some(cols) if !cols.is_empty() => cols.clone(),
            _ => target_relation
                .columns
                .iter()
                .filter(|c| c.name != "_row_id")
                .map(|c| c.name.clone())
                .collect(),
        };

        let plan = Arc::new(plan);
        let query_sql = statement.query_sql.clone();
        let target_columns = Arc::new(target_columns);

        let make_tasks = move |compute_nodes: &[analyticsdb_control::ClusterNode]| -> Vec<(
            analyticsdb_control::ClusterNode,
            distributed::ExecutePartitionWriteRequest,
        )> {
            let ranges = slice_int_range(plan.start, plan.end, compute_nodes.len());
            ranges
                .into_iter()
                .zip(compute_nodes.iter().cloned())
                .map(|((s, e), node)| {
                    let worker_sql = rewrite_generate_series_range(
                        &query_sql,
                        &plan,
                        s,
                        e,
                        target_columns.as_ref(),
                    )
                    .unwrap_or_else(|| query_sql.clone());
                    let req = distributed::ExecutePartitionWriteRequest {
                        query_id: admission.query_id.clone(),
                        initial_query_id: admission.query_id.clone(),
                        coordinator_node_id: admission.coordinator_node_id.clone(),
                        sql: worker_sql,
                        session: request.session.clone(),
                        partition_files: Vec::new(),
                        source_columns: Vec::new(),
                        write_prefix: target_storage.to_string(),
                        attempt_id: String::new(), // overwritten by run_distributed_insert
                    };
                    (node, req)
                })
                .collect()
        };

        let relation_lock = self.relation_lock(target_relation).await?;
        let _write_guard = relation_lock.write().await;

        self.run_distributed_insert(
            request,
            target_relation,
            target_storage,
            total_size,
            synthetic_file_count,
            make_tasks,
            admission,
            started,
        )
        .await
    }
}
