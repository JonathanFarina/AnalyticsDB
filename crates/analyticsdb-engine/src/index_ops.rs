use super::*;

#[derive(Debug, Clone)]
pub(crate) struct IndexedSelectStatement {
    pub database: Option<String>,
    pub schema: Option<String>,
    pub table: String,
    pub projection: Option<Vec<String>>,
    pub predicates: BTreeMap<String, IndexPredicate>,
}

#[derive(Debug, Clone)]
pub(crate) enum IndexPredicate {
    Eq(String),
    In(Vec<String>),
    Range {
        lower: Option<(String, bool)>,
        upper: Option<(String, bool)>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct IndexSnapshotManifest {
    pub version: String,
    pub snapshot_object: String,
    pub row_count: usize,
    pub published_at_epoch_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct IndexSnapshot {
    pub database: String,
    pub schema: String,
    pub table: String,
    pub index: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub primary: bool,
    pub entries_object: String,
    pub row_count: usize,
}

pub(crate) fn parse_indexed_select_statement(sql: &str) -> Result<Option<IndexedSelectStatement>> {
    let dialect = PostgreSqlDialect {};
    let statements = match Parser::parse_sql(&dialect, sql) {
        Ok(statements) => statements,
        Err(_) => return Ok(None),
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return Ok(None);
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
        || select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !matches!(
            &select.group_by,
            sqlparser::ast::GroupByExpr::Expressions(expressions, modifiers)
                if expressions.is_empty() && modifiers.is_empty()
        )
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
    {
        return Ok(None);
    }
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Ok(None);
    }
    let TableFactor::Table { name, .. } = &select.from[0].relation else {
        return Ok(None);
    };
    let idents = name
        .0
        .iter()
        .map(|ident| ident.to_string())
        .collect::<Vec<_>>();
    let (database, schema, table) = match idents.as_slice() {
        [table] => (None, None, table.clone()),
        [schema, table] => (None, Some(schema.clone()), table.clone()),
        [database, schema, table] => (Some(database.clone()), Some(schema.clone()), table.clone()),
        _ => return Ok(None),
    };

    let projection = match select_projection_columns(&select.projection)? {
        Some(p) => Some(p),
        None => {
            // If select_projection_columns returns None, it might be a wildcard (*)
            // or a complex projection. We only support wildcards in indexed select
            // if all columns are simple.
            if select
                .projection
                .iter()
                .any(|item| !matches!(item, SelectItem::Wildcard(_)))
            {
                return Ok(None);
            }
            None
        }
    };
    let Some(selection) = &select.selection else {
        return Ok(None);
    };

    let mut predicates = BTreeMap::new();
    if !extract_index_predicates(selection, &mut predicates)? || predicates.is_empty() {
        return Ok(None);
    }

    Ok(Some(IndexedSelectStatement {
        database,
        schema,
        table,
        projection,
        predicates,
    }))
}

pub(crate) fn select_projection_columns(projection: &[SelectItem]) -> Result<Option<Vec<String>>> {
    let mut columns = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard(_) => return Ok(None),
            SelectItem::UnnamedExpr(Expr::Identifier(identifier)) => {
                columns.push(identifier.to_string());
            }
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                let last = parts.last().ok_or_else(|| anyhow::anyhow!("empty compound identifier"))?;
                columns.push(last.to_string());
            }
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(identifier),
                alias,
            } if identifier == alias => {
                columns.push(identifier.to_string());
            }
            SelectItem::ExprWithAlias {
                expr: Expr::CompoundIdentifier(parts),
                alias,
            } if parts.last().map(|p| p == alias).unwrap_or(false) => {
                columns.push(alias.to_string());
            }
            _ => return Ok(None),
        }
    }
    Ok(Some(columns))
}

pub(crate) fn extract_index_predicates(
    expr: &Expr,
    predicates: &mut BTreeMap<String, IndexPredicate>,
) -> Result<bool> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(extract_index_predicates(left, predicates)?
                && extract_index_predicates(right, predicates)?),
            BinaryOperator::Eq => Ok(store_eq_predicate(predicates, left, right)
                .or_else(|| store_eq_predicate(predicates, right, left))
                .is_some()),
            BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq => Ok(store_range_binary_predicate(
                predicates, left, op, right, true,
            )
            .or_else(|| store_range_binary_predicate(predicates, right, op, left, false))
            .is_some()),
            _ => Ok(false),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } if !negated => Ok(store_in_predicate(predicates, expr, list).is_ok()),
        Expr::Nested(inner) => extract_index_predicates(inner, predicates),
        _ => Ok(false),
    }
}

pub(crate) fn literal_index_value(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(value) => Some(normalize_index_literal(&value.to_string())),
        Expr::UnaryOp { op, expr } => {
            let value = literal_index_value(expr)?;
            match op {
                sqlparser::ast::UnaryOperator::Minus => Some(format!("-{value}")),
                sqlparser::ast::UnaryOperator::Plus => Some(value),
                _ => None,
            }
        }
        _ => None,
    }
}

pub(crate) fn normalize_index_literal(value: &str) -> String {
    value.trim().trim_matches('\'').replace("''", "'")
}

pub(crate) fn index_predicate_column(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(identifier) => Some(identifier.to_string()),
        Expr::CompoundIdentifier(parts) => parts.last().map(ToString::to_string),
        _ => None,
    }
}

pub(crate) fn store_eq_predicate(
    predicates: &mut BTreeMap<String, IndexPredicate>,
    column_expr: &Expr,
    value_expr: &Expr,
) -> Option<()> {
    let column = index_predicate_column(column_expr)?;
    let value = literal_index_value(value_expr)?;
    if predicates.contains_key(&column) {
        return None;
    }
    predicates.insert(column, IndexPredicate::Eq(value));
    Some(())
}

pub(crate) fn store_in_predicate(
    predicates: &mut BTreeMap<String, IndexPredicate>,
    expr: &Expr,
    list: &[Expr],
) -> Result<()> {
    let Some(column) = index_predicate_column(expr) else {
        anyhow::bail!("unsupported index IN predicate");
    };
    if predicates.contains_key(&column) {
        anyhow::bail!("duplicate predicates on indexed column '{}'", column);
    }
    let mut values = Vec::with_capacity(list.len());
    for item in list {
        let Some(value) = literal_index_value(item) else {
            anyhow::bail!("unsupported index IN predicate");
        };
        values.push(value);
    }
    predicates.insert(column, IndexPredicate::In(values));
    Ok(())
}

pub(crate) fn store_range_binary_predicate(
    predicates: &mut BTreeMap<String, IndexPredicate>,
    column_expr: &Expr,
    operator: &BinaryOperator,
    value_expr: &Expr,
    column_on_left: bool,
) -> Option<()> {
    let column = index_predicate_column(column_expr)?;
    let value = literal_index_value(value_expr)?;
    let mut lower = None;
    let mut upper = None;
    match (operator, column_on_left) {
        (BinaryOperator::Gt, true) | (BinaryOperator::Lt, false) => lower = Some((value, false)),
        (BinaryOperator::GtEq, true) | (BinaryOperator::LtEq, false) => lower = Some((value, true)),
        (BinaryOperator::Lt, true) | (BinaryOperator::Gt, false) => upper = Some((value, false)),
        (BinaryOperator::LtEq, true) | (BinaryOperator::GtEq, false) => upper = Some((value, true)),
        _ => return None,
    }

    match predicates.get_mut(&column) {
        None => {
            predicates.insert(column, IndexPredicate::Range { lower, upper });
            Some(())
        }
        Some(IndexPredicate::Range {
            lower: existing_lower,
            upper: existing_upper,
        }) => {
            if let Some(bound) = lower {
                if existing_lower.is_some() {
                    return None;
                }
                *existing_lower = Some(bound);
            }
            if let Some(bound) = upper {
                if existing_upper.is_some() {
                    return None;
                }
                *existing_upper = Some(bound);
            }
            Some(())
        }
        Some(_) => None,
    }
}

pub(crate) fn table_store_prefix(
    relation: &analyticsdb_control::CatalogRelation,
) -> Result<(Arc<dyn ObjectStore>, OPath)> {
    let location = relation.storage_path.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "Managed table '{}.{}.{}' is missing a storage path",
            relation.database,
            relation.schema,
            relation.name
        )
    })?;
    storage::store_for_location(location)
}

pub(crate) fn index_manifest_key(table_prefix: &OPath, index_name: &str) -> OPath {
    table_prefix
        .clone()
        .join(".analyticsdb_indexes")
        .join(index_name)
        .join("manifest.json")
}

pub(crate) fn index_version_metadata_key(table_prefix: &OPath, index_name: &str, version: &str) -> OPath {
    table_prefix
        .clone()
        .join(".analyticsdb_indexes")
        .join(index_name)
        .join("versions")
        .join(version)
        .join("metadata.json")
}

pub(crate) fn index_data_key(table_prefix: &OPath, index_name: &str, entries_object: &str) -> OPath {
    table_prefix
        .clone()
        .join(".analyticsdb_indexes")
        .join(index_name)
        .join("versions")
        .join(entries_object)
        .join("data.parquet")
}

pub(crate) fn index_prefix_key(table_prefix: &OPath, index_name: &str) -> OPath {
    table_prefix
        .clone()
        .join(".analyticsdb_indexes")
        .join(index_name)
}

pub(crate) fn listing_table_url_for_storage_location(
    storage_location: &str,
) -> Result<datafusion::datasource::listing::ListingTableUrl> {
    datafusion::datasource::listing::ListingTableUrl::parse(storage_location).map_err(Into::into)
}

pub(crate) async fn read_index_snapshot(
    store: &Arc<dyn ObjectStore>,
    table_prefix: &OPath,
    index_name: &str,
) -> Result<Option<IndexSnapshot>> {
    let manifest_key = index_manifest_key(table_prefix, index_name);
    let Some(manifest_json) = storage::read_json(store, &manifest_key).await? else {
        return Ok(None);
    };
    let manifest: IndexSnapshotManifest = serde_json::from_str(&manifest_json)?;
    let metadata_key = index_version_metadata_key(table_prefix, index_name, &manifest.version);
    let Some(metadata_json) = storage::read_json(store, &metadata_key).await? else {
        anyhow::bail!(
            "Published index snapshot for index '{}' is missing its metadata object",
            index_name
        );
    };
    Ok(Some(serde_json::from_str(&metadata_json)?))
}

pub(crate) async fn write_index_snapshot(
    store: &Arc<dyn ObjectStore>,
    table_prefix: &OPath,
    snapshot: &IndexSnapshot,
    version: &str,
) -> Result<()> {
    let metadata_key = index_version_metadata_key(table_prefix, &snapshot.index, version);
    storage::write_json(
        store,
        &metadata_key,
        &serde_json::to_string_pretty(snapshot)?,
    )
    .await?;

    let manifest = IndexSnapshotManifest {
        version: version.to_string(),
        snapshot_object: snapshot.entries_object.clone(),
        row_count: snapshot.row_count,
        published_at_epoch_ms: chrono::Utc::now().timestamp_millis(),
    };
    let manifest_key = index_manifest_key(table_prefix, &snapshot.index);
    storage::write_json(
        store,
        &manifest_key,
        &serde_json::to_string_pretty(&manifest)?,
    )
    .await?;
    Ok(())
}

pub(crate) async fn remove_index_snapshot(
    store: &Arc<dyn ObjectStore>,
    table_prefix: &OPath,
    index_name: &str,
) -> Result<()> {
    let prefix = index_prefix_key(table_prefix, index_name);
    storage::delete_prefix(store, &prefix).await
}

pub(crate) fn index_column_positions(
    relation: &analyticsdb_control::CatalogRelation,
    columns: &[String],
) -> Result<Vec<usize>> {
    columns
        .iter()
        .map(|column| {
            relation
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(column))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Column '{}' not found in table '{}.{}.{}'",
                        column,
                        relation.database,
                        relation.schema,
                        relation.name
                    )
                })
        })
        .collect()
}

pub(crate) fn find_index_predicate<'a>(
    predicates: &'a BTreeMap<String, IndexPredicate>,
    column: &str,
) -> Option<&'a IndexPredicate> {
    predicates
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(column))
        .map(|(_, predicate)| predicate)
}

pub(crate) fn validate_unique_index_rows(
    relation: &analyticsdb_control::CatalogRelation,
    index: &analyticsdb_control::CatalogIndex,
    rows: &[Vec<String>],
) -> Result<()> {
    if !index.is_unique && !index.is_primary {
        return Ok(());
    }

    let index_positions = index_column_positions(relation, &index.columns)?;
    let mut seen = std::collections::HashMap::new();

    for row in rows {
        let key = index_positions
            .iter()
            .map(|pos| row.get(*pos).cloned().unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\u{1f}");
        *seen.entry(key).or_insert(0) += 1;
    }

    if let Some((key, _)) = seen.into_iter().find(|(_, count)| *count > 1) {
        anyhow::bail!(
            "Unique index '{}' on '{}.{}.{}' would contain duplicate key '{}'",
            index.name,
            relation.database,
            relation.schema,
            relation.name,
            key.replace('\u{1f}', ",")
        );
    }
    Ok(())
}
