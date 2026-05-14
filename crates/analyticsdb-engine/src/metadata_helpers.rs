use super::*;

pub(crate) fn metadata_statement_schema(statement: &MetadataStatement) -> Option<SchemaRef> {
    match statement {
        MetadataStatement::ShowDatabases => Some(utf8_schema(&["database_name"])),
        MetadataStatement::ShowSchemas { .. } => Some(utf8_schema(&["schema_name"])),
        MetadataStatement::ShowNodes => Some(utf8_schema(&[
            "node_id",
            "kind",
            "status",
            "last_heartbeat_ms",
        ])),
        MetadataStatement::ShowTables { .. } => Some(utf8_schema(&["table_name"])),
        MetadataStatement::ShowViews { .. } => Some(utf8_schema(&["view_name"])),
        MetadataStatement::ShowColumns { .. } => {
            Some(utf8_schema(&["column_name", "data_type", "is_nullable"]))
        }
        MetadataStatement::InformationSchemaSchemata { .. } => Some(utf8_schema(&[
            "catalog_name",
            "schema_name",
            "schema_owner",
            "default_character_set_catalog",
            "default_character_set_schema",
            "default_character_set_name",
            "sql_path",
        ])),
        MetadataStatement::InformationSchemaTables { .. } => Some(utf8_schema(&[
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
        ])),
        MetadataStatement::InformationSchemaColumns { .. } => Some(utf8_schema(&[
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
        ])),
        MetadataStatement::InformationSchemaViews { .. } => Some(utf8_schema(&[
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
        ])),
        MetadataStatement::InformationSchemaTableConstraints { .. } => Some(utf8_schema(&[
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
        ])),
        MetadataStatement::InformationSchemaKeyColumnUsage { .. } => Some(utf8_schema(&[
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "ordinal_position",
            "position_in_unique_constraint",
        ])),
        MetadataStatement::InformationSchemaConstraintColumnUsage { .. } => Some(utf8_schema(&[
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
        ])),
        MetadataStatement::InformationSchemaConstraintTableUsage { .. } => Some(utf8_schema(&[
            "table_catalog",
            "table_schema",
            "table_name",
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
        ])),
        MetadataStatement::InformationSchemaReferentialConstraints { .. } => Some(utf8_schema(&[
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
            "unique_constraint_catalog",
            "unique_constraint_schema",
            "unique_constraint_name",
            "match_option",
            "update_rule",
            "delete_rule",
        ])),
        _ => None,
    }
}

pub(crate) fn metadata_statement_sql(statement: &MetadataStatement) -> Option<&str> {
    match statement {
        MetadataStatement::InformationSchemaSchemata { sql }
        | MetadataStatement::InformationSchemaTables { sql }
        | MetadataStatement::InformationSchemaColumns { sql }
        | MetadataStatement::InformationSchemaViews { sql }
        | MetadataStatement::InformationSchemaTableConstraints { sql }
        | MetadataStatement::InformationSchemaKeyColumnUsage { sql }
        | MetadataStatement::InformationSchemaConstraintColumnUsage { sql }
        | MetadataStatement::InformationSchemaConstraintTableUsage { sql }
        | MetadataStatement::InformationSchemaReferentialConstraints { sql } => Some(sql),
        _ => None,
    }
}

pub(crate) fn projected_metadata_schema(sql: &str, base_schema: &SchemaRef) -> Result<SchemaRef> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    let Some(Statement::Query(query)) = statements.first() else {
        return Ok(Arc::clone(base_schema));
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(Arc::clone(base_schema));
    };

    if select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard(_)))
    {
        return Ok(Arc::clone(base_schema));
    }

    let mut fields = Vec::new();
    for item in &select.projection {
        let name = match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => ident.to_string(),
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) => idents
                .last()
                .map(|ident| ident.to_string())
                .unwrap_or_else(|| item.to_string()),
            SelectItem::ExprWithAlias { alias, .. } => alias.to_string(),
            SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => {
                return Ok(Arc::clone(base_schema));
            }
            _ => item.to_string(),
        };
        fields.push(Field::new(name, DataType::Utf8, false));
    }

    Ok(Arc::new(Schema::new(fields)))
}

pub(crate) fn execute_pg_catalog_select(
    sql: &str,
    _table: &str,
    columns: &[&str],
    rows: &[Vec<String>],
) -> Result<(RecordBatch, usize)> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    let Some(Statement::Query(query)) = statements.first() else {
        let batch = utf8_record_batch(columns, rows)?;
        return Ok((batch, rows.len()));
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        let batch = utf8_record_batch(columns, rows)?;
        return Ok((batch, rows.len()));
    };

    let mut filtered_rows = rows.to_vec();
    if let Some(selection) = &select.selection {
        filtered_rows.retain(|row| metadata_row_matches(selection, columns, row));
    }

    if let Some(order_by) = &query.order_by {
        if let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind {
            filtered_rows.sort_by(|left, right| {
                for order_expr in exprs {
                    let Some(column_name) = metadata_expr_column_name(&order_expr.expr) else {
                        continue;
                    };
                    let Some(idx) = columns.iter().position(|c| *c == column_name) else {
                        continue;
                    };
                    let ord = left[idx].cmp(&right[idx]);
                    if ord != std::cmp::Ordering::Equal {
                        return if order_expr.options.asc == Some(false) {
                            ord.reverse()
                        } else {
                            ord
                        };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
    }

    let projected = metadata_projection_indices(select, columns)?;
    let projected_columns = projected
        .iter()
        .map(|idx| columns[*idx])
        .collect::<Vec<_>>();
    let projected_rows = filtered_rows
        .iter()
        .map(|row| {
            projected
                .iter()
                .map(|idx| row[*idx].clone())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let batch = utf8_record_batch(&projected_columns, &projected_rows)?;
    let count = projected_rows.len();
    Ok((batch, count))
}

pub(crate) fn metadata_projection_indices(
    select: &sqlparser::ast::Select,
    columns: &[&str],
) -> Result<Vec<usize>> {
    if select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard(_)))
    {
        return Ok((0..columns.len()).collect());
    }

    let mut indices = Vec::new();
    for item in &select.projection {
        let Some(column_name) = (match item {
            SelectItem::UnnamedExpr(expr) => metadata_expr_column_name(expr),
            SelectItem::ExprWithAlias { expr, .. } => metadata_expr_column_name(expr),
            _ => None,
        }) else {
            bail!("Unsupported metadata projection '{}'", item);
        };
        let Some(index) = columns.iter().position(|c| *c == column_name) else {
            bail!("Unknown metadata projection column '{}'", column_name);
        };
        indices.push(index);
    }
    Ok(indices)
}

pub(crate) fn metadata_row_matches(expr: &Expr, columns: &[&str], row: &[String]) -> bool {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let Some(column_name) = metadata_expr_column_name(left) else {
                return true;
            };
            let Some(idx) = columns.iter().position(|c| *c == column_name) else {
                return true;
            };
            metadata_literal_value(right)
                .map(|value| row[idx] == value)
                .unwrap_or(true)
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let Some(column_name) = metadata_expr_column_name(expr) else {
                return true;
            };
            let Some(idx) = columns.iter().position(|c| *c == column_name) else {
                return true;
            };
            let matched = list
                .iter()
                .filter_map(metadata_literal_value)
                .any(|value| row[idx] == value);
            if *negated {
                !matched
            } else {
                matched
            }
        }
        Expr::Nested(expr) => metadata_row_matches(expr, columns, row),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => metadata_row_matches(left, columns, row) && metadata_row_matches(right, columns, row),
        _ => true,
    }
}

pub(crate) fn metadata_expr_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.as_str()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.value.as_str()),
        _ => None,
    }
}

pub(crate) fn metadata_literal_value(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(value) => match &value.value {
            sqlparser::ast::Value::SingleQuotedString(value)
            | sqlparser::ast::Value::DoubleQuotedString(value)
            | sqlparser::ast::Value::Number(value, _) => Some(value.clone()),
            _ => None,
        },
        _ => None,
    }
}
