use super::*;

#[derive(Debug, Clone)]
pub(crate) struct InsertSelectStatement {
    pub database: Option<String>,
    pub schema: Option<String>,
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub query_sql: String,
}

/// Parsed shape of a `generate_series(start, end)` source used as the FROM
/// of an `INSERT … SELECT`.  Only inclusive integer ranges with literal bounds
/// are supported — partition slicing requires constant endpoints known to the
/// coordinator.
#[derive(Debug, Clone)]
pub(crate) struct GenerateSeriesPlan {
    pub start: i64,
    pub end: i64,
    /// Original function name as it appeared in the SQL (case preserved).
    pub func_name: String,
}

pub(crate) fn parse_insert_select_statement(sql: &str) -> Result<Option<InsertSelectStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("INSERT INTO ") {
        return Ok(None);
    }

    // Use regex to find SELECT and extract table name. Use (?is) for case-insensitive and dot-matches-all.
    let re =
        regex::Regex::new(r"(?is)INSERT\s+INTO\s+([^\s\(\)]+)\s*(\([^\)]+\))?\s*(SELECT\s+.*)")?;
    if let Some(caps) = re.captures(trimmed) {
        let name = caps
            .get(1)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();
        let columns = caps.get(2).map(|m| {
            m.as_str()
                .trim_start_matches('(')
                .trim_end_matches(')')
                .split(',')
                .map(|s| s.trim().to_string())
                .collect::<Vec<_>>()
        });
        let query_sql = caps
            .get(3)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();

        return Ok(Some(InsertSelectStatement {
            database: None,
            schema: None,
            name,
            columns,
            query_sql,
        }));
    }

    Ok(None)
}

/// Returns `(database, schema, table)` if `sql` is a plain SELECT that reads
/// exactly one managed table and has no sub-queries, CTEs, or JOINs.
///
/// The result is used by `try_execute_distributed_select` to decide whether the
/// query can be fanned out to Compute nodes.  Conservative: returns `None` for
/// anything that doesn't look like `SELECT … FROM [db.][ schema.]table [WHERE …]`.
pub(crate) fn parse_plain_select_table(
    sql: &str,
) -> Option<(Option<String>, Option<String>, String)> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, trimmed).ok()?;
    let statement = statements.into_iter().next()?;
    let Statement::Query(query) = statement else {
        return None;
    };
    // Reject CTEs.
    if query.with.is_some() {
        return None;
    }
    let SetExpr::Select(select) = *query.body else {
        return None;
    };
    // Exactly one FROM item, no JOINs.
    if select.from.len() != 1 {
        return None;
    }
    let table_with_joins = &select.from[0];
    if !table_with_joins.joins.is_empty() {
        return None;
    }
    let TableFactor::Table { name, .. } = &table_with_joins.relation else {
        return None;
    };
    let parts: Vec<String> = name.0.iter().map(|i| i.to_string()).collect();
    match parts.as_slice() {
        [table] => Some((None, None, table.clone())),
        [schema, table] => Some((None, Some(schema.clone()), table.clone())),
        [db, schema, table] => Some((Some(db.clone()), Some(schema.clone()), table.clone())),
        _ => None,
    }
}

/// Rewrites `sql` so that the FROM clause referencing `source_table` (any level
/// of qualification) is replaced with `__partition__`.  The caller registers
/// the worker's local Parquet files as a DataFusion table named `__partition__`
/// before executing the returned SQL, which preserves WHERE, LIMIT, projections,
/// and other clauses so DataFusion can push them into the scan.
///
/// Returns `None` if the SQL cannot be parsed or the table is not found.
pub(crate) fn rewrite_sql_for_partition(sql: &str, source_table: &str) -> Option<String> {
    use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

    let trimmed = sql.trim().trim_end_matches(';').trim();
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, trimmed).ok()?;
    let statement = statements.get_mut(0)?;

    let Statement::Query(ref mut query) = statement else {
        return None;
    };
    let SetExpr::Select(ref mut select) = *query.body else {
        return None;
    };

    let mut replaced = false;
    for twj in &mut select.from {
        if let TableFactor::Table {
            ref mut name,
            ref mut alias,
            ..
        } = twj.relation
        {
            let last = name.0.last().and_then(|p| match p {
                ObjectNamePart::Identifier(id) => Some(id.value.clone()),
                _ => None,
            });
            if last
                .as_deref()
                .map(|t| t.eq_ignore_ascii_case(source_table))
                .unwrap_or(false)
            {
                *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                    "__partition__",
                ))]);
                // Remove any alias so the query can reference columns directly.
                *alias = None;
                replaced = true;
            }
        }
    }

    if replaced {
        Some(statement.to_string())
    } else {
        None
    }
}

pub(crate) fn distributed_aggregate_plan(
    sql: &str,
    _source_table: &str,
) -> Option<(String, String)> {
    let trimmed = sql.trim().trim_end_matches(';').trim();

    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, trimmed).ok()?;
    let statement = statements.into_iter().next()?;
    let Statement::Query(query) = statement else {
        return None;
    };
    if query.with.is_some() {
        return None;
    }
    let SetExpr::Select(select) = *query.body else {
        return None;
    };
    if select.having.is_some() || select.distinct.is_some() {
        return None;
    }

    let group_by_exprs: Vec<String> = match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) if modifiers.is_empty() => {
            exprs.iter().map(|e| e.to_string()).collect()
        }
        _ => return None,
    };
    let has_group_by = !group_by_exprs.is_empty();

    let where_clause = select
        .selection
        .as_ref()
        .map(|w| format!("WHERE {w}"))
        .unwrap_or_default();

    let group_by_clause = if has_group_by {
        format!("GROUP BY {}", group_by_exprs.join(", "))
    } else {
        String::new()
    };

    let has_any_agg = select.projection.iter().any(|item| {
        let expr = match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
            _ => return false,
        };
        aggregate_expr_parts(expr).is_some()
    });
    if !has_any_agg {
        return None;
    }

    let mut worker_items = Vec::new();
    let mut final_items = Vec::new();
    let mut agg_idx = 0usize;

    for item in &select.projection {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, quote_sql_identifier(&expr.to_string())),
            SelectItem::ExprWithAlias { expr, alias } => {
                (expr, quote_sql_identifier(alias.value.as_str()))
            }
            _ => return None,
        };

        if let Some(aggregate) = aggregate_expr_parts(expr) {
            let output = format!("__adb_agg_{agg_idx}");
            agg_idx += 1;
            match aggregate.function.as_str() {
                "count" => {
                    worker_items.push(format!(
                        "COUNT({}) AS {}",
                        aggregate.argument,
                        quote_sql_identifier(&output)
                    ));
                    final_items.push(format!("SUM({}) AS {alias}", quote_sql_identifier(&output)));
                }
                "sum" => {
                    worker_items.push(format!(
                        "SUM({}) AS {}",
                        aggregate.argument,
                        quote_sql_identifier(&output)
                    ));
                    final_items.push(format!("SUM({}) AS {alias}", quote_sql_identifier(&output)));
                }
                "min" => {
                    worker_items.push(format!(
                        "MIN({}) AS {}",
                        aggregate.argument,
                        quote_sql_identifier(&output)
                    ));
                    final_items.push(format!("MIN({}) AS {alias}", quote_sql_identifier(&output)));
                }
                "max" => {
                    worker_items.push(format!(
                        "MAX({}) AS {}",
                        aggregate.argument,
                        quote_sql_identifier(&output)
                    ));
                    final_items.push(format!("MAX({}) AS {alias}", quote_sql_identifier(&output)));
                }
                "avg" => {
                    let sum_output = format!("{output}_sum");
                    let count_output = format!("{output}_count");
                    worker_items.push(format!(
                        "SUM({}) AS {}, COUNT({}) AS {}",
                        aggregate.argument,
                        quote_sql_identifier(&sum_output),
                        aggregate.argument,
                        quote_sql_identifier(&count_output)
                    ));
                    final_items.push(format!(
                        "SUM({}) / SUM({}) AS {alias}",
                        quote_sql_identifier(&sum_output),
                        quote_sql_identifier(&count_output)
                    ));
                }
                _ => return None,
            }
        } else if has_group_by {
            let col_name = quote_sql_identifier(&expr.to_string());
            worker_items.push(format!("{expr} AS {col_name}"));
            final_items.push(format!("{col_name} AS {alias}"));
        } else {
            return None;
        }
    }

    let worker_select = worker_items.join(", ");
    let final_select = final_items.join(", ");

    let worker_sql = match (where_clause.is_empty(), group_by_clause.is_empty()) {
        (true, true) => format!("SELECT {worker_select} FROM __partition__"),
        (true, false) => format!("SELECT {worker_select} FROM __partition__ {group_by_clause}"),
        (false, true) => format!("SELECT {worker_select} FROM __partition__ {where_clause}"),
        (false, false) => {
            format!("SELECT {worker_select} FROM __partition__ {where_clause} {group_by_clause}")
        }
    };

    let final_sql = if group_by_clause.is_empty() {
        format!("SELECT {final_select} FROM __partition__")
    } else {
        format!("SELECT {final_select} FROM __partition__ {group_by_clause}")
    };

    Some((worker_sql, final_sql))
}

pub(crate) fn projection_has_aggregate(projection: &[SelectItem]) -> bool {
    projection.iter().any(|item| {
        let expr = match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
            _ => return false,
        };
        aggregate_expr_parts(expr).is_some()
    })
}

pub(crate) fn has_window_functions(sql: &str) -> bool {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let dialect = PostgreSqlDialect {};
    let Ok(statements) = Parser::parse_sql(&dialect, trimmed) else {
        return false;
    };
    let Some(Statement::Query(query)) = statements.into_iter().next() else {
        return false;
    };
    let SetExpr::Select(select) = *query.body else {
        return false;
    };
    select.projection.iter().any(|item| {
        let expr = match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
            _ => return false,
        };
        if let Expr::Function(f) = expr {
            return f.over.is_some();
        }
        false
    })
}

/// 2-phase DISTINCT: both worker and coordinator run `SELECT DISTINCT … FROM __partition__`.
pub(crate) fn distributed_distinct_plan(sql: &str, source_table: &str) -> Option<(String, String)> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, trimmed).ok()?;
    let statement = statements.into_iter().next()?;
    let Statement::Query(query) = statement else {
        return None;
    };
    if query.with.is_some() {
        return None;
    }
    let SetExpr::Select(select) = *query.body else {
        return None;
    };
    if select.distinct.is_none() || select.having.is_some() {
        return None;
    }
    match &select.group_by {
        GroupByExpr::Expressions(exprs, _) if !exprs.is_empty() => return None,
        _ => {}
    }
    if projection_has_aggregate(&select.projection) {
        return None;
    }

    let rewritten = rewrite_sql_for_partition(trimmed, source_table)?;
    Some((rewritten.clone(), rewritten))
}

/// 2-phase ORDER BY / LIMIT: workers return their local top-N, coordinator re-sorts.
pub(crate) fn distributed_order_limit_plan(
    sql: &str,
    source_table: &str,
) -> Option<(String, String)> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, trimmed).ok()?;
    let statement = statements.into_iter().next()?;
    let Statement::Query(query) = statement else {
        return None;
    };
    if query.with.is_some() {
        return None;
    }
    if query.order_by.is_none() && query.limit_clause.is_none() {
        return None;
    }
    let SetExpr::Select(select) = *query.body else {
        return None;
    };
    if select.distinct.is_some() {
        return None;
    }
    match &select.group_by {
        GroupByExpr::Expressions(exprs, _) if !exprs.is_empty() => return None,
        _ => {}
    }
    if projection_has_aggregate(&select.projection) {
        return None;
    }

    let rewritten = rewrite_sql_for_partition(trimmed, source_table)?;
    Some((rewritten.clone(), rewritten))
}

pub(crate) fn quote_sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub(crate) fn select_projection_contains_function(sql: &str) -> bool {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let dialect = PostgreSqlDialect {};
    let Ok(statements) = Parser::parse_sql(&dialect, trimmed) else {
        return false;
    };
    let Some(Statement::Query(query)) = statements.first() else {
        return false;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::UnnamedExpr(Expr::Function(_))
                | SelectItem::ExprWithAlias {
                    expr: Expr::Function(_),
                    ..
                }
        )
    })
}

pub(crate) struct AggregateExprParts {
    pub function: String,
    pub argument: String,
}

pub(crate) fn aggregate_expr_parts(expr: &Expr) -> Option<AggregateExprParts> {
    let Expr::Function(function) = expr else {
        return None;
    };
    if function.filter.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
        || !matches!(function.parameters, FunctionArguments::None)
    {
        return None;
    }

    let function_name = function
        .name
        .to_string()
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .trim_matches('"')
        .to_ascii_lowercase();

    let FunctionArguments::List(args) = &function.args else {
        return None;
    };
    if args.duplicate_treatment.is_some() || !args.clauses.is_empty() || args.args.len() != 1 {
        return None;
    }
    let FunctionArg::Unnamed(arg) = &args.args[0] else {
        return None;
    };
    let argument = match arg {
        FunctionArgExpr::Wildcard => "*".to_string(),
        FunctionArgExpr::Expr(expr) => expr.to_string(),
        FunctionArgExpr::QualifiedWildcard(_) => return None,
    };
    if argument == "*" && function_name != "count" {
        return None;
    }

    Some(AggregateExprParts {
        function: function_name,
        argument,
    })
}

/// Detects `SELECT … FROM generate_series(<int>, <int>) [AS …]` and extracts
/// the integer range.  Returns `None` for anything more complex (step argument,
/// non-literal bounds, joins, CTEs, …).
pub(crate) fn parse_generate_series_select(sql: &str) -> Option<GenerateSeriesPlan> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Value};
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, trimmed).ok()?;
    let statement = statements.into_iter().next()?;
    let Statement::Query(query) = statement else {
        return None;
    };
    if query.with.is_some() {
        return None;
    }
    let SetExpr::Select(select) = *query.body else {
        return None;
    };
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return None;
    }
    let TableFactor::Table { name, args, .. } = &select.from[0].relation else {
        return None;
    };
    let last_part = name.0.last()?.to_string().to_lowercase();
    if last_part != "generate_series" {
        return None;
    }
    let sqlparser::ast::TableFunctionArgs { args: arg_list, .. } = args.as_ref()?;
    if arg_list.len() != 2 {
        return None;
    }

    let extract_int = |fa: &FunctionArg| -> Option<i64> {
        let expr = match fa {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
            FunctionArg::Named {
                arg: FunctionArgExpr::Expr(e),
                ..
            } => e,
            _ => return None,
        };
        match expr {
            Expr::Value(v) => match &v.value {
                Value::Number(s, _) => s.parse::<i64>().ok(),
                _ => None,
            },
            Expr::UnaryOp {
                op: sqlparser::ast::UnaryOperator::Minus,
                expr,
            } => match &**expr {
                Expr::Value(v) => match &v.value {
                    Value::Number(s, _) => s.parse::<i64>().ok().map(|n| -n),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        }
    };

    let start = extract_int(&arg_list[0])?;
    let end = extract_int(&arg_list[1])?;
    if end < start {
        return None;
    }

    Some(GenerateSeriesPlan {
        start,
        end,
        func_name: name.0.last()?.to_string(),
    })
}

/// Slices `[start, end]` (inclusive, integer) into at most `num_workers` chunks.
/// The first `remainder` chunks are one element larger so every element is
/// assigned to exactly one chunk and the chunk sizes differ by at most one.
/// Returns an empty Vec if the range is empty or `num_workers == 0`.
pub(crate) fn slice_int_range(start: i64, end: i64, num_workers: usize) -> Vec<(i64, i64)> {
    if num_workers == 0 || end < start {
        return Vec::new();
    }
    let total = (end - start + 1) as u128;
    let workers = (num_workers as u128).min(total);
    let base = total / workers;
    let remainder = total - base * workers;
    let mut chunks = Vec::with_capacity(workers as usize);
    let mut cursor = start;
    for i in 0..workers {
        let extra = if i < remainder { 1 } else { 0 };
        let size = (base + extra) as i64;
        let chunk_end = cursor + size - 1;
        chunks.push((cursor, chunk_end));
        cursor = chunk_end + 1;
    }
    chunks
}

/// Replaces the two literal integer arguments of the `generate_series(...)` call
/// in `sql` with `start` and `end`, and rewrites each projection item so its
/// output column name matches the corresponding entry in `target_columns`.
/// Returns the rewritten SQL or `None` if the SQL cannot be parsed or the
/// projection arity does not match `target_columns`.
pub(crate) fn rewrite_generate_series_range(
    sql: &str,
    plan: &GenerateSeriesPlan,
    start: i64,
    end: i64,
    target_columns: &[String],
) -> Option<String> {
    use sqlparser::ast::{
        Expr, FunctionArg, FunctionArgExpr, Ident, SelectItem, Value, ValueWithSpan,
    };
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).ok()?;
    let statement = statements.get_mut(0)?;
    let Statement::Query(query) = statement else {
        return None;
    };
    let SetExpr::Select(select) = &mut *query.body else {
        return None;
    };
    if select.from.len() != 1 {
        return None;
    }
    let TableFactor::Table { name, args, .. } = &mut select.from[0].relation else {
        return None;
    };
    if name.0.last()?.to_string().to_lowercase() != plan.func_name.to_lowercase() {
        return None;
    }
    let make_int = |n: i64| {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(ValueWithSpan {
            value: Value::Number(n.to_string(), false),
            span: sqlparser::tokenizer::Span::empty(),
        })))
    };
    let ta = args.as_mut()?;
    if ta.args.len() == 2 {
        ta.args[0] = make_int(start);
        ta.args[1] = make_int(end);
    } else {
        return None;
    }

    if !target_columns.is_empty() {
        if select.projection.len() != target_columns.len() {
            return None;
        }
        for (item, target) in select.projection.iter_mut().zip(target_columns.iter()) {
            let expr = match item.clone() {
                SelectItem::UnnamedExpr(e) => e,
                SelectItem::ExprWithAlias { expr, .. } => expr,
                _ => return None,
            };
            *item = SelectItem::ExprWithAlias {
                expr,
                alias: Ident::with_quote('"', target.clone()),
            };
        }
    }

    Some(statement.to_string())
}

pub(crate) async fn delete_written_files(
    store: &Arc<dyn ObjectStore>,
    files: &[String],
) -> Result<()> {
    let keys: Vec<object_store::path::Path> = files
        .iter()
        .filter_map(|p| object_store::path::Path::parse(p.trim_start_matches('/')).ok())
        .collect();
    storage::delete_objects(store, &keys).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distributed_aggregate_plan_supports_group_by() {
        let sql =
            "SELECT department, COUNT(*) as cnt, SUM(salary) as total FROM employees GROUP BY department";
        let (worker_sql, final_sql) =
            distributed_aggregate_plan(sql, "employees").expect("should produce a plan");
        assert!(
            worker_sql.contains("__partition__"),
            "worker_sql should reference __partition__"
        );
        assert!(
            worker_sql.contains("GROUP BY"),
            "worker_sql should have GROUP BY: {worker_sql}"
        );
        assert!(
            final_sql.contains("__partition__"),
            "final_sql should reference __partition__"
        );
        assert!(
            final_sql.contains("GROUP BY"),
            "final_sql should have GROUP BY: {final_sql}"
        );
        assert!(
            worker_sql.contains("COUNT(*)"),
            "worker_sql should use COUNT(*): {worker_sql}"
        );
        assert!(
            final_sql.contains("SUM"),
            "final_sql should use SUM to finalize counts: {final_sql}"
        );
    }

    #[test]
    fn distributed_aggregate_plan_plain_aggregate_no_group_by() {
        let sql = "SELECT COUNT(*) AS n FROM customers";
        let (worker_sql, final_sql) =
            distributed_aggregate_plan(sql, "customers").expect("should produce a plan");
        assert!(worker_sql.contains("__partition__"));
        assert!(!worker_sql.contains("GROUP BY"));
        assert!(final_sql.contains("__partition__"));
        assert!(!final_sql.contains("GROUP BY"));
    }

    #[test]
    fn distributed_aggregate_plan_rejects_no_aggregate() {
        let sql = "SELECT id, name FROM customers";
        assert!(
            distributed_aggregate_plan(sql, "customers").is_none(),
            "non-aggregate SELECT without GROUP BY should not produce a plan"
        );
    }

    #[test]
    fn distributed_aggregate_plan_rejects_distinct() {
        let sql = "SELECT DISTINCT COUNT(*) FROM customers";
        assert!(distributed_aggregate_plan(sql, "customers").is_none());
    }

    #[test]
    fn distributed_aggregate_plan_rejects_having() {
        let sql = "SELECT department, COUNT(*) as cnt FROM employees GROUP BY department HAVING COUNT(*) > 5";
        assert!(distributed_aggregate_plan(sql, "employees").is_none());
    }

    #[test]
    fn distributed_distinct_plan_works() {
        let sql = "SELECT DISTINCT status FROM orders";
        let (worker_sql, final_sql) =
            distributed_distinct_plan(sql, "orders").expect("should produce a plan");
        assert!(
            worker_sql.contains("__partition__"),
            "worker_sql: {worker_sql}"
        );
        assert!(
            final_sql.contains("__partition__"),
            "final_sql: {final_sql}"
        );
        assert_eq!(worker_sql, final_sql);
        assert!(
            worker_sql.to_uppercase().contains("DISTINCT"),
            "worker_sql should be DISTINCT: {worker_sql}"
        );
    }

    #[test]
    fn distributed_distinct_plan_rejects_non_distinct() {
        assert!(distributed_distinct_plan("SELECT status FROM orders", "orders").is_none());
    }

    #[test]
    fn distributed_distinct_plan_rejects_aggregate() {
        assert!(
            distributed_distinct_plan("SELECT DISTINCT COUNT(*) FROM orders", "orders").is_none()
        );
    }

    #[test]
    fn distributed_distinct_plan_rejects_group_by() {
        assert!(distributed_distinct_plan(
            "SELECT DISTINCT status FROM orders GROUP BY status",
            "orders"
        )
        .is_none());
    }

    #[test]
    fn distributed_order_limit_plan_works() {
        let sql = "SELECT id FROM orders ORDER BY id LIMIT 10";
        let (worker_sql, final_sql) =
            distributed_order_limit_plan(sql, "orders").expect("should produce a plan");
        assert!(
            worker_sql.contains("__partition__"),
            "worker_sql: {worker_sql}"
        );
        assert!(
            final_sql.contains("__partition__"),
            "final_sql: {final_sql}"
        );
        assert_eq!(worker_sql, final_sql);
        assert!(
            worker_sql.to_uppercase().contains("ORDER BY"),
            "worker_sql: {worker_sql}"
        );
        assert!(worker_sql.contains("LIMIT"), "worker_sql: {worker_sql}");
    }

    #[test]
    fn distributed_order_limit_plan_order_only() {
        let sql = "SELECT id FROM orders ORDER BY id";
        assert!(distributed_order_limit_plan(sql, "orders").is_some());
    }

    #[test]
    fn distributed_order_limit_plan_rejects_no_order_or_limit() {
        assert!(distributed_order_limit_plan("SELECT id FROM orders", "orders").is_none());
    }

    #[test]
    fn distributed_order_limit_plan_rejects_distinct() {
        assert!(distributed_order_limit_plan(
            "SELECT DISTINCT id FROM orders ORDER BY id LIMIT 5",
            "orders"
        )
        .is_none());
    }

    #[test]
    fn distributed_order_limit_plan_rejects_aggregate() {
        assert!(distributed_order_limit_plan(
            "SELECT COUNT(*) FROM orders ORDER BY COUNT(*) LIMIT 5",
            "orders"
        )
        .is_none());
    }

    #[test]
    fn has_window_functions_detects_over_clause() {
        assert!(has_window_functions(
            "SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) FROM emp"
        ));
    }

    #[test]
    fn has_window_functions_plain_select_returns_false() {
        assert!(!has_window_functions("SELECT id, name FROM customers"));
    }

    #[test]
    fn has_window_functions_aggregate_returns_false() {
        assert!(!has_window_functions("SELECT COUNT(*) FROM customers"));
    }
}
