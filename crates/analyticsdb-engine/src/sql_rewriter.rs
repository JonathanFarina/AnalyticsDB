use anyhow::Result;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, Ident, ObjectNamePart, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, Statement, TableFactor,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;

use analyticsdb_control::ControlPlane;
use analyticsdb_core::SessionContext;

pub async fn rewrite_sql_for_postgres_compatibility(
    sql: &str,
    control_plane: &ControlPlane,
    session: &SessionContext,
) -> Result<String> {
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)?;

    for stmt in statements.iter_mut() {
        rewrite_statement_recursive(stmt, control_plane, session).await?;
    }

    let result = statements
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("; ");

    Ok(result)
}

fn rewrite_statement_recursive<'a>(
    stmt: &'a mut Statement,
    control_plane: &'a ControlPlane,
    session: &'a SessionContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        match stmt {
            Statement::Query(query) => rewrite_query_recursive(query, control_plane, session).await,
            Statement::CreateTable(create_table) => {
                if let Some(ref mut q) = create_table.query {
                    rewrite_query_recursive(q, control_plane, session).await?;
                }
                Ok(())
            }
            Statement::Insert(insert) => {
                if let Some(ref mut q) = insert.source {
                    rewrite_query_recursive(q, control_plane, session).await?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    })
}

fn rewrite_query_recursive<'a>(
    query: &'a mut Query,
    control_plane: &'a ControlPlane,
    session: &'a SessionContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        rewrite_set_expr_recursive(&mut query.body, control_plane, session).await?;

        if let Some(order_by) = &mut query.order_by {
            if let sqlparser::ast::OrderByKind::Expressions(exprs) = &mut order_by.kind {
                for order_by_expr in exprs {
                    rewrite_expr_recursive(&mut order_by_expr.expr, control_plane, session).await?;
                }
            }
        }

        if let Some(limit_clause) = &mut query.limit_clause {
            match limit_clause {
                sqlparser::ast::LimitClause::LimitOffset {
                    limit,
                    offset,
                    limit_by,
                } => {
                    if let Some(limit_expr) = limit {
                        rewrite_expr_recursive(limit_expr, control_plane, session).await?;
                    }
                    if let Some(offset_struct) = offset {
                        rewrite_expr_recursive(&mut offset_struct.value, control_plane, session)
                            .await?;
                    }
                    for e in limit_by {
                        rewrite_expr_recursive(e, control_plane, session).await?;
                    }
                }
                sqlparser::ast::LimitClause::OffsetCommaLimit { offset, limit } => {
                    rewrite_expr_recursive(offset, control_plane, session).await?;
                    rewrite_expr_recursive(limit, control_plane, session).await?;
                }
            }
        }

        Ok(())
    })
}

fn rewrite_set_expr_recursive<'a>(
    set_expr: &'a mut SetExpr,
    control_plane: &'a ControlPlane,
    session: &'a SessionContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        match set_expr {
            SetExpr::Select(select) => {
                rewrite_select_recursive(select, control_plane, session).await
            }
            SetExpr::Query(query) => rewrite_query_recursive(query, control_plane, session).await,
            SetExpr::SetOperation { left, right, .. } => {
                rewrite_set_expr_recursive(left, control_plane, session).await?;
                rewrite_set_expr_recursive(right, control_plane, session).await?;
                Ok(())
            }
            _ => Ok(()),
        }
    })
}

fn rewrite_select_recursive<'a>(
    select: &'a mut Select,
    control_plane: &'a ControlPlane,
    session: &'a SessionContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        // 1. Resolve table aliases and schemas in FROM clause
        let mut table_schemas = HashMap::new();
        for table_with_joins in select.from.iter_mut() {
            resolve_table_schemas_recursive(
                &mut table_with_joins.relation,
                control_plane,
                session,
                &mut table_schemas,
            )
            .await?;
            for join in table_with_joins.joins.iter_mut() {
                resolve_table_schemas_recursive(
                    &mut join.relation,
                    control_plane,
                    session,
                    &mut table_schemas,
                )
                .await?;

                match &mut join.join_operator {
                    sqlparser::ast::JoinOperator::Inner(constraint)
                    | sqlparser::ast::JoinOperator::LeftOuter(constraint)
                    | sqlparser::ast::JoinOperator::RightOuter(constraint)
                    | sqlparser::ast::JoinOperator::FullOuter(constraint) => {
                        if let sqlparser::ast::JoinConstraint::On(expr) = constraint {
                            rewrite_expr_recursive(expr, control_plane, session).await?;
                        }
                    }
                    _ => {}
                }
            }
        }

        // 2. Expand wildcards and collect initial projection names
        let mut new_projection = Vec::new();
        for item in select.projection.iter() {
            match item {
                SelectItem::Wildcard(_) => {
                    // Expand all tables
                    for (_alias, columns) in table_schemas.iter() {
                        for col in columns {
                            if col != "_row_id" {
                                new_projection.push(SelectItem::UnnamedExpr(Expr::Identifier(
                                    Ident::new(col.clone()),
                                )));
                            }
                        }
                    }
                    if table_schemas.is_empty() {
                        new_projection.push(item.clone());
                    }
                }
                SelectItem::QualifiedWildcard(kind, _) => {
                    let alias = match kind {
                        SelectItemQualifiedWildcardKind::ObjectName(name) => name
                            .0
                            .last()
                            .map(|i| i.to_string())
                            .unwrap_or_else(|| kind.to_string()),
                        SelectItemQualifiedWildcardKind::Expr(expr) => expr.to_string(),
                    };

                    if let Some(columns) = table_schemas.get(&alias) {
                        for col in columns {
                            if col != "_row_id" {
                                new_projection.push(SelectItem::UnnamedExpr(Expr::CompoundIdentifier(
                                    vec![Ident::new(alias.clone()), Ident::new(col.clone())],
                                )));
                            }
                        }
                    } else {
                        new_projection.push(item.clone());
                    }
                }
                _ => new_projection.push(item.clone()),
            }
        }
        select.projection = new_projection;

        // 3. De-duplicate names by aliasing and rewrite expressions
        let mut seen_names = HashSet::new();
        for item in select.projection.iter_mut() {
            let (current_name, is_unnamed) = match item {
                SelectItem::UnnamedExpr(expr) => (get_canonical_name(expr), true),
                SelectItem::ExprWithAlias { alias, .. } => (alias.value.clone(), false),
                _ => continue,
            };

            if seen_names.contains(&current_name) && is_unnamed {
                let alias_name = make_unique_alias(&current_name, &seen_names);
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        *item = SelectItem::ExprWithAlias {
                            expr: expr.clone(),
                            alias: Ident::new(alias_name.clone()),
                        };
                    }
                    _ => unreachable!(),
                }
                seen_names.insert(alias_name);
            } else {
                seen_names.insert(current_name);
            }

            // Recurse into expressions
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    rewrite_expr_recursive(expr, control_plane, session).await?
                }
                SelectItem::ExprWithAlias { expr, .. } => {
                    rewrite_expr_recursive(expr, control_plane, session).await?
                }
                _ => {}
            }
        }

        // 4. Process WHERE, GROUP BY, HAVING clause
        if let Some(selection) = &mut select.selection {
            rewrite_expr_recursive(selection, control_plane, session).await?;
        }

        if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &mut select.group_by {
            for expr in exprs {
                rewrite_expr_recursive(expr, control_plane, session).await?;
            }
        }

        if let Some(having) = &mut select.having {
            rewrite_expr_recursive(having, control_plane, session).await?;
        }

        Ok(())
    })
}

fn resolve_table_schemas_recursive<'a>(
    tf: &'a mut TableFactor,
    control_plane: &'a ControlPlane,
    session: &'a SessionContext,
    schemas: &'a mut HashMap<String, Vec<String>>,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        match tf {
            TableFactor::Table {
                name, alias, args, ..
            } => {
                if args.is_some() {
                    return Ok(());
                }

                let idents: Vec<String> = name
                    .0
                    .iter()
                    .map(|i| i.to_string().to_lowercase())
                    .collect();
                let effective_alias = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| idents.last().cloned().unwrap_or_default());

                // Standard PostgreSQL system catalog schemas for JDBC parity
                let matched_pg_catalog = match idents.as_slice() {
                    [s, n] if s == "pg_catalog" => Some(n.as_str()),
                    [n] if n.starts_with("pg_") => {
                        // Qualify unqualified pg_ tables to pg_catalog for DataFusion
                        *name = sqlparser::ast::ObjectName(vec![
                            ObjectNamePart::Identifier(Ident::new("pg_catalog")),
                            ObjectNamePart::Identifier(Ident::new(n.clone())),
                        ]);
                        Some(n.as_str())
                    }
                    _ => None,
                };

                if let Some(lower_n) = matched_pg_catalog {
                    match lower_n {
                        "pg_database" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    "oid".to_string(),
                                    "datname".to_string(),
                                    "datdba".to_string(),
                                    "encoding".to_string(),
                                    "datcollate".to_string(),
                                    "datctype".to_string(),
                                    "datistemplate".to_string(),
                                    "datallowconn".to_string(),
                                    "datconnlimit".to_string(),
                                    "datlastsysoid".to_string(),
                                    "datfrozenxid".to_string(),
                                    "datminmxid".to_string(),
                                    "dattablespace".to_string(),
                                    "datacl".to_string(),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_namespace" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    "oid".to_string(),
                                    "nspname".to_string(),
                                    "nspowner".to_string(),
                                    "nspacl".to_string(),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_tables" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    "schemaname".to_string(),
                                    "tablename".to_string(),
                                    "tableowner".to_string(),
                                    "tablespace".to_string(),
                                    "hasindexes".to_string(),
                                    "hasrules".to_string(),
                                    "hastriggers".to_string(),
                                    "rowsecurity".to_string(),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_views" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    "schemaname".to_string(),
                                    "viewname".to_string(),
                                    "viewowner".to_string(),
                                    "definition".to_string(),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_roles" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    "oid".to_string(),
                                    "rolname".to_string(),
                                    "rolsuper".to_string(),
                                    "rolinherit".to_string(),
                                    "rolcreaterole".to_string(),
                                    "rolcreatedb".to_string(),
                                    "rolcanlogin".to_string(),
                                    "rolreplication".to_string(),
                                    "rolbypassrls".to_string(),
                                    "rolconnlimit".to_string(),
                                    "rolpassword".to_string(),
                                    "rolvaliduntil".to_string(),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_type" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    "oid".to_string(),
                                    "typname".to_string(),
                                    "typnamespace".to_string(),
                                    "typowner".to_string(),
                                    "typlen".to_string(),
                                    "typbyval".to_string(),
                                    "typtype".to_string(),
                                    "typcategory".to_string(),
                                    "typispreferred".to_string(),
                                    "typisdefined".to_string(),
                                    "typdelim".to_string(),
                                    "typrelid".to_string(),
                                    "typelem".to_string(),
                                    "typarray".to_string(),
                                    "typinput".to_string(),
                                    "typbasetype".to_string(),
                                    "typtypmod".to_string(),
                                    "typndims".to_string(),
                                    "typcollation".to_string(),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_class" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    "oid".to_string(),
                                    "relname".to_string(),
                                    "relnamespace".to_string(),
                                    "reltype".to_string(),
                                    "reloftype".to_string(),
                                    "relowner".to_string(),
                                    "relam".to_string(),
                                    "relfilenode".to_string(),
                                    "reltablespace".to_string(),
                                    "relpages".to_string(),
                                    "reltuples".to_string(),
                                    "relallvisible".to_string(),
                                    "reltoastrelid".to_string(),
                                    "relhasindex".to_string(),
                                    "relisshared".to_string(),
                                    "relpersistence".to_string(),
                                    "relkind".to_string(),
                                    "relnatts".to_string(),
                                    "relchecks".to_string(),
                                    "relhasrules".to_string(),
                                    "relhastriggers".to_string(),
                                    "relhassubclass".to_string(),
                                    "relrowsecurity".to_string(),
                                    "relforcerowsecurity".to_string(),
                                    "relispartition".to_string(),
                                    "relrewrite".to_string(),
                                    "relfrozenxid".to_string(),
                                    "relminmxid".to_string(),
                                    "relacl".to_string(),
                                    "reloptions".to_string(),
                                    "relpartbound".to_string(),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_attribute" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    "attrelid".to_string(),
                                    "attname".to_string(),
                                    "atttypid".to_string(),
                                    "attnum".to_string(),
                                    "attnotnull".to_string(),
                                    "atttypmod".to_string(),
                                    "attndims".to_string(),
                                    "atthasdef".to_string(),
                                    "attidentity".to_string(),
                                    "attgenerated".to_string(),
                                    "attisdropped".to_string(),
                                    "attislocal".to_string(),
                                    "attinhcount".to_string(),
                                    "attcollation".to_string(),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_description" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    "objoid".to_string(),
                                    "classoid".to_string(),
                                    "objsubid".to_string(),
                                    "description".to_string(),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_attrdef" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    "oid".to_string(),
                                    "adrelid".to_string(),
                                    "adnum".to_string(),
                                    "adbin".to_string(),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_depend" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    "classid".to_string(),
                                    "objid".to_string(),
                                    "objsubid".to_string(),
                                    "refclassid".to_string(),
                                    "refobjid".to_string(),
                                    "refobjsubid".to_string(),
                                    "deptype".to_string(),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_constraint" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    "oid".to_string(),
                                    "conname".to_string(),
                                    "connamespace".to_string(),
                                    "contype".to_string(),
                                    "condeferrable".to_string(),
                                    "condeferred".to_string(),
                                    "convalidated".to_string(),
                                    "conrelid".to_string(),
                                    "contypid".to_string(),
                                    "conindid".to_string(),
                                    "confrelid".to_string(),
                                    "confupdtype".to_string(),
                                    "confdeltype".to_string(),
                                    "confmatchtype".to_string(),
                                    "conislocal".to_string(),
                                    "coninhcount".to_string(),
                                    "connoinherit".to_string(),
                                    "conkey".to_string(),
                                    "confkey".to_string(),
                                ],
                            );
                            return Ok(());
                        }
                        _ => {}
                    }
                }

                // Attempt to fetch columns from ControlPlane
                let db_name = if idents.len() == 3 {
                    Some(idents[0].as_str())
                } else {
                    None
                };
                let schema_name = if idents.len() >= 2 {
                    Some(idents[idents.len() - 2].as_str())
                } else {
                    None
                };
                let table_name = idents.last().unwrap();

                if let Ok(columns) = control_plane
                    .relation_columns(session, db_name, schema_name, table_name)
                    .await
                {
                    let col_names = columns.into_iter().map(|c| c.name).collect::<Vec<_>>();
                    schemas.insert(effective_alias, col_names);
                }
            }
            TableFactor::Derived { subquery, .. } => {
                rewrite_query_recursive(subquery, control_plane, session).await?;
            }
            _ => {}
        }
        Ok(())
    })
}

fn make_unique_alias(base: &str, seen: &HashSet<String>) -> String {
    let base = base.split('.').next_back().unwrap_or(base);
    let safe_base = if !base.is_empty()
        && base
            .chars()
            .next()
            .map(|c| c.is_ascii_alphabetic() || c == '_')
            .unwrap_or(false)
    {
        base.to_string()
    } else {
        format!(
            "col_{}",
            base.replace(|c: char| !c.is_ascii_alphanumeric(), "_")
        )
    };

    let mut i = 1;
    let mut name = format!("{}_{}", safe_base, i);
    while seen.contains(&name) {
        i += 1;
        name = format!("{}_{}", safe_base, i);
    }
    name
}

fn rewrite_expr_recursive<'a>(
    expr: &'a mut Expr,
    control_plane: &'a ControlPlane,
    session: &'a SessionContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        match expr {
            Expr::Subquery(query) => rewrite_query_recursive(query, control_plane, session).await?,
            Expr::Exists { subquery, .. } => {
                rewrite_query_recursive(subquery, control_plane, session).await?
            }
            Expr::InSubquery {
                expr: inner,
                subquery,
                ..
            } => {
                rewrite_expr_recursive(inner, control_plane, session).await?;
                rewrite_query_recursive(subquery, control_plane, session).await?;
            }
            Expr::Cast {
                expr: inner,
                data_type,
                ..
            } => {
                rewrite_expr_recursive(inner, control_plane, session).await?;
                let type_name = data_type.to_string().to_uppercase();
                if type_name == "REGCLASS" {
                    // JDBC often uses 'table_name'::regclass. We rewrite to OID.
                    // If the inner is a string literal, we can hash it and REPLACE the whole cast.
                    match inner.as_mut() {
                        Expr::Value(v) => match &mut v.value {
                            sqlparser::ast::Value::SingleQuotedString(s) => {
                                let oid = synthetic_relation_oid_from_name(s);
                                *expr = Expr::Value(
                                    sqlparser::ast::Value::Number(oid.to_string(), false).into(),
                                );
                                return Ok(());
                            }
                            _ => {
                                *data_type = sqlparser::ast::DataType::Text;
                            }
                        },
                        _ => {
                            // General fallback to TEXT if we can't OID it
                            *data_type = sqlparser::ast::DataType::Text;
                        }
                    }
                }
            }
            Expr::Identifier(_) => {}
            Expr::CompoundIdentifier(_) => {}
            Expr::BinaryOp { left, op, right } => {
                rewrite_expr_recursive(left, control_plane, session).await?;
                rewrite_expr_recursive(right, control_plane, session).await?;

                if let sqlparser::ast::BinaryOperator::Multiply = op {
                    let left_is_interval = matches!(**left, Expr::Interval(_));
                    let right_is_interval = matches!(**right, Expr::Interval(_));

                    if left_is_interval || right_is_interval {
                        let (interval_expr, factor_expr) = if left_is_interval {
                            (left.clone(), right.clone())
                        } else {
                            (right.clone(), left.clone())
                        };

                        *expr = Expr::Function(sqlparser::ast::Function {
                            name: sqlparser::ast::ObjectName(vec![
                                sqlparser::ast::ObjectNamePart::Identifier(Ident::new(
                                    "multiply_interval",
                                )),
                            ]),
                            uses_odbc_syntax: false,
                            parameters: sqlparser::ast::FunctionArguments::None,
                            args: sqlparser::ast::FunctionArguments::List(
                                sqlparser::ast::FunctionArgumentList {
                                    args: vec![
                                        sqlparser::ast::FunctionArg::Unnamed(
                                            sqlparser::ast::FunctionArgExpr::Expr(*interval_expr),
                                        ),
                                        sqlparser::ast::FunctionArg::Unnamed(
                                            sqlparser::ast::FunctionArgExpr::Expr(*factor_expr),
                                        ),
                                    ],
                                    duplicate_treatment: None,
                                    clauses: Vec::new(),
                                },
                            ),
                            over: None,
                            filter: None,
                            null_treatment: None,
                            within_group: Vec::new(),
                        });
                    }
                } else if let sqlparser::ast::BinaryOperator::Divide = op {
                    let left_is_interval = matches!(**left, Expr::Interval(_));

                    if left_is_interval {
                        let (interval_expr, factor_expr) = (left.clone(), right.clone());

                        *expr = Expr::Function(sqlparser::ast::Function {
                            name: sqlparser::ast::ObjectName(vec![
                                sqlparser::ast::ObjectNamePart::Identifier(Ident::new(
                                    "divide_interval",
                                )),
                            ]),
                            uses_odbc_syntax: false,
                            parameters: sqlparser::ast::FunctionArguments::None,
                            args: sqlparser::ast::FunctionArguments::List(
                                sqlparser::ast::FunctionArgumentList {
                                    args: vec![
                                        sqlparser::ast::FunctionArg::Unnamed(
                                            sqlparser::ast::FunctionArgExpr::Expr(*interval_expr),
                                        ),
                                        sqlparser::ast::FunctionArg::Unnamed(
                                            sqlparser::ast::FunctionArgExpr::Expr(*factor_expr),
                                        ),
                                    ],
                                    duplicate_treatment: None,
                                    clauses: Vec::new(),
                                },
                            ),
                            over: None,
                            filter: None,
                            null_treatment: None,
                            within_group: Vec::new(),
                        });
                    }
                }
            }
            Expr::UnaryOp { expr: inner, .. } => {
                rewrite_expr_recursive(inner, control_plane, session).await?;
            }
            Expr::Nested(inner) => {
                rewrite_expr_recursive(inner, control_plane, session).await?;
            }
            Expr::Function(f) => {
                // Strip pg_catalog. prefix from functions for easier resolution
                if f.name.0.len() == 2 && f.name.0[0].to_string().to_lowercase() == "pg_catalog" {
                    f.name.0.remove(0);
                }

                if let sqlparser::ast::FunctionArguments::List(arg_list) = &mut f.args {
                    for arg in &mut arg_list.args {
                        match arg {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                                rewrite_expr_recursive(e, control_plane, session).await?;
                            }
                            FunctionArg::Named {
                                arg: FunctionArgExpr::Expr(e),
                                ..
                            } => {
                                rewrite_expr_recursive(e, control_plane, session).await?;
                            }
                            _ => {}
                        }
                    }
                }
            }
            Expr::InList {
                expr: inner, list, ..
            } => {
                rewrite_expr_recursive(inner, control_plane, session).await?;
                for e in list {
                    rewrite_expr_recursive(e, control_plane, session).await?;
                }
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(o) = operand {
                    rewrite_expr_recursive(o, control_plane, session).await?;
                }
                for condition in conditions {
                    rewrite_expr_recursive(&mut condition.condition, control_plane, session)
                        .await?;
                    rewrite_expr_recursive(&mut condition.result, control_plane, session).await?;
                }
                if let Some(e) = else_result {
                    rewrite_expr_recursive(e, control_plane, session).await?;
                }
            }
            _ => {}
        }
        Ok(())
    })
}

fn get_canonical_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(parts) => parts
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join("."),
        _ => expr.to_string(),
    }
}

fn synthetic_relation_oid_from_name(name: &str) -> u32 {
    let lower_name = name.to_lowercase();
    let parts: Vec<&str> = lower_name.split('.').collect();

    // If unqualified and starts with pg_, assume postgres.pg_catalog
    let qualified_name = if parts.len() == 1 && parts[0].starts_with("pg_") {
        format!("postgres.pg_catalog.{}", parts[0])
    } else if parts.len() == 2 && parts[0].starts_with("pg_") {
        format!("postgres.{}", lower_name)
    } else {
        lower_name
    };

    let mut hash = 2166136261_u32;
    for byte in qualified_name.bytes() {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }
    if hash < 16384 {
        hash + 16384
    } else {
        hash
    }
}
