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
    let mut statements = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return Ok(sql.to_string()),
    };

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
                    let dummy_schemas = HashMap::new();
                    rewrite_expr_recursive(
                        &mut order_by_expr.expr,
                        control_plane,
                        session,
                        &dummy_schemas,
                    )
                    .await?;
                }
            }
        }

        if let Some(limit_clause) = &mut query.limit_clause {
            let dummy_schemas = HashMap::new();
            match limit_clause {
                sqlparser::ast::LimitClause::LimitOffset {
                    limit,
                    offset,
                    limit_by,
                } => {
                    if let Some(limit_expr) = limit {
                        rewrite_expr_recursive(limit_expr, control_plane, session, &dummy_schemas)
                            .await?;
                    }
                    if let Some(offset_struct) = offset {
                        rewrite_expr_recursive(
                            &mut offset_struct.value,
                            control_plane,
                            session,
                            &dummy_schemas,
                        )
                        .await?;
                    }
                    for e in limit_by {
                        rewrite_expr_recursive(e, control_plane, session, &dummy_schemas).await?;
                    }
                }
                sqlparser::ast::LimitClause::OffsetCommaLimit { offset, limit } => {
                    rewrite_expr_recursive(offset, control_plane, session, &dummy_schemas).await?;
                    rewrite_expr_recursive(limit, control_plane, session, &dummy_schemas).await?;
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
                            rewrite_expr_recursive(expr, control_plane, session, &table_schemas)
                                .await?;
                        }
                    }
                    _ => {}
                }
            }
        }

        let mut new_projection = Vec::new();
        for item in select.projection.iter() {
            match item {
                SelectItem::Wildcard(_) => {
                    for (alias, columns) in table_schemas.iter() {
                        for (col, default_val) in columns {
                            if col != "_row_id" {
                                let ident_expr = if table_schemas.len() > 1 {
                                    Expr::CompoundIdentifier(vec![
                                        Ident::new(alias.clone()),
                                        Ident::new(col.clone()),
                                    ])
                                } else {
                                    Expr::Identifier(Ident::new(col.clone()))
                                };

                                let expr = if let Some(dv) = default_val {
                                    wrap_with_coalesce(ident_expr, dv)
                                } else {
                                    ident_expr
                                };

                                new_projection.push(SelectItem::ExprWithAlias {
                                    expr,
                                    alias: Ident::new(col.clone()),
                                });
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
                        for (col, default_val) in columns {
                            if col != "_row_id" {
                                let ident_expr = Expr::CompoundIdentifier(vec![
                                    Ident::new(alias.clone()),
                                    Ident::new(col.clone()),
                                ]);

                                let expr = if let Some(dv) = default_val {
                                    wrap_with_coalesce(ident_expr, dv)
                                } else {
                                    ident_expr
                                };

                                new_projection.push(SelectItem::ExprWithAlias {
                                    expr,
                                    alias: Ident::new(col.clone()),
                                });
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

        let mut seen_names = HashSet::new();
        for item in select.projection.iter_mut() {
            // 1. Rewrite and potentially alias if rewritten
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let original_name = get_canonical_name(expr);
                    let old_expr_str = expr.to_string();
                    rewrite_expr_recursive(expr, control_plane, session, &table_schemas).await?;
                    if expr.to_string() != old_expr_str && is_safe_bare_identifier(&original_name) {
                        *item = SelectItem::ExprWithAlias {
                            expr: expr.clone(),
                            alias: Ident::new(original_name),
                        };
                    }
                }
                SelectItem::ExprWithAlias { expr, .. } => {
                    rewrite_expr_recursive(expr, control_plane, session, &table_schemas).await?;
                }
                _ => {}
            }

            // 2. Determine current name and uniquely alias if needed
            let current_name = match item {
                SelectItem::UnnamedExpr(expr) => get_canonical_name(expr),
                SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                _ => continue,
            };

            if seen_names.contains(&current_name) {
                let unique_name = make_unique_alias(&current_name, &seen_names);
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        *item = SelectItem::ExprWithAlias {
                            expr: expr.clone(),
                            alias: Ident::new(unique_name.clone()),
                        };
                    }
                    SelectItem::ExprWithAlias { alias, .. } => {
                        *alias = Ident::new(unique_name.clone());
                    }
                    _ => unreachable!(),
                }
                seen_names.insert(unique_name);
            } else {
                seen_names.insert(current_name);
            }
        }

        if let Some(selection) = &mut select.selection {
            rewrite_expr_recursive(selection, control_plane, session, &table_schemas).await?;
        }

        if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &mut select.group_by {
            for expr in exprs {
                rewrite_expr_recursive(expr, control_plane, session, &table_schemas).await?;
            }
        }

        if let Some(having) = &mut select.having {
            rewrite_expr_recursive(having, control_plane, session, &table_schemas).await?;
        }

        Ok(())
    })
}

fn resolve_table_schemas_recursive<'a>(
    tf: &'a mut TableFactor,
    control_plane: &'a ControlPlane,
    session: &'a SessionContext,
    schemas: &'a mut HashMap<String, Vec<(String, Option<String>)>>,
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

                let matched_pg_catalog = match idents.as_slice() {
                    [s, n] if s == "pg_catalog" => Some(n.as_str()),
                    [n] if n.starts_with("pg_") => {
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
                                    ("oid".to_string(), None),
                                    ("datname".to_string(), None),
                                    ("datdba".to_string(), None),
                                    ("encoding".to_string(), None),
                                    ("datcollate".to_string(), None),
                                    ("datctype".to_string(), None),
                                    ("datistemplate".to_string(), None),
                                    ("datallowconn".to_string(), None),
                                    ("datconnlimit".to_string(), None),
                                    ("datlastsysoid".to_string(), None),
                                    ("datfrozenxid".to_string(), None),
                                    ("datminmxid".to_string(), None),
                                    ("dattablespace".to_string(), None),
                                    ("datacl".to_string(), None),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_namespace" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    ("oid".to_string(), None),
                                    ("nspname".to_string(), None),
                                    ("nspowner".to_string(), None),
                                    ("nspacl".to_string(), None),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_tables" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    ("schemaname".to_string(), None),
                                    ("tablename".to_string(), None),
                                    ("tableowner".to_string(), None),
                                    ("tablespace".to_string(), None),
                                    ("hasindexes".to_string(), None),
                                    ("hasrules".to_string(), None),
                                    ("hastriggers".to_string(), None),
                                    ("rowsecurity".to_string(), None),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_views" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    ("schemaname".to_string(), None),
                                    ("viewname".to_string(), None),
                                    ("viewowner".to_string(), None),
                                    ("definition".to_string(), None),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_roles" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    ("oid".to_string(), None),
                                    ("rolname".to_string(), None),
                                    ("rolsuper".to_string(), None),
                                    ("rolinherit".to_string(), None),
                                    ("rolcreaterole".to_string(), None),
                                    ("rolcreatedb".to_string(), None),
                                    ("rolcanlogin".to_string(), None),
                                    ("rolreplication".to_string(), None),
                                    ("rolbypassrls".to_string(), None),
                                    ("rolconnlimit".to_string(), None),
                                    ("rolpassword".to_string(), None),
                                    ("rolvaliduntil".to_string(), None),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_type" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    ("oid".to_string(), None),
                                    ("typname".to_string(), None),
                                    ("typnamespace".to_string(), None),
                                    ("typowner".to_string(), None),
                                    ("typlen".to_string(), None),
                                    ("typbyval".to_string(), None),
                                    ("typtype".to_string(), None),
                                    ("typcategory".to_string(), None),
                                    ("typispreferred".to_string(), None),
                                    ("typisdefined".to_string(), None),
                                    ("typdelim".to_string(), None),
                                    ("typrelid".to_string(), None),
                                    ("typelem".to_string(), None),
                                    ("typarray".to_string(), None),
                                    ("typinput".to_string(), None),
                                    ("typbasetype".to_string(), None),
                                    ("typtypmod".to_string(), None),
                                    ("typndims".to_string(), None),
                                    ("typcollation".to_string(), None),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_class" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    ("oid".to_string(), None),
                                    ("relname".to_string(), None),
                                    ("relnamespace".to_string(), None),
                                    ("reltype".to_string(), None),
                                    ("reloftype".to_string(), None),
                                    ("relowner".to_string(), None),
                                    ("relam".to_string(), None),
                                    ("relfilenode".to_string(), None),
                                    ("reltablespace".to_string(), None),
                                    ("relpages".to_string(), None),
                                    ("reltuples".to_string(), None),
                                    ("relallvisible".to_string(), None),
                                    ("reltoastrelid".to_string(), None),
                                    ("relhasindex".to_string(), None),
                                    ("relisshared".to_string(), None),
                                    ("relpersistence".to_string(), None),
                                    ("relkind".to_string(), None),
                                    ("relnatts".to_string(), None),
                                    ("relchecks".to_string(), None),
                                    ("relhasrules".to_string(), None),
                                    ("relhastriggers".to_string(), None),
                                    ("relhassubclass".to_string(), None),
                                    ("relrowsecurity".to_string(), None),
                                    ("relforcerowsecurity".to_string(), None),
                                    ("relispartition".to_string(), None),
                                    ("relrewrite".to_string(), None),
                                    ("relfrozenxid".to_string(), None),
                                    ("relminmxid".to_string(), None),
                                    ("relacl".to_string(), None),
                                    ("reloptions".to_string(), None),
                                    ("relpartbound".to_string(), None),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_attribute" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    ("attrelid".to_string(), None),
                                    ("attname".to_string(), None),
                                    ("atttypid".to_string(), None),
                                    ("attnum".to_string(), None),
                                    ("attnotnull".to_string(), None),
                                    ("atttypmod".to_string(), None),
                                    ("attndims".to_string(), None),
                                    ("atthasdef".to_string(), None),
                                    ("attidentity".to_string(), None),
                                    ("attgenerated".to_string(), None),
                                    ("attisdropped".to_string(), None),
                                    ("attislocal".to_string(), None),
                                    ("attinhcount".to_string(), None),
                                    ("attcollation".to_string(), None),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_description" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    ("objoid".to_string(), None),
                                    ("classoid".to_string(), None),
                                    ("objsubid".to_string(), None),
                                    ("description".to_string(), None),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_attrdef" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    ("oid".to_string(), None),
                                    ("adrelid".to_string(), None),
                                    ("adnum".to_string(), None),
                                    ("adbin".to_string(), None),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_depend" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    ("classid".to_string(), None),
                                    ("objid".to_string(), None),
                                    ("objsubid".to_string(), None),
                                    ("refclassid".to_string(), None),
                                    ("refobjid".to_string(), None),
                                    ("refobjsubid".to_string(), None),
                                    ("deptype".to_string(), None),
                                ],
                            );
                            return Ok(());
                        }
                        "pg_constraint" => {
                            schemas.insert(
                                effective_alias,
                                vec![
                                    ("oid".to_string(), None),
                                    ("conname".to_string(), None),
                                    ("connamespace".to_string(), None),
                                    ("contype".to_string(), None),
                                    ("condeferrable".to_string(), None),
                                    ("condeferred".to_string(), None),
                                    ("convalidated".to_string(), None),
                                    ("conrelid".to_string(), None),
                                    ("contypid".to_string(), None),
                                    ("conindid".to_string(), None),
                                    ("confrelid".to_string(), None),
                                    ("confupdtype".to_string(), None),
                                    ("confdeltype".to_string(), None),
                                    ("confmatchtype".to_string(), None),
                                    ("conislocal".to_string(), None),
                                    ("coninhcount".to_string(), None),
                                    ("connoinherit".to_string(), None),
                                    ("conkey".to_string(), None),
                                    ("confkey".to_string(), None),
                                ],
                            );
                            return Ok(());
                        }
                        _ => {}
                    }
                }

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

                if let Ok(relation) = control_plane
                    .find_relation(session, db_name, schema_name, table_name)
                    .await
                {
                    let col_info = relation
                        .columns
                        .iter()
                        .map(|c| (c.name.clone(), c.default_value.clone()))
                        .collect::<Vec<_>>();
                    schemas.insert(effective_alias.clone(), col_info);

                    if relation.kind == analyticsdb_control::CatalogRelationKind::View {
                        let Some(definition_sql) = relation.definition_sql else {
                            return Ok(());
                        };
                        let dialect = PostgreSqlDialect {};
                        let mut statements = Parser::parse_sql(&dialect, &definition_sql)?;
                        if statements.len() != 1 {
                            return Ok(());
                        }
                        let Statement::Query(mut subquery) = statements.remove(0) else {
                            return Ok(());
                        };
                        rewrite_query_recursive(&mut subquery, control_plane, session).await?;

                        let derived_alias = alias.clone().or_else(|| {
                            Some(sqlparser::ast::TableAlias {
                                explicit: false,
                                name: Ident::new(table_name.clone()),
                                columns: Vec::new(),
                            })
                        });
                        *tf = TableFactor::Derived {
                            lateral: false,
                            subquery,
                            alias: derived_alias,
                            sample: None,
                        };
                    }
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

fn is_safe_bare_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn make_unique_alias(base: &str, seen: &HashSet<String>) -> String {
    let mut safe_base = base.replace(|c: char| !c.is_ascii_alphanumeric(), "_");

    if safe_base.is_empty()
        || (!safe_base.chars().next().unwrap().is_ascii_alphabetic() && !safe_base.starts_with('_'))
    {
        safe_base = format!("col_{}", safe_base);
    }

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
    table_schemas: &'a HashMap<String, Vec<(String, Option<String>)>>,
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
                rewrite_expr_recursive(inner, control_plane, session, table_schemas).await?;
                rewrite_query_recursive(subquery, control_plane, session).await?;
            }
            Expr::Cast {
                expr: inner,
                data_type,
                ..
            } => {
                rewrite_expr_recursive(inner, control_plane, session, table_schemas).await?;
                let type_name = data_type.to_string().to_uppercase();
                if type_name == "REGCLASS" {
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
                            *data_type = sqlparser::ast::DataType::Text;
                        }
                    }
                }
            }
            Expr::Identifier(ident) => {
                let col_name = ident.value.clone();
                for columns in table_schemas.values() {
                    if let Some((_, Some(default_val))) =
                        columns.iter().find(|(c, _)| c == &col_name)
                    {
                        *expr = wrap_with_coalesce(expr.clone(), default_val);
                        return Ok(());
                    }
                }
            }
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                let alias = parts[parts.len() - 2].to_string();
                let col_name = parts.last().unwrap().to_string();
                if let Some(columns) = table_schemas.get(&alias) {
                    if let Some((_, Some(default_val))) =
                        columns.iter().find(|(c, _)| c == &col_name)
                    {
                        *expr = wrap_with_coalesce(expr.clone(), default_val);
                        return Ok(());
                    }
                }
            }
            Expr::BinaryOp { left, op, right } => {
                rewrite_expr_recursive(left, control_plane, session, table_schemas).await?;
                rewrite_expr_recursive(right, control_plane, session, table_schemas).await?;

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
                rewrite_expr_recursive(inner, control_plane, session, table_schemas).await?;
            }
            Expr::Nested(inner) => {
                rewrite_expr_recursive(inner, control_plane, session, table_schemas).await?;
            }
            Expr::Function(f) => {
                if f.name.0.len() == 2 && f.name.0[0].to_string().to_lowercase() == "pg_catalog" {
                    f.name.0.remove(0);
                }

                if let sqlparser::ast::FunctionArguments::List(arg_list) = &mut f.args {
                    for arg in &mut arg_list.args {
                        match arg {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                                rewrite_expr_recursive(e, control_plane, session, table_schemas)
                                    .await?;
                            }
                            FunctionArg::Named {
                                arg: FunctionArgExpr::Expr(e),
                                ..
                            } => {
                                rewrite_expr_recursive(e, control_plane, session, table_schemas)
                                    .await?;
                            }
                            _ => {}
                        }
                    }
                }
            }
            Expr::InList {
                expr: inner, list, ..
            } => {
                rewrite_expr_recursive(inner, control_plane, session, table_schemas).await?;
                for e in list {
                    rewrite_expr_recursive(e, control_plane, session, table_schemas).await?;
                }
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(o) = operand {
                    rewrite_expr_recursive(o, control_plane, session, table_schemas).await?;
                }
                for condition in conditions {
                    rewrite_expr_recursive(
                        &mut condition.condition,
                        control_plane,
                        session,
                        table_schemas,
                    )
                    .await?;
                    rewrite_expr_recursive(
                        &mut condition.result,
                        control_plane,
                        session,
                        table_schemas,
                    )
                    .await?;
                }
                if let Some(e) = else_result {
                    rewrite_expr_recursive(e, control_plane, session, table_schemas).await?;
                }
            }
            _ => {}
        }
        Ok(())
    })
}

fn wrap_with_coalesce(expr: Expr, default_val: &str) -> Expr {
    let dialect = PostgreSqlDialect {};
    let default_expr = Parser::parse_sql(&dialect, &format!("SELECT {}", default_val))
        .ok()
        .and_then(|mut stmts| {
            if stmts.len() == 1 {
                if let Statement::Query(q) = stmts.remove(0) {
                    if let SetExpr::Select(s) = *q.body {
                        if s.projection.len() == 1 {
                            if let SelectItem::UnnamedExpr(e) = s.projection[0].clone() {
                                return Some(e);
                            }
                        }
                    }
                }
            }
            None
        })
        .unwrap_or(Expr::Value(
            sqlparser::ast::Value::SingleQuotedString(default_val.to_string()).into(),
        ));

    Expr::Function(sqlparser::ast::Function {
        name: sqlparser::ast::ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("COALESCE"),
        )]),
        uses_odbc_syntax: false,
        parameters: sqlparser::ast::FunctionArguments::None,
        args: sqlparser::ast::FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
            args: vec![
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(expr)),
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                    default_expr,
                )),
            ],
            duplicate_treatment: None,
            clauses: Vec::new(),
        }),
        over: None,
        filter: None,
        null_treatment: None,
        within_group: Vec::new(),
    })
}

fn get_canonical_name(expr: &Expr) -> String {
    let name = match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|i| i.to_string())
            .unwrap_or_else(|| expr.to_string()),
        _ => expr.to_string(),
    };
    name.to_lowercase()
}

fn synthetic_relation_oid_from_name(name: &str) -> u32 {
    let lower_name = name.to_lowercase();
    let parts: Vec<&str> = lower_name.split('.').collect();

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
