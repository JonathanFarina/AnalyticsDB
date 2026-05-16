use super::*;

impl PrototypeEngine {
    pub(super) async fn information_schema_schemata_rows(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let schemas = self.control_plane.list_schemas(session, None).await?;
        Ok(schemas
            .into_iter()
            .map(|s| {
                vec![
                    session.database.clone(),
                    s,
                    session.user.clone(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                ]
            })
            .collect())
    }

    pub(super) async fn information_schema_tables_rows(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;

        let mut rows = Vec::new();
        for rel in relations {
            let (table_type, is_insertable) = match rel.kind {
                CatalogRelationKind::Table => ("BASE TABLE", "YES"),
                CatalogRelationKind::View => ("VIEW", "NO"),
            };
            rows.push(vec![
                rel.database,
                rel.schema,
                rel.name,
                table_type.to_string(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                is_insertable.to_string(),
                "NO".to_string(),
                String::new(),
            ]);
        }
        Ok(rows)
    }

    pub(super) async fn information_schema_columns_rows(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;
        let mut rows = Vec::new();
        for rel in relations {
            for (i, col) in rel
                .columns
                .into_iter()
                .filter(|column| column.name != "_row_id")
                .enumerate()
            {
                rows.push(vec![
                    rel.database.clone(),
                    rel.schema.clone(),
                    rel.name.clone(),
                    col.name,
                    (i + 1).to_string(),
                    col.default_value.unwrap_or_default(),
                    if col.nullable {
                        "YES".to_string()
                    } else {
                        "NO".to_string()
                    },
                    col.data_type.to_ascii_lowercase(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                ]);
            }
        }
        Ok(rows)
    }

    pub(super) async fn information_schema_views_rows(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let views = self
            .control_plane
            .list_all_relations(session)
            .await?
            .into_iter()
            .filter(|relation| relation.kind == CatalogRelationKind::View)
            .collect::<Vec<_>>();
        Ok(views
            .into_iter()
            .map(|v| {
                vec![
                    v.database,
                    v.schema,
                    v.name,
                    v.definition_sql.unwrap_or_default(),
                    "NONE".to_string(),
                    "NO".to_string(),
                    "NO".to_string(),
                    "NO".to_string(),
                    "NO".to_string(),
                    "NO".to_string(),
                ]
            })
            .collect())
    }

    pub(super) async fn information_schema_table_constraints_rows(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;
        let mut rows = Vec::new();
        for rel in relations {
            for constraint in rel.constraints {
                rows.push(vec![
                    rel.database.clone(),
                    rel.schema.clone(),
                    constraint.name.clone(),
                    rel.database.clone(),
                    rel.schema.clone(),
                    rel.name.clone(),
                    format!("{:?}", constraint.kind).to_ascii_uppercase(),
                    "NO".to_string(),
                    "NO".to_string(),
                    "YES".to_string(),
                    String::new(),
                ]);
            }
            // Add NOT NULL constraints
            for col in rel.columns {
                if !col.nullable {
                    let cname = format!("{}_{}_not_null", rel.name, col.name);
                    rows.push(vec![
                        rel.database.clone(),
                        rel.schema.clone(),
                        cname,
                        rel.database.clone(),
                        rel.schema.clone(),
                        rel.name.clone(),
                        "CHECK".to_string(),
                        "NO".to_string(),
                        "NO".to_string(),
                        "YES".to_string(),
                        String::new(),
                    ]);
                }
            }
        }
        Ok(rows)
    }

    pub(super) async fn information_schema_key_column_usage_rows(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;
        let mut rows = Vec::new();
        for rel in relations {
            for constraint in rel.constraints {
                if matches!(
                    constraint.kind,
                    CatalogTableConstraintKind::PrimaryKey
                        | CatalogTableConstraintKind::ForeignKey
                        | CatalogTableConstraintKind::Unique
                ) {
                    for (i, col) in constraint.columns.into_iter().enumerate() {
                        rows.push(vec![
                            rel.database.clone(),
                            rel.schema.clone(),
                            constraint.name.clone(),
                            rel.database.clone(),
                            rel.schema.clone(),
                            rel.name.clone(),
                            col,
                            (i + 1).to_string(),
                            String::new(),
                        ]);
                    }
                }
            }
        }
        Ok(rows)
    }

    pub(super) async fn information_schema_constraint_column_usage_rows(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;
        let mut rows = Vec::new();
        for rel in relations {
            // Include NOT NULL constraints
            for col in &rel.columns {
                if !col.nullable {
                    let cname = format!("{}_{}_not_null", rel.name, col.name);
                    rows.push(vec![
                        rel.database.clone(),
                        rel.schema.clone(),
                        rel.name.clone(),
                        col.name.clone(),
                        rel.database.clone(),
                        rel.schema.clone(),
                        cname,
                    ]);
                }
            }
            // Include explicit constraints
            for constraint in rel.constraints {
                for col in constraint.columns {
                    rows.push(vec![
                        rel.database.clone(),
                        rel.schema.clone(),
                        rel.name.clone(),
                        col,
                        rel.database.clone(),
                        rel.schema.clone(),
                        constraint.name.clone(),
                    ]);
                }
            }
        }
        Ok(rows)
    }

    pub(super) async fn information_schema_constraint_table_usage_rows(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;
        let mut rows = Vec::new();
        for rel in relations {
            // NOT NULLs
            for col in &rel.columns {
                if !col.nullable {
                    let cname = format!("{}_{}_not_null", rel.name, col.name);
                    rows.push(vec![
                        rel.database.clone(),
                        rel.schema.clone(),
                        rel.name.clone(),
                        rel.database.clone(),
                        rel.schema.clone(),
                        cname,
                    ]);
                }
            }
            for constraint in rel.constraints {
                rows.push(vec![
                    rel.database.clone(),
                    rel.schema.clone(),
                    rel.name.clone(),
                    rel.database.clone(),
                    rel.schema.clone(),
                    constraint.name.clone(),
                ]);
            }
        }
        Ok(rows)
    }

    pub(super) fn information_schema_routines_rows(&self) -> Vec<Vec<String>> {
        // Always empty - schema stub for client compatibility
        vec![]
    }

    pub(super) fn information_schema_parameters_rows(&self) -> Vec<Vec<String>> {
        // Always empty - schema stub for client compatibility
        vec![]
    }

    pub(super) fn information_schema_triggers_rows(&self) -> Vec<Vec<String>> {
        // Always empty - schema stub for client compatibility
        vec![]
    }

    pub(super) async fn information_schema_referential_constraints_rows(
        &self,
        session: &SessionContext,
    ) -> Result<Vec<Vec<String>>> {
        let relations = self.control_plane.list_all_relations(session).await?;
        let mut rows = Vec::new();
        for rel in &relations {
            for constraint in &rel.constraints {
                if let CatalogTableConstraintKind::ForeignKey = constraint.kind {
                    let referenced_database = constraint
                        .referenced_database
                        .clone()
                        .unwrap_or_else(|| session.database.clone());
                    let referenced_schema = constraint
                        .referenced_schema
                        .clone()
                        .unwrap_or_else(|| session.schema.clone());
                    let referenced_table = constraint.referenced_table.clone().unwrap_or_default();
                    let unique_constraint_name = relations
                        .iter()
                        .find(|candidate| {
                            candidate.database == referenced_database
                                && candidate.schema == referenced_schema
                                && candidate.name == referenced_table
                        })
                        .and_then(|candidate| {
                            candidate.constraints.iter().find(|candidate_constraint| {
                                matches!(
                                    candidate_constraint.kind,
                                    CatalogTableConstraintKind::PrimaryKey
                                        | CatalogTableConstraintKind::Unique
                                )
                            })
                        })
                        .map(|constraint| constraint.name.clone())
                        .unwrap_or_else(|| referenced_table.clone());
                    rows.push(vec![
                        rel.database.clone(),
                        rel.schema.clone(),
                        constraint.name.clone(),
                        referenced_database,
                        referenced_schema,
                        unique_constraint_name,
                        "MATCH SIMPLE".to_string(),
                        "NO ACTION".to_string(),
                        "NO ACTION".to_string(),
                    ]);
                }
            }
        }
        Ok(rows)
    }
}
