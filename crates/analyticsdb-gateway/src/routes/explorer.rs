//! Explorer routes - live metadata browsing backed by the catalog.

use std::sync::Arc;

use axum::{
    extract::{Extension, Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use analyticsdb_control::CatalogRelationKind;

use crate::error::{GatewayError, GatewayResult};
use crate::session::SessionClaims;
use crate::GatewayState;

#[derive(Debug, Deserialize)]
pub struct ExplorerQuery {
    pub database: Option<String>,
    pub schema: Option<String>,
}

// The shapes below mirror the admin console's `domain.ts` types exactly so the
// JSON deserializes directly on the client.
#[derive(Debug, Serialize)]
pub struct ExplorerSnapshot {
    #[serde(rename = "generatedAt")]
    pub generated_at: String,
    pub databases: Vec<DatabaseInfo>,
}

#[derive(Debug, Serialize)]
pub struct DatabaseInfo {
    pub name: String,
    pub owner: String,
    pub schemas: Vec<SchemaInfo>,
}

#[derive(Debug, Serialize)]
pub struct SchemaInfo {
    pub database: String,
    pub name: String,
    pub relations: Vec<RelationInfo>,
}

#[derive(Debug, Serialize)]
pub struct RelationInfo {
    pub database: String,
    pub schema: String,
    pub name: String,
    pub kind: String,
    pub storage: String,
    pub columns: Vec<ColumnInfo>,
    pub description: String,
}

#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    pub nullable: bool,
}

const SYSTEM_SCHEMAS: [&str; 2] = ["pg_catalog", "information_schema"];

/// Build the full catalog snapshot for the explorer tree.
pub async fn get_explorer_snapshot(
    Extension(_claims): Extension<SessionClaims>,
    State(state): State<Arc<GatewayState>>,
) -> GatewayResult<Json<ExplorerSnapshot>> {
    let control_plane = state.open_catalog().await.map_err(GatewayError::Internal)?;
    let snapshot = control_plane.cluster_snapshot().await;

    let databases = snapshot
        .databases
        .iter()
        .map(|database| {
            let schemas = database
                .schemas
                .iter()
                .map(|schema_name| {
                    let relations = snapshot
                        .relations
                        .iter()
                        .filter(|relation| {
                            relation.database == database.name && &relation.schema == schema_name
                        })
                        .map(|relation| {
                            let kind = match relation.kind {
                                CatalogRelationKind::View => "view",
                                CatalogRelationKind::Table => "table",
                            };
                            let storage = if SYSTEM_SCHEMAS.contains(&schema_name.as_str()) {
                                "system"
                            } else if relation.external_format.is_some() {
                                "external"
                            } else {
                                "managed"
                            };
                            RelationInfo {
                                database: relation.database.clone(),
                                schema: relation.schema.clone(),
                                name: relation.name.clone(),
                                kind: kind.to_string(),
                                storage: storage.to_string(),
                                description: format!("{kind} in {schema_name}"),
                                columns: relation
                                    .columns
                                    .iter()
                                    .map(|column| ColumnInfo {
                                        name: column.name.clone(),
                                        data_type: column.data_type.clone(),
                                        nullable: column.nullable,
                                    })
                                    .collect(),
                            }
                        })
                        .collect();
                    SchemaInfo {
                        database: database.name.clone(),
                        name: schema_name.clone(),
                        relations,
                    }
                })
                .collect();
            DatabaseInfo {
                name: database.name.clone(),
                owner: database.owner.clone(),
                schemas,
            }
        })
        .collect();

    Ok(Json(ExplorerSnapshot {
        generated_at: format!("v{}", snapshot.catalogue_version),
        databases,
    }))
}

/// List databases
pub async fn list_databases(
    State(_state): State<std::sync::Arc<crate::GatewayState>>,
) -> GatewayResult<Json<Vec<serde_json::Value>>> {
    let result = vec![
        json!({ "name": "default", "owner": "admin" }),
    ];
    Ok(Json(result))
}

/// List schemas
pub async fn list_schemas(
    Query(_query): Query<ExplorerQuery>,
    State(_state): State<std::sync::Arc<crate::GatewayState>>,
) -> GatewayResult<Json<Vec<serde_json::Value>>> {
    let result = vec![
        json!({ "name": "public" }),
    ];
    Ok(Json(result))
}

/// List tables
pub async fn list_tables(
    Query(_query): Query<ExplorerQuery>,
    State(_state): State<std::sync::Arc<crate::GatewayState>>,
) -> GatewayResult<Json<Vec<serde_json::Value>>> {
    let result = vec![
        json!({ "name": "sample_table", "schema": "public" }),
    ];
    Ok(Json(result))
}

/// List views
pub async fn list_views(
    Query(_query): Query<ExplorerQuery>,
    State(_state): State<std::sync::Arc<crate::GatewayState>>,
) -> GatewayResult<Json<Vec<serde_json::Value>>> {
    let result: Vec<serde_json::Value> = vec![];
    Ok(Json(result))
}

/// List columns for a table
pub async fn list_columns(
    Query(_query): Query<ExplorerQuery>,
    State(_state): State<std::sync::Arc<crate::GatewayState>>,
) -> GatewayResult<Json<Vec<serde_json::Value>>> {
    let result = vec![
        json!({ "name": "id", "data_type": "INTEGER", "nullable": false }),
        json!({ "name": "name", "data_type": "TEXT", "nullable": true }),
    ];
    Ok(Json(result))
}
