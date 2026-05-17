//! Explorer routes - live metadata browsing

use axum::{
    extract::{Extension, Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::session::SessionClaims;
use crate::error::GatewayResult;

#[derive(Debug, Deserialize)]
pub struct ExplorerQuery {
    pub database: Option<String>,
    pub schema: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ExplorerSnapshot {
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
    pub name: String,
    pub relations: Vec<RelationInfo>,
}

#[derive(Debug, Serialize)]
pub struct RelationInfo {
    pub name: String,
    pub kind: String,
    pub schema: String,
    pub storage: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Get full explorer snapshot
pub async fn get_explorer_snapshot(
    Extension(_claims): Extension<SessionClaims>,
    State(_state): State<std::sync::Arc<crate::GatewayState>>,
) -> GatewayResult<Json<ExplorerSnapshot>> {
    // Placeholder implementation
    let snapshot = ExplorerSnapshot {
        databases: vec![
            DatabaseInfo {
                name: "default".to_string(),
                owner: "admin".to_string(),
                schemas: vec![
                    SchemaInfo {
                        name: "public".to_string(),
                        relations: vec![
                            RelationInfo {
                                name: "sample_table".to_string(),
                                kind: "table".to_string(),
                                schema: "public".to_string(),
                                storage: "managed".to_string(),
                                columns: vec![
                                    ColumnInfo {
                                        name: "id".to_string(),
                                        data_type: "INTEGER".to_string(),
                                        nullable: false,
                                    },
                                    ColumnInfo {
                                        name: "name".to_string(),
                                        data_type: "TEXT".to_string(),
                                        nullable: true,
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
        ],
    };

    Ok(Json(snapshot))
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
