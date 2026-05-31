//! Admin routes — users and groups management.
//!
//! Reads (listing users/groups) come from a fresh view of the shared catalog,
//! reflecting the server's latest persisted state. Mutations are sent to the
//! server as DDL over pg-wire so the **server** applies them to its live
//! catalog — there is no separate writer in the gateway to diverge. The acting
//! user must be an effective administrator (a member of `Administrators`).

use analyticsdb_control::{ControlPlane, ADMINISTRATORS_GROUP};
use axum::{
    extract::{Extension, Path, State},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::error::{GatewayError, GatewayResult};
use crate::proxy::sql_literal;
use crate::session::SessionClaims;
use crate::GatewayState;

const DEFAULT_DATABASE: &str = "postgres";

#[derive(Debug, Deserialize)]
pub struct CreateDatabaseRequest {
    pub name: String,
    pub owner: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    pub name: String,
    pub password: String,
    /// Optional group memberships to grant on creation (e.g. `["Administrators"]`).
    #[serde(default)]
    pub groups: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateGroupRequest {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct GroupMemberRequest {
    pub user: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct ResetPasswordRequest {
    /// Explicit replacement password. When absent/empty, a random one is
    /// generated server-side and returned once.
    #[serde(default)]
    pub password: Option<String>,
}

/// Opens a fresh read-only catalog view and verifies the session user is an
/// effective administrator. Used by the listing endpoints.
async fn admin_read(
    state: &Arc<GatewayState>,
    claims: &SessionClaims,
) -> GatewayResult<ControlPlane> {
    let control_plane = state.open_catalog().await.map_err(GatewayError::Internal)?;
    if !control_plane.is_admin(&claims.sub).await {
        return Err(GatewayError::Forbidden);
    }
    Ok(control_plane)
}

/// Runs administrative DDL on the server over pg-wire as the session user. The
/// server enforces administrator authorization and applies the change to its
/// live catalog; any error (e.g. "User already exists") is surfaced to the UI.
async fn run_admin_sql(
    state: &Arc<GatewayState>,
    claims: &SessionClaims,
    statements: &[String],
) -> GatewayResult<()> {
    // Early, friendly rejection for non-admins (the server also enforces this).
    let control_plane = state.open_catalog().await.map_err(GatewayError::Internal)?;
    if !control_plane.is_admin(&claims.sub).await {
        return Err(GatewayError::Forbidden);
    }
    drop(control_plane);

    let password = state
        .session_store
        .password_for(&claims.session_id)
        .ok_or(GatewayError::Unauthorized)?;

    crate::proxy::execute_statements(
        &state.pg_endpoint(),
        &claims.sub,
        &password,
        DEFAULT_DATABASE,
        statements,
    )
    .await
    .map_err(|error| {
        // Surface the deepest cause (the db error) for a useful message.
        GatewayError::BadRequest(
            error
                .root_cause()
                .to_string()
                .trim_start_matches("db error: ")
                .trim_start_matches("ERROR: ")
                .to_string(),
        )
    })?;
    Ok(())
}

/// Lists all users (excludes group accounts), annotated with admin status and
/// group memberships.
pub async fn list_users(
    Extension(claims): Extension<SessionClaims>,
    State(state): State<Arc<GatewayState>>,
) -> GatewayResult<Json<Vec<serde_json::Value>>> {
    let control_plane = admin_read(&state, &claims).await?;
    let snapshot = control_plane.cluster_snapshot().await;

    // A "group" is an account with no password and at least the capacity to hold
    // members; we treat password-less accounts as groups.
    let groups: Vec<&analyticsdb_control::CatalogUser> = snapshot
        .users
        .iter()
        .filter(|u| u.password.is_none())
        .collect();

    let result = snapshot
        .users
        .iter()
        .filter(|u| u.password.is_some())
        .map(|u| {
            let memberships: Vec<String> = groups
                .iter()
                .filter(|g| g.members.contains(&u.name))
                .map(|g| g.name.clone())
                .collect();
            let is_admin = u.is_admin || memberships.iter().any(|g| g == ADMINISTRATORS_GROUP);
            json!({
                "name": u.name,
                "is_admin": is_admin,
                "groups": memberships,
                "password_version": u.password_version,
                "password_rotated_at_epoch_ms": u.password_rotated_at_epoch_ms,
            })
        })
        .collect();
    Ok(Json(result))
}

/// Creates a new user, optionally enrolling them into the requested groups.
pub async fn create_user(
    Extension(claims): Extension<SessionClaims>,
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<CreateUserRequest>,
) -> GatewayResult<Json<serde_json::Value>> {
    let mut statements = vec![format!(
        "CREATE USER {} PASSWORD {}",
        req.name,
        sql_literal(&req.password)
    )];
    for group in &req.groups {
        statements.push(format!("ALTER GROUP {} ADD USER {}", group, req.name));
    }
    run_admin_sql(&state, &claims, &statements).await?;
    Ok(Json(json!({ "message": format!("User '{}' created.", req.name) })))
}

/// Drops a user.
pub async fn drop_user(
    Extension(claims): Extension<SessionClaims>,
    State(state): State<Arc<GatewayState>>,
    Path(name): Path<String>,
) -> GatewayResult<Json<serde_json::Value>> {
    run_admin_sql(&state, &claims, &[format!("DROP USER {name}")]).await?;
    Ok(Json(json!({ "message": format!("User '{name}' dropped.") })))
}

/// Resets a user's password. If the request supplies a password it is used as
/// given; otherwise a strong random one is generated server-side. The plaintext
/// is returned only when it was generated (so the operator can record it once).
pub async fn reset_user_password(
    Extension(claims): Extension<SessionClaims>,
    State(state): State<Arc<GatewayState>>,
    Path(name): Path<String>,
    Json(req): Json<ResetPasswordRequest>,
) -> GatewayResult<Json<serde_json::Value>> {
    let (password, generated) = match req.password {
        Some(p) if !p.trim().is_empty() => (p, false),
        _ => (analyticsdb_control::generate_random_password(), true),
    };

    run_admin_sql(
        &state,
        &claims,
        &[format!("ALTER USER {name} PASSWORD {}", sql_literal(&password))],
    )
    .await?;

    let mut response = json!({
        "name": name,
        "generated": generated,
        "message": format!("Password for '{}' updated.", name),
    });
    if generated {
        response["password"] = json!(password);
        response["message"] =
            json!(format!("Password for '{}' reset. Store it now — it is shown once.", name));
    }
    Ok(Json(response))
}

/// Lists all groups with their members.
pub async fn list_groups(
    Extension(claims): Extension<SessionClaims>,
    State(state): State<Arc<GatewayState>>,
) -> GatewayResult<Json<Vec<serde_json::Value>>> {
    let control_plane = admin_read(&state, &claims).await?;
    let snapshot = control_plane.cluster_snapshot().await;
    let result = snapshot
        .users
        .iter()
        .filter(|u| u.password.is_none())
        .map(|g| {
            json!({
                "name": g.name,
                "members": g.members.iter().cloned().collect::<Vec<_>>(),
                "member_count": g.members.len(),
            })
        })
        .collect();
    Ok(Json(result))
}

/// Creates a new group.
pub async fn create_group(
    Extension(claims): Extension<SessionClaims>,
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<CreateGroupRequest>,
) -> GatewayResult<Json<serde_json::Value>> {
    run_admin_sql(&state, &claims, &[format!("CREATE GROUP {}", req.name)]).await?;
    Ok(Json(json!({ "message": format!("Group '{}' created.", req.name) })))
}

/// Drops a group.
pub async fn drop_group(
    Extension(claims): Extension<SessionClaims>,
    State(state): State<Arc<GatewayState>>,
    Path(name): Path<String>,
) -> GatewayResult<Json<serde_json::Value>> {
    run_admin_sql(&state, &claims, &[format!("DROP GROUP {name}")]).await?;
    Ok(Json(json!({ "message": format!("Group '{name}' dropped.") })))
}

/// Adds a member to a group.
pub async fn add_group_member(
    Extension(claims): Extension<SessionClaims>,
    State(state): State<Arc<GatewayState>>,
    Path(name): Path<String>,
    Json(req): Json<GroupMemberRequest>,
) -> GatewayResult<Json<serde_json::Value>> {
    run_admin_sql(
        &state,
        &claims,
        &[format!("ALTER GROUP {name} ADD USER {}", req.user)],
    )
    .await?;
    Ok(Json(json!({ "message": format!("User '{}' added to '{name}'.", req.user) })))
}

/// Removes a member from a group.
pub async fn remove_group_member(
    Extension(claims): Extension<SessionClaims>,
    State(state): State<Arc<GatewayState>>,
    Path((name, user)): Path<(String, String)>,
) -> GatewayResult<Json<serde_json::Value>> {
    run_admin_sql(
        &state,
        &claims,
        &[format!("ALTER GROUP {name} DROP USER {user}")],
    )
    .await?;
    Ok(Json(json!({ "message": format!("User '{user}' removed from '{name}'.") })))
}

// --- Database endpoints (still placeholders) ---

/// List all databases (placeholder)
pub async fn list_databases(
    State(_state): State<Arc<GatewayState>>,
) -> GatewayResult<Json<Vec<serde_json::Value>>> {
    let result = vec![json!({ "name": "default", "owner": "admin" })];
    Ok(Json(result))
}

/// Create a new database (placeholder)
pub async fn create_database(
    Extension(_claims): Extension<SessionClaims>,
    Json(_req): Json<CreateDatabaseRequest>,
) -> GatewayResult<Json<serde_json::Value>> {
    Ok(Json(json!({ "message": "Database created (placeholder)" })))
}

/// Get a specific database (placeholder)
pub async fn get_database(Path(_name): Path<String>) -> GatewayResult<Json<serde_json::Value>> {
    Ok(Json(json!({ "name": "default", "owner": "admin" })))
}

/// Drop a database (placeholder)
pub async fn drop_database(Path(_name): Path<String>) -> GatewayResult<Json<serde_json::Value>> {
    Ok(Json(json!({ "message": "Database dropped (placeholder)" })))
}
