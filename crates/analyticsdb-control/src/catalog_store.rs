//! Pluggable persistence for `CatalogState`.
//!
//! Two backends:
//!
//! - [`JsonCatalogStore`]: the legacy single-file JSON format. Kept for
//!   backwards compatibility, manual inspection, and disaster recovery export.
//! - [`SqliteCatalogStore`]: SQLite in WAL mode with a one-row-per-entity
//!   schema. Each top-level catalogue map (databases, users, nodes, relations,
//!   functions, etc.) is a table where the row is keyed by name and the value
//!   is a JSON-encoded entity. This gives us:
//!     * ACID transactional writes (no half-written corruption on crash).
//!     * WAL mode — readers never block writers and vice versa.
//!     * A path to true partial updates: future work can add `save_relation()`,
//!       `delete_relation()`, etc. without rewriting the whole catalogue.
//!
//! Backend selection is based on the file extension of the catalogue path:
//! `.db` / `.sqlite` / `.sqlite3` → SQLite; anything else → JSON.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use async_trait::async_trait;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::{
    CatalogAggregate, CatalogCollation, CatalogConversion, CatalogDatabase, CatalogFunction,
    CatalogRelation, CatalogState, CatalogUser, ClusterConfig, ClusterNode,
};

/// Backend-agnostic persistence interface for the cluster catalogue.
#[async_trait]
pub trait CatalogStore: Send + Sync + std::fmt::Debug {
    async fn load(&self) -> Result<CatalogState>;
    async fn save_state(&self, state: &CatalogState) -> Result<()>;
}

/// Choose a backend based on the path extension. Defaults to JSON for
/// unrecognised extensions to preserve existing behaviour.
pub fn open_store(path: &Path) -> Result<Box<dyn CatalogStore>> {
    if is_sqlite_path(path) {
        Ok(Box::new(SqliteCatalogStore::new(path.to_path_buf())))
    } else {
        Ok(Box::new(JsonCatalogStore::new(path.to_path_buf())))
    }
}

pub fn is_sqlite_path(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|s| s.to_str()),
        Some("db") | Some("sqlite") | Some("sqlite3")
    )
}

/// Migrate a JSON catalogue file into a SQLite catalogue file. The source
/// file is left untouched — callers can keep the JSON as a backup or remove
/// it once they are confident the SQLite copy is good.
pub async fn migrate_json_to_sqlite(json_path: &Path, sqlite_path: &Path) -> Result<()> {
    if !is_sqlite_path(sqlite_path) {
        anyhow::bail!(
            "destination must have a .db / .sqlite / .sqlite3 extension, got {}",
            sqlite_path.display()
        );
    }
    let src = JsonCatalogStore::new(json_path.to_path_buf());
    let state = src.load().await?;
    let dst = SqliteCatalogStore::new(sqlite_path.to_path_buf());
    dst.save_state(&state).await?;
    Ok(())
}

// -------- JSON backend --------

#[derive(Debug)]
pub struct JsonCatalogStore {
    path: PathBuf,
}

impl JsonCatalogStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[async_trait]
impl CatalogStore for JsonCatalogStore {
    async fn load(&self) -> Result<CatalogState> {
        let raw = fs::read_to_string(&self.path)
            .await
            .with_context(|| format!("reading catalogue at {}", self.path.display()))?;
        let state: CatalogState = serde_json::from_str(&raw)
            .with_context(|| format!("parsing JSON catalogue at {}", self.path.display()))?;
        Ok(state)
    }

    async fn save_state(&self, state: &CatalogState) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let raw = serde_json::to_string_pretty(state)?;
        fs::write(&self.path, raw).await?;
        Ok(())
    }
}

// -------- SQLite backend --------

#[derive(Debug)]
pub struct SqliteCatalogStore {
    path: PathBuf,
}

impl SqliteCatalogStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn open_conn(&self) -> Result<Connection> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let conn = Connection::open(&self.path)
            .with_context(|| format!("opening sqlite at {}", self.path.display()))?;
        // Tune for embedded metadata: WAL gives concurrent reads with writers,
        // NORMAL sync is the right durability/speed tradeoff for a metadata
        // store (full sync only at checkpoint, not on every commit).
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        Self::ensure_schema(&conn)?;
        Ok(conn)
    }

    fn ensure_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS databases   (name TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS users       (name TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS nodes       (id   TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS relations   (key  TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS functions   (key  TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS aggregates  (key  TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS collations  (key  TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS conversions (key  TEXT PRIMARY KEY, data TEXT NOT NULL);
            "#,
        )?;
        Ok(())
    }

    fn load_blocking(path: PathBuf) -> Result<CatalogState> {
        let store = Self::new(path);
        let conn = store.open_conn()?;

        let databases = load_keyed::<CatalogDatabase>(&conn, "SELECT name, data FROM databases")?;
        let users = load_keyed::<CatalogUser>(&conn, "SELECT name, data FROM users")?;
        let nodes = load_keyed::<ClusterNode>(&conn, "SELECT id, data FROM nodes")?;
        let relations = load_keyed::<CatalogRelation>(&conn, "SELECT key, data FROM relations")?;
        let functions = load_keyed::<CatalogFunction>(&conn, "SELECT key, data FROM functions")?;
        let aggregates = load_keyed::<CatalogAggregate>(&conn, "SELECT key, data FROM aggregates")?;
        let collations = load_keyed::<CatalogCollation>(&conn, "SELECT key, data FROM collations")?;
        let conversions =
            load_keyed::<CatalogConversion>(&conn, "SELECT key, data FROM conversions")?;

        let config: Option<ClusterConfig> = match meta_get(&conn, "config")? {
            Some(raw) => Some(serde_json::from_str(&raw)?),
            None => None,
        };

        let catalogue_version: u64 = match meta_get(&conn, "catalogue_version")? {
            Some(raw) => raw.parse().unwrap_or(0),
            None => 0,
        };

        Ok(CatalogState {
            databases,
            users,
            nodes,
            relations,
            aggregates,
            collations,
            conversions,
            functions,
            config,
            catalogue_version,
        })
    }

    fn save_blocking(path: PathBuf, state: SerializableState) -> Result<()> {
        let store = Self::new(path);
        let mut conn = store.open_conn()?;
        let tx = conn.transaction()?;

        replace_keyed(&tx, "databases", "name", &state.databases)?;
        replace_keyed(&tx, "users", "name", &state.users)?;
        replace_keyed(&tx, "nodes", "id", &state.nodes)?;
        replace_keyed(&tx, "relations", "key", &state.relations)?;
        replace_keyed(&tx, "functions", "key", &state.functions)?;
        replace_keyed(&tx, "aggregates", "key", &state.aggregates)?;
        replace_keyed(&tx, "collations", "key", &state.collations)?;
        replace_keyed(&tx, "conversions", "key", &state.conversions)?;

        match &state.config {
            Some(raw) => {
                tx.execute(
                    "INSERT INTO meta(key, value) VALUES('config', ?1)
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    params![raw],
                )?;
            }
            None => {
                tx.execute("DELETE FROM meta WHERE key = 'config'", [])?;
            }
        }

        tx.execute(
            "INSERT INTO meta(key, value) VALUES('catalogue_version', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![state.catalogue_version.to_string()],
        )?;

        tx.commit()?;
        Ok(())
    }
}

#[async_trait]
impl CatalogStore for SqliteCatalogStore {
    async fn load(&self) -> Result<CatalogState> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || Self::load_blocking(path)).await?
    }

    async fn save_state(&self, state: &CatalogState) -> Result<()> {
        // Serialize on the async task (cheap) so the blocking work is just IO.
        let serializable = SerializableState::try_from(state)?;
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || Self::save_blocking(path, serializable)).await?
    }
}

/// Pre-serialized form of `CatalogState` so the blocking save task does not
/// need access to non-Send references.
struct SerializableState {
    databases: Vec<(String, String)>,
    users: Vec<(String, String)>,
    nodes: Vec<(String, String)>,
    relations: Vec<(String, String)>,
    functions: Vec<(String, String)>,
    aggregates: Vec<(String, String)>,
    collations: Vec<(String, String)>,
    conversions: Vec<(String, String)>,
    config: Option<String>,
    catalogue_version: u64,
}

impl SerializableState {
    fn try_from(state: &CatalogState) -> Result<Self> {
        Ok(Self {
            databases: encode_map(&state.databases)?,
            users: encode_map(&state.users)?,
            nodes: encode_map(&state.nodes)?,
            relations: encode_map(&state.relations)?,
            functions: encode_map(&state.functions)?,
            aggregates: encode_map(&state.aggregates)?,
            collations: encode_map(&state.collations)?,
            conversions: encode_map(&state.conversions)?,
            config: state
                .config
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?,
            catalogue_version: state.catalogue_version,
        })
    }
}

fn encode_map<V: Serialize>(map: &BTreeMap<String, V>) -> Result<Vec<(String, String)>> {
    map.iter()
        .map(|(k, v)| Ok((k.clone(), serde_json::to_string(v)?)))
        .collect()
}

fn load_keyed<V: for<'de> Deserialize<'de>>(
    conn: &Connection,
    sql: &str,
) -> Result<BTreeMap<String, V>> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map([], |row| {
        let key: String = row.get(0)?;
        let data: String = row.get(1)?;
        Ok((key, data))
    })?;

    let mut out = BTreeMap::new();
    for row in rows {
        let (key, data) = row?;
        let value: V = serde_json::from_str(&data)
            .with_context(|| format!("decoding catalogue row '{}'", key))?;
        out.insert(key, value);
    }
    Ok(out)
}

fn replace_keyed(
    tx: &rusqlite::Transaction<'_>,
    table: &str,
    key_col: &str,
    rows: &[(String, String)],
) -> Result<()> {
    // 1) Upsert all incoming rows.
    let upsert_sql = format!(
        "INSERT INTO {table}({key_col}, data) VALUES(?1, ?2)
         ON CONFLICT({key_col}) DO UPDATE SET data = excluded.data"
    );
    {
        let mut stmt = tx.prepare(&upsert_sql)?;
        for (key, data) in rows {
            stmt.execute(params![key, data])?;
        }
    }

    // 2) Delete any rows not present in the new state. Use a bound IN-list
    //    when feasible; for very large catalogues this could be batched.
    let keys: BTreeSet<&str> = rows.iter().map(|(k, _)| k.as_str()).collect();
    if keys.is_empty() {
        tx.execute(&format!("DELETE FROM {table}"), [])?;
    } else {
        let placeholders = (1..=keys.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let delete_sql = format!("DELETE FROM {table} WHERE {key_col} NOT IN ({placeholders})");
        let params: Vec<&dyn rusqlite::ToSql> =
            keys.iter().map(|k| k as &dyn rusqlite::ToSql).collect();
        tx.execute(&delete_sql, params.as_slice())?;
    }
    Ok(())
}

fn meta_get(conn: &Connection, key: &str) -> Result<Option<String>> {
    let mut stmt = conn.prepare("SELECT value FROM meta WHERE key = ?1")?;
    let mut rows = stmt.query(params![key])?;
    match rows.next()? {
        Some(row) => Ok(Some(row.get(0)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CatalogDatabase;
    use std::collections::BTreeSet;
    use uuid::Uuid;

    fn temp_path(suffix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("adb-store-{}-{}", Uuid::now_v7(), suffix))
    }

    fn sample_state() -> CatalogState {
        let mut state = CatalogState::default();
        state.databases.insert(
            "postgres".to_string(),
            CatalogDatabase {
                name: "postgres".to_string(),
                schemas: BTreeSet::from(["public".to_string()]),
                owner: "postgres".to_string(),
                parameters: BTreeMap::new(),
            },
        );
        state
    }

    #[test]
    fn sqlite_store_roundtrip() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let path = temp_path("catalog.db");
            let store = SqliteCatalogStore::new(path.clone());
            let state = sample_state();

            store.save_state(&state).await.expect("save");
            let loaded = store.load().await.expect("load");

            assert_eq!(loaded.databases.len(), 1);
            assert!(loaded.databases.contains_key("postgres"));
            std::fs::remove_file(&path).ok();
            // Best-effort cleanup of WAL sidecars.
            std::fs::remove_file(path.with_extension("db-wal")).ok();
            std::fs::remove_file(path.with_extension("db-shm")).ok();
        });
    }

    #[test]
    fn sqlite_store_overwrites_removed_entries() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let path = temp_path("catalog2.db");
            let store = SqliteCatalogStore::new(path.clone());

            let mut state = sample_state();
            state.databases.insert(
                "extra".to_string(),
                CatalogDatabase {
                    name: "extra".to_string(),
                    schemas: BTreeSet::from(["public".to_string()]),
                    owner: "postgres".to_string(),
                    parameters: BTreeMap::new(),
                },
            );
            store.save_state(&state).await.expect("save 1");

            state.databases.remove("extra");
            store.save_state(&state).await.expect("save 2");

            let loaded = store.load().await.expect("load");
            assert!(!loaded.databases.contains_key("extra"));

            std::fs::remove_file(&path).ok();
            std::fs::remove_file(path.with_extension("db-wal")).ok();
            std::fs::remove_file(path.with_extension("db-shm")).ok();
        });
    }

    #[test]
    fn open_store_picks_backend_by_extension() {
        assert!(is_sqlite_path(Path::new("foo.db")));
        assert!(is_sqlite_path(Path::new("foo.sqlite")));
        assert!(is_sqlite_path(Path::new("foo.sqlite3")));
        assert!(!is_sqlite_path(Path::new("foo.json")));
        assert!(!is_sqlite_path(Path::new("foo")));
    }
}
