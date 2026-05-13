use crate::raft::{LogEntry, NodeId, Response, TypeConfig};
use crate::{CatalogState, ClusterNode};
use openraft::{
    Entry, LogId, RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("RocksDB error: {0}")]
    RocksDB(#[from] rocksdb::Error),
    #[error("JSON error: {0}")]
    JSON(#[from] serde_json::Error),
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),
    #[error("Internal error: {0}")]
    Internal(String),
}

pub struct CatalogStore {
    db: Arc<DB>,
}

impl CatalogStore {
    #[allow(clippy::result_large_err)]
    pub fn new(path: impl AsRef<Path>) -> Result<Self, StorageError<NodeId>> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let cf_logs = ColumnFamilyDescriptor::new("logs", Options::default());
        let cf_sm = ColumnFamilyDescriptor::new("sm", Options::default());
        let cf_meta = ColumnFamilyDescriptor::new("meta", Options::default());

        let db = DB::open_cf_descriptors(&options, path, vec![cf_logs, cf_sm, cf_meta]).map_err(
            |e| StorageError::IO {
                source: StorageIOError::new(
                    openraft::ErrorSubject::Store,
                    openraft::ErrorVerb::Write,
                    &e,
                ),
            },
        )?;

        Ok(Self { db: Arc::new(db) })
    }

    fn store_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> StorageError<NodeId> {
        StorageError::IO {
            source: StorageIOError::new(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Read,
                &e,
            ),
        }
    }
}

impl RaftLogReader<TypeConfig> for CatalogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let cf = self.db.cf_handle("logs").ok_or_else(|| {
            Self::store_err(StoreError::Internal("logs cf not found".to_string()))
        })?;

        let start = match range.start_bound() {
            std::ops::Bound::Included(&s) => s.to_be_bytes(),
            std::ops::Bound::Excluded(&s) => (s + 1).to_be_bytes(),
            std::ops::Bound::Unbounded => 0_u64.to_be_bytes(),
        };

        let mut entries = Vec::new();
        let iter = self.db.iterator_cf(
            cf,
            rocksdb::IteratorMode::From(&start, rocksdb::Direction::Forward),
        );

        for item in iter {
            let (key, value) = item.map_err(Self::store_err)?;
            let index = u64::from_be_bytes(key.as_ref().try_into().map_err(|_| {
                Self::store_err(StoreError::Internal("invalid log key".to_string()))
            })?);

            match range.end_bound() {
                std::ops::Bound::Included(&e) if index > e => break,
                std::ops::Bound::Excluded(&e) if index >= e => break,
                _ => {}
            }

            let entry: Entry<TypeConfig> =
                serde_json::from_slice(&value).map_err(Self::store_err)?;
            entries.push(entry);
        }

        Ok(entries)
    }
}

impl RaftStorage<TypeConfig> for CatalogStore {
    type SnapshotBuilder = Self;
    type LogReader = Self;

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let cf = self.db.cf_handle("meta").ok_or_else(|| {
            Self::store_err(StoreError::Internal("meta cf not found".to_string()))
        })?;
        let value = self
            .db
            .get_cf(cf, b"vote")
            .map_err(Self::store_err)?;

        match value {
            Some(v) => Ok(Some(
                serde_json::from_slice(&v).map_err(Self::store_err)?,
            )),
            None => Ok(None),
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        Self {
            db: Arc::clone(&self.db),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let cf = self.db.cf_handle("meta").ok_or_else(|| {
            Self::store_err(StoreError::Internal("meta cf not found".to_string()))
        })?;
        let value = serde_json::to_vec(vote).map_err(Self::store_err)?;
        self.db
            .put_cf(cf, b"vote", value)
            .map_err(Self::store_err)?;
        Ok(())
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<openraft::LogState<TypeConfig>, StorageError<NodeId>> {
        let cf = self.db.cf_handle("logs").ok_or_else(|| {
            Self::store_err(StoreError::Internal("logs cf not found".to_string()))
        })?;

        let last = self.db.iterator_cf(cf, rocksdb::IteratorMode::End).next();
        let last_log_id = match last {
            Some(item) => {
                let (_, value) = item.map_err(Self::store_err)?;
                let entry: Entry<TypeConfig> =
                    serde_json::from_slice(&value).map_err(Self::store_err)?;
                Some(entry.log_id)
            }
            None => None,
        };

        let cf_meta = self.db.cf_handle("meta").ok_or_else(|| {
            Self::store_err(StoreError::Internal("meta cf not found".to_string()))
        })?;
        let last_purged = self
            .db
            .get_cf(cf_meta, b"last_purged_log_id")
            .map_err(Self::store_err)?;
        let last_purged_log_id = match last_purged {
            Some(v) => Some(serde_json::from_slice(&v).map_err(Self::store_err)?),
            None => None,
        };

        Ok(openraft::LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let cf = self.db.cf_handle("logs").ok_or_else(|| {
            Self::store_err(StoreError::Internal("logs cf not found".to_string()))
        })?;
        for entry in entries {
            let key = entry.log_id.index.to_be_bytes();
            let value = serde_json::to_vec(&entry).map_err(Self::store_err)?;
            self.db
                .put_cf(cf, key, value)
                .map_err(Self::store_err)?;
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let cf = self.db.cf_handle("logs").ok_or_else(|| {
            Self::store_err(StoreError::Internal("logs cf not found".to_string()))
        })?;
        let start = log_id.index.to_be_bytes();
        let iter = self.db.iterator_cf(
            cf,
            rocksdb::IteratorMode::From(&start, rocksdb::Direction::Forward),
        );

        for item in iter {
            let (key, _) = item.map_err(Self::store_err)?;
            self.db.delete_cf(cf, key).map_err(Self::store_err)?;
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let cf = self.db.cf_handle("logs").ok_or_else(|| {
            Self::store_err(StoreError::Internal("logs cf not found".to_string()))
        })?;
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);

        for item in iter {
            let (key, _) = item.map_err(Self::store_err)?;
            let index = u64::from_be_bytes(key.as_ref().try_into().map_err(|_| {
                Self::store_err(StoreError::Internal("invalid log key".to_string()))
            })?);
            if index > log_id.index {
                break;
            }
            self.db.delete_cf(cf, key).map_err(Self::store_err)?;
        }

        let cf_meta = self.db.cf_handle("meta").ok_or_else(|| {
            Self::store_err(StoreError::Internal("meta cf not found".to_string()))
        })?;
        let value = serde_json::to_vec(&log_id).map_err(Self::store_err)?;
        self.db
            .put_cf(cf_meta, b"last_purged_log_id", value)
            .map_err(Self::store_err)?;
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, ClusterNode>), StorageError<NodeId>>
    {
        let cf = self.db.cf_handle("meta").ok_or_else(|| {
            Self::store_err(StoreError::Internal("meta cf not found".to_string()))
        })?;

        let last_applied = match self
            .db
            .get_cf(cf, b"last_applied")
            .map_err(Self::store_err)?
        {
            Some(v) => Some(serde_json::from_slice(&v).map_err(Self::store_err)?),
            None => None,
        };

        let last_membership = match self
            .db
            .get_cf(cf, b"last_membership")
            .map_err(Self::store_err)?
        {
            Some(v) => serde_json::from_slice(&v).map_err(Self::store_err)?,
            None => StoredMembership::default(),
        };

        Ok((last_applied, last_membership))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<Response>, StorageError<NodeId>> {
        let cf_sm = self
            .db
            .cf_handle("sm")
            .ok_or_else(|| Self::store_err(StoreError::Internal("sm cf not found".to_string())))?;
        let cf_meta = self.db.cf_handle("meta").ok_or_else(|| {
            Self::store_err(StoreError::Internal("meta cf not found".to_string()))
        })?;

        let mut responses = Vec::new();

        for entry in entries {
            let mut catalog_state: CatalogState = match self
                .db
                .get_cf(cf_sm, b"state")
                .map_err(Self::store_err)?
            {
                Some(v) => serde_json::from_slice(&v).map_err(Self::store_err)?,
                None => CatalogState::default(),
            };

            match entry.payload {
                openraft::EntryPayload::Normal(ref data) => match data {
                    LogEntry::CreateDatabase { name } => {
                        catalog_state.databases.insert(
                            name.clone(),
                            crate::CatalogDatabase {
                                name: name.clone(),
                                schemas: BTreeSet::from(["public".to_string()]),
                                owner: String::new(),
                                parameters: BTreeMap::new(),
                            },
                        );
                    }
                    LogEntry::CreateSchema { database, name } => {
                        let db_name = database.as_deref().unwrap_or("postgres");
                        if let Some(db) = catalog_state.databases.get_mut(db_name) {
                            db.schemas.insert(name.clone());
                        }
                    }
                    LogEntry::RegisterRelation(relation) => {
                        let key = format!(
                            "{}.{}.{}",
                            relation.database, relation.schema, relation.name
                        );
                        catalog_state.relations.insert(key, relation.clone());
                    }
                    LogEntry::DropRelation {
                        database,
                        schema,
                        name,
                        ..
                    } => {
                        let db_name = database.as_deref().unwrap_or("postgres");
                        let schema_name = schema.as_deref().unwrap_or("public");
                        let key = format!("{}.{}.{}", db_name, schema_name, name);
                        catalog_state.relations.remove(&key);
                    }
                    LogEntry::AddColumn {
                        database,
                        schema,
                        table_name,
                        column,
                    } => {
                        let db_name = database.as_deref().unwrap_or("postgres");
                        let schema_name = schema.as_deref().unwrap_or("public");
                        let key = format!("{}.{}.{}", db_name, schema_name, table_name);
                        if let Some(rel) = catalog_state.relations.get_mut(&key) {
                            rel.columns.push(column.clone());
                        }
                    }
                    LogEntry::RenameRelation {
                        database,
                        schema,
                        name,
                        new_name,
                    } => {
                        let db_name = database.as_deref().unwrap_or("postgres");
                        let schema_name = schema.as_deref().unwrap_or("public");
                        let old_key = format!("{}.{}.{}", db_name, schema_name, name);
                        if let Some(mut rel) = catalog_state.relations.remove(&old_key) {
                            rel.name = new_name.clone();
                            let new_key = format!("{}.{}.{}", db_name, schema_name, new_name);
                            catalog_state.relations.insert(new_key, rel);
                        }
                    }
                    LogEntry::UpdateRelationStoragePath {
                        database,
                        schema,
                        name,
                        new_storage_path,
                    } => {
                        let db_name = database.as_deref().unwrap_or("postgres");
                        let schema_name = schema.as_deref().unwrap_or("public");
                        let key = format!("{}.{}.{}", db_name, schema_name, name);
                        if let Some(rel) = catalog_state.relations.get_mut(&key) {
                            rel.storage_path = Some(new_storage_path.clone());
                        }
                    }
                    LogEntry::SetUserPassword {
                        name,
                        password,
                        version,
                        rotated_at,
                    } => {
                        if let Some(user) = catalog_state.users.get_mut(name) {
                            user.password = Some(password.clone());
                            user.password_version = *version;
                            user.password_rotated_at_epoch_ms = *rotated_at;
                        }
                    }
                    LogEntry::RegisterNode(node) => {
                        catalog_state.nodes.insert(node.id.clone(), node.clone());
                    }
                    LogEntry::UpdateNodeStatus {
                        node_id,
                        status,
                        last_heartbeat_at_epoch_ms,
                    } => {
                        if let Some(node) = catalog_state.nodes.get_mut(node_id) {
                            node.status = status.clone();
                            node.last_heartbeat_at_epoch_ms = *last_heartbeat_at_epoch_ms;
                        }
                    }
                },
                openraft::EntryPayload::Membership(ref mem) => {
                    let membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    let value = serde_json::to_vec(&membership).map_err(Self::store_err)?;
                    self.db
                        .put_cf(cf_meta, b"last_membership", value)
                        .map_err(Self::store_err)?;
                }
                _ => {}
            }

            // Save state
            let value = serde_json::to_vec(&catalog_state).map_err(Self::store_err)?;
            self.db
                .put_cf(cf_sm, b"state", value)
                .map_err(Self::store_err)?;

            // Update last applied
            let value = serde_json::to_vec(&entry.log_id).map_err(Self::store_err)?;
            self.db
                .put_cf(cf_meta, b"last_applied", value)
                .map_err(Self::store_err)?;

            responses.push(Response {
                message: "Applied".to_string(),
            });
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        Self {
            db: Arc::clone(&self.db),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, ClusterNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let catalog_state: CatalogState = serde_json::from_slice(snapshot.into_inner().as_slice())
            .map_err(Self::store_err)?;

        let cf_sm = self
            .db
            .cf_handle("sm")
            .ok_or_else(|| Self::store_err(StoreError::Internal("sm cf not found".to_string())))?;
        let cf_meta = self.db.cf_handle("meta").ok_or_else(|| {
            Self::store_err(StoreError::Internal("meta cf not found".to_string()))
        })?;

        let value = serde_json::to_vec(&catalog_state).map_err(Self::store_err)?;
        self.db
            .put_cf(cf_sm, b"state", value)
            .map_err(Self::store_err)?;

        let value = serde_json::to_vec(&meta.last_log_id).map_err(Self::store_err)?;
        self.db
            .put_cf(cf_meta, b"last_applied", value)
            .map_err(Self::store_err)?;

        let value = serde_json::to_vec(&meta.last_membership).map_err(Self::store_err)?;
        self.db
            .put_cf(cf_meta, b"last_membership", value)
            .map_err(Self::store_err)?;

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let (last_applied, last_membership) = self.last_applied_state().await?;
        if last_applied.is_none() {
            return Ok(None);
        }

        let cf_sm = self
            .db
            .cf_handle("sm")
            .ok_or_else(|| Self::store_err(StoreError::Internal("sm cf not found".to_string())))?;
        let catalog_state: CatalogState = match self
            .db
            .get_cf(cf_sm, b"state")
            .map_err(Self::store_err)?
        {
            Some(v) => serde_json::from_slice(&v).map_err(Self::store_err)?,
            None => return Ok(None),
        };

        let snapshot_data = serde_json::to_vec(&catalog_state).map_err(Self::store_err)?;

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id: format!(
                "{}-{}",
                last_applied.unwrap_or_default().index,
                uuid::Uuid::now_v7()
            ),
        };

        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(snapshot_data)),
        }))
    }
}

impl RaftSnapshotBuilder<TypeConfig> for CatalogStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (last_applied, last_membership) = self.last_applied_state().await?;

        let cf_sm = self
            .db
            .cf_handle("sm")
            .ok_or_else(|| Self::store_err(StoreError::Internal("sm cf not found".to_string())))?;
        let catalog_state: CatalogState = match self
            .db
            .get_cf(cf_sm, b"state")
            .map_err(Self::store_err)?
        {
            Some(v) => serde_json::from_slice(&v).map_err(Self::store_err)?,
            None => CatalogState::default(),
        };

        let snapshot_data = serde_json::to_vec(&catalog_state).map_err(Self::store_err)?;

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id: format!(
                "{}-{}",
                last_applied.unwrap_or_default().index,
                uuid::Uuid::now_v7()
            ),
        };

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(snapshot_data)),
        })
    }
}

impl Clone for CatalogStore {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
        }
    }
}
