use anyhow::Result;
use bytes::Bytes;
use chrono::Utc;
use datafusion::arrow::array::RecordBatch;
use futures::StreamExt;
use object_store::path::Path as OPath;
use object_store::{Error as OsError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

use crate::storage;

const MAX_CAS_RETRIES: usize = 10;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    /// Filename relative to the table prefix (e.g. "7f3a...v7.parquet").
    pub path: String,
    pub size: u64,
    pub row_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Manifest {
    pub snapshot_id: String,
    pub created_at_ms: i64,
    pub files: Vec<ManifestEntry>,
}

impl Manifest {
    pub fn new(files: Vec<ManifestEntry>) -> Self {
        Self {
            snapshot_id: Uuid::now_v7().to_string(),
            created_at_ms: Utc::now().timestamp_millis(),
            files,
        }
    }

    fn bump_snapshot(&mut self) {
        self.snapshot_id = Uuid::now_v7().to_string();
        self.created_at_ms = Utc::now().timestamp_millis();
    }
}

/// Returns the OPath key for the manifest file under a given table prefix.
pub fn manifest_key(prefix: &OPath) -> OPath {
    prefix.clone().join("meta").join("manifest.json")
}

/// Reads the manifest and its e_tag for the table at `prefix`.
///
/// Returns `(None, None)` if no manifest exists.  Any access error (e.g. the
/// prefix is a single file, the meta/ sub-path doesn't exist, transient I/O)
/// is treated as "no manifest" so the caller can fall back to a directory scan.
async fn read_manifest_versioned(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
) -> Result<(Option<Manifest>, Option<String>)> {
    let key = manifest_key(prefix);
    match store.get(&key).await {
        Ok(result) => {
            let e_tag = result.meta.e_tag.clone();
            let bytes = result.bytes().await?;
            let manifest: Manifest = serde_json::from_slice(&bytes)?;
            Ok((Some(manifest), e_tag))
        }
        Err(OsError::NotFound { .. }) => Ok((None, None)),
        Err(_) => Ok((None, None)),
    }
}

/// Reads the manifest for the table at `prefix`. Returns `None` if no manifest exists.
///
/// Any access error (e.g. the prefix is a single file rather than a directory,
/// the meta/ sub-path doesn't exist, or a transient I/O failure) is treated as
/// "no manifest" so the caller can fall back to a directory scan.
pub async fn read_manifest(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
) -> Result<Option<Manifest>> {
    let (manifest, _) = read_manifest_versioned(store, prefix).await?;
    Ok(manifest)
}

/// Writes `manifest` as JSON using the given `PutMode`.
///
/// Use `put_manifest_cas` for the public API — this is the raw write primitive.
async fn put_manifest(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
    manifest: &Manifest,
    mode: PutMode,
) -> Result<String, OsError> {
    let key = manifest_key(prefix);
    let json = serde_json::to_string_pretty(manifest)
        .map_err(|e| OsError::Generic { store: "manifest", source: Box::new(e) })?;
    let payload: object_store::PutPayload = Bytes::from(json.into_bytes()).into();
    let result = store
        .put_opts(&key, payload, PutOptions { mode, ..Default::default() })
        .await?;
    Ok(result.e_tag.unwrap_or_default())
}

/// Writes `manifest` to `<prefix>/meta/manifest.json`, replacing any existing manifest.
#[allow(dead_code)]
pub async fn write_manifest(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
    manifest: &Manifest,
) -> Result<()> {
    put_manifest(store, prefix, manifest, PutMode::Overwrite)
        .await
        .map(|_| ())
        .map_err(Into::into)
}

/// Returns the absolute file paths listed in `manifest` (suitable for DataFusion's read_parquet).
/// Paths are formatted as `/<prefix>/<filename>`.
pub fn manifest_file_paths(prefix: &OPath, manifest: &Manifest) -> Vec<String> {
    manifest
        .files
        .iter()
        .map(|e| format!("/{}/{}", prefix.as_ref(), e.path))
        .collect()
}

/// Returns the committed file paths for the table at `prefix`.
///
/// If a manifest exists, uses it. Falls back to a directory scan via
/// `storage::list_parquet_files` for tables that predate manifests.
pub async fn list_files(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
) -> Result<Vec<String>> {
    if let Some(manifest) = read_manifest(store, prefix).await? {
        return Ok(manifest_file_paths(prefix, &manifest));
    }
    // Fallback: directory scan for pre-manifest tables.
    storage::list_parquet_files(store, prefix).await
}

/// Returns the committed file paths and sizes for the table at `prefix`.
///
/// If a manifest exists, uses it. Falls back to a directory scan.
pub async fn list_files_with_sizes(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
) -> Result<Vec<(String, u64)>> {
    if let Some(manifest) = read_manifest(store, prefix).await? {
        return Ok(manifest
            .files
            .iter()
            .map(|e| {
                let path = format!("/{}/{}", prefix.as_ref(), e.path);
                (path, e.size)
            })
            .collect());
    }
    // Fallback: directory scan.
    storage::list_parquet_files_with_sizes(store, prefix).await
}

/// Appends a new entry to the manifest at `prefix`, creating it if it doesn't exist.
///
/// Uses optimistic concurrency (read current e_tag → CAS write → retry on conflict)
/// so concurrent writers converge without data loss.
pub async fn append_to_manifest(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
    filename: &str,
    size: u64,
    row_count: i64,
) -> Result<()> {
    for attempt in 0..MAX_CAS_RETRIES {
        let (existing, e_tag) = read_manifest_versioned(store, prefix).await?;
        let mut manifest = existing.unwrap_or_default();
        manifest.files.push(ManifestEntry {
            path: filename.to_string(),
            size,
            row_count,
        });
        manifest.bump_snapshot();

        let mode = match e_tag {
            Some(tag) => PutMode::Update(UpdateVersion { e_tag: Some(tag), version: None }),
            None => PutMode::Create,
        };
        match put_manifest(store, prefix, &manifest, mode).await {
            Ok(_) => return Ok(()),
            Err(OsError::AlreadyExists { .. } | OsError::Precondition { .. }) => {
                // Another writer committed between our read and write; retry.
                if attempt + 1 == MAX_CAS_RETRIES {
                    anyhow::bail!(
                        "manifest CAS failed after {} retries for prefix {}",
                        MAX_CAS_RETRIES,
                        prefix
                    );
                }
                continue;
            }
            Err(OsError::NotImplemented { .. } | OsError::NotSupported { .. }) => {
                // Backend doesn't support conditional writes (e.g. LocalFileSystem).
                // Fall back to a plain overwrite — no CAS protection on this backend.
                return put_manifest(store, prefix, &manifest, PutMode::Overwrite)
                    .await
                    .map(|_| ())
                    .map_err(Into::into);
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Replaces the manifest with a fresh snapshot listing the given files.
///
/// Uses the same optimistic CAS loop as `append_to_manifest` to prevent
/// races with concurrent writers.
pub async fn replace_manifest(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
    entries: Vec<ManifestEntry>,
) -> Result<()> {
    for attempt in 0..MAX_CAS_RETRIES {
        let (_, e_tag) = read_manifest_versioned(store, prefix).await?;
        let manifest = Manifest::new(entries.clone());

        let mode = match e_tag {
            Some(tag) => PutMode::Update(UpdateVersion { e_tag: Some(tag), version: None }),
            None => PutMode::Create,
        };
        match put_manifest(store, prefix, &manifest, mode).await {
            Ok(_) => return Ok(()),
            Err(OsError::AlreadyExists { .. } | OsError::Precondition { .. }) => {
                if attempt + 1 == MAX_CAS_RETRIES {
                    anyhow::bail!(
                        "manifest CAS failed after {} retries for prefix {}",
                        MAX_CAS_RETRIES,
                        prefix
                    );
                }
                continue;
            }
            Err(OsError::NotImplemented { .. } | OsError::NotSupported { .. }) => {
                return put_manifest(store, prefix, &manifest, PutMode::Overwrite)
                    .await
                    .map(|_| ())
                    .map_err(Into::into);
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Writes `batch` as a new UUID-named Parquet file under `prefix` and updates the manifest.
pub async fn append_batch(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
    batch: RecordBatch,
) -> Result<()> {
    let filename = format!("{}.parquet", Uuid::now_v7());
    let key = prefix.clone().join(filename.as_str());
    let bytes = storage::encode_parquet_batches(batch.schema(), std::slice::from_ref(&batch))?;
    let size = bytes.len() as u64;
    let row_count = batch.num_rows() as i64;
    store.put(&key, bytes.into()).await?;
    append_to_manifest(store, prefix, &filename, size, row_count).await
}

/// Deletes any `.parquet` files under `prefix` that are not listed in the current manifest.
///
/// These orphans arise when a coordinator crashes after staging a data file but
/// before publishing the manifest update.  Safe to call concurrently with readers
/// (orphans are never visible at the SQL surface) and with writers (a file that
/// was just staged will be in the manifest within the same atomic write that
/// created it, so it will not be mistaken for an orphan by a concurrent vacuum).
///
/// Returns the number of files deleted.
pub async fn vacuum_orphans(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
) -> Result<usize> {
    let manifest = read_manifest(store, prefix).await?;
    let committed: HashSet<String> = match &manifest {
        Some(m) => m.files.iter().map(|e| e.path.clone()).collect(),
        // No manifest yet — nothing is committed, so nothing is an orphan.
        None => return Ok(0),
    };

    let prefix_str = prefix.as_ref().to_owned();
    let mut list = store.list(Some(prefix));
    let mut orphans: Vec<OPath> = Vec::new();
    loop {
        match list.next().await {
            None => break,
            Some(Ok(meta)) => {
                let loc = meta.location.as_ref();
                let relative = loc
                    .strip_prefix(&*prefix_str)
                    .unwrap_or(loc)
                    .trim_start_matches('/');
                // Only consider direct children that are Parquet files; leave
                // subdirectories (meta/, .analyticsdb_indexes/) untouched.
                if !relative.contains('/') && relative.ends_with(".parquet") && !committed.contains(relative) {
                    orphans.push(meta.location);
                }
            }
            Some(Err(OsError::NotFound { .. })) => break,
            Some(Err(e)) => return Err(e.into()),
        }
    }

    let count = orphans.len();
    storage::delete_objects(store, &orphans).await?;
    Ok(count)
}
