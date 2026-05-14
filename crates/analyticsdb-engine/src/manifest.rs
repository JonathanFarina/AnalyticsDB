use anyhow::Result;
use chrono::Utc;
use datafusion::arrow::array::RecordBatch;
use object_store::path::Path as OPath;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::storage;

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
}

/// Returns the OPath key for the manifest file under a given table prefix.
pub fn manifest_key(prefix: &OPath) -> OPath {
    prefix.clone().join("meta").join("manifest.json")
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
    let key = manifest_key(prefix);
    match storage::read_json(store, &key).await {
        Ok(Some(json)) => Ok(Some(serde_json::from_str(&json)?)),
        Ok(None) | Err(_) => Ok(None),
    }
}

/// Writes `manifest` as JSON to `<prefix>/meta/manifest.json`.
pub async fn write_manifest(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
    manifest: &Manifest,
) -> Result<()> {
    let key = manifest_key(prefix);
    let json = serde_json::to_string_pretty(manifest)?;
    storage::write_json(store, &key, &json).await
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
/// `filename` is the bare filename (e.g. "abc123.parquet"), `size` is byte size,
/// `row_count` is the number of rows.
pub async fn append_to_manifest(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
    filename: &str,
    size: u64,
    row_count: i64,
) -> Result<()> {
    let mut manifest = read_manifest(store, prefix).await?.unwrap_or_default();
    manifest.files.push(ManifestEntry {
        path: filename.to_string(),
        size,
        row_count,
    });
    manifest.snapshot_id = Uuid::now_v7().to_string();
    manifest.created_at_ms = Utc::now().timestamp_millis();
    write_manifest(store, prefix, &manifest).await
}

/// Replaces the manifest with a fresh snapshot listing the given files.
/// Existing entries are discarded.
pub async fn replace_manifest(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
    entries: Vec<ManifestEntry>,
) -> Result<()> {
    let manifest = Manifest::new(entries);
    write_manifest(store, prefix, &manifest).await
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
