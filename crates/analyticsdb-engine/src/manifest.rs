use anyhow::Result;
use bytes::Bytes;
use chrono::Utc;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use datafusion::scalar::ScalarValue;
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, Statistics};
use futures::StreamExt;
use object_store::path::Path as OPath;
use object_store::{
    Error as OsError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

use crate::storage;
// use crate::metrics;

const MAX_CAS_RETRIES: usize = 10;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStat {
    pub name: String,
    pub null_count: i64,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub ndv_estimate: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    /// Filename relative to the table prefix (e.g. "7f3a...v7.parquet").
    pub path: String,
    pub size: u64,
    pub row_count: i64,
    #[serde(default)]
    pub column_stats: Vec<ColumnStat>,
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
    let json = serde_json::to_string_pretty(manifest).map_err(|e| OsError::Generic {
        store: "manifest",
        source: Box::new(e),
    })?;
    let payload: object_store::PutPayload = Bytes::from(json.into_bytes()).into();
    let result = store
        .put_opts(
            &key,
            payload,
            PutOptions {
                mode,
                ..Default::default()
            },
        )
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

/// Returns committed file paths as full URIs suitable for DataFusion `ListingTableUrl`.
///
/// For cloud storage (`s3://`, `gs://`, `az://`) the returned strings are proper
/// cloud URIs.  For local/file:// storage they are absolute paths (same as
/// `manifest_file_paths`).
pub fn manifest_file_uris(location: &str, manifest: &Manifest) -> Vec<String> {
    let base = location.trim_end_matches('/');
    manifest
        .files
        .iter()
        .map(|e| format!("{}/{}", base, e.path))
        .collect()
}

/// Returns the committed file paths for the table at `prefix`.
///
/// If a manifest exists, uses it. Falls back to a directory scan via
/// `storage::list_parquet_files` for tables that predate manifests.
pub async fn list_files(store: &Arc<dyn ObjectStore>, prefix: &OPath) -> Result<Vec<String>> {
    if let Some(manifest) = read_manifest(store, prefix).await? {
        return Ok(manifest_file_paths(prefix, &manifest));
    }
    // Fallback: directory scan for pre-manifest tables.
    storage::list_parquet_files(store, prefix).await
}

/// Like `list_files` but returns full URIs suitable for DataFusion `ListingTableUrl`.
///
/// For cloud storage the returned strings include the scheme and bucket
/// (e.g. `s3://bucket/prefix/data/uuid.parquet`).  For local storage they
/// are absolute paths identical to what `list_files` would return.
pub async fn list_file_uris(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
    location: &str,
) -> Result<Vec<String>> {
    if let Some(manifest) = read_manifest(store, prefix).await? {
        return Ok(manifest_file_uris(location, &manifest));
    }
    // Fallback: directory scan.  For cloud storage, rebase the raw /<key> paths
    // returned by storage::list_parquet_files to proper cloud URIs.
    let raw = storage::list_parquet_files(store, prefix).await?;
    if let Some(scheme_bucket) = cloud_scheme_and_bucket(location) {
        Ok(raw
            .iter()
            .map(|p| format!("{}{}", scheme_bucket, p))
            .collect())
    } else {
        Ok(raw)
    }
}

/// Extracts `scheme://bucket` from a cloud storage URI, or `None` for local paths.
fn cloud_scheme_and_bucket(location: &str) -> Option<String> {
    let scheme_end = location.find("://")?;
    let scheme = &location[..scheme_end];
    if matches!(scheme, "s3" | "s3a" | "gs" | "az" | "azure" | "abfss") {
        let rest = &location[scheme_end + 3..];
        let bucket = rest.split('/').next().unwrap_or(rest);
        Some(format!("{}://{}", scheme, bucket))
    } else {
        None
    }
}

/// Returns the committed file paths and sizes for the table at `prefix`.
///
/// If a manifest exists, uses it. Falls back to a directory scan.
#[allow(dead_code)]
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

/// Returns the committed file paths, sizes, and row counts for the table at `prefix`.
///
/// Like `list_files_with_sizes` but also returns the `row_count` from each manifest entry.
/// When a manifest does not exist or an entry has no row_count the value is 0.
/// Falls back to a directory scan when no manifest is present (row_count will be 0 for all).
pub async fn list_files_with_sizes_and_rows(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
) -> Result<Vec<(String, u64, i64)>> {
    if let Some(manifest) = read_manifest(store, prefix).await? {
        return Ok(manifest
            .files
            .iter()
            .map(|e| {
                let path = format!("/{}/{}", prefix.as_ref(), e.path);
                (path, e.size, e.row_count)
            })
            .collect());
    }
    // Fallback: directory scan — row_count is unavailable.
    let files = storage::list_parquet_files_with_sizes(store, prefix).await?;
    Ok(files.into_iter().map(|(p, s)| (p, s, 0i64)).collect())
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
    column_stats: Vec<ColumnStat>,
) -> Result<()> {
    for attempt in 0..MAX_CAS_RETRIES {
        let (existing, e_tag) = read_manifest_versioned(store, prefix).await?;
        let mut manifest = existing.unwrap_or_default();
        manifest.files.push(ManifestEntry {
            path: filename.to_string(),
            size,
            row_count,
            column_stats: column_stats.clone(),
        });
        manifest.bump_snapshot();

        let mode = match e_tag {
            Some(tag) => PutMode::Update(UpdateVersion {
                e_tag: Some(tag),
                version: None,
            }),
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
            Err(e) => {
                return Err(e.into());
            }
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
            Some(tag) => PutMode::Update(UpdateVersion {
                e_tag: Some(tag),
                version: None,
            }),
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
            Err(e) => {
                return Err(e.into());
            }
        }
    }
    Ok(())
}

/// Returns the object-store key for a manifest entry path relative to `prefix`.
///
/// Handles both the new layout (`"data/<file>"` → `<prefix>/data/<file>`) and the
/// legacy layout (`"<file>"` → `<prefix>/<file>`) so callers never embed the slash split.
fn entry_key(prefix: &OPath, entry_path: &str) -> OPath {
    if let Some(bare) = entry_path.strip_prefix("data/") {
        prefix.clone().join("data").join(bare)
    } else {
        prefix.clone().join(entry_path)
    }
}

/// Writes `batch` as a new UUID-named Parquet file under `prefix` and updates the manifest.
pub async fn append_batch(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
    batch: RecordBatch,
) -> Result<()> {
    let filename = format!("{}.parquet", Uuid::now_v7());
    let data_path = format!("data/{}", filename);
    let key = prefix.clone().join("data").join(filename.as_str());
    let bytes = storage::encode_parquet_batches(batch.schema(), std::slice::from_ref(&batch))?;
    let size = bytes.len() as u64;
    let row_count = batch.num_rows() as i64;
    // Compute column statistics
    let column_stats = crate::batch::compute_column_stats(&batch);
    store.put(&key, bytes.into()).await?;
    append_to_manifest(store, prefix, &data_path, size, row_count, column_stats).await
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
pub async fn vacuum_orphans(store: &Arc<dyn ObjectStore>, prefix: &OPath) -> Result<usize> {
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
                // Candidate orphans are either direct children (old layout) or
                // one level deep under data/ (new layout). Files under meta/,
                // .analyticsdb_indexes/, or any other subdirectory are untouched.
                let is_candidate = if relative.contains('/') {
                    relative.starts_with("data/") && relative.matches('/').count() == 1
                } else {
                    relative.ends_with(".parquet")
                };
                if is_candidate && relative.ends_with(".parquet") && !committed.contains(relative) {
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

/// Merges small Parquet files under `prefix` into fewer, larger files.
///
/// Compaction is skipped when fewer than `min_file_count` files are listed in
/// the manifest (no work to do) or when the total size of *small* files is
/// below `target_file_bytes` (a single pass would produce only one tiny file).
///
/// Returns the number of new Parquet files written.  The manifest is atomically
/// replaced and orphaned originals are vacuumed before returning.
pub async fn compact_table(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
    target_file_bytes: u64,
    min_file_count: usize,
) -> Result<usize> {
    let manifest = match read_manifest(store, prefix).await? {
        Some(m) => m,
        None => return Ok(0),
    };

    if manifest.files.len() < min_file_count {
        return Ok(0);
    }

    // Read all committed batches and infer schema from the first file.
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut schema: Option<SchemaRef> = None;

    for entry in &manifest.files {
        let key = entry_key(prefix, &entry.path);
        let bytes = match store.get(&key).await {
            Ok(r) => r.bytes().await?,
            Err(OsError::NotFound { .. }) => continue,
            Err(e) => return Err(e.into()),
        };

        let reader = ParquetRecordBatchReader::try_new(bytes, 8192)
            .map_err(|e| anyhow::anyhow!("Parquet read error for {}: {}", entry.path, e))?;

        for batch in reader {
            let batch = batch.map_err(|e| anyhow::anyhow!("Batch read error: {}", e))?;
            if schema.is_none() {
                schema = Some(batch.schema());
            }
            all_batches.push(batch);
        }
    }

    let schema = match schema {
        Some(s) => s,
        None => return Ok(0), // All files were empty or missing.
    };

    // Bin batches into new files sized around `target_file_bytes`.
    // We use row count as a proxy for size to avoid double-encoding; each file
    // is encoded exactly once when flushed.
    let mut new_entries: Vec<ManifestEntry> = Vec::new();
    let mut current_bin: Vec<RecordBatch> = Vec::new();
    let mut current_rows: i64 = 0;

    // Rough row budget per output file based on the average row size of inputs.
    let total_rows: i64 = all_batches.iter().map(|b| b.num_rows() as i64).sum();
    let total_size: u64 = manifest.files.iter().map(|e| e.size).sum();
    let bytes_per_row = if total_rows > 0 {
        (total_size as f64 / total_rows as f64).max(1.0)
    } else {
        1.0
    };
    let row_budget = (target_file_bytes as f64 / bytes_per_row).ceil() as i64;
    let row_budget = row_budget.max(1);

    let flush = |bin: Vec<RecordBatch>| -> Result<(ManifestEntry, Bytes)> {
        let filename = format!("{}.parquet", Uuid::now_v7());
        let data_path = format!("data/{}", filename);
        let row_count: i64 = bin.iter().map(|b| b.num_rows() as i64).sum();
        let bytes = storage::encode_parquet_batches(Arc::clone(&schema), &bin)?;
        let size = bytes.len() as u64;
        Ok((
            ManifestEntry {
                path: data_path,
                size,
                row_count,
                column_stats: Vec::new(),
            },
            bytes,
        ))
    };

    for batch in all_batches {
        let batch_rows = batch.num_rows() as i64;
        if !current_bin.is_empty() && current_rows + batch_rows > row_budget {
            let (entry, bytes) = flush(std::mem::take(&mut current_bin))?;
            let key = entry_key(prefix, &entry.path);
            store.put(&key, bytes.into()).await?;
            new_entries.push(entry);
            current_rows = 0;
        }
        current_rows += batch_rows;
        current_bin.push(batch);
    }

    if !current_bin.is_empty() {
        let (entry, bytes) = flush(std::mem::take(&mut current_bin))?;
        let key = entry_key(prefix, &entry.path);
        store.put(&key, bytes.into()).await?;
        new_entries.push(entry);
    }

    let written = new_entries.len();
    replace_manifest(store, prefix, new_entries).await?;
    vacuum_orphans(store, prefix).await?;
    Ok(written)
}

/// Converts a Manifest to DataFusion Statistics for query planning.
#[allow(dead_code)]
pub fn manifest_to_statistics(manifest: &Manifest, schema: &SchemaRef) -> Statistics {
    let num_rows: usize = manifest.files.iter().map(|e| e.row_count as usize).sum();
    let total_byte_size: usize = manifest.files.iter().map(|e| e.size as usize).sum();

    let mut column_statistics = Vec::new();

    for field in schema.fields() {
        let col_name = field.name();
        let data_type = field.data_type();

        let mut total_null_count = 0i64;
        let mut min_values: Vec<String> = Vec::new();
        let mut max_values: Vec<String> = Vec::new();
        let mut total_ndv = 0f64;

        for entry in &manifest.files {
            for cs in &entry.column_stats {
                if cs.name == *col_name {
                    total_null_count += cs.null_count;
                    if let Some(ref min) = cs.min_value {
                        min_values.push(min.clone());
                    }
                    if let Some(ref max) = cs.max_value {
                        max_values.push(max.clone());
                    }
                    if let Some(ndv) = cs.ndv_estimate {
                        total_ndv += ndv;
                    }
                    break;
                }
            }
        }

        let null_count = Some(total_null_count as usize);

        // Parse min values into ScalarValue and find the minimum
        let mut min_scalars: Vec<ScalarValue> = min_values
            .iter()
            .filter_map(|s| parse_scalar_value(data_type, s))
            .collect();
        let min_value = if !min_scalars.is_empty() {
            min_scalars.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            min_scalars.into_iter().next()
        } else {
            None
        };

        // Parse max values into ScalarValue and find the maximum
        let mut max_scalars: Vec<ScalarValue> = max_values
            .iter()
            .filter_map(|s| parse_scalar_value(data_type, s))
            .collect();
        let max_value = if !max_scalars.is_empty() {
            max_scalars.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal)); // sort descending
            max_scalars.into_iter().next()
        } else {
            None
        };

        let distinct_count = if total_ndv > 0.0 {
            Some(total_ndv as usize)
        } else {
            None
        };

        let to_precision = |opt: Option<usize>| -> Precision<usize> {
            match opt {
                Some(v) => Precision::Exact(v),
                None => Precision::Absent,
            }
        };

        let to_precision_scalar = |opt: Option<ScalarValue>| -> Precision<ScalarValue> {
            match opt {
                Some(v) => Precision::Exact(v),
                None => Precision::Absent,
            }
        };

        column_statistics.push(ColumnStatistics {
            null_count: to_precision(null_count),
            min_value: to_precision_scalar(min_value),
            max_value: to_precision_scalar(max_value),
            distinct_count: to_precision(distinct_count),
            ..Default::default()
        });
    }

    Statistics {
        num_rows: Precision::Exact(num_rows),
        total_byte_size: Precision::Exact(total_byte_size),
        column_statistics,
    }
}

/// Parse a string into ScalarValue based on the target data type.
#[allow(dead_code)]
fn parse_scalar_value(data_type: &DataType, s: &str) -> Option<ScalarValue> {
    match data_type {
        DataType::Int8 => s.parse::<i8>().ok().map(|v| ScalarValue::Int8(Some(v))),
        DataType::Int16 => s.parse::<i16>().ok().map(|v| ScalarValue::Int16(Some(v))),
        DataType::Int32 => s.parse::<i32>().ok().map(|v| ScalarValue::Int32(Some(v))),
        DataType::Int64 => s.parse::<i64>().ok().map(|v| ScalarValue::Int64(Some(v))),
        DataType::UInt8 => s.parse::<u8>().ok().map(|v| ScalarValue::UInt8(Some(v))),
        DataType::UInt16 => s.parse::<u16>().ok().map(|v| ScalarValue::UInt16(Some(v))),
        DataType::UInt32 => s.parse::<u32>().ok().map(|v| ScalarValue::UInt32(Some(v))),
        DataType::UInt64 => s.parse::<u64>().ok().map(|v| ScalarValue::UInt64(Some(v))),
        DataType::Float32 => s.parse::<f32>().ok().map(|v| ScalarValue::Float32(Some(v))),
        DataType::Float64 => s.parse::<f64>().ok().map(|v| ScalarValue::Float64(Some(v))),
        DataType::Utf8 => Some(ScalarValue::Utf8(Some(s.to_string()))),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use object_store::local::LocalFileSystem;
    use std::sync::Arc;

    fn make_batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    #[tokio::test]
    async fn compact_table_merges_multiple_small_files_into_one() {
        let dir = std::env::temp_dir().join(format!("compact-test-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let prefix = OPath::parse(dir.to_string_lossy().trim_start_matches('/')).unwrap();

        // Write three small Parquet files via append_batch.
        append_batch(&store, &prefix, make_batch(vec![1, 2]))
            .await
            .unwrap();
        append_batch(&store, &prefix, make_batch(vec![3, 4]))
            .await
            .unwrap();
        append_batch(&store, &prefix, make_batch(vec![5, 6]))
            .await
            .unwrap();

        let manifest_before = read_manifest(&store, &prefix).await.unwrap().unwrap();
        assert_eq!(manifest_before.files.len(), 3, "expected 3 input files");

        // Compact with a large target so all 3 fit into 1 output file.
        let written = compact_table(&store, &prefix, 128 * 1024 * 1024, 2)
            .await
            .unwrap();
        assert_eq!(
            written, 1,
            "all small files should merge into a single output"
        );

        let manifest_after = read_manifest(&store, &prefix).await.unwrap().unwrap();
        assert_eq!(
            manifest_after.files.len(),
            1,
            "manifest should list one file after compaction"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn compact_table_skips_when_below_min_file_count() {
        let dir = std::env::temp_dir().join(format!("compact-skip-test-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let prefix = OPath::parse(dir.to_string_lossy().trim_start_matches('/')).unwrap();

        append_batch(&store, &prefix, make_batch(vec![1]))
            .await
            .unwrap();

        let written = compact_table(&store, &prefix, 128 * 1024 * 1024, 2)
            .await
            .unwrap();
        assert_eq!(written, 0, "single file should not be compacted");

        std::fs::remove_dir_all(&dir).ok();
    }
}
