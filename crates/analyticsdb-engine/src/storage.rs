use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::parquet::arrow::ArrowWriter;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path as OPath;
use object_store::{Error as OsError, ObjectStore, ObjectStoreExt};

/// Creates a `(store, prefix)` pair from a storage-location string.
///
/// Supports `file://` URIs and plain paths (absolute or relative) for the
/// local filesystem.  Relative paths are resolved against the current working
/// directory so they match what DataFusion's `ListingTableUrl` would resolve
/// to.  The returned prefix is a key within the store (no leading `/`).
pub fn store_for_location(location: &str) -> Result<(Arc<dyn ObjectStore>, OPath)> {
    let local_path = if let Some(p) = location.strip_prefix("file://") {
        p
    } else if !location.contains("://") {
        location
    } else {
        anyhow::bail!(
            "Storage location '{}' uses an unsupported scheme. \
             Only 'file://' and local paths are supported in the current prototype.",
            location
        );
    };

    // LocalFileSystem::new() is rooted at the filesystem root (/), so it
    // requires absolute paths.  Resolve relative paths against cwd so they
    // point to the same location DataFusion's ListingTableUrl would use.
    let abs_path = {
        let p = std::path::Path::new(local_path);
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|e| anyhow::anyhow!("Cannot determine current directory: {}", e))?
                .join(p)
        }
    };

    let store = Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>;
    // Object-store Path keys must not begin with '/'.
    let abs_str = abs_path.to_string_lossy();
    let path_str = abs_str.trim_start_matches('/');
    let prefix = OPath::parse(path_str)
        .map_err(|e| anyhow::anyhow!("Invalid storage path '{}': {}", path_str, e))?;
    Ok((store, prefix))
}

/// Returns the local filesystem path for a storage-location string.
///
/// Strips the `file://` scheme if present; otherwise returns the string unchanged.
/// Suitable for passing to DataFusion's `read_parquet` / `write_parquet`.
pub fn local_path_for_location(location: &str) -> &str {
    location.strip_prefix("file://").unwrap_or(location)
}

/// Appends `batch` as a new UUID-named Parquet file directly under `prefix`.
pub async fn append_parquet_batch(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
    batch: RecordBatch,
) -> Result<()> {
    let key = prefix.clone().join(format!("{}.parquet", uuid::Uuid::now_v7()).as_str());
    let bytes = encode_parquet_batches(batch.schema(), std::slice::from_ref(&batch))?;
    store.put(&key, bytes.into()).await?;
    Ok(())
}

/// Writes an empty Parquet file with `schema` to `key`.
pub async fn write_empty_parquet(
    store: &Arc<dyn ObjectStore>,
    key: &OPath,
    schema: &SchemaRef,
) -> Result<()> {
    let bytes = encode_empty_parquet(schema)?;
    store.put(key, bytes.into()).await?;
    Ok(())
}

/// Writes one or more `RecordBatch`es as a single Parquet file to `key`.
pub async fn write_parquet_batches(
    store: &Arc<dyn ObjectStore>,
    key: &OPath,
    schema: SchemaRef,
    batches: &[RecordBatch],
) -> Result<()> {
    let bytes = encode_parquet_batches(schema, batches)?;
    store.put(key, bytes.into()).await?;
    Ok(())
}

/// Returns the local filesystem paths of all `.parquet` files that are direct children
/// of `prefix` (non-recursive). Paths are returned as absolute strings suitable for
/// DataFusion's `read_parquet([...])` function.
pub async fn list_parquet_files(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
) -> Result<Vec<String>> {
    let prefix_str = prefix.as_ref().to_owned();
    let mut list = store.list(Some(prefix));
    let mut files = Vec::new();
    loop {
        match list.next().await {
            None => break,
            Some(Ok(meta)) => {
                let loc = meta.location.as_ref();
                let relative = loc
                    .strip_prefix(&*prefix_str)
                    .unwrap_or(loc)
                    .trim_start_matches('/');
                if !relative.contains('/') && relative.ends_with(".parquet") {
                    // Prepend '/' so DataFusion sees an absolute path.
                    files.push(format!("/{}", loc));
                }
            }
            Some(Err(OsError::NotFound { .. })) => break,
            Some(Err(e)) => return Err(e.into()),
        }
    }
    Ok(files)
}

/// Deletes all `.parquet` files that are direct children of `prefix` (non-recursive).
///
/// Objects in sub-prefixes (e.g. `.analyticsdb_indexes/`) are left untouched.
pub async fn clear_parquet_files(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
) -> Result<()> {
    let prefix_str = prefix.as_ref().to_owned();
    let mut list = store.list(Some(prefix));
    let mut to_delete = Vec::new();
    loop {
        match list.next().await {
            None => break,
            Some(Ok(meta)) => {
                let loc = meta.location.as_ref();
                let relative = loc
                    .strip_prefix(&*prefix_str)
                    .unwrap_or(loc)
                    .trim_start_matches('/');
                // Only delete direct children ending in .parquet.
                if !relative.contains('/') && relative.ends_with(".parquet") {
                    to_delete.push(meta.location);
                }
            }
            // Non-existent prefix is not an error — there is simply nothing to clear.
            Some(Err(OsError::NotFound { .. })) => break,
            Some(Err(e)) => return Err(e.into()),
        }
    }
    for key in to_delete {
        match store.delete(&key).await {
            Ok(_) | Err(OsError::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Writes `contents` as UTF-8 bytes to `key`.
pub async fn write_json(store: &Arc<dyn ObjectStore>, key: &OPath, contents: &str) -> Result<()> {
    store
        .put(key, Bytes::copy_from_slice(contents.as_bytes()).into())
        .await?;
    Ok(())
}

/// Reads UTF-8 bytes from `key`, returning `None` if the object does not exist.
pub async fn read_json(store: &Arc<dyn ObjectStore>, key: &OPath) -> Result<Option<String>> {
    match store.get(key).await {
        Ok(result) => {
            let bytes = result.bytes().await?;
            Ok(Some(String::from_utf8(bytes.to_vec())?))
        }
        Err(OsError::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Returns `true` if an object exists at `key`.
pub async fn object_exists(store: &Arc<dyn ObjectStore>, key: &OPath) -> Result<bool> {
    match store.head(key).await {
        Ok(_) => Ok(true),
        Err(OsError::NotFound { .. }) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Recursively deletes all objects whose path starts with `prefix`.
pub async fn delete_prefix(store: &Arc<dyn ObjectStore>, prefix: &OPath) -> Result<()> {
    let mut list = store.list(Some(prefix));
    let mut keys = Vec::new();
    loop {
        match list.next().await {
            None => break,
            Some(Ok(meta)) => keys.push(meta.location),
            Some(Err(OsError::NotFound { .. })) => break,
            Some(Err(e)) => return Err(e.into()),
        }
    }
    for key in keys {
        match store.delete(&key).await {
            Ok(_) | Err(OsError::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Copies all objects under `old_prefix` to corresponding paths under `new_prefix`,
/// then deletes the originals. Works across all object-store backends.
pub async fn rename_prefix(
    store: &Arc<dyn ObjectStore>,
    old_prefix: &OPath,
    new_prefix: &OPath,
) -> Result<()> {
    let old_str = old_prefix.as_ref().to_owned();
    let new_str = new_prefix.as_ref().to_owned();
    let mut list = store.list(Some(old_prefix));
    let mut moves: Vec<(OPath, OPath)> = Vec::new();
    loop {
        match list.next().await {
            None => break,
            Some(Ok(meta)) => {
                let loc = meta.location.as_ref();
                let relative = loc
                    .strip_prefix(&*old_str)
                    .unwrap_or("")
                    .trim_start_matches('/');
                let new_key = if relative.is_empty() {
                    new_prefix.clone()
                } else {
                    OPath::parse(&format!("{}/{}", new_str, relative))
                        .map_err(|e| anyhow::anyhow!("Invalid renamed path: {}", e))?
                };
                moves.push((meta.location, new_key));
            }
            Some(Err(OsError::NotFound { .. })) => break,
            Some(Err(e)) => return Err(e.into()),
        }
    }
    for (from, to) in moves {
        store.copy(&from, &to).await?;
        match store.delete(&from).await {
            Ok(_) | Err(OsError::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

// ─── Internal Parquet encoding helpers ─────────────────────────────────────

pub(crate) fn encode_parquet_batches(schema: SchemaRef, batches: &[RecordBatch]) -> Result<Bytes> {
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, None)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(Bytes::from(buf))
}

fn encode_empty_parquet(schema: &SchemaRef) -> Result<Bytes> {
    let mut buf = Vec::new();
    let writer = ArrowWriter::try_new(&mut buf, Arc::clone(schema), None)?;
    writer.close()?;
    Ok(Bytes::from(buf))
}
