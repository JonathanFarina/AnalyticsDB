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
/// Supported URI schemes:
///   - `file://` and plain local paths (absolute or relative)
///   - `s3://bucket/key-prefix`   — AWS S3 (standard credential chain)
///   - `gs://bucket/key-prefix`   — Google Cloud Storage (ADC chain)
///   - `az://container/key-prefix` or `azure://container/...`  — Azure Blob Storage
///
/// Cloud credentials are resolved at call time from the standard provider chain
/// for each backend (environment variables, instance metadata, workload identity,
/// etc.).  Long-lived secrets must never be embedded in catalog state; reference
/// them by name and resolve here at runtime.
pub fn store_for_location(location: &str) -> Result<(Arc<dyn ObjectStore>, OPath)> {
    if let Some(rest) = location
        .strip_prefix("s3://")
        .or_else(|| location.strip_prefix("s3a://"))
    {
        return build_s3_store(rest);
    }
    if let Some(rest) = location.strip_prefix("gs://") {
        return build_gcs_store(rest);
    }
    if let Some(rest) = location
        .strip_prefix("az://")
        .or_else(|| location.strip_prefix("azure://"))
        .or_else(|| location.strip_prefix("abfss://"))
    {
        return build_azure_store(rest, location);
    }

    // Local / file:// paths.
    let local_path = if let Some(p) = location.strip_prefix("file://") {
        p
    } else if !location.contains("://") {
        location
    } else {
        anyhow::bail!(
            "Storage location '{}' uses an unsupported scheme. \
             Supported schemes: file://, s3://, gs://, az://, azure://.",
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

fn build_s3_store(rest: &str) -> Result<(Arc<dyn ObjectStore>, OPath)> {
    let (bucket, key_prefix) = split_bucket_and_prefix(rest);

    let mut builder = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(bucket);

    // SSE: ANALYTICSDB_S3_SSE takes precedence over ClusterConfig; the engine
    // propagates ClusterConfig values into these env vars at startup if needed.
    // Accepted values: "AES256" (SSE-S3) or "aws:kms" (SSE-KMS).
    if let Ok(sse) = std::env::var("ANALYTICSDB_S3_SSE") {
        let key: object_store::aws::AmazonS3ConfigKey = "aws_server_side_encryption"
            .parse()
            .map_err(|_| anyhow::anyhow!("Failed to parse S3 SSE config key"))?;
        builder = builder.with_config(key, sse);
    }
    if let Ok(kms_key_id) = std::env::var("ANALYTICSDB_S3_SSE_KMS_KEY_ID") {
        let key: object_store::aws::AmazonS3ConfigKey = "aws_sse_kms_key_id"
            .parse()
            .map_err(|_| anyhow::anyhow!("Failed to parse S3 KMS key config key"))?;
        builder = builder.with_config(key, kms_key_id);
    }

    let store = builder
        .build()
        .map_err(|e| anyhow::anyhow!("S3 store init failed for bucket '{}': {}", bucket, e))?;
    let prefix = OPath::parse(key_prefix)
        .map_err(|e| anyhow::anyhow!("Invalid S3 prefix '{}': {}", key_prefix, e))?;
    Ok((Arc::new(store), prefix))
}

fn build_gcs_store(rest: &str) -> Result<(Arc<dyn ObjectStore>, OPath)> {
    let (bucket, key_prefix) = split_bucket_and_prefix(rest);
    let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(bucket)
        .build()
        .map_err(|e| anyhow::anyhow!("GCS store init failed for bucket '{}': {}", bucket, e))?;
    let prefix = OPath::parse(key_prefix)
        .map_err(|e| anyhow::anyhow!("Invalid GCS prefix '{}': {}", key_prefix, e))?;
    Ok((Arc::new(store), prefix))
}

fn build_azure_store(rest: &str, original_location: &str) -> Result<(Arc<dyn ObjectStore>, OPath)> {
    // Normalise: strip the account-FQDN for abfss:// (container@account.dfs.core.windows.net/path)
    // and treat remainder as container/prefix like the other schemes.
    let normalised = if original_location.starts_with("abfss://") {
        if let Some((container_at_host, path)) = rest.split_once('/') {
            let container = container_at_host.split('@').next().unwrap_or(container_at_host);
            format!("{}/{}", container, path)
        } else {
            rest.to_string()
        }
    } else {
        rest.to_string()
    };

    let (container, key_prefix) = split_bucket_and_prefix(&normalised);
    let store = object_store::azure::MicrosoftAzureBuilder::from_env()
        .with_container_name(container)
        .build()
        .map_err(|e| {
            anyhow::anyhow!(
                "Azure store init failed for container '{}': {}",
                container,
                e
            )
        })?;
    let prefix = OPath::parse(key_prefix)
        .map_err(|e| anyhow::anyhow!("Invalid Azure prefix '{}': {}", key_prefix, e))?;
    Ok((Arc::new(store), prefix))
}

/// Splits `bucket/key/prefix` into `("bucket", "key/prefix")`.
/// If there is no `/`, the prefix is empty.
fn split_bucket_and_prefix(s: &str) -> (&str, &str) {
    match s.split_once('/') {
        Some((bucket, prefix)) => (bucket, prefix),
        None => (s, ""),
    }
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
    let key = prefix
        .clone()
        .join(format!("{}.parquet", uuid::Uuid::now_v7()).as_str());
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

/// Returns the local filesystem paths and sizes of all `.parquet` files that are
/// direct children of `prefix` (non-recursive).
pub async fn list_parquet_files_with_sizes(
    store: &Arc<dyn ObjectStore>,
    prefix: &OPath,
) -> Result<Vec<(String, u64)>> {
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
                    files.push((format!("/{}", loc), meta.size));
                }
            }
            Some(Err(OsError::NotFound { .. })) => break,
            Some(Err(e)) => return Err(e.into()),
        }
    }
    Ok(files)
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
pub async fn clear_parquet_files(store: &Arc<dyn ObjectStore>, prefix: &OPath) -> Result<()> {
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

/// Deletes the given object keys.  Missing objects are treated as success
/// so callers can use this idempotently to clean up partial writes.
pub async fn delete_objects(store: &Arc<dyn ObjectStore>, keys: &[OPath]) -> Result<()> {
    for key in keys {
        match store.delete(key).await {
            Ok(_) | Err(OsError::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
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
                    OPath::parse(format!("{}/{}", new_str, relative))
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

#[cfg(test)]
mod tests {
    use super::*;

    /// B8: verify that ANALYTICSDB_S3_SSE is parsed into a valid AmazonS3ConfigKey.
    /// The builder construction itself validates the key name — if the key string is
    /// wrong the parse() call returns Err and build_s3_store propagates it.
    #[test]
    fn s3_sse_config_key_parses_correctly() {
        let sse_key: Result<object_store::aws::AmazonS3ConfigKey, _> =
            "aws_server_side_encryption".parse();
        assert!(sse_key.is_ok(), "aws_server_side_encryption must be a valid AmazonS3ConfigKey");

        let kms_key: Result<object_store::aws::AmazonS3ConfigKey, _> =
            "aws_sse_kms_key_id".parse();
        assert!(kms_key.is_ok(), "aws_sse_kms_key_id must be a valid AmazonS3ConfigKey");
    }

    /// B8: verify that build_s3_store picks up ANALYTICSDB_S3_SSE from the environment.
    ///
    /// We only test that the function does NOT error when the env var is set to a
    /// valid SSE value. A full round-trip test requires a real S3 endpoint (see
    /// cli_s3_storage_root_supports_full_dml_ddl_lifecycle in the CLI test suite).
    #[test]
    fn build_s3_store_accepts_sse_env_var() {
        // SAFETY: single-threaded test; no concurrent env-var access.
        unsafe {
            std::env::set_var("ANALYTICSDB_S3_SSE", "AES256");
        }
        // build_s3_store will fail to connect (no real bucket) but the SSE key
        // parsing happens before the network call, so an auth/network error
        // confirms the key was accepted.
        let result = build_s3_store("test-bucket/prefix");
        unsafe {
            std::env::remove_var("ANALYTICSDB_S3_SSE");
        }
        // A missing-credentials error means we got past the SSE key parse.
        match result {
            Ok(_) => {} // built successfully (won't happen without real creds)
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    !msg.contains("Failed to parse S3 SSE config key"),
                    "SSE config key must parse without error, got: {msg}"
                );
            }
        }
    }

    /// B8: verify that ANALYTICSDB_S3_SSE_KMS_KEY_ID is parsed into a valid key.
    #[test]
    fn build_s3_store_accepts_kms_key_id_env_var() {
        unsafe {
            std::env::set_var("ANALYTICSDB_S3_SSE_KMS_KEY_ID", "arn:aws:kms:us-east-1:123:key/abc");
        }
        let result = build_s3_store("test-bucket/prefix");
        unsafe {
            std::env::remove_var("ANALYTICSDB_S3_SSE_KMS_KEY_ID");
        }
        match result {
            Ok(_) => {}
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    !msg.contains("Failed to parse S3 KMS key config key"),
                    "KMS key ID config key must parse without error, got: {msg}"
                );
            }
        }
    }
}
