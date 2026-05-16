# Day-2 Operations Runbook

This document covers operational tasks for AnalyticsDB operators.

## Scaling Compute

To scale the compute pool:

```bash
# Using kubectl
kubectl scale deployment analyticsdb-compute --replicas=5

# Using helm
helm upgrade analyticsdb ./helm/analyticsdb --set computePool.replicaCount=5
```

The coordinator will automatically detect new nodes via heartbeat and distribute queries.

## Rotating the Cluster CA

1. Generate new CA and leaf certificates:
   ```bash
   analyticsdb ca init --output-dir ./new-certs
   ```

2. Update the cluster config (if using JSON):
   ```bash
   # Edit cluster-config.json to point to new cert paths
   {
     "tls_cert_path": "./new-certs/ca.pem",
     "tls_key_path": "./new-certs/leaf.pem"
   }
   ```

3. Rolling restart of nodes:
   ```bash
   kubectl rollout restart deployment/analyticsdb-control
   kubectl rollout restart deployment/analyticsdb-compute
   ```

## Rotating Object-Store Credentials

For S3, update environment variables or IAM roles:

```bash
# If using env vars
export AWS_ACCESS_KEY_ID=new_key
export AWS_SECRET_ACCESS_KEY=new_secret

# Restart nodes to pick up new credentials
kubectl rollout restart deployment/analyticsdb-control
kubectl rollout restart deployment/analyticsdb-compute
```

For GCS or Azure, follow their credential rotation procedures.

## Restoring a Corrupt Manifest

If a table's manifest becomes corrupt:

1. Identify the table's prefix in object storage:
   `s3://bucket/cluster=id/db=db_name/schema=schema_name/table=table_name/`

2. List files in the table prefix:
   ```bash
   aws s3 ls s3://bucket/cluster=id/db=db_name/schema=schema_name/table=table_name/data/
   ```

3. Rebuild manifest by creating a new JSON file with all Parquet files:
   ```json
   {
     "snapshot_id": "new-snapshot",
     "created_at_ms": <current_timestamp>,
     "files": [
       {"path": "data/file1.parquet", "size": 12345, "row_count": 100},
       ...
     ]
   }
   ```

4. Upload the new manifest to `meta/manifest.json`.

## Draining a Node

To drain a node for maintenance:

1. Mark node as `Unavailable` in the control plane (via SQL or API):
   ```sql
   -- Not yet implemented; use control plane API directly
   ```

2. Wait for in-flight queries to complete (or cancel them).

3. Perform maintenance.

4. Restart node; it will re-register and become `Ready`.

## Reading the Audit Log

Query the `system.audit_log` table:

```sql
SELECT * FROM system.audit_log
WHERE event_type = 'CREATE_TABLE'
ORDER BY event_time_ms DESC
LIMIT 100;
```

## Trigering a Compaction

To compact small Parquet files in a table:

```sql
VACUUM my_table;
```

This will merge small files into larger ones (target ~128 MiB per file) and vacuum orphaned files.
