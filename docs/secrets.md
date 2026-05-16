# Secrets Management

AnalyticsDB does not embed secrets in catalog state. All credentials are
resolved at runtime from one of the supported sources below.

## Storage Credentials

| Backend | Env vars | Notes |
|---------|----------|-------|
| S3 / S3A | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION`, `AWS_ENDPOINT_URL` | EC2/ECS instance-metadata provider also supported |
| GCS | `GOOGLE_APPLICATION_CREDENTIALS` (path to service-account JSON) | Workload Identity via metadata server also supported |
| Azure Blob / ADLS | `AZURE_STORAGE_ACCOUNT` + `AZURE_STORAGE_ACCOUNT_KEY` **or** `AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET` + `AZURE_TENANT_ID` | Managed Identity via metadata server also supported |

Set the relevant env vars before starting the server. The `object_store`
builder reads them via `.from_env()` at table-open time. Credentials are
never written to the catalog.

## TLS Keys

Set file paths in `cluster-config.json`:

```json
{
  "tls_cert_path": "/etc/analyticsdb/tls/server.crt",
  "tls_key_path":  "/etc/analyticsdb/tls/server.key",
  "tls_ca_cert_path": "/etc/analyticsdb/tls/ca.crt"
}
```

Paths may point to Kubernetes Secret mounts or any file accessible to the
server process. Keys are loaded at startup; rotate by updating the files and
restarting. Use `analyticsdb ca init` to generate a self-signed cluster CA
and leaf certificate for development.

## JWT Session Signing Key

Set `jwt_secret` in `cluster-config.json`. If absent, a random 32-byte key
is generated at startup — sessions will not survive a server restart.

For multi-node clusters, set an explicit shared secret so all coordinators
issue compatible tokens. In Kubernetes, use a Secret mounted as a file and
read via a startup script that sets `jwt_secret`.

## Encryption At Rest (S3 SSE)

Set in `cluster-config.json` **or** via env vars (env takes precedence):

| Config key | Env var | Example values |
|------------|---------|----------------|
| `s3_sse` | `ANALYTICSDB_S3_SSE` | `AES256`, `aws:kms`, `aws:kms:dsse` |
| `s3_sse_kms_key_id` | `ANALYTICSDB_S3_SSE_KMS_KEY_ID` | `arn:aws:kms:…:key/mrk-abc` |

The KMS key ARN is a reference, not the key material itself.

## What Is Never Stored In The Catalog

- Plaintext passwords (Argon2id PHC hashes only — enforced by the
  `catalog_state_contains_no_plaintext_passwords` test)
- Storage access keys
- TLS private keys
- JWT signing keys

## Supported Secret Providers (Roadmap)

The current implementation supports:

- **Env vars** — for storage credentials and SSE settings
- **File paths** — for TLS cert/key material
- **Config file fields** — for JWT secret and SSE key references

Future releases will add:

- AWS Secrets Manager / Parameter Store integration
- HashiCorp Vault transit / KV backends
- GCP Secret Manager
- Azure Key Vault
