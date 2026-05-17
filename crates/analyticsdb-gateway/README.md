# AnalyticsDB Gateway

Web admin console gateway for AnalyticsDB - terminates sessions, handles authentication, and provides API endpoints for the web console.

## Status

**Current state: Partial** - Core structure is in place with placeholder implementations. See [docs/agents/feature-status.md](../../docs/agents/feature-status.md) for the official status.

## Features Implemented

- **Session Management**: JWT-based session tokens with configurable timeout
- **Authentication**:
  - Local login endpoint (placeholder: accepts admin/admin)
  - JWT token creation and validation
  - Session refresh and logout endpoints
  - OIDC scaffold (requires full implementation)
- **API Structure**:
  - Health check endpoints (/healthz, /readyz)
  - Explorer API scaffold (live metadata - currently returns placeholder data)
  - Query execution endpoint (scaffold)
  - Admin API scaffold (databases, users, grants - placeholder implementations)
  - System metrics and logs API scaffold

## Configuration

Configuration is via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `ANALYTICSDB_GATEWAY_BIND_ADDR` | Gateway bind address | `0.0.0.0:8080` |
| `ANALYTICSDB_SESSION_TIMEOUT_SECONDS` | Session timeout | `3600` |
| `ANALYTICSDB_JWT_SECRET` | JWT signing secret | `change-me-in-production` |
| `ANALYTICSDB_OIDC_ENABLED` | Enable OIDC | `false` |
| `ANALYTICSDB_OIDC_ISSUER_URL` | OIDC issuer URL | - |
| `ANALYTICSDB_OIDC_CLIENT_ID` | OAuth2 client ID | - |
| `ANALYTICSDB_OIDC_CLIENT_SECRET` | OAuth2 client secret | - |
| `ANALYTICSDB_OIDC_REDIRECT_URL` | OAuth2 redirect URL | `http://localhost:8080/api/auth/oidc/callback` |

## API Endpoints

### Health
- `GET /healthz` - Liveness probe
- `GET /readyz` - Readiness probe

### Authentication
- `POST /api/auth/login` - Local login (placeholder: accepts admin/admin)
- `POST /api/auth/logout` - Logout
- `POST /api/auth/refresh` - Refresh session
- `GET /api/auth/oidc/authorize` - OIDC authorize (scaffold)
- `GET /api/auth/oidc/callback` - OIDC callback (scaffold)

### Explorer
- `GET /api/explorer` - Full explorer snapshot (placeholder data)
- `GET /api/explorer/databases` - List databases (placeholder)
- `GET /api/explorer/schemas` - List schemas (placeholder)
- `GET /api/explorer/tables` - List tables (placeholder)
- `GET /api/explorer/views` - List views (placeholder)
- `GET /api/explorer/columns` - List columns (placeholder)

### Query
- `POST /api/query` - Execute SQL query (scaffold)

### Admin
- `GET/POST /api/admin/databases` - List/create databases (placeholder)
- `DELETE /api/admin/databases/:name` - Drop database (placeholder)
- `GET/POST /api/admin/users` - List/create users (placeholder)
- `DELETE /api/admin/users/:name` - Drop user (placeholder)

### System
- `GET /api/system/metrics` - Get system metrics (scaffold)
- `GET /api/system/query-log` - Get query log (scaffold)
- `GET /api/system/audit-log` - Get audit log (scaffold)

## Building and Running

```bash
# Build
cargo build -p analyticsdb-gateway

# Run
cargo run -p analyticsdb-gateway
```

## Next Steps

To move from `Partial` to `Complete`:

1. **Integrate with ControlPlane**: Replace placeholder implementations with actual calls to `analyticsdb-control` methods
2. **Complete OIDC implementation**: Finish the OAuth2 flow with token exchange
3. **Query execution**: Implement query proxying to AnalyticsDB engine (PG or Flight SQL)
4. **Live metadata**: Connect explorer endpoints to live catalog data
5. **Web console integration**: Update the web console to use the live gateway API
6. **Playwright tests**: Complete end-to-end tests and integrate with CI
7. **Production hardening**: Add rate limiting, audit logging, security headers

## Testing

```bash
# Run gateway tests
cargo test -p analyticsdb-gateway

# Run web console Playwright tests
cd web/admin-console
npm install
npx playwright install
npm run test:e2e
```
