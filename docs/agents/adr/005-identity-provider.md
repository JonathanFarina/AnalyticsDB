# ADR 005: Identity Provider And SSO Integration

**Status:** Deferred — decision required before Phase J (Surfaces) begins  
**Blocks:** Phase J (Web Console live gateway), Phase D (auth hardening) for
SSO flows only — Phase D's core work (SCRAM-SHA-256, roles, audit) is not
blocked by this ADR.

## Question

Where does OIDC / SSO integration live?

- **Option A — Gateway-only:** A reverse proxy or dedicated gateway service
  (e.g. nginx + oauth2-proxy, Envoy + ext_authz, or a custom AnalyticsDB
  gateway binary) handles OIDC token exchange and forwards resolved identity
  (user, roles) to the engine. The engine never speaks OIDC directly.
- **Option B — Engine-direct:** The AnalyticsDB server speaks OIDC / JWT
  validation directly. More self-contained but more auth code in the engine.

## Why This Is Deferred

The answer depends on whether we want the web console to go through a
dedicated gateway service or directly to the engine. That gateway vs. direct
architecture question has implications beyond auth (it also affects how the
web console connects, whether we do connection pooling at the gateway, and
how we handle multi-tenancy). Rather than pick one and bake it in silently,
we are deferring until the web console architecture is clearer.

## What Must Be Decided Before Phase J

1. Whether a gateway binary is a first-class component of the deployment
   model (Helm chart gets a `gateway` Deployment) or an optional integration
   shim.
2. Whether the web console connects to the engine via PG wire, Flight SQL, or
   a custom HTTP/WebSocket API through the gateway.
3. Which OIDC provider is the Phase J baseline (Google, Azure AD, Okta,
   generic OIDC).

## What Is Not Blocked

- Phase D core work: SCRAM-SHA-256, roles, groups, audit log. These are
  based on the engine's own user model and proceed regardless.
- Phase D bearer token auth for Flight SQL: uses the engine's user model
  directly, not OIDC.
- Phase H Kubernetes deployment: proceeds with SCRAM-SHA-256 PG auth; SSO
  is additive.

## Decision Trigger

Revisit this ADR when Phase H is `Complete` and Phase J planning begins.
At that point, prototype the web console gateway shape, pick Option A or B,
and update this document with the decision, rationale, and consequences.
