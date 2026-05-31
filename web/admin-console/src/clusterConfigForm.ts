import type { ClusterConfig, QueryLogConfig } from "./adminClient";

export const DEFAULT_QUERY_LOG: QueryLogConfig = {
  enabled: true,
  sample_rate: 1.0,
  min_duration_ms: 0,
  batch_size: 1024,
  batch_interval_ms: 5000,
  max_query_length_bytes: 65536,
  retention_days: 30,
};

/**
 * Build the payload that would actually be written to cluster-config.json.
 *
 * Fields are emitted conservatively so we never silently add keys that the
 * on-disk file did not already contain:
 *   - required fields are always sent
 *   - optional string fields (TLS paths) are sent as `null` when empty
 *   - `base_node_port` is sent when set by the user; if the baseline had
 *     a value and the user cleared it, we send `null` to surface the intent
 *   - `query_log` is sent only if the baseline had it OR the draft differs
 *     from the engine defaults — that way saving an unedited form keeps the
 *     file as minimal as it was
 */
export function buildSavePayload(
  draft: ClusterConfig,
  baseline: ClusterConfig,
): ClusterConfig {
  const payload: ClusterConfig = {
    base_postgres_port: toFiniteNumber(draft.base_postgres_port, baseline.base_postgres_port),
    base_flight_sql_port: toFiniteNumber(
      draft.base_flight_sql_port,
      baseline.base_flight_sql_port,
    ),
    catalog_path: String(draft.catalog_path ?? baseline.catalog_path ?? ""),
    next_available_port_offset: toFiniteNumber(
      draft.next_available_port_offset,
      baseline.next_available_port_offset,
    ),
  };

  if (draft.base_node_port !== undefined && draft.base_node_port !== null) {
    payload.base_node_port = toFiniteNumber(draft.base_node_port, baseline.base_node_port);
  } else if (baseline.base_node_port !== undefined && baseline.base_node_port !== null) {
    payload.base_node_port = null;
  }

  const tlsCert = emptyToNull(draft.tls_cert_path);
  if (tlsCert !== null || baseline.tls_cert_path !== undefined) {
    payload.tls_cert_path = tlsCert;
  }
  const tlsKey = emptyToNull(draft.tls_key_path);
  if (tlsKey !== null || baseline.tls_key_path !== undefined) {
    payload.tls_key_path = tlsKey;
  }

  const jwtSecret = emptyToNull(draft.jwt_secret);
  if (jwtSecret !== null || baseline.jwt_secret !== undefined) {
    payload.jwt_secret = jwtSecret;
  }

  const baselineHadQueryLog = baseline.query_log !== undefined;
  const draftQueryLog = draft.query_log;
  if (baselineHadQueryLog) {
    payload.query_log = normaliseQueryLog(draftQueryLog ?? baseline.query_log ?? DEFAULT_QUERY_LOG);
  } else if (draftQueryLog && !queryLogEqualsDefault(draftQueryLog)) {
    payload.query_log = normaliseQueryLog(draftQueryLog);
  }

  return payload;
}

/** Engine defaults applied for display when the file omits `query_log`. */
export function withDisplayDefaults(config: ClusterConfig): ClusterConfig {
  return {
    ...config,
    query_log: config.query_log ?? { ...DEFAULT_QUERY_LOG },
  };
}

/**
 * Dirty iff the payload we would PUT differs from the file we last loaded.
 *
 * Using the would-be payload (rather than the raw draft) means transient UI
 * state — for example, surfacing engine defaults for missing `query_log`
 * fields — does not register as a pending change.
 */
export function isDirty(loaded: ClusterConfig, draft: ClusterConfig): boolean {
  const payload = buildSavePayload(draft, loaded);
  return !configsEqual(loaded, payload);
}

export function configsEqual(a: ClusterConfig, b: ClusterConfig): boolean {
  return canonicalJson(a) === canonicalJson(b);
}

function canonicalJson(value: ClusterConfig): string {
  return JSON.stringify(value, sortedReplacer);
}

function sortedReplacer(_key: string, value: unknown): unknown {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    const sorted: Record<string, unknown> = {};
    for (const key of Object.keys(value as Record<string, unknown>).sort()) {
      sorted[key] = (value as Record<string, unknown>)[key];
    }
    return sorted;
  }
  return value;
}

export function normaliseQueryLog(value: QueryLogConfig): QueryLogConfig {
  return {
    enabled: Boolean(value.enabled),
    sample_rate: clampFloat(value.sample_rate, 0, 1, DEFAULT_QUERY_LOG.sample_rate),
    min_duration_ms: clampNonNegInt(value.min_duration_ms, DEFAULT_QUERY_LOG.min_duration_ms),
    batch_size: clampNonNegInt(value.batch_size, DEFAULT_QUERY_LOG.batch_size),
    batch_interval_ms: clampNonNegInt(
      value.batch_interval_ms,
      DEFAULT_QUERY_LOG.batch_interval_ms,
    ),
    max_query_length_bytes: clampNonNegInt(
      value.max_query_length_bytes,
      DEFAULT_QUERY_LOG.max_query_length_bytes,
    ),
    retention_days: clampNonNegInt(value.retention_days, DEFAULT_QUERY_LOG.retention_days),
  };
}

function queryLogEqualsDefault(value: QueryLogConfig): boolean {
  const normalised = normaliseQueryLog(value);
  const keys = Object.keys(DEFAULT_QUERY_LOG) as Array<keyof QueryLogConfig>;
  return keys.every((key) => normalised[key] === DEFAULT_QUERY_LOG[key]);
}

function toFiniteNumber(value: unknown, fallback: unknown): number {
  const candidate = typeof value === "number" ? value : Number(value);
  if (Number.isFinite(candidate)) {
    return candidate;
  }
  const fallbackNumber =
    typeof fallback === "number" ? fallback : Number(fallback);
  return Number.isFinite(fallbackNumber) ? fallbackNumber : 0;
}

export function clampFloat(value: unknown, min: number, max: number, fallback: number): number {
  const numeric = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }
  return Math.min(Math.max(numeric, min), max);
}

export function clampNonNegInt(value: unknown, fallback: number): number {
  const numeric = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numeric) || numeric < 0) {
    return fallback;
  }
  return Math.floor(numeric);
}

export function emptyToNull(value: string | null | undefined): string | null {
  if (value === undefined || value === null) {
    return null;
  }
  const trimmed = String(value).trim();
  return trimmed === "" ? null : trimmed;
}

export function getPath(source: ClusterConfig, key: string): unknown {
  const parts = key.split(".");
  let cursor: unknown = source;
  for (const part of parts) {
    if (cursor === undefined || cursor === null || typeof cursor !== "object") {
      return undefined;
    }
    cursor = (cursor as Record<string, unknown>)[part];
  }
  return cursor;
}

export function setPath(target: ClusterConfig, key: string, value: unknown): void {
  const parts = key.split(".");
  if (parts.length === 1) {
    (target as unknown as Record<string, unknown>)[parts[0]] = value;
    return;
  }

  let cursor = target as unknown as Record<string, unknown>;
  for (let index = 0; index < parts.length - 1; index += 1) {
    const part = parts[index];
    const existing = cursor[part];
    if (existing === undefined || existing === null || typeof existing !== "object") {
      cursor[part] = {};
    }
    cursor = cursor[part] as Record<string, unknown>;
  }
  cursor[parts[parts.length - 1]] = value;
}
