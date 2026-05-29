export interface QueryLogConfig {
  enabled: boolean;
  sample_rate: number;
  min_duration_ms: number;
  batch_size: number;
  batch_interval_ms: number;
  max_query_length_bytes: number;
  retention_days: number;
}

export interface ClusterConfig {
  base_postgres_port: number;
  base_flight_sql_port: number;
  base_node_port?: number | null;
  catalog_path: string;
  tls_cert_path?: string | null;
  tls_key_path?: string | null;
  next_available_port_offset: number;
  query_log?: QueryLogConfig;
  jwt_secret?: string | null;
}

export interface ClusterConfigEnvelope {
  readonly path: string;
  readonly config: ClusterConfig;
}

export type NodeRole = "Control" | "Coordinator" | "Compute" | string;
export type NodeStatus = "Ready" | "Unavailable" | string;

export interface ClusterNode {
  readonly id: string;
  readonly role: NodeRole;
  readonly endpoint: string;
  readonly internal_endpoint?: string | null;
  readonly status: NodeStatus;
  readonly last_heartbeat_at_epoch_ms?: number;
}

export interface CatalogDatabase {
  readonly name: string;
  readonly schemas: readonly string[];
  readonly owner: string;
  readonly parameters: Readonly<Record<string, string>>;
}

export interface CatalogUser {
  readonly name: string;
  readonly is_admin: boolean;
  readonly password_version?: number;
  readonly password_rotated_at_epoch_ms?: number;
  readonly members?: readonly string[];
}

export interface ClusterCatalog {
  readonly catalogue_version: number;
  readonly databases: Readonly<Record<string, CatalogDatabase>>;
  readonly users: Readonly<Record<string, CatalogUser>>;
  readonly nodes: Readonly<Record<string, ClusterNode>>;
  readonly relations: Readonly<Record<string, unknown>>;
  readonly aggregates?: Readonly<Record<string, unknown>>;
  readonly collations?: Readonly<Record<string, unknown>>;
  readonly conversions?: Readonly<Record<string, unknown>>;
  readonly functions?: Readonly<Record<string, unknown>>;
  readonly config?: ClusterConfig;
}

export interface ClusterCatalogEnvelope {
  readonly path: string;
  readonly catalog: ClusterCatalog;
  readonly modifiedAtEpochMs?: number;
  readonly source?: "sqlite" | "json-legacy";
}

export async function fetchClusterConfig(): Promise<ClusterConfigEnvelope> {
  return getJson<ClusterConfigEnvelope>("/api/cluster-config");
}

export async function fetchClusterCatalog(): Promise<ClusterCatalogEnvelope> {
  return getJson<ClusterCatalogEnvelope>("/api/cluster-catalog");
}

export async function saveClusterConfig(
  config: ClusterConfig,
): Promise<ClusterConfigEnvelope> {
  const response = await fetch("/api/cluster-config", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
  });
  if (!response.ok) {
    throw new Error(await formatError(response, "save cluster config"));
  }
  return (await response.json()) as ClusterConfigEnvelope;
}

async function getJson<T>(url: string): Promise<T> {
  const response = await fetch(url, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    throw new Error(await formatError(response, `GET ${url}`));
  }
  return (await response.json()) as T;
}

async function formatError(response: Response, action: string): Promise<string> {
  let detail = "";
  try {
    const body = await response.json();
    if (body && typeof body === "object" && "error" in body) {
      detail = String((body as { error: unknown }).error);
    }
  } catch {
    // ignore — error body not JSON
  }
  const suffix = detail ? `: ${detail}` : "";
  return `Failed to ${action} (${response.status} ${response.statusText})${suffix}`;
}
