import {
  fetchClusterCatalog,
  fetchClusterConfig,
  type ClusterCatalog,
  type ClusterCatalogEnvelope,
  type ClusterConfigEnvelope,
  type ClusterNode,
} from "../adminClient";
import { icon } from "../icons";

interface SystemViewModel {
  readonly config: ClusterConfigEnvelope;
  readonly catalog: ClusterCatalogEnvelope;
}

export function mountSystemView(container: HTMLElement): void {
  container.innerHTML = `
    <div class="page-header">
      <div class="page-header-title">
        <p class="page-pretitle">Diagnostics</p>
        <h2 class="page-title">System Information</h2>
      </div>
      <div class="page-header-actions">
        <button class="btn" type="button" id="system-refresh">${icon("refresh", 14)}<span>Refresh</span></button>
      </div>
    </div>
    <div class="page-body" id="system-body">
      ${renderLoading()}
    </div>
  `;

  const refresh = container.querySelector<HTMLButtonElement>("#system-refresh");
  const body = container.querySelector<HTMLElement>("#system-body");
  if (!refresh || !body) {
    return;
  }

  refresh.addEventListener("click", () => {
    void load(body);
  });

  void load(body);
}

async function load(body: HTMLElement): Promise<void> {
  body.innerHTML = renderLoading();
  try {
    const [config, catalog] = await Promise.all([
      fetchClusterConfig(),
      fetchClusterCatalog(),
    ]);
    renderSystem(body, { config, catalog });
  } catch (error) {
    body.innerHTML = renderError(error);
  }
}

function renderSystem(body: HTMLElement, model: SystemViewModel): void {
  const config = model.config.config;
  const catalog = model.catalog.catalog;
  const nodes = Object.values(catalog.nodes ?? {});
  const tlsEnabled = Boolean(config.tls_cert_path && config.tls_key_path);
  const updatedAt = model.catalog.modifiedAtEpochMs
    ? new Date(model.catalog.modifiedAtEpochMs).toLocaleString()
    : "—";

  body.innerHTML = `
    <div class="row-cards row-cards-4">
      ${metricCard("Catalogue version", String(catalog.catalogue_version ?? 0), "Bumped on every persisted write.")}
      ${metricCard("Databases", String(Object.keys(catalog.databases ?? {}).length), Object.keys(catalog.databases ?? {}).join(", ") || "—")}
      ${metricCard("Users", String(Object.keys(catalog.users ?? {}).length), `${adminCount(catalog)} admin · ${nonAdminCount(catalog)} standard`)}
      ${metricCard("Nodes registered", String(nodes.length), nodes.length === 0 ? "Coordinator only (single-node)" : `${countByStatus(nodes, "Ready")} ready`)}
    </div>

    <div class="settings-grid">
      <section class="card">
        <div class="card-header">
          <h3 class="card-title">Wire protocols</h3>
          <span class="badge ${tlsEnabled ? "badge-soft" : "badge-outline"}">${tlsEnabled ? "TLS enabled" : "TLS disabled"}</span>
        </div>
        <div class="card-body">
          <dl class="detail-grid detail-grid-flush">
            <dt>PostgreSQL port</dt><dd>${escapeHtml(String(config.base_postgres_port))}</dd>
            <dt>Arrow Flight SQL port</dt><dd>${escapeHtml(String(config.base_flight_sql_port))}</dd>
            <dt>Internal node port</dt><dd>${escapeHtml(String(config.base_node_port ?? "—"))}</dd>
            <dt>Next port offset</dt><dd>${escapeHtml(String(config.next_available_port_offset))}</dd>
            <dt>TLS certificate</dt><dd>${renderOptionalPath(config.tls_cert_path)}</dd>
            <dt>TLS key</dt><dd>${renderOptionalPath(config.tls_key_path)}</dd>
            <dt>JWT signing key</dt><dd>${config.jwt_secret ? "Configured (custom)" : `<span class="text-muted">Ephemeral</span>`}</dd>
          </dl>
        </div>
      </section>

      <section class="card">
        <div class="card-header">
          <h3 class="card-title">Storage &amp; catalog</h3>
          <span class="badge badge-outline">${escapeHtml(catalogBackendLabel(config.catalog_path))}</span>
        </div>
        <div class="card-body">
          <dl class="detail-grid detail-grid-flush">
            <dt>Catalog backend</dt><dd>${escapeHtml(catalogBackendLabel(config.catalog_path))}</dd>
            <dt>Catalog path</dt><dd>${escapeHtml(config.catalog_path)}</dd>
            <dt>Config file</dt><dd>${escapeHtml(model.config.path)}</dd>
            <dt>Catalogue file</dt><dd>${escapeHtml(model.catalog.path)}</dd>
            <dt>Catalogue updated</dt><dd>${escapeHtml(updatedAt)}</dd>
            <dt>Relations tracked</dt><dd>${escapeHtml(String(Object.keys(catalog.relations ?? {}).length))}</dd>
          </dl>
        </div>
      </section>
    </div>

    <section class="card">
      <div class="card-header">
        <h3 class="card-title">Databases</h3>
        <span class="badge badge-soft">${Object.keys(catalog.databases ?? {}).length}</span>
      </div>
      <div class="table-wrap">
        <table class="data-grid data-grid-roomy">
          <thead>
            <tr>
              <th>Name</th>
              <th>Owner</th>
              <th>Schemas</th>
              <th>Parameters</th>
            </tr>
          </thead>
          <tbody>
            ${renderDatabaseRows(catalog)}
          </tbody>
        </table>
      </div>
    </section>

    <section class="card">
      <div class="card-header">
        <h3 class="card-title">Cluster nodes</h3>
        <span class="badge badge-soft">${nodes.length}</span>
      </div>
      ${nodes.length === 0
        ? `<div class="card-body"><div class="empty-state">No nodes have registered with this coordinator yet. Single-node deployments will report zero entries here.</div></div>`
        : `
          <div class="table-wrap">
            <table class="data-grid data-grid-roomy">
              <thead>
                <tr>
                  <th>Id</th>
                  <th>Role</th>
                  <th>Endpoint</th>
                  <th>Status</th>
                  <th>Last heartbeat</th>
                </tr>
              </thead>
              <tbody>
                ${nodes.map(renderNodeRow).join("")}
              </tbody>
            </table>
          </div>`}
    </section>
  `;
}

function renderDatabaseRows(catalog: ClusterCatalog): string {
  const databases = Object.values(catalog.databases ?? {});
  if (databases.length === 0) {
    return `<tr><td colspan="4"><div class="empty-state">No databases registered.</div></td></tr>`;
  }
  return databases
    .map(
      (database) => `
        <tr>
          <td><strong>${escapeHtml(database.name)}</strong></td>
          <td class="text-muted">${escapeHtml(database.owner || "—")}</td>
          <td>
            <div class="badge-row">
              ${(database.schemas ?? [])
                .map((schema) => `<span class="badge badge-soft">${escapeHtml(schema)}</span>`)
                .join(" ") || `<span class="text-muted">—</span>`}
            </div>
          </td>
          <td class="text-muted">${renderParameters(database.parameters)}</td>
        </tr>`,
    )
    .join("");
}

function renderParameters(parameters: Readonly<Record<string, string>>): string {
  const entries = Object.entries(parameters ?? {});
  if (entries.length === 0) {
    return "—";
  }
  return entries
    .map(([key, value]) => `<code>${escapeHtml(key)}=${escapeHtml(value)}</code>`)
    .join(" ");
}

function renderNodeRow(node: ClusterNode): string {
  const heartbeat = node.last_heartbeat_at_epoch_ms
    ? new Date(node.last_heartbeat_at_epoch_ms).toLocaleString()
    : "—";
  return `
    <tr>
      <td><strong>${escapeHtml(node.id)}</strong></td>
      <td><span class="badge badge-outline">${escapeHtml(String(node.role))}</span></td>
      <td class="text-muted">${escapeHtml(node.endpoint)}</td>
      <td>${statusPill(node.status)}</td>
      <td class="text-muted">${escapeHtml(heartbeat)}</td>
    </tr>
  `;
}

function statusPill(status: ClusterNode["status"]): string {
  const normalized = String(status);
  const variant =
    normalized === "Ready"
      ? "status-pill-success"
      : normalized === "Unavailable"
        ? "status-pill-danger"
        : "status-pill-muted";
  return `<span class="status-pill ${variant}"><span class="status-dot"></span>${escapeHtml(normalized)}</span>`;
}

function metricCard(label: string, value: string, hint?: string): string {
  return `
    <div class="card metric-card">
      <div class="card-body">
        <div class="metric-label">${escapeHtml(label)}</div>
        <div class="metric-value">${escapeHtml(value)}</div>
        ${hint ? `<div class="metric-hint">${escapeHtml(hint)}</div>` : ""}
      </div>
    </div>
  `;
}

function renderOptionalPath(path: string | null | undefined): string {
  if (!path) {
    return `<span class="text-muted">not configured</span>`;
  }
  return escapeHtml(path);
}

function catalogBackendLabel(catalogPath: string): string {
  if (catalogPath.endsWith(".sqlite")) {
    return "SQLite";
  }
  if (catalogPath.endsWith(".db")) {
    return "SQLite";
  }
  if (catalogPath.endsWith(".json")) {
    return "JSON file";
  }
  return "Custom";
}

function adminCount(catalog: ClusterCatalog): number {
  return Object.values(catalog.users ?? {}).filter((user) => user.is_admin).length;
}

function nonAdminCount(catalog: ClusterCatalog): number {
  return Object.values(catalog.users ?? {}).filter((user) => !user.is_admin).length;
}

function countByStatus(nodes: readonly ClusterNode[], status: string): number {
  return nodes.filter((node) => String(node.status) === status).length;
}

function renderLoading(): string {
  return `<div class="card"><div class="card-body"><div class="loading-card">Loading system information from the cluster catalog…</div></div></div>`;
}

function renderError(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return `
    <div class="card">
      <div class="card-body">
        <div class="message message-error">
          ${icon("x-circle", 14)}
          <span>Could not load system information: ${escapeHtml(message)}. Confirm the dev server is running (npm run dev) so the cluster admin API is mounted.</span>
        </div>
      </div>
    </div>
  `;
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
