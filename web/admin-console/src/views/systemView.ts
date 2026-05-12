interface NodeRecord {
  readonly hostname: string;
  readonly role: "coordinator" | "worker";
  readonly version: string;
  readonly status: "healthy" | "degraded" | "offline";
  readonly cpuPercent: number;
  readonly memoryGb: number;
}

const SAMPLE_NODES: readonly NodeRecord[] = [
  {
    hostname: "adb-coord-1",
    role: "coordinator",
    version: "0.1.0",
    status: "healthy",
    cpuPercent: 14,
    memoryGb: 12.3,
  },
  {
    hostname: "adb-worker-1",
    role: "worker",
    version: "0.1.0",
    status: "healthy",
    cpuPercent: 38,
    memoryGb: 21.7,
  },
  {
    hostname: "adb-worker-2",
    role: "worker",
    version: "0.1.0",
    status: "healthy",
    cpuPercent: 26,
    memoryGb: 18.9,
  },
  {
    hostname: "adb-worker-3",
    role: "worker",
    version: "0.0.9",
    status: "degraded",
    cpuPercent: 72,
    memoryGb: 28.1,
  },
];

export function mountSystemView(container: HTMLElement): void {
  container.innerHTML = `
    <div class="page-header">
      <div class="page-header-title">
        <p class="page-pretitle">Diagnostics</p>
        <h2 class="page-title">System Information</h2>
      </div>
      <div class="page-header-actions">
        <span class="status-pill status-pill-success">
          <span class="status-dot"></span>
          Cluster healthy
        </span>
      </div>
    </div>

    <div class="page-body">
      <div class="row-cards row-cards-4">
        ${metricCard("Version", "0.1.0", "Engine release running on coordinator.")}
        ${metricCard("Uptime", "6d 12h", "Since last coordinator restart.")}
        ${metricCard("Active sessions", "27", "Across PostgreSQL and Flight SQL.")}
        ${metricCard("Catalog backend", "SQLite", "cluster-catalog.managed/catalog.sqlite")}
      </div>

      <div class="settings-grid">
        <section class="card">
          <div class="card-header">
            <h3 class="card-title">Build information</h3>
          </div>
          <div class="card-body">
            <dl class="detail-grid detail-grid-flush">
              <dt>Version</dt><dd>0.1.0</dd>
              <dt>Commit</dt><dd>a5bb733</dd>
              <dt>Built on</dt><dd>2026-05-09</dd>
              <dt>Rust toolchain</dt><dd>1.84.0 (stable)</dd>
              <dt>Arrow Flight SQL</dt><dd>enabled</dd>
              <dt>PostgreSQL wire</dt><dd>enabled (port 5433)</dd>
            </dl>
          </div>
        </section>

        <section class="card">
          <div class="card-header">
            <h3 class="card-title">Runtime</h3>
          </div>
          <div class="card-body">
            <dl class="detail-grid detail-grid-flush">
              <dt>Cluster id</dt><dd>adb-prototype-01</dd>
              <dt>Coordinator host</dt><dd>adb-coord-1.local</dd>
              <dt>Workers online</dt><dd>3 of 3</dd>
              <dt>TLS</dt><dd>enabled (certs/server.pem)</dd>
              <dt>Catalog</dt><dd>SQLite (default)</dd>
              <dt>Storage</dt><dd>local + external S3</dd>
            </dl>
          </div>
        </section>
      </div>

      <div class="card">
        <div class="card-header">
          <h3 class="card-title">Cluster nodes</h3>
          <span class="badge badge-soft">${SAMPLE_NODES.length} nodes</span>
        </div>
        <div class="table-wrap">
          <table class="data-grid data-grid-roomy">
            <thead>
              <tr>
                <th>Host</th>
                <th>Role</th>
                <th>Version</th>
                <th>Status</th>
                <th>CPU</th>
                <th>Memory</th>
              </tr>
            </thead>
            <tbody>
              ${SAMPLE_NODES.map(renderNodeRow).join("")}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  `;
}

function renderNodeRow(node: NodeRecord): string {
  return `
    <tr>
      <td><strong>${node.hostname}</strong></td>
      <td><span class="badge badge-outline">${node.role}</span></td>
      <td class="text-muted">${node.version}</td>
      <td>${statusPill(node.status)}</td>
      <td>${renderBar(node.cpuPercent, `${node.cpuPercent}%`)}</td>
      <td class="text-muted">${node.memoryGb.toFixed(1)} GB</td>
    </tr>
  `;
}

function renderBar(percent: number, label: string): string {
  const tone = percent > 70 ? "bar-warning" : percent > 50 ? "bar-accent" : "bar-success";
  return `
    <div class="cell-bar">
      <div class="cell-bar-track">
        <div class="cell-bar-fill ${tone}" style="width: ${Math.min(percent, 100)}%"></div>
      </div>
      <span class="cell-bar-label">${label}</span>
    </div>
  `;
}

function statusPill(status: NodeRecord["status"]): string {
  const variant =
    status === "healthy"
      ? "status-pill-success"
      : status === "degraded"
        ? "status-pill-warning"
        : "status-pill-danger";
  return `<span class="status-pill ${variant}"><span class="status-dot"></span>${capitalize(status)}</span>`;
}

function metricCard(label: string, value: string, hint?: string): string {
  return `
    <div class="card metric-card">
      <div class="card-body">
        <div class="metric-label">${label}</div>
        <div class="metric-value">${value}</div>
        ${hint ? `<div class="metric-hint">${hint}</div>` : ""}
      </div>
    </div>
  `;
}

function capitalize(value: string): string {
  return value.charAt(0).toUpperCase() + value.slice(1);
}
