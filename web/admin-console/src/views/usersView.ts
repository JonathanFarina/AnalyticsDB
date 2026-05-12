import { icon } from "../icons";

interface UserRecord {
  readonly name: string;
  readonly email: string;
  readonly role: string;
  readonly groups: readonly string[];
  readonly status: "active" | "invited" | "disabled";
  readonly lastSeen: string;
}

const SAMPLE_USERS: readonly UserRecord[] = [
  {
    name: "Avery Lin",
    email: "avery.lin@analyticsdb.io",
    role: "Administrator",
    groups: ["platform", "oncall"],
    status: "active",
    lastSeen: "2 minutes ago",
  },
  {
    name: "Jordan Park",
    email: "jordan.park@analyticsdb.io",
    role: "Operator",
    groups: ["platform"],
    status: "active",
    lastSeen: "14 minutes ago",
  },
  {
    name: "Priya Shah",
    email: "priya.shah@analyticsdb.io",
    role: "Analyst",
    groups: ["reporting"],
    status: "active",
    lastSeen: "1 hour ago",
  },
  {
    name: "Mateo Alvarez",
    email: "mateo.alvarez@analyticsdb.io",
    role: "Analyst",
    groups: ["reporting", "growth"],
    status: "invited",
    lastSeen: "—",
  },
  {
    name: "Service: ingest-pipeline",
    email: "ingest-pipeline@svc.local",
    role: "Service",
    groups: ["ingest"],
    status: "active",
    lastSeen: "live",
  },
  {
    name: "Robin Cole",
    email: "robin.cole@analyticsdb.io",
    role: "Analyst",
    groups: ["reporting"],
    status: "disabled",
    lastSeen: "12 days ago",
  },
];

export function mountUsersView(container: HTMLElement): void {
  container.innerHTML = `
    <div class="page-header">
      <div class="page-header-title">
        <p class="page-pretitle">Access control</p>
        <h2 class="page-title">Users</h2>
      </div>
      <div class="page-header-actions">
        <button class="btn" type="button">${icon("refresh", 14)}<span>Refresh</span></button>
        <button class="btn btn-primary" type="button">${icon("plus", 14)}<span>Invite user</span></button>
      </div>
    </div>

    <div class="page-body">
      <div class="row-cards row-cards-3">
        ${metricCard("Total users", String(SAMPLE_USERS.length))}
        ${metricCard("Active", String(SAMPLE_USERS.filter((user) => user.status === "active").length))}
        ${metricCard("Pending invites", String(SAMPLE_USERS.filter((user) => user.status === "invited").length))}
      </div>

      <div class="card">
        <div class="card-header">
          <h3 class="card-title">All users</h3>
          <div class="card-actions">
            <div class="input-icon input-icon-compact">
              ${icon("search", 14)}
              <input type="search" class="form-control" placeholder="Search users…" />
            </div>
          </div>
        </div>
        <div class="table-wrap">
          <table class="data-grid data-grid-roomy">
            <thead>
              <tr>
                <th>Name</th>
                <th>Role</th>
                <th>Groups</th>
                <th>Status</th>
                <th>Last seen</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              ${SAMPLE_USERS.map(renderUserRow).join("")}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  `;
}

function renderUserRow(user: UserRecord): string {
  const initials = user.name
    .split(/\s+/)
    .map((part) => part[0]?.toUpperCase())
    .filter(Boolean)
    .slice(0, 2)
    .join("");

  const groupBadges = user.groups
    .map((group) => `<span class="badge badge-soft">${group}</span>`)
    .join(" ");

  return `
    <tr>
      <td>
        <div class="user-cell">
          <span class="avatar avatar-sm">${initials}</span>
          <div>
            <div class="user-name">${user.name}</div>
            <div class="user-email">${user.email}</div>
          </div>
        </div>
      </td>
      <td>${user.role}</td>
      <td><div class="badge-row">${groupBadges}</div></td>
      <td>${statusPill(user.status)}</td>
      <td class="text-muted">${user.lastSeen}</td>
      <td class="row-actions">
        <button class="btn btn-ghost" type="button">Manage</button>
      </td>
    </tr>
  `;
}

function statusPill(status: UserRecord["status"]): string {
  const variant =
    status === "active"
      ? "status-pill-success"
      : status === "invited"
        ? "status-pill-warning"
        : "status-pill-muted";
  return `<span class="status-pill ${variant}"><span class="status-dot"></span>${capitalize(status)}</span>`;
}

function metricCard(label: string, value: string): string {
  return `
    <div class="card metric-card">
      <div class="card-body">
        <div class="metric-label">${label}</div>
        <div class="metric-value">${value}</div>
      </div>
    </div>
  `;
}

function capitalize(value: string): string {
  return value.charAt(0).toUpperCase() + value.slice(1);
}
