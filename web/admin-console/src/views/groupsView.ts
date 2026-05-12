import { icon } from "../icons";

interface GroupRecord {
  readonly name: string;
  readonly description: string;
  readonly members: number;
  readonly databases: readonly string[];
  readonly privileges: readonly string[];
}

const SAMPLE_GROUPS: readonly GroupRecord[] = [
  {
    name: "platform",
    description: "Engine operators with cluster-wide write privileges.",
    members: 4,
    databases: ["postgres", "analytics"],
    privileges: ["read", "write", "ddl", "admin"],
  },
  {
    name: "oncall",
    description: "Rotating responders with break-glass access during incidents.",
    members: 6,
    databases: ["postgres", "analytics"],
    privileges: ["read", "write"],
  },
  {
    name: "reporting",
    description: "Analysts who own warehouse reporting models.",
    members: 12,
    databases: ["analytics"],
    privileges: ["read"],
  },
  {
    name: "growth",
    description: "Cross-functional growth squad with sandboxed schemas.",
    members: 5,
    databases: ["analytics"],
    privileges: ["read", "write"],
  },
  {
    name: "ingest",
    description: "Service accounts that stream events into the engine.",
    members: 3,
    databases: ["postgres"],
    privileges: ["write"],
  },
];

export function mountGroupsView(container: HTMLElement): void {
  container.innerHTML = `
    <div class="page-header">
      <div class="page-header-title">
        <p class="page-pretitle">Access control</p>
        <h2 class="page-title">Groups</h2>
      </div>
      <div class="page-header-actions">
        <button class="btn btn-primary" type="button">${icon("plus", 14)}<span>New group</span></button>
      </div>
    </div>

    <div class="page-body">
      <div class="row-cards row-cards-3">
        ${metricCard("Groups", String(SAMPLE_GROUPS.length))}
        ${metricCard("Total members", String(SAMPLE_GROUPS.reduce((total, group) => total + group.members, 0)))}
        ${metricCard("Databases covered", "2")}
      </div>

      <div class="row-cards row-cards-2">
        ${SAMPLE_GROUPS.map(renderGroupCard).join("")}
      </div>
    </div>
  `;
}

function renderGroupCard(group: GroupRecord): string {
  const privilegeBadges = group.privileges
    .map((privilege) => `<span class="badge badge-soft">${privilege}</span>`)
    .join(" ");
  const databaseBadges = group.databases
    .map((database) => `<span class="badge badge-outline">${database}</span>`)
    .join(" ");

  return `
    <div class="card">
      <div class="card-header">
        <div>
          <h3 class="card-title">${group.name}</h3>
          <p class="card-subtitle">${group.description}</p>
        </div>
        <span class="status-pill status-pill-muted">
          ${icon("users", 12)}
          ${group.members} members
        </span>
      </div>
      <div class="card-body">
        <dl class="detail-grid detail-grid-flush">
          <dt>Privileges</dt>
          <dd><div class="badge-row">${privilegeBadges}</div></dd>
          <dt>Databases</dt>
          <dd><div class="badge-row">${databaseBadges}</div></dd>
        </dl>
      </div>
      <div class="card-footer">
        <button class="btn btn-ghost" type="button">Edit members</button>
        <button class="btn btn-ghost" type="button">Edit privileges</button>
      </div>
    </div>
  `;
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
