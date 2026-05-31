import { icon } from "../icons";
import { liveClient, type AdminGroup, type AdminUser } from "../liveClient";

export function mountGroupsView(container: HTMLElement): void {
  container.innerHTML = `
    <div class="page-header">
      <div class="page-header-title">
        <p class="page-pretitle">Access control</p>
        <h2 class="page-title">Groups</h2>
      </div>
      <div class="page-header-actions">
        <button class="btn" type="button" id="groups-refresh">${icon("refresh", 14)}<span>Refresh</span></button>
        <button class="btn btn-primary" type="button" id="groups-create">${icon("plus", 14)}<span>New group</span></button>
      </div>
    </div>
    <div class="page-body" id="groups-body">
      ${renderLoading()}
    </div>
  `;

  const body = container.querySelector<HTMLElement>("#groups-body");
  const refresh = container.querySelector<HTMLButtonElement>("#groups-refresh");
  const create = container.querySelector<HTMLButtonElement>("#groups-create");
  if (!body || !refresh || !create) {
    return;
  }

  refresh.addEventListener("click", () => void load(body));
  create.addEventListener("click", () => void createGroup(body));

  void load(body);
}

let knownUsers: readonly AdminUser[] = [];

async function load(body: HTMLElement): Promise<void> {
  body.innerHTML = renderLoading();
  try {
    const [groups, users] = await Promise.all([
      liveClient.listGroups(),
      liveClient.listAdminUsers(),
    ]);
    knownUsers = users;
    renderGroups(body, groups);
  } catch (error) {
    body.innerHTML = renderError(error);
  }
}

function renderGroups(body: HTMLElement, groups: readonly AdminGroup[]): void {
  const totalMembers = groups.reduce((sum, g) => sum + g.member_count, 0);

  body.innerHTML = `
    <div class="row-cards row-cards-3">
      ${metricCard("Groups", String(groups.length))}
      ${metricCard("Total memberships", String(totalMembers))}
      ${metricCard("Users", String(knownUsers.length))}
    </div>

    <div class="row-cards row-cards-2">
      ${
        groups.length === 0
          ? `<div class="card"><div class="card-body"><span class="text-muted">No groups found.</span></div></div>`
          : groups.map(renderGroupCard).join("")
      }
    </div>
  `;

  body.querySelectorAll<HTMLButtonElement>("[data-action]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const action = btn.dataset.action;
      const group = btn.dataset.group ?? "";
      const user = btn.dataset.user;
      if (action === "delete") {
        void deleteGroup(body, group);
      } else if (action === "add-member") {
        void addMember(body, group);
      } else if (action === "remove-member" && user) {
        void removeMember(body, group, user);
      }
    });
  });
}

function renderGroupCard(group: AdminGroup): string {
  const members =
    group.members.length > 0
      ? group.members
          .map(
            (m) => `
            <span class="badge badge-soft">
              ${escapeHtml(m)}
              <button class="badge-remove" type="button" title="Remove from group"
                data-action="remove-member" data-group="${escapeHtml(group.name)}" data-user="${escapeHtml(m)}">×</button>
            </span>`,
          )
          .join(" ")
      : `<span class="text-muted">No members</span>`;

  return `
    <div class="card">
      <div class="card-header">
        <div>
          <h3 class="card-title">${escapeHtml(group.name)}</h3>
        </div>
        <span class="status-pill status-pill-muted">
          ${icon("users", 12)}
          ${group.member_count} members
        </span>
      </div>
      <div class="card-body">
        <dl class="detail-grid detail-grid-flush">
          <dt>Members</dt>
          <dd><div class="badge-row">${members}</div></dd>
        </dl>
      </div>
      <div class="card-footer">
        <button class="btn btn-ghost" type="button" data-action="add-member" data-group="${escapeHtml(group.name)}">Add member</button>
        <button class="btn btn-ghost" type="button" data-action="delete" data-group="${escapeHtml(group.name)}">Delete group</button>
      </div>
    </div>
  `;
}

async function createGroup(body: HTMLElement): Promise<void> {
  const name = window.prompt("New group name:");
  if (!name) {
    return;
  }
  try {
    await liveClient.createGroup(name);
    await load(body);
  } catch (error) {
    window.alert(formatError(error));
  }
}

async function deleteGroup(body: HTMLElement, name: string): Promise<void> {
  if (!window.confirm(`Delete group '${name}'? It must have no members.`)) {
    return;
  }
  try {
    await liveClient.dropGroup(name);
    await load(body);
  } catch (error) {
    window.alert(formatError(error));
  }
}

async function addMember(body: HTMLElement, group: string): Promise<void> {
  const candidates = knownUsers.map((u) => u.name).join(", ");
  const user = window.prompt(
    `Add which user to '${group}'?${candidates ? `\n\nKnown users: ${candidates}` : ""}`,
  );
  if (!user) {
    return;
  }
  try {
    await liveClient.addGroupMember(group, user);
    await load(body);
  } catch (error) {
    window.alert(formatError(error));
  }
}

async function removeMember(
  body: HTMLElement,
  group: string,
  user: string,
): Promise<void> {
  if (!window.confirm(`Remove '${user}' from '${group}'?`)) {
    return;
  }
  try {
    await liveClient.removeGroupMember(group, user);
    await load(body);
  } catch (error) {
    window.alert(formatError(error));
  }
}

function metricCard(label: string, value: string): string {
  return `
    <div class="card metric-card">
      <div class="card-body">
        <div class="metric-label">${escapeHtml(label)}</div>
        <div class="metric-value">${escapeHtml(value)}</div>
      </div>
    </div>
  `;
}

function renderLoading(): string {
  return `<div class="card"><div class="card-body"><div class="loading-card">Loading groups…</div></div></div>`;
}

function renderError(error: unknown): string {
  return `
    <div class="card">
      <div class="card-body">
        <div class="message message-error">
          ${icon("x-circle", 14)}
          <span>Could not load groups: ${escapeHtml(formatError(error))}. Sign in as an administrator and confirm the gateway is running.</span>
        </div>
      </div>
    </div>
  `;
}

function formatError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
