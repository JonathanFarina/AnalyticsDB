import { icon } from "../icons";
import { liveClient, type AdminUser, type AdminGroup } from "../liveClient";

const ADMINISTRATORS_GROUP = "Administrators";

export function mountUsersView(container: HTMLElement): void {
  container.innerHTML = `
    <div class="page-header">
      <div class="page-header-title">
        <p class="page-pretitle">Access control</p>
        <h2 class="page-title">Users</h2>
      </div>
      <div class="page-header-actions">
        <button class="btn" type="button" id="users-refresh">${icon("refresh", 14)}<span>Refresh</span></button>
        <button class="btn btn-primary" type="button" id="users-create">${icon("plus", 14)}<span>New user</span></button>
      </div>
    </div>
    <div class="page-body" id="users-body">
      ${renderLoading()}
    </div>
  `;

  const body = container.querySelector<HTMLElement>("#users-body");
  const refresh = container.querySelector<HTMLButtonElement>("#users-refresh");
  const create = container.querySelector<HTMLButtonElement>("#users-create");
  if (!body || !refresh || !create) {
    return;
  }

  refresh.addEventListener("click", () => void load(body));
  create.addEventListener("click", () => void createUser(body));

  void load(body);
}

async function load(body: HTMLElement): Promise<void> {
  body.innerHTML = renderLoading();
  try {
    const [users, groups] = await Promise.all([
      liveClient.listAdminUsers(),
      liveClient.listGroups(),
    ]);
    renderUsers(body, users, groups);
  } catch (error) {
    body.innerHTML = renderError(error);
  }
}

function renderUsers(
  body: HTMLElement,
  users: readonly AdminUser[],
  _groups: readonly AdminGroup[],
): void {
  const adminCount = users.filter((u) => u.is_admin).length;

  body.innerHTML = `
    <div class="row-cards row-cards-3">
      ${metricCard("Total users", String(users.length))}
      ${metricCard("Administrators", String(adminCount))}
      ${metricCard("Standard", String(users.length - adminCount))}
    </div>

    <div class="card">
      <div class="card-header">
        <h3 class="card-title">All users</h3>
      </div>
      <div class="table-wrap">
        <table class="data-grid data-grid-roomy">
          <thead>
            <tr>
              <th>Name</th>
              <th>Role</th>
              <th>Groups</th>
              <th>Password rotated</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            ${
              users.length === 0
                ? `<tr><td colspan="5" class="text-muted">No users found.</td></tr>`
                : users.map(renderUserRow).join("")
            }
          </tbody>
        </table>
      </div>
    </div>
  `;

  body.querySelectorAll<HTMLButtonElement>("[data-action]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const action = btn.dataset.action;
      const name = btn.dataset.user ?? "";
      if (action === "reset") {
        void resetPassword(body, name);
      } else if (action === "delete") {
        void deleteUser(body, name);
      }
    });
  });
}

function renderUserRow(user: AdminUser): string {
  const initials = user.name.slice(0, 2).toUpperCase();
  const groupBadges =
    user.groups.length > 0
      ? user.groups
          .map((g) => `<span class="badge badge-soft">${escapeHtml(g)}</span>`)
          .join(" ")
      : `<span class="text-muted">—</span>`;
  const rotated = user.password_rotated_at_epoch_ms
    ? new Date(user.password_rotated_at_epoch_ms).toLocaleString()
    : "—";
  const roleLabel = user.is_admin ? "Administrator" : "Standard";

  return `
    <tr>
      <td>
        <div class="user-cell">
          <span class="avatar avatar-sm">${escapeHtml(initials)}</span>
          <div>
            <div class="user-name">${escapeHtml(user.name)}</div>
          </div>
        </div>
      </td>
      <td>${escapeHtml(roleLabel)}</td>
      <td><div class="badge-row">${groupBadges}</div></td>
      <td class="text-muted">${escapeHtml(rotated)}</td>
      <td class="row-actions">
        <button class="btn btn-ghost" type="button" data-action="reset" data-user="${escapeHtml(user.name)}">Reset password</button>
        <button class="btn btn-ghost" type="button" data-action="delete" data-user="${escapeHtml(user.name)}">Delete</button>
      </td>
    </tr>
  `;
}

async function createUser(body: HTMLElement): Promise<void> {
  const result = await openCreateUserDialog();
  if (!result) {
    return;
  }
  try {
    await liveClient.createUser(
      result.name,
      result.password,
      result.admin ? [ADMINISTRATORS_GROUP] : [],
    );
    await load(body);
  } catch (error) {
    window.alert(formatError(error));
  }
}

/**
 * Opens a modal to create a user: a username, a hidden password entered twice
 * (validated to match), and an option to grant admin rights. Resolves with the
 * entered values, or null if cancelled.
 */
function openCreateUserDialog(): Promise<{
  name: string;
  password: string;
  admin: boolean;
} | null> {
  return new Promise((resolve) => {
    const overlay = document.createElement("div");
    overlay.className = "modal-overlay";
    overlay.innerHTML = `
      <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="create-user-title">
        <div class="modal-header">
          <h3 id="create-user-title" class="card-title">New user</h3>
          <p class="card-subtitle">Create a user account and set its password.</p>
        </div>
        <div class="modal-body">
          <div class="form-group">
            <label class="form-label" for="cu-name">Username</label>
            <input id="cu-name" class="form-control" type="text" autocomplete="off"
              spellcheck="false" placeholder="e.g. analyst_jane" />
          </div>
          <div class="form-group">
            <label class="form-label" for="cu-pw">Password</label>
            <input id="cu-pw" class="form-control" type="password" autocomplete="new-password" />
          </div>
          <div class="form-group">
            <label class="form-label" for="cu-pw2">Confirm password</label>
            <input id="cu-pw2" class="form-control" type="password" autocomplete="new-password" />
          </div>
          <label class="checkbox-row">
            <input type="checkbox" id="cu-show" />
            <span>Show password</span>
          </label>
          <label class="checkbox-row">
            <input type="checkbox" id="cu-admin" />
            <span>Add to the <strong>${escapeHtml(ADMINISTRATORS_GROUP)}</strong> group (grants admin rights)</span>
          </label>
          <div class="modal-error message message-error" hidden>
            ${icon("x-circle", 14)}<span class="modal-error-text"></span>
          </div>
        </div>
        <div class="modal-footer">
          <button class="btn" type="button" id="cu-cancel">Cancel</button>
          <button class="btn btn-primary" type="button" id="cu-create">${icon("plus", 14)}<span>Create user</span></button>
        </div>
      </div>
    `;
    document.body.append(overlay);

    const nameInput = overlay.querySelector<HTMLInputElement>("#cu-name")!;
    const pwInput = overlay.querySelector<HTMLInputElement>("#cu-pw")!;
    const pw2Input = overlay.querySelector<HTMLInputElement>("#cu-pw2")!;
    const showToggle = overlay.querySelector<HTMLInputElement>("#cu-show")!;
    const adminToggle = overlay.querySelector<HTMLInputElement>("#cu-admin")!;
    const createBtn = overlay.querySelector<HTMLButtonElement>("#cu-create")!;
    const cancelBtn = overlay.querySelector<HTMLButtonElement>("#cu-cancel")!;
    const errorBox = overlay.querySelector<HTMLDivElement>(".modal-error")!;
    const errorText = overlay.querySelector<HTMLSpanElement>(".modal-error-text")!;

    const close = (
      result: { name: string; password: string; admin: boolean } | null,
    ): void => {
      document.removeEventListener("keydown", onKey);
      overlay.remove();
      resolve(result);
    };
    const onKey = (event: KeyboardEvent): void => {
      if (event.key === "Escape") close(null);
    };
    document.addEventListener("keydown", onKey);
    overlay.addEventListener("mousedown", (event) => {
      if (event.target === overlay) close(null);
    });

    const showError = (message: string): void => {
      errorText.textContent = message;
      errorBox.hidden = false;
    };

    showToggle.addEventListener("change", () => {
      const type = showToggle.checked ? "text" : "password";
      pwInput.type = type;
      pw2Input.type = type;
    });

    cancelBtn.addEventListener("click", () => close(null));
    createBtn.addEventListener("click", () => {
      const name = nameInput.value.trim();
      const password = pwInput.value;
      const confirm = pw2Input.value;

      if (!name) {
        showError("Enter a username.");
        return;
      }
      if (!/^[A-Za-z0-9_]+$/.test(name)) {
        showError("Username may only contain letters, digits, and underscores.");
        return;
      }
      if (!password) {
        showError("Enter a password.");
        return;
      }
      if (password !== confirm) {
        showError("Passwords do not match.");
        return;
      }
      close({ name, password, admin: adminToggle.checked });
    });

    // Enter advances/submits.
    overlay.querySelectorAll("input").forEach((field) => {
      field.addEventListener("keydown", (event) => {
        if ((event as KeyboardEvent).key === "Enter") {
          event.preventDefault();
          createBtn.click();
        }
      });
    });

    setTimeout(() => nameInput.focus(), 0);
  });
}

async function resetPassword(body: HTMLElement, name: string): Promise<void> {
  const choice = await openResetPasswordDialog(name);
  if (!choice) {
    return;
  }
  try {
    await liveClient.resetUserPassword(name, choice.password);
    if (choice.generated) {
      window.alert(
        `Generated password for '${name}':\n\n${choice.password}\n\nStore it now — it won't be shown again.`,
      );
    } else {
      window.alert(`Password for '${name}' updated.`);
    }
    await load(body);
  } catch (error) {
    window.alert(formatError(error));
  }
}

/**
 * Opens a modal letting the admin type a replacement password or generate a
 * strong random one (shown so it can be copied). Resolves with the chosen
 * password, or null if cancelled.
 */
function openResetPasswordDialog(
  name: string,
): Promise<{ password: string; generated: boolean } | null> {
  return new Promise((resolve) => {
    const overlay = document.createElement("div");
    overlay.className = "modal-overlay";
    overlay.innerHTML = `
      <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="reset-pw-title">
        <div class="modal-header">
          <h3 id="reset-pw-title" class="card-title">Reset password</h3>
          <p class="card-subtitle">Set a new password for <strong>${escapeHtml(name)}</strong>.</p>
        </div>
        <div class="modal-body">
          <label class="form-label" for="reset-pw-input">New password</label>
          <div class="input-row">
            <input id="reset-pw-input" class="form-control" type="text"
              autocomplete="new-password" spellcheck="false"
              placeholder="Type a password or generate one" />
            <button class="btn" type="button" id="reset-pw-generate">${icon("refresh", 14)}<span>Generate</span></button>
          </div>
          <p class="form-hint">Type your own password, or generate a strong random one. It is shown here so you can copy it before applying.</p>
          <div class="modal-error message message-error" hidden>
            ${icon("x-circle", 14)}<span class="modal-error-text"></span>
          </div>
        </div>
        <div class="modal-footer">
          <button class="btn" type="button" id="reset-pw-cancel">Cancel</button>
          <button class="btn btn-primary" type="button" id="reset-pw-apply">Apply</button>
        </div>
      </div>
    `;
    document.body.append(overlay);

    const input = overlay.querySelector<HTMLInputElement>("#reset-pw-input")!;
    const generateBtn = overlay.querySelector<HTMLButtonElement>("#reset-pw-generate")!;
    const applyBtn = overlay.querySelector<HTMLButtonElement>("#reset-pw-apply")!;
    const cancelBtn = overlay.querySelector<HTMLButtonElement>("#reset-pw-cancel")!;
    const errorBox = overlay.querySelector<HTMLDivElement>(".modal-error")!;
    const errorText = overlay.querySelector<HTMLSpanElement>(".modal-error-text")!;
    let generated = false;

    const close = (result: { password: string; generated: boolean } | null): void => {
      document.removeEventListener("keydown", onKey);
      overlay.remove();
      resolve(result);
    };
    const onKey = (event: KeyboardEvent): void => {
      if (event.key === "Escape") close(null);
    };
    document.addEventListener("keydown", onKey);
    overlay.addEventListener("mousedown", (event) => {
      if (event.target === overlay) close(null);
    });

    // Typing clears the "generated" marker so the success notice is accurate.
    input.addEventListener("input", () => {
      generated = false;
    });
    generateBtn.addEventListener("click", () => {
      input.value = generatePassword();
      generated = true;
      errorBox.hidden = true;
      input.focus();
      input.select();
    });
    cancelBtn.addEventListener("click", () => close(null));
    applyBtn.addEventListener("click", () => {
      const value = input.value;
      if (!value.trim()) {
        errorText.textContent = "Enter a password or click Generate.";
        errorBox.hidden = false;
        return;
      }
      close({ password: value, generated });
    });
    input.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        applyBtn.click();
      }
    });

    setTimeout(() => input.focus(), 0);
  });
}

/** Generates a strong, URL-safe random password using the Web Crypto API. */
function generatePassword(): string {
  const bytes = new Uint8Array(18);
  crypto.getRandomValues(bytes);
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function deleteUser(body: HTMLElement, name: string): Promise<void> {
  if (!window.confirm(`Delete user '${name}'? This cannot be undone.`)) {
    return;
  }
  try {
    await liveClient.dropUser(name);
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
  return `<div class="card"><div class="card-body"><div class="loading-card">Loading users…</div></div></div>`;
}

function renderError(error: unknown): string {
  return `
    <div class="card">
      <div class="card-body">
        <div class="message message-error">
          ${icon("x-circle", 14)}
          <span>Could not load users: ${escapeHtml(formatError(error))}. Sign in as an administrator and confirm the gateway is running.</span>
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
