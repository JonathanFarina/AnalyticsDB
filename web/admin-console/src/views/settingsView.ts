import {
  fetchClusterConfig,
  saveClusterConfig,
  type ClusterConfig,
  type ClusterConfigEnvelope,
} from "../adminClient";
import { icon } from "../icons";

interface SettingsState {
  loaded: ClusterConfigEnvelope | null;
  draft: ClusterConfig | null;
  saving: boolean;
  error: string | null;
  notice: string | null;
}

interface FieldDescriptor {
  readonly key: keyof ClusterConfig;
  readonly label: string;
  readonly type: "number" | "text";
  readonly hint?: string;
  readonly optional?: boolean;
  readonly placeholder?: string;
  readonly group: "ports" | "paths" | "tls" | "internal";
}

const FIELDS: readonly FieldDescriptor[] = [
  {
    key: "base_postgres_port",
    label: "PostgreSQL wire port",
    type: "number",
    hint: "Base TCP port the coordinator exposes for the PostgreSQL protocol.",
    group: "ports",
  },
  {
    key: "base_flight_sql_port",
    label: "Arrow Flight SQL port",
    type: "number",
    hint: "Base port for Arrow Flight SQL client connections.",
    group: "ports",
  },
  {
    key: "base_node_port",
    label: "Internal node port",
    type: "number",
    hint: "Base port workers and the coordinator use for internal gRPC traffic.",
    optional: true,
    group: "ports",
  },
  {
    key: "next_available_port_offset",
    label: "Next port offset",
    type: "number",
    hint: "Offset added when allocating the next node's ports. Increments as nodes join.",
    group: "internal",
  },
  {
    key: "catalog_path",
    label: "Catalog path",
    type: "text",
    hint: "Location of the SQLite (or JSON) file that holds the persisted catalogue.",
    placeholder: "cluster-catalog.db",
    group: "paths",
  },
  {
    key: "tls_cert_path",
    label: "TLS certificate path",
    type: "text",
    hint: "Server certificate used for TLS-enabled wire protocols. Leave blank to disable TLS.",
    optional: true,
    placeholder: "certs/server.crt",
    group: "tls",
  },
  {
    key: "tls_key_path",
    label: "TLS private key path",
    type: "text",
    hint: "Private key paired with the TLS certificate above.",
    optional: true,
    placeholder: "certs/server.key",
    group: "tls",
  },
];

export function mountSettingsView(container: HTMLElement): void {
  const state: SettingsState = {
    loaded: null,
    draft: null,
    saving: false,
    error: null,
    notice: null,
  };

  container.innerHTML = `
    <div class="page-header">
      <div class="page-header-title">
        <p class="page-pretitle">Administration</p>
        <h2 class="page-title">System Settings</h2>
      </div>
      <div class="page-header-actions" id="settings-actions">
        <button class="btn" type="button" id="settings-discard" disabled>Discard changes</button>
        <button class="btn btn-primary" type="button" id="settings-save" disabled>Save settings</button>
      </div>
    </div>
    <div class="page-body" id="settings-body">
      ${renderLoading()}
    </div>
  `;

  const bodyNode = container.querySelector<HTMLElement>("#settings-body");
  const saveNode = container.querySelector<HTMLButtonElement>("#settings-save");
  const discardNode = container.querySelector<HTMLButtonElement>("#settings-discard");
  if (!bodyNode || !saveNode || !discardNode) {
    return;
  }
  const body: HTMLElement = bodyNode;
  const saveButton: HTMLButtonElement = saveNode;
  const discardButton: HTMLButtonElement = discardNode;

  saveButton.addEventListener("click", () => {
    void save();
  });

  discardButton.addEventListener("click", () => {
    if (state.loaded) {
      state.draft = { ...state.loaded.config };
      state.error = null;
      state.notice = null;
      renderAll();
    }
  });

  void load();

  function renderAll(): void {
    if (!state.loaded || !state.draft) {
      body.innerHTML = state.error ? renderError(state.error) : renderLoading();
      saveButton.disabled = true;
      discardButton.disabled = true;
      return;
    }
    body.innerHTML = renderBody(state);
    bindFieldHandlers();
    const dirty = isDirty(state);
    saveButton.disabled = !dirty || state.saving;
    discardButton.disabled = !dirty || state.saving;
    saveButton.classList.toggle("is-running", state.saving);
    const label = saveButton.querySelector("span") ?? saveButton;
    if (saveButton.lastChild?.nodeType === Node.TEXT_NODE) {
      saveButton.lastChild.textContent = state.saving ? "Saving…" : "Save settings";
    } else {
      label.textContent = state.saving ? "Saving…" : "Save settings";
    }
  }

  function bindFieldHandlers(): void {
    for (const field of FIELDS) {
      const input = body.querySelector<HTMLInputElement>(`[data-key="${field.key}"]`);
      if (!input) {
        continue;
      }
      input.addEventListener("input", () => {
        if (!state.draft) {
          return;
        }
        state.notice = null;
        if (field.type === "number") {
          const value = input.value === "" ? undefined : Number(input.value);
          if (field.optional && value === undefined) {
            (state.draft as unknown as Record<string, unknown>)[field.key] = undefined;
          } else if (Number.isFinite(value)) {
            (state.draft as unknown as Record<string, unknown>)[field.key] = value;
          }
        } else {
          const trimmed = input.value;
          if (field.optional && trimmed === "") {
            (state.draft as unknown as Record<string, unknown>)[field.key] = null;
          } else {
            (state.draft as unknown as Record<string, unknown>)[field.key] = trimmed;
          }
        }
        const dirty = isDirty(state);
        saveButton.disabled = !dirty || state.saving;
        discardButton.disabled = !dirty || state.saving;
      });
    }
  }

  async function load(): Promise<void> {
    state.error = null;
    state.notice = null;
    renderAll();
    try {
      const envelope = await fetchClusterConfig();
      state.loaded = envelope;
      state.draft = { ...envelope.config };
    } catch (error) {
      state.error = error instanceof Error ? error.message : String(error);
    }
    renderAll();
  }

  async function save(): Promise<void> {
    if (!state.draft) {
      return;
    }
    state.saving = true;
    state.error = null;
    state.notice = null;
    renderAll();
    try {
      const payload = normaliseForSave(state.draft);
      const envelope = await saveClusterConfig(payload);
      state.loaded = envelope;
      state.draft = { ...envelope.config };
      state.notice = `Saved to ${envelope.path}.`;
    } catch (error) {
      state.error = error instanceof Error ? error.message : String(error);
    } finally {
      state.saving = false;
      renderAll();
    }
  }
}

function isDirty(state: SettingsState): boolean {
  if (!state.loaded || !state.draft) {
    return false;
  }
  return !shallowEqual(state.loaded.config, state.draft);
}

function shallowEqual(a: ClusterConfig, b: ClusterConfig): boolean {
  const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
  const aMap = a as unknown as Record<string, unknown>;
  const bMap = b as unknown as Record<string, unknown>;
  for (const key of keys) {
    if (aMap[key] !== bMap[key]) {
      return false;
    }
  }
  return true;
}

function normaliseForSave(draft: ClusterConfig): ClusterConfig {
  return {
    base_postgres_port: Number(draft.base_postgres_port),
    base_flight_sql_port: Number(draft.base_flight_sql_port),
    base_node_port:
      draft.base_node_port === undefined || draft.base_node_port === null
        ? undefined
        : Number(draft.base_node_port),
    catalog_path: String(draft.catalog_path),
    tls_cert_path: emptyToNull(draft.tls_cert_path),
    tls_key_path: emptyToNull(draft.tls_key_path),
    next_available_port_offset: Number(draft.next_available_port_offset),
  };
}

function emptyToNull(value: string | null | undefined): string | null {
  if (value === undefined || value === null) {
    return null;
  }
  const trimmed = String(value).trim();
  return trimmed === "" ? null : trimmed;
}

function renderBody(state: SettingsState): string {
  const envelope = state.loaded!;
  const draft = state.draft!;
  const tlsEnabled = Boolean(emptyToNull(draft.tls_cert_path) && emptyToNull(draft.tls_key_path));
  return `
    ${state.notice ? renderNotice(state.notice, "success") : ""}
    ${state.error ? renderNotice(state.error, "error") : ""}
    <div class="settings-grid">
      ${sectionCard("Wire protocols", "Ports the coordinator listens on for client traffic.", ["ports"], draft, [
        renderTlsBadge(tlsEnabled),
      ])}
      ${sectionCard("Storage &amp; catalog", "Where AnalyticsDB persists database, schema, and table metadata.", ["paths"], draft, [])}
      ${sectionCard("TLS", "Certificate and private-key paths used for TLS-enabled protocols.", ["tls"], draft, [])}
      ${sectionCard("Internal coordination", "Knobs used by the coordinator when allocating ports to new nodes.", ["internal"], draft, [])}
    </div>
    <section class="card">
      <div class="card-header">
        <h3 class="card-title">Source file</h3>
        <span class="badge badge-outline">${escapeHtml(envelope.path)}</span>
      </div>
      <div class="card-body">
        <p class="text-muted">Edits made here are written back to <code>${escapeHtml(envelope.path)}</code>. The coordinator reads this file at boot time; restart the process to pick up changes.</p>
      </div>
    </section>
  `;
}

function sectionCard(
  title: string,
  subtitle: string,
  groups: readonly FieldDescriptor["group"][],
  draft: ClusterConfig,
  extras: readonly string[],
): string {
  const fields = FIELDS.filter((field) => groups.includes(field.group));
  return `
    <section class="card">
      <div class="card-header">
        <div>
          <h3 class="card-title">${title}</h3>
          <p class="card-subtitle">${subtitle}</p>
        </div>
        ${extras.join("")}
      </div>
      <div class="card-body">
        <div class="form-grid">
          ${fields.map((field) => renderField(field, draft)).join("")}
        </div>
      </div>
    </section>
  `;
}

function renderField(field: FieldDescriptor, draft: ClusterConfig): string {
  const raw = (draft as unknown as Record<string, unknown>)[field.key];
  const value =
    raw === undefined || raw === null
      ? ""
      : field.type === "number"
        ? String(raw)
        : String(raw);
  const optionalTag = field.optional
    ? `<span class="badge badge-outline">optional</span>`
    : "";
  return `
    <label class="form-field">
      <span class="form-label">${field.label}${optionalTag ? ` ${optionalTag}` : ""}</span>
      <input
        class="form-control"
        type="${field.type === "number" ? "number" : "text"}"
        data-key="${field.key}"
        value="${escapeAttribute(value)}"
        ${field.placeholder ? `placeholder="${escapeAttribute(field.placeholder)}"` : ""}
      />
      ${field.hint ? `<span class="form-hint">${field.hint}</span>` : ""}
    </label>
  `;
}

function renderTlsBadge(enabled: boolean): string {
  return enabled
    ? `<span class="status-pill status-pill-success"><span class="status-dot"></span>TLS configured</span>`
    : `<span class="status-pill status-pill-muted"><span class="status-dot"></span>TLS disabled</span>`;
}

function renderNotice(message: string, kind: "success" | "error"): string {
  const iconName = kind === "success" ? "circle-check" : "x-circle";
  return `
    <div class="message message-${kind === "success" ? "info" : "error"}">
      ${icon(iconName, 14)}
      <span>${escapeHtml(message)}</span>
    </div>
  `;
}

function renderLoading(): string {
  return `<div class="card"><div class="card-body"><div class="loading-card">Loading cluster configuration…</div></div></div>`;
}

function renderError(message: string): string {
  return `
    <div class="card">
      <div class="card-body">
        <div class="message message-error">
          ${icon("x-circle", 14)}
          <span>${escapeHtml(message)}. Confirm the dev server is running (npm run dev) so the cluster admin API is reachable.</span>
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

function escapeAttribute(value: string): string {
  return escapeHtml(value);
}
