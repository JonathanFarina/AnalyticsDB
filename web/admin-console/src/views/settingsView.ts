import {
  fetchClusterConfig,
  saveClusterConfig,
  type ClusterConfig,
  type ClusterConfigEnvelope,
  type QueryLogConfig,
} from "../adminClient";
import { icon } from "../icons";

interface SettingsState {
  loaded: ClusterConfigEnvelope | null;
  draft: ClusterConfig | null;
  saving: boolean;
  error: string | null;
  notice: string | null;
}

type FieldGroup = "ports" | "paths" | "tls" | "internal" | "query-log";
type FieldKind = "number" | "text" | "boolean";

interface FieldDescriptor {
  readonly key: string; // dotted path, e.g. "base_postgres_port" or "query_log.enabled"
  readonly label: string;
  readonly type: FieldKind;
  readonly hint?: string;
  readonly optional?: boolean;
  readonly placeholder?: string;
  readonly group: FieldGroup;
  readonly step?: string;
}

const DEFAULT_QUERY_LOG: QueryLogConfig = {
  enabled: true,
  sample_rate: 1.0,
  min_duration_ms: 0,
  batch_size: 1024,
  batch_interval_ms: 5000,
  max_query_length_bytes: 65536,
  retention_days: 30,
};

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
  {
    key: "query_log.enabled",
    label: "Query log enabled",
    type: "boolean",
    hint: "Persist completed queries into the engine's query log table.",
    group: "query-log",
  },
  {
    key: "query_log.sample_rate",
    label: "Sample rate",
    type: "number",
    step: "0.01",
    hint: "Fraction of queries to record (0.0 – 1.0).",
    group: "query-log",
  },
  {
    key: "query_log.min_duration_ms",
    label: "Minimum duration (ms)",
    type: "number",
    hint: "Skip queries that completed faster than this threshold.",
    group: "query-log",
  },
  {
    key: "query_log.batch_size",
    label: "Batch size",
    type: "number",
    hint: "Maximum entries buffered before a flush is triggered.",
    group: "query-log",
  },
  {
    key: "query_log.batch_interval_ms",
    label: "Batch interval (ms)",
    type: "number",
    hint: "Maximum time the writer waits before flushing a partial batch.",
    group: "query-log",
  },
  {
    key: "query_log.max_query_length_bytes",
    label: "Max query length (bytes)",
    type: "number",
    hint: "SQL longer than this is truncated when stored.",
    group: "query-log",
  },
  {
    key: "query_log.retention_days",
    label: "Retention (days)",
    type: "number",
    hint: "How long the engine keeps stored query log entries.",
    group: "query-log",
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
        <button class="btn btn-primary" type="button" id="settings-save" disabled>
          <span>Save settings</span>
        </button>
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
      state.draft = cloneConfig(state.loaded.config);
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
    refreshActionButtons();
  }

  function refreshActionButtons(): void {
    const dirty = isDirty(state);
    saveButton.disabled = !dirty || state.saving;
    discardButton.disabled = !dirty || state.saving;
    saveButton.classList.toggle("is-running", state.saving);
    const label = saveButton.querySelector<HTMLElement>("span");
    if (label) {
      label.textContent = state.saving ? "Saving…" : "Save settings";
    }
  }

  function bindFieldHandlers(): void {
    for (const field of FIELDS) {
      const input = body.querySelector<HTMLInputElement>(`[data-key="${field.key}"]`);
      if (!input) {
        continue;
      }
      const handler = () => {
        if (!state.draft) {
          return;
        }
        state.notice = null;
        state.draft = applyFieldChange(state.draft, field, input);
        refreshActionButtons();
      };
      input.addEventListener("input", handler);
      if (field.type === "boolean") {
        input.addEventListener("change", handler);
      }
    }
  }

  async function load(): Promise<void> {
    state.error = null;
    state.notice = null;
    renderAll();
    try {
      const envelope = await fetchClusterConfig();
      state.loaded = envelope;
      state.draft = cloneConfig(envelope.config);
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
      state.draft = cloneConfig(envelope.config);
      state.notice = `Saved to ${envelope.path}.`;
    } catch (error) {
      state.error = error instanceof Error ? error.message : String(error);
    } finally {
      state.saving = false;
      renderAll();
    }
  }
}

function applyFieldChange(
  draft: ClusterConfig,
  field: FieldDescriptor,
  input: HTMLInputElement,
): ClusterConfig {
  const next = cloneConfig(draft);
  if (field.type === "boolean") {
    setPath(next, field.key, input.checked);
    return next;
  }

  if (field.type === "number") {
    if (input.value === "") {
      setPath(next, field.key, field.optional ? undefined : 0);
      return next;
    }
    const numeric = Number(input.value);
    if (Number.isFinite(numeric)) {
      setPath(next, field.key, numeric);
    }
    return next;
  }

  // text
  if (field.optional && input.value === "") {
    setPath(next, field.key, null);
  } else {
    setPath(next, field.key, input.value);
  }
  return next;
}

function isDirty(state: SettingsState): boolean {
  if (!state.loaded || !state.draft) {
    return false;
  }
  return !deepEqual(state.loaded.config, state.draft);
}

function deepEqual(a: unknown, b: unknown): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

function cloneConfig(config: ClusterConfig): ClusterConfig {
  const cloned = JSON.parse(JSON.stringify(config)) as ClusterConfig;
  // Ensure query_log is always present when editing so toggles render.
  if (!cloned.query_log) {
    cloned.query_log = { ...DEFAULT_QUERY_LOG };
  }
  return cloned;
}

function normaliseForSave(draft: ClusterConfig): ClusterConfig {
  const queryLog = draft.query_log ?? DEFAULT_QUERY_LOG;
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
    query_log: {
      enabled: Boolean(queryLog.enabled),
      sample_rate: clampFloat(queryLog.sample_rate, 0, 1, DEFAULT_QUERY_LOG.sample_rate),
      min_duration_ms: clampNonNegInt(
        queryLog.min_duration_ms,
        DEFAULT_QUERY_LOG.min_duration_ms,
      ),
      batch_size: clampNonNegInt(queryLog.batch_size, DEFAULT_QUERY_LOG.batch_size),
      batch_interval_ms: clampNonNegInt(
        queryLog.batch_interval_ms,
        DEFAULT_QUERY_LOG.batch_interval_ms,
      ),
      max_query_length_bytes: clampNonNegInt(
        queryLog.max_query_length_bytes,
        DEFAULT_QUERY_LOG.max_query_length_bytes,
      ),
      retention_days: clampNonNegInt(queryLog.retention_days, DEFAULT_QUERY_LOG.retention_days),
    },
  };
}

function clampFloat(value: unknown, min: number, max: number, fallback: number): number {
  const numeric = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }
  return Math.min(Math.max(numeric, min), max);
}

function clampNonNegInt(value: unknown, fallback: number): number {
  const numeric = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numeric) || numeric < 0) {
    return fallback;
  }
  return Math.floor(numeric);
}

function emptyToNull(value: string | null | undefined): string | null {
  if (value === undefined || value === null) {
    return null;
  }
  const trimmed = String(value).trim();
  return trimmed === "" ? null : trimmed;
}

function setPath(target: ClusterConfig, key: string, value: unknown): void {
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

function getPath(source: ClusterConfig, key: string): unknown {
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

function renderBody(state: SettingsState): string {
  const envelope = state.loaded!;
  const draft = state.draft!;
  const tlsEnabled = Boolean(emptyToNull(draft.tls_cert_path) && emptyToNull(draft.tls_key_path));
  const queryLogEnabled = Boolean(draft.query_log?.enabled);
  return `
    ${state.notice ? renderNotice(state.notice, "success") : ""}
    ${state.error ? renderNotice(state.error, "error") : ""}
    <div class="settings-grid">
      ${sectionCard(
        "Wire protocols",
        "Ports the coordinator listens on for client traffic.",
        ["ports"],
        draft,
        [renderTlsBadge(tlsEnabled)],
      )}
      ${sectionCard(
        "Storage &amp; catalog",
        "Where AnalyticsDB persists database, schema, and table metadata.",
        ["paths"],
        draft,
        [],
      )}
      ${sectionCard(
        "TLS",
        "Certificate and private-key paths used for TLS-enabled protocols.",
        ["tls"],
        draft,
        [],
      )}
      ${sectionCard(
        "Internal coordination",
        "Knobs used by the coordinator when allocating ports to new nodes.",
        ["internal"],
        draft,
        [],
      )}
      ${sectionCard(
        "Query log",
        "Persistent log of completed queries written by the engine.",
        ["query-log"],
        draft,
        [
          queryLogEnabled
            ? `<span class="status-pill status-pill-success"><span class="status-dot"></span>logging</span>`
            : `<span class="status-pill status-pill-muted"><span class="status-dot"></span>disabled</span>`,
        ],
      )}
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
  groups: readonly FieldGroup[],
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
  const raw = getPath(draft, field.key);
  const optionalTag = field.optional
    ? `<span class="badge badge-outline">optional</span>`
    : "";

  if (field.type === "boolean") {
    const checked = Boolean(raw);
    return `
      <label class="form-field form-field-toggle">
        <span class="form-label">${field.label}${optionalTag ? ` ${optionalTag}` : ""}</span>
        <input type="checkbox" data-key="${field.key}" ${checked ? "checked" : ""} />
        ${field.hint ? `<span class="form-hint">${field.hint}</span>` : ""}
      </label>
    `;
  }

  const value =
    raw === undefined || raw === null
      ? ""
      : field.type === "number"
        ? String(raw)
        : String(raw);
  return `
    <label class="form-field">
      <span class="form-label">${field.label}${optionalTag ? ` ${optionalTag}` : ""}</span>
      <input
        class="form-control"
        type="${field.type === "number" ? "number" : "text"}"
        data-key="${field.key}"
        value="${escapeAttribute(value)}"
        ${field.step ? `step="${escapeAttribute(field.step)}"` : ""}
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
