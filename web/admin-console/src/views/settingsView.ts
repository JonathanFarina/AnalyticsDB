import {
  fetchClusterConfig,
  saveClusterConfig,
  type ClusterConfig,
  type ClusterConfigEnvelope,
} from "../adminClient";
import {
  DEFAULT_QUERY_LOG,
  buildSavePayload,
  emptyToNull,
  getPath,
  isDirty,
  setPath,
  withDisplayDefaults,
} from "../clusterConfigForm";
import { icon } from "../icons";

interface SettingsState {
  loaded: ClusterConfigEnvelope | null;
  draft: ClusterConfig | null;
  saving: boolean;
  error: string | null;
  notice: string | null;
}

type FieldGroup = "ports" | "paths" | "tls" | "internal" | "query-log" | "security";
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
  {
    key: "jwt_secret",
    label: "JWT secret",
    type: "text",
    hint: "HS256 secret used to sign Flight SQL JWT bearer tokens. Leave blank to generate an ephemeral key at startup.",
    optional: true,
    placeholder: "change-me-in-production-jwt-secret-default-key-32-bytes",
    group: "security",
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
      state.draft = withDisplayDefaults(state.loaded.config);
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
    if (!state.loaded || !state.draft) {
      saveButton.disabled = true;
      discardButton.disabled = true;
      return;
    }
    const dirty = isDirty(state.loaded.config, state.draft);
    saveButton.disabled = !dirty || state.saving;
    discardButton.disabled = !dirty || state.saving;
    saveButton.classList.toggle("is-running", state.saving);
    const label = saveButton.querySelector<HTMLElement>("span");
    if (label) {
      label.textContent = state.saving ? "Saving…" : "Save settings";
    }
  }

  function clearInlineNotice(): void {
    body.querySelectorAll(".settings-notice").forEach((node) => node.remove());
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
        if (state.notice !== null) {
          state.notice = null;
          clearInlineNotice();
        }
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
      state.draft = withDisplayDefaults(envelope.config);
    } catch (error) {
      state.error = error instanceof Error ? error.message : String(error);
    }
    renderAll();
  }

  async function save(): Promise<void> {
    if (!state.draft || !state.loaded) {
      return;
    }
    state.saving = true;
    state.error = null;
    state.notice = null;
    refreshActionButtons();
    try {
      const payload = buildSavePayload(state.draft, state.loaded.config);
      const envelope = await saveClusterConfig(payload);
      state.loaded = envelope;
      state.draft = withDisplayDefaults(envelope.config);
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
  const next = cloneDraft(draft);
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

function cloneDraft(config: ClusterConfig): ClusterConfig {
  return withDisplayDefaults(JSON.parse(JSON.stringify(config)) as ClusterConfig);
}

function renderBody(state: SettingsState): string {
  const envelope = state.loaded!;
  const draft = state.draft!;
  const tlsEnabled = Boolean(emptyToNull(draft.tls_cert_path) && emptyToNull(draft.tls_key_path));
  const queryLogEnabled = Boolean(draft.query_log?.enabled);
  const baselineHasQueryLog = envelope.config.query_log !== undefined;
  const queryLogBadge = baselineHasQueryLog
    ? queryLogEnabled
      ? `<span class="status-pill status-pill-success"><span class="status-dot"></span>logging</span>`
      : `<span class="status-pill status-pill-muted"><span class="status-dot"></span>disabled</span>`
    : `<span class="status-pill status-pill-muted"><span class="status-dot"></span>defaults (not in file)</span>`;
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
        "Security &amp; auth",
        "Credential and token-signing configurations for database connections.",
        ["security"],
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
        baselineHasQueryLog
          ? "Persistent log of completed queries written by the engine."
          : "Engine defaults are shown — they are only persisted to the file if you change one.",
        ["query-log"],
        draft,
        [queryLogBadge],
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

  const fallback = field.type === "number" ? DEFAULT_NUMBER_FALLBACKS[field.key] : undefined;
  const value =
    raw === undefined || raw === null
      ? ""
      : field.type === "number"
        ? String(raw)
        : String(raw);
  const placeholder = field.placeholder ?? (fallback !== undefined ? String(fallback) : undefined);
  return `
    <label class="form-field">
      <span class="form-label">${field.label}${optionalTag ? ` ${optionalTag}` : ""}</span>
      <input
        class="form-control"
        type="${field.type === "number" ? "number" : "text"}"
        data-key="${field.key}"
        value="${escapeAttribute(value)}"
        ${field.step ? `step="${escapeAttribute(field.step)}"` : ""}
        ${placeholder ? `placeholder="${escapeAttribute(placeholder)}"` : ""}
      />
      ${field.hint ? `<span class="form-hint">${field.hint}</span>` : ""}
    </label>
  `;
}

const DEFAULT_NUMBER_FALLBACKS: Readonly<Record<string, number>> = {
  "query_log.sample_rate": DEFAULT_QUERY_LOG.sample_rate,
  "query_log.min_duration_ms": DEFAULT_QUERY_LOG.min_duration_ms,
  "query_log.batch_size": DEFAULT_QUERY_LOG.batch_size,
  "query_log.batch_interval_ms": DEFAULT_QUERY_LOG.batch_interval_ms,
  "query_log.max_query_length_bytes": DEFAULT_QUERY_LOG.max_query_length_bytes,
  "query_log.retention_days": DEFAULT_QUERY_LOG.retention_days,
};

function renderTlsBadge(enabled: boolean): string {
  return enabled
    ? `<span class="status-pill status-pill-success"><span class="status-dot"></span>TLS configured</span>`
    : `<span class="status-pill status-pill-muted"><span class="status-dot"></span>TLS disabled</span>`;
}

function renderNotice(message: string, kind: "success" | "error"): string {
  const iconName = kind === "success" ? "circle-check" : "x-circle";
  return `
    <div class="message message-${kind === "success" ? "info" : "error"} settings-notice">
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
        <div class="message message-error settings-notice">
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
