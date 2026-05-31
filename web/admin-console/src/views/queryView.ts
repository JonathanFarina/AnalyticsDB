import { liveClient } from "../liveClient";
import type {
  AnalyticsConsoleClient,
  CellValue,
  DatabaseMetadata,
  ExplorerSnapshot,
  Protocol,
  QueryMessage,
  QueryResult,
  QueryResultChunk,
  QueryTiming,
  RelationMetadata,
  SchemaMetadata,
} from "../domain";
import { icon } from "../icons";
import { highlightSql } from "../sqlHighlight";
import {
  countQuery,
  describeQuery,
  previewQuery,
  showSchemaTablesQuery,
} from "../queryTemplates";

interface QueryViewState {
  readonly client: AnalyticsConsoleClient;
  snapshot?: ExplorerSnapshot;
  selectedDatabase: string;
  selectedSchema: string;
  selectedRelation?: RelationMetadata;
  protocol: Protocol;
  queryText: string;
  filter: string;
  expanded: Set<string>;
  isRunning: boolean;
  latestResult?: QueryResult;
  streamingRows: readonly (readonly CellValue[])[];
  streamingColumns: readonly string[];
  streamingTimings?: QueryTiming;
  streamingMessages: readonly QueryMessage[];
  isStreaming: boolean;
}

export function mountQueryView(container: HTMLElement): void {
  const state: QueryViewState = {
    client: liveClient,
    selectedDatabase: "postgres",
    selectedSchema: "public",
    protocol: "postgres",
    queryText:
      "-- Run with ⌘/Ctrl+Enter. Pick a table in the Explorer to query it.\nSELECT 1 AS hello;",
    filter: "",
    expanded: new Set<string>(),
    isRunning: false,
    streamingRows: [],
    streamingColumns: [],
    streamingMessages: [],
    isStreaming: false,
  };

  container.innerHTML = `
    <div class="page-header">
      <div class="page-header-title">
        <p class="page-pretitle">Workspace</p>
        <h2 class="page-title">SQL Query</h2>
      </div>
      <div class="page-header-actions">
        <span class="text-muted query-hint">Run with ${cmdKeyLabel()}+Enter</span>
        <span class="status-pill status-pill-success">
          <span class="status-dot"></span>
          Engine online
        </span>
      </div>
    </div>

    <div class="page-body sql-page">
      <div class="sql-workspace">
        <aside class="card explorer-card" aria-labelledby="explorer-heading">
          <div class="card-header">
            <h3 id="explorer-heading" class="card-title">Explorer</h3>
            <span class="badge badge-soft">Catalog</span>
          </div>
          <div class="explorer-search">
            <div class="input-icon">
              ${icon("search", 16)}
              <input id="explorer-filter" class="form-control" type="search"
                placeholder="Filter objects…" aria-label="Filter catalog objects" />
            </div>
          </div>
          <div id="explorer-tree" class="explorer-tree" aria-live="polite"></div>
        </aside>

        <section class="sql-main">
          <div class="card editor-card" aria-labelledby="query-heading">
            <div class="card-header">
              <h3 id="query-heading" class="card-title">Query editor</h3>
              <div class="session-controls">
                <label>
                  <span>Protocol</span>
                  <select id="protocol-select" class="form-select">
                    <option value="postgres">PostgreSQL</option>
                    <option value="flight-sql">Arrow Flight SQL</option>
                  </select>
                </label>
                <label>
                  <span>Database</span>
                  <select id="database-select" class="form-select"></select>
                </label>
                <label>
                  <span>Schema</span>
                  <select id="schema-select" class="form-select"></select>
                </label>
              </div>
            </div>
            <div class="card-body editor-body">
              <div class="code-editor" id="code-editor">
                <div class="code-gutter" aria-hidden="true"><div class="code-gutter-inner" id="code-gutter"></div></div>
                <div class="code-scroll">
                  <pre class="code-pre" aria-hidden="true"><code class="code-hl" id="code-hl"></code></pre>
                  <textarea id="query-input" class="code-input" spellcheck="false"
                    autocomplete="off" autocapitalize="off" wrap="off"
                    aria-label="SQL query editor"></textarea>
                </div>
              </div>
              <div class="query-actions">
                <button id="run-query" class="btn btn-primary" type="button">
                  ${icon("play", 14)}<span>Run query</span>
                </button>
                <div class="query-actions-spacer"></div>
                <button id="preview-relation" class="btn btn-sm" type="button">Preview</button>
                <button id="describe-relation" class="btn btn-sm" type="button">Describe</button>
                <button id="count-relation" class="btn btn-sm" type="button">Count rows</button>
              </div>
            </div>
          </div>

          <div class="card results-card" aria-labelledby="results-heading">
            <div class="card-header result-header">
              <h3 id="results-heading" class="card-title">Results</h3>
              <div class="result-header-right">
                <span id="result-rows" class="text-muted result-rows"></span>
                <span id="result-state" class="badge">Idle</span>
                <div id="result-timing-header" class="result-timing-header"></div>
              </div>
            </div>
            <div class="card-body results-body">
              <div id="result-messages" class="message-list"></div>
              <div id="result-grid" class="result-grid" aria-live="polite"></div>
            </div>
          </div>
        </section>
      </div>
    </div>
  `;

  const explorerFilter = mustQuery<HTMLInputElement>(container, "#explorer-filter");
  const explorerTree = mustQuery<HTMLDivElement>(container, "#explorer-tree");
  const queryInput = mustQuery<HTMLTextAreaElement>(container, "#query-input");
  const codeHighlight = mustQuery<HTMLElement>(container, "#code-hl");
  const codeGutter = mustQuery<HTMLElement>(container, "#code-gutter");
  const codeScroll = mustQuery<HTMLElement>(container, ".code-scroll");
  const protocolSelect = mustQuery<HTMLSelectElement>(container, "#protocol-select");
  const databaseSelect = mustQuery<HTMLSelectElement>(container, "#database-select");
  const schemaSelect = mustQuery<HTMLSelectElement>(container, "#schema-select");
  const runQueryButton = mustQuery<HTMLButtonElement>(container, "#run-query");
  const previewButton = mustQuery<HTMLButtonElement>(container, "#preview-relation");
  const describeButton = mustQuery<HTMLButtonElement>(container, "#describe-relation");
  const countButton = mustQuery<HTMLButtonElement>(container, "#count-relation");
  const resultState = mustQuery<HTMLSpanElement>(container, "#result-state");
  const resultRows = mustQuery<HTMLSpanElement>(container, "#result-rows");
  const resultGrid = mustQuery<HTMLDivElement>(container, "#result-grid");
  const resultMessages = mustQuery<HTMLDivElement>(container, "#result-messages");

  void bootstrap();

  async function bootstrap(): Promise<void> {
    state.snapshot = await state.client.getExplorerSnapshot();
    const firstDb = state.snapshot.databases[0];
    const firstSchema = firstDb?.schemas[0];
    state.selectedRelation = firstSchema?.relations[0];
    // Expand the active database, its first schema, and the Tables folder.
    if (firstDb) state.expanded.add(dbKey(firstDb.name));
    if (firstDb && firstSchema) {
      state.expanded.add(schemaKey(firstDb.name, firstSchema.name));
      state.expanded.add(folderKey(firstDb.name, firstSchema.name, "table"));
    }
    bindEvents();
    renderAll();
  }

  // Node keys for the expand/collapse set.
  function dbKey(db: string): string {
    return `db:${db}`;
  }
  function schemaKey(db: string, schema: string): string {
    return `db:${db}/schema:${schema}`;
  }
  function folderKey(db: string, schema: string, kind: "table" | "view"): string {
    return `db:${db}/schema:${schema}/folder:${kind}`;
  }
  function isExpanded(key: string): boolean {
    // While filtering, expand everything so matches are always visible.
    return state.filter.trim().length > 0 || state.expanded.has(key);
  }
  function toggleExpanded(key: string): void {
    if (state.expanded.has(key)) {
      state.expanded.delete(key);
    } else {
      state.expanded.add(key);
    }
    renderExplorer();
  }

  // ── Code editor (highlight overlay + gutter) ──────────────────────────────

  function syncEditor(): void {
    codeHighlight.innerHTML = highlightSql(state.queryText);
    const lineCount = Math.max(1, state.queryText.split("\n").length);
    let gutter = "";
    for (let line = 1; line <= lineCount; line += 1) {
      gutter += `${line}\n`;
    }
    codeGutter.textContent = gutter;
  }

  function syncScroll(): void {
    const top = -queryInput.scrollTop;
    const left = -queryInput.scrollLeft;
    codeHighlight.style.transform = `translate(${left}px, ${top}px)`;
    codeGutter.style.transform = `translateY(${top}px)`;
  }

  function bindEvents(): void {
    explorerFilter.addEventListener("input", () => {
      state.filter = explorerFilter.value;
      renderExplorer();
    });

    queryInput.addEventListener("input", () => {
      state.queryText = queryInput.value;
      syncEditor();
      syncScroll();
    });
    queryInput.addEventListener("scroll", syncScroll);

    // Tab inserts two spaces instead of leaving the editor.
    queryInput.addEventListener("keydown", (event) => {
      if (event.key === "Tab") {
        event.preventDefault();
        insertAtCursor("  ");
      } else if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
        event.preventDefault();
        void runCurrentQuery();
      }
    });

    protocolSelect.addEventListener("change", () => {
      state.protocol = protocolSelect.value as Protocol;
    });

    databaseSelect.addEventListener("change", () => {
      selectDatabase(databaseSelect.value);
    });

    schemaSelect.addEventListener("change", () => {
      selectSchema(schemaSelect.value, { loadTemplate: true });
    });

    runQueryButton.addEventListener("click", () => void runCurrentQuery());
    previewButton.addEventListener("click", () => applySelectedRelationTemplate(previewQuery));
    describeButton.addEventListener("click", () => applySelectedRelationTemplate(describeQuery));
    countButton.addEventListener("click", () => applySelectedRelationTemplate(countQuery));
  }

  function insertAtCursor(text: string): void {
    const start = queryInput.selectionStart;
    const end = queryInput.selectionEnd;
    queryInput.setRangeText(text, start, end, "end");
    state.queryText = queryInput.value;
    syncEditor();
    syncScroll();
  }

  // ── Explorer selection ────────────────────────────────────────────────────

  function selectDatabase(name: string): void {
    const snapshot = requireSnapshot();
    const database =
      snapshot.databases.find((candidate) => candidate.name === name) ?? snapshot.databases[0];
    if (!database) return;
    state.selectedDatabase = database.name;
    state.selectedSchema = database.schemas[0]?.name ?? "public";
    state.selectedRelation = database.schemas[0]?.relations[0];
    renderAll();
  }

  function selectSchema(name: string, options?: { loadTemplate?: boolean }): void {
    const database = selectedDatabase();
    const schema =
      database.schemas.find((candidate) => candidate.name === name) ?? database.schemas[0];
    if (!schema) return;
    state.selectedSchema = schema.name;
    state.selectedRelation = schema.relations[0];
    if (options?.loadTemplate) {
      setQueryText(showSchemaTablesQuery(schema.database, schema.name));
    }
    renderAll();
  }

  function selectRelation(relation: RelationMetadata): void {
    state.selectedDatabase = relation.database;
    state.selectedSchema = relation.schema;
    state.selectedRelation = relation;
    setQueryText(previewQuery(relation));
    renderAll();
  }

  function setQueryText(text: string): void {
    state.queryText = text;
    queryInput.value = text;
    syncEditor();
    syncScroll();
  }

  // ── Rendering ─────────────────────────────────────────────────────────────

  function renderAll(): void {
    renderSessionControls();
    renderExplorer();
    renderEditor();
    renderResult();
  }

  function renderSessionControls(): void {
    const snapshot = requireSnapshot();
    protocolSelect.value = state.protocol;
    renderOptions(
      databaseSelect,
      snapshot.databases.map((database) => database.name),
      state.selectedDatabase,
    );
    renderOptions(
      schemaSelect,
      selectedDatabase().schemas.map((schema) => schema.name),
      state.selectedSchema,
    );
  }

  function renderExplorer(): void {
    const snapshot = requireSnapshot();
    explorerTree.replaceChildren();

    for (const database of snapshot.databases) {
      const matchingSchemas = database.schemas
        .map((schema) => ({ schema, relations: filteredRelations(schema) }))
        .filter(
          ({ schema, relations }) =>
            matchesFilter(schema.name) || relations.length > 0 || matchesFilter(database.name),
        );

      if (matchingSchemas.length === 0) {
        continue;
      }

      const dKey = dbKey(database.name);
      const dbOpen = isExpanded(dKey);
      explorerTree.append(
        treeRow({
          depth: 0,
          twisty: true,
          open: dbOpen,
          iconName: "database",
          label: database.name,
          active: database.name === state.selectedDatabase,
          onClick: () => toggleExpanded(dKey),
        }),
      );
      if (!dbOpen) continue;

      for (const { schema, relations } of matchingSchemas) {
        const sKey = schemaKey(database.name, schema.name);
        const schemaOpen = isExpanded(sKey);
        explorerTree.append(
          treeRow({
            depth: 1,
            twisty: true,
            open: schemaOpen,
            iconName: "folder",
            iconClass: "tree-icon-schema",
            label: schema.name,
            active:
              schema.name === state.selectedSchema &&
              database.name === state.selectedDatabase,
            onClick: () => toggleExpanded(sKey),
          }),
        );
        if (!schemaOpen) continue;

        const tables = relations.filter((relation) => relation.kind !== "view");
        const views = relations.filter((relation) => relation.kind === "view");

        for (const group of [
          { kind: "table" as const, label: "Tables", items: tables },
          { kind: "view" as const, label: "Views", items: views },
        ]) {
          if (group.items.length === 0) continue;
          const fKey = folderKey(database.name, schema.name, group.kind);
          const folderOpen = isExpanded(fKey);
          explorerTree.append(
            treeRow({
              depth: 2,
              twisty: true,
              open: folderOpen,
              iconName: "folder",
              iconClass: "tree-icon-folder",
              label: group.label,
              count: group.items.length,
              onClick: () => toggleExpanded(fKey),
            }),
          );
          if (!folderOpen) continue;

          for (const relation of group.items) {
            explorerTree.append(
              treeRow({
                depth: 3,
                twisty: false,
                iconName: relation.kind === "view" ? "view" : "table",
                iconClass: relation.kind === "view" ? "tree-icon-view" : "tree-icon-table",
                label: relation.name,
                selected: isSelectedRelation(relation),
                onClick: () => selectRelation(relation),
              }),
            );
          }
        }
      }
    }

    if (explorerTree.children.length === 0) {
      explorerTree.append(emptyState("No catalog metadata matched this filter."));
    }
  }

  interface TreeRowOptions {
    depth: number;
    twisty: boolean;
    open?: boolean;
    iconName: Parameters<typeof icon>[0];
    iconClass?: string;
    label: string;
    count?: number;
    active?: boolean;
    selected?: boolean;
    onClick: () => void;
  }

  function treeRow(options: TreeRowOptions): HTMLElement {
    const row = element("button", "tree-row");
    row.type = "button";
    if (options.selected) row.classList.add("is-selected");
    if (options.active) row.classList.add("is-active");
    // Indent by depth; the twisty column is a fixed width so labels align.
    row.style.paddingLeft = `${6 + options.depth * 14}px`;
    row.addEventListener("click", options.onClick);

    const twisty = element("span", "tree-twisty");
    if (options.twisty) {
      if (options.open) twisty.classList.add("is-open");
      twisty.innerHTML = icon("chevron-right", 12);
    }

    const iconWrap = element("span", `tree-icon ${options.iconClass ?? ""}`.trim());
    iconWrap.innerHTML = icon(options.iconName, 14);

    const label = element("span", "tree-label");
    label.textContent = options.label;

    row.append(twisty, iconWrap, label);

    if (options.count !== undefined) {
      const count = element("span", "tree-count");
      count.textContent = String(options.count);
      row.append(count);
    }
    return row;
  }

  function renderEditor(): void {
    if (queryInput.value !== state.queryText) {
      queryInput.value = state.queryText;
    }
    syncEditor();
    syncScroll();
    runQueryButton.disabled = state.isRunning;
    previewButton.disabled = !state.selectedRelation;
    describeButton.disabled = !state.selectedRelation;
    countButton.disabled = !state.selectedRelation;
    runQueryButton.classList.toggle("is-running", state.isRunning);
    const label = runQueryButton.querySelector("span");
    if (label) {
      label.textContent = state.isRunning ? "Running…" : "Run query";
    }
  }

  function renderResult(): void {
    const running = state.isRunning || state.isStreaming;
    resultState.textContent = state.isRunning
      ? "Running"
      : state.isStreaming
        ? "Streaming"
        : state.latestResult
          ? state.latestResult.statementType
          : "Idle";
    resultState.classList.toggle("badge-running", running);

    resultGrid.replaceChildren();
    resultMessages.replaceChildren();
    resultRows.textContent = "";

    const timingHeader = mustQuery<HTMLDivElement>(container, "#result-timing-header");
    timingHeader.replaceChildren();

    if (running) {
      if (state.isStreaming && state.streamingColumns.length > 0) {
        resultGrid.append(renderGridFromState());
        resultRows.textContent = `${state.streamingRows.length} rows`;
        if (state.streamingMessages.length > 0) renderMessages(state.streamingMessages);
        if (state.streamingTimings) renderTiming(state.streamingTimings, timingHeader);
      } else {
        resultGrid.append(loadingCard());
      }
      return;
    }

    const result = state.latestResult;
    if (!result) {
      resultGrid.append(
        emptyState("Run a query to see rows, messages, and timings here."),
      );
      return;
    }

    renderMessages(result.messages);
    resultRows.textContent = `${result.rows.length} rows`;
    renderTiming(result.timings, timingHeader);
    resultGrid.append(renderGrid(result));
  }

  function renderMessages(messages: readonly QueryMessage[]): void {
    resultMessages.replaceChildren();
    for (const message of messages) {
      const messageNode = element("div", `message message-${message.level}`);
      const iconName =
        message.level === "error"
          ? "x-circle"
          : message.level === "warning"
            ? "alert-triangle"
            : "circle-check";
      messageNode.innerHTML = `${icon(iconName, 14)}<span>${escapeHtml(message.text)}</span>`;
      resultMessages.append(messageNode);
    }
  }

  function renderTiming(timings: QueryTiming, header: HTMLElement): void {
    const block = element("div", "timing-inline");
    block.innerHTML = `
      <span class="timing-item" title="Execution time">Execute: ${timings.executeMs}ms</span>
      <span class="timing-item" title="Fetch time">Fetch: ${timings.fetchMs}ms</span>
      <span class="timing-item timing-total" title="Total time">Total: ${timings.totalMs}ms</span>
    `;
    header.append(block);
  }

  async function runCurrentQuery(): Promise<void> {
    if (state.isRunning || state.isStreaming) return;
    state.queryText = queryInput.value;
    state.isRunning = true;
    state.isStreaming = false;
    state.streamingRows = [];
    state.streamingColumns = [];
    state.streamingMessages = [];
    state.streamingTimings = undefined;
    renderEditor();
    renderResult();

    const streamingResult = state.client.executeQueryStreaming({
      sql: state.queryText,
      protocol: state.protocol,
      database: state.selectedDatabase,
      schema: state.selectedSchema,
    });

    state.isRunning = false;
    state.isStreaming = true;
    renderEditor();
    renderResult();

    streamingResult.onChunk(async (chunk: QueryResultChunk) => {
      if (chunk.columns) state.streamingColumns = chunk.columns;
      state.streamingRows = [...state.streamingRows, ...chunk.rows];
      if (chunk.messages) state.streamingMessages = chunk.messages;
      if (chunk.timings) state.streamingTimings = chunk.timings;
      renderResult();

      if (chunk.isLast) {
        state.isStreaming = false;
        const finalResult = await streamingResult.onComplete();
        state.latestResult = finalResult;
        state.streamingRows = [];
        state.streamingColumns = [];
        state.streamingMessages = [];
        state.streamingTimings = undefined;
        renderEditor();
        renderResult();
      }
    });
  }

  function applySelectedRelationTemplate(
    template: (relation: RelationMetadata) => string,
  ): void {
    if (!state.selectedRelation) return;
    setQueryText(template(state.selectedRelation));
  }

  // ── Small helpers ───────────────────────────────────────────────────────

  function renderOptions(
    select: HTMLSelectElement,
    options: readonly string[],
    selectedValue: string,
  ): void {
    select.replaceChildren();
    for (const optionValue of options) {
      const option = document.createElement("option");
      option.value = optionValue;
      option.textContent = optionValue;
      option.selected = optionValue === selectedValue;
      select.append(option);
    }
  }

  function renderGrid(result: QueryResult): HTMLElement {
    if (result.columns.length === 0) {
      return emptyState("This statement returned no result columns.");
    }
    return buildTable(result.columns, result.rows);
  }

  function renderGridFromState(): HTMLElement {
    if (state.streamingColumns.length === 0) {
      return emptyState("Waiting for column information…");
    }
    const table = buildTable(state.streamingColumns, state.streamingRows, "streaming");
    if (state.isStreaming) {
      const tbody = table.querySelector("tbody")!;
      const progressRow = document.createElement("tr");
      const progressCell = document.createElement("td");
      progressCell.colSpan = state.streamingColumns.length;
      progressCell.className = "streaming-progress";
      progressCell.textContent = `Loading… (${state.streamingRows.length} rows)`;
      progressRow.append(progressCell);
      tbody.append(progressRow);
    }
    return table;
  }

  function buildTable(
    columns: readonly string[],
    rows: readonly (readonly CellValue[])[],
    extraClass = "",
  ): HTMLElement {
    const table = element("table", `data-grid${extraClass ? ` ${extraClass}` : ""}`);
    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    for (const column of columns) {
      const th = document.createElement("th");
      th.textContent = column;
      headRow.append(th);
    }
    thead.append(headRow);

    const tbody = document.createElement("tbody");
    for (const row of rows) {
      const tr = document.createElement("tr");
      for (const cell of row) {
        const td = document.createElement("td");
        if (cell === null) {
          td.textContent = "NULL";
          td.classList.add("cell-null");
        } else {
          td.textContent = String(cell);
        }
        tr.append(td);
      }
      tbody.append(tr);
    }
    table.append(thead, tbody);
    return table;
  }

  function filteredRelations(schema: SchemaMetadata): readonly RelationMetadata[] {
    if (!state.filter.trim()) return schema.relations;
    return schema.relations.filter(
      (relation) =>
        matchesFilter(relation.name) ||
        matchesFilter(relation.kind) ||
        matchesFilter(relation.storage) ||
        relation.columns.some((column) => matchesFilter(column.name)),
    );
  }

  function matchesFilter(value: string): boolean {
    return value.toLowerCase().includes(state.filter.trim().toLowerCase());
  }

  function selectedDatabase(): DatabaseMetadata {
    const snapshot = requireSnapshot();
    return (
      snapshot.databases.find((database) => database.name === state.selectedDatabase) ??
      snapshot.databases[0]
    );
  }

  function requireSnapshot(): ExplorerSnapshot {
    if (!state.snapshot) {
      throw new Error("Explorer metadata has not loaded yet");
    }
    return state.snapshot;
  }

  function loadingCard(): HTMLElement {
    const card = element("div", "loading-card");
    card.textContent = "Submitting query to the prototype console client…";
    return card;
  }

  function emptyState(message: string): HTMLElement {
    const empty = element("div", "empty-state");
    empty.textContent = message;
    return empty;
  }

  function isSelectedRelation(relation: RelationMetadata): boolean {
    return (
      state.selectedRelation?.database === relation.database &&
      state.selectedRelation.schema === relation.schema &&
      state.selectedRelation.name === relation.name
    );
  }

  function element<TagName extends keyof HTMLElementTagNameMap>(
    tagName: TagName,
    className: string,
  ): HTMLElementTagNameMap[TagName] {
    const node = document.createElement(tagName);
    node.className = className;
    return node;
  }

  function escapeHtml(text: string): string {
    return text
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function cmdKeyLabel(): string {
    return navigator.platform.toLowerCase().includes("mac") ? "⌘" : "Ctrl";
  }

  function mustQuery<ElementType extends Element>(
    rootNode: ParentNode,
    selector: string,
  ): ElementType {
    const node = rootNode.querySelector<ElementType>(selector);
    if (!node) {
      throw new Error(`Missing required element: ${selector}`);
    }
    return node;
  }
}
