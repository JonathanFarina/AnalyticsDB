import { PrototypeConsoleClient } from "../consoleClient";
import type {
  CellValue,
  DatabaseMetadata,
  ExplorerSnapshot,
  Protocol,
  QueryResult,
  RelationMetadata,
  SchemaMetadata,
} from "../domain";
import { icon } from "../icons";
import {
  countQuery,
  describeQuery,
  previewQuery,
  showSchemaTablesQuery,
} from "../queryTemplates";

interface QueryViewState {
  readonly client: PrototypeConsoleClient;
  snapshot?: ExplorerSnapshot;
  selectedDatabase: string;
  selectedSchema: string;
  selectedRelation?: RelationMetadata;
  protocol: Protocol;
  queryText: string;
  filter: string;
  isRunning: boolean;
  latestResult?: QueryResult;
  history: readonly QueryResult[];
}

export function mountQueryView(container: HTMLElement): void {
  const state: QueryViewState = {
    client: new PrototypeConsoleClient(),
    selectedDatabase: "postgres",
    selectedSchema: "public",
    protocol: "postgres",
    queryText:
      'SELECT *\nFROM "postgres"."public"."fact_metrics"\nLIMIT 100;',
    filter: "",
    isRunning: false,
    history: [],
  };

  container.innerHTML = `
    <div class="page-header">
      <div class="page-header-title">
        <p class="page-pretitle">Workspace</p>
        <h2 class="page-title">SQL Query</h2>
      </div>
      <div class="page-header-actions">
        <span class="status-pill status-pill-success">
          <span class="status-dot"></span>
          Engine online
        </span>
      </div>
    </div>

    <div class="page-body">
      <div class="row-cards row-cards-3">
        <div class="card metric-card">
          <div class="card-body">
            <div class="metric-label">Active database</div>
            <div class="metric-value" id="metric-database">postgres</div>
          </div>
        </div>
        <div class="card metric-card">
          <div class="card-body">
            <div class="metric-label">Active schema</div>
            <div class="metric-value" id="metric-schema">public</div>
          </div>
        </div>
        <div class="card metric-card">
          <div class="card-body">
            <div class="metric-label">Client mode</div>
            <div class="metric-value">Prototype</div>
          </div>
        </div>
      </div>

      <div class="workspace-grid">
        <aside class="card explorer-card" aria-labelledby="explorer-heading">
          <div class="card-header">
            <h3 id="explorer-heading" class="card-title">Explorer</h3>
            <span class="badge badge-soft">Catalog</span>
          </div>
          <div class="card-body">
            <div class="input-icon">
              ${icon("search", 16)}
              <input
                id="explorer-filter"
                class="form-control"
                type="search"
                placeholder="Filter objects…"
                aria-label="Filter catalog objects"
              />
            </div>
            <div id="explorer-tree" class="explorer-tree" aria-live="polite"></div>
          </div>
        </aside>

        <section class="editor-stack">
          <div class="card query-card" aria-labelledby="query-heading">
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
            <div class="card-body">
              <div class="editor-frame">
                <div class="line-rail" aria-hidden="true">1<br />2<br />3<br />4<br />5<br />6<br />7<br />8<br />9<br />10</div>
                <textarea id="query-input" spellcheck="false" aria-label="SQL query editor"></textarea>
              </div>
              <div class="query-actions">
                <button id="run-query" class="btn btn-primary" type="button">
                  ${icon("play", 14)}
                  <span>Run query</span>
                </button>
                <button id="preview-relation" class="btn" type="button">Preview</button>
                <button id="describe-relation" class="btn" type="button">Describe</button>
                <button id="count-relation" class="btn" type="button">Count rows</button>
              </div>
            </div>
          </div>

          <div class="card result-card" aria-labelledby="results-heading">
            <div class="card-header">
              <h3 id="results-heading" class="card-title">Results</h3>
              <span id="result-state" class="badge">Idle</span>
            </div>
            <div class="card-body">
              <div id="result-summary" class="result-summary"></div>
              <div id="result-grid" class="result-grid" aria-live="polite"></div>
              <div id="result-messages" class="message-list"></div>
            </div>
          </div>
        </section>

        <aside class="card inspector-card" aria-labelledby="inspector-heading">
          <div class="card-header">
            <h3 id="inspector-heading" class="card-title">Inspector</h3>
          </div>
          <div class="card-body">
            <div id="relation-inspector"></div>
            <div class="history-block">
              <h4 class="subheader">Recent runs</h4>
              <div id="query-history"></div>
            </div>
          </div>
        </aside>
      </div>
    </div>
  `;

  const metricDatabase = mustQuery<HTMLElement>(container, "#metric-database");
  const metricSchema = mustQuery<HTMLElement>(container, "#metric-schema");
  const explorerFilter = mustQuery<HTMLInputElement>(container, "#explorer-filter");
  const explorerTree = mustQuery<HTMLDivElement>(container, "#explorer-tree");
  const queryInput = mustQuery<HTMLTextAreaElement>(container, "#query-input");
  const protocolSelect = mustQuery<HTMLSelectElement>(container, "#protocol-select");
  const databaseSelect = mustQuery<HTMLSelectElement>(container, "#database-select");
  const schemaSelect = mustQuery<HTMLSelectElement>(container, "#schema-select");
  const runQueryButton = mustQuery<HTMLButtonElement>(container, "#run-query");
  const previewButton = mustQuery<HTMLButtonElement>(container, "#preview-relation");
  const describeButton = mustQuery<HTMLButtonElement>(container, "#describe-relation");
  const countButton = mustQuery<HTMLButtonElement>(container, "#count-relation");
  const resultState = mustQuery<HTMLSpanElement>(container, "#result-state");
  const resultSummary = mustQuery<HTMLDivElement>(container, "#result-summary");
  const resultGrid = mustQuery<HTMLDivElement>(container, "#result-grid");
  const resultMessages = mustQuery<HTMLDivElement>(container, "#result-messages");
  const relationInspector = mustQuery<HTMLDivElement>(container, "#relation-inspector");
  const queryHistory = mustQuery<HTMLDivElement>(container, "#query-history");

  void bootstrap();

  async function bootstrap(): Promise<void> {
    state.snapshot = await state.client.getExplorerSnapshot();
    state.selectedRelation = state.snapshot.databases[0].schemas[0].relations[0];
    bindEvents();
    renderAll();
  }

  function bindEvents(): void {
    explorerFilter.addEventListener("input", () => {
      state.filter = explorerFilter.value;
      renderExplorer();
    });

    queryInput.addEventListener("input", () => {
      state.queryText = queryInput.value;
    });

    protocolSelect.addEventListener("change", () => {
      state.protocol = protocolSelect.value as Protocol;
    });

    databaseSelect.addEventListener("change", () => {
      const snapshot = requireSnapshot();
      const database =
        snapshot.databases.find((candidate) => candidate.name === databaseSelect.value) ??
        snapshot.databases[0];
      state.selectedDatabase = database.name;
      state.selectedSchema = database.schemas[0]?.name ?? "public";
      state.selectedRelation = database.schemas[0]?.relations[0];
      renderAll();
    });

    schemaSelect.addEventListener("change", () => {
      const database = selectedDatabase();
      const schema =
        database.schemas.find((candidate) => candidate.name === schemaSelect.value) ??
        database.schemas[0];
      state.selectedSchema = schema.name;
      state.selectedRelation = schema.relations[0];
      state.queryText = showSchemaTablesQuery(schema.database, schema.name);
      renderAll();
    });

    runQueryButton.addEventListener("click", () => {
      void runCurrentQuery();
    });

    previewButton.addEventListener("click", () => applySelectedRelationTemplate(previewQuery));
    describeButton.addEventListener("click", () => applySelectedRelationTemplate(describeQuery));
    countButton.addEventListener("click", () => applySelectedRelationTemplate(countQuery));
  }

  function renderAll(): void {
    renderSessionControls();
    renderExplorer();
    renderEditor();
    renderInspector();
    renderResult();
    renderHistory();
  }

  function renderSessionControls(): void {
    const snapshot = requireSnapshot();
    protocolSelect.value = state.protocol;
    metricDatabase.textContent = state.selectedDatabase;
    metricSchema.textContent = state.selectedSchema;
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
        .map((schema) => ({
          schema,
          relations: filteredRelations(schema),
        }))
        .filter(
          ({ schema, relations }) =>
            matchesFilter(schema.name) ||
            relations.length > 0 ||
            matchesFilter(database.name),
        );

      if (matchingSchemas.length === 0) {
        continue;
      }

      const databaseNode = element("section", "database-node");
      databaseNode.append(
        metadataHeader(
          database.name,
          `${database.schemas.length} schemas`,
          database.name === state.selectedDatabase,
          "database",
        ),
      );

      for (const { schema, relations } of matchingSchemas) {
        const schemaNode = element("section", "schema-node");
        schemaNode.append(
          metadataHeader(
            schema.name,
            `${schema.relations.length} relations`,
            schema.name === state.selectedSchema,
            "chevron",
          ),
        );

        const relationList = element("div", "relation-list");
        for (const relation of relations) {
          const relationButton = element("button", "relation-row");
          if (isSelectedRelation(relation)) {
            relationButton.classList.add("is-selected");
          }
          relationButton.type = "button";
          relationButton.addEventListener("click", () => selectRelation(relation));
          const iconWrap = document.createElement("span");
          iconWrap.className = "relation-icon";
          iconWrap.innerHTML = icon(relation.kind === "view" ? "view" : "table", 14);
          relationButton.append(iconWrap);
          relationButton.append(
            textBlock(relation.name, `${relation.kind} · ${relation.storage}`),
          );
          relationButton.append(countPill(relation.rowEstimate));
          relationList.append(relationButton);
        }

        schemaNode.append(relationList);
        databaseNode.append(schemaNode);
      }

      explorerTree.append(databaseNode);
    }

    if (explorerTree.children.length === 0) {
      explorerTree.append(emptyState("No catalog metadata matched this filter."));
    }
  }

  function renderEditor(): void {
    queryInput.value = state.queryText;
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

  function renderInspector(): void {
    relationInspector.replaceChildren();

    const relation = state.selectedRelation;
    if (!relation) {
      relationInspector.append(
        emptyState("Select a table or view to inspect its columns."),
      );
      return;
    }

    const summary = element("div", "relation-summary");
    summary.append(textBlock(relation.name, relation.description));
    summary.append(
      detailGrid([
        ["Database", relation.database],
        ["Schema", relation.schema],
        ["Kind", relation.kind],
        ["Storage", relation.storage],
        ["Rows", formatCount(relation.rowEstimate)],
      ]),
    );

    const columnList = element("div", "column-list");
    for (const column of relation.columns) {
      const row = element("div", "column-row");
      row.append(textBlock(column.name, column.type));
      row.append(badge(column.nullable ? "nullable" : "required"));
      columnList.append(row);
    }

    relationInspector.append(summary, subheader("Columns"), columnList);
  }

  function renderResult(): void {
    resultState.textContent = state.isRunning
      ? "Running"
      : state.latestResult
        ? state.latestResult.statementType
        : "Idle";
    resultState.classList.toggle("badge-running", state.isRunning);
    resultSummary.replaceChildren();
    resultGrid.replaceChildren();
    resultMessages.replaceChildren();

    if (state.isRunning) {
      resultSummary.append(loadingCard());
      return;
    }

    const result = state.latestResult;
    if (!result) {
      resultSummary.append(
        emptyState("Run SQL from the editor to inspect rows, messages, query id, and timings."),
      );
      return;
    }

    resultSummary.append(
      statCard("Query id", result.queryId),
      statCard("Rows", String(result.rows.length)),
      statCard("Total", `${result.timings.totalMs} ms`),
      statCard("Plan", `${result.timings.planMs} ms`),
    );

    resultGrid.append(renderGrid(result));

    for (const message of result.messages) {
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

  function renderHistory(): void {
    queryHistory.replaceChildren();

    if (state.history.length === 0) {
      queryHistory.append(emptyState("No queries have run in this browser session."));
      return;
    }

    for (const result of state.history) {
      const item = element("button", "history-item");
      item.type = "button";
      item.addEventListener("click", () => {
        state.latestResult = result;
        renderResult();
      });
      item.append(
        textBlock(result.queryId, `${result.statementType} · ${result.timings.totalMs} ms`),
      );
      queryHistory.append(item);
    }
  }

  async function runCurrentQuery(): Promise<void> {
    state.queryText = queryInput.value;
    state.isRunning = true;
    renderEditor();
    renderResult();

    const result = await state.client.executeQuery({
      sql: state.queryText,
      protocol: state.protocol,
      database: state.selectedDatabase,
      schema: state.selectedSchema,
    });

    state.latestResult = result;
    state.history = [result, ...state.history].slice(0, 6);
    state.isRunning = false;
    renderEditor();
    renderResult();
    renderHistory();
  }

  function selectRelation(relation: RelationMetadata): void {
    state.selectedDatabase = relation.database;
    state.selectedSchema = relation.schema;
    state.selectedRelation = relation;
    state.queryText = previewQuery(relation);
    renderAll();
  }

  function applySelectedRelationTemplate(
    template: (relation: RelationMetadata) => string,
  ): void {
    if (!state.selectedRelation) {
      return;
    }

    state.queryText = template(state.selectedRelation);
    renderEditor();
  }

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

    const table = element("table", "data-grid");
    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    for (const column of result.columns) {
      const th = document.createElement("th");
      th.textContent = column;
      headRow.append(th);
    }
    thead.append(headRow);

    const tbody = document.createElement("tbody");
    for (const row of result.rows) {
      const tr = document.createElement("tr");
      for (const cell of row) {
        const td = document.createElement("td");
        td.textContent = formatCell(cell);
        tr.append(td);
      }
      tbody.append(tr);
    }

    table.append(thead, tbody);
    return table;
  }

  function filteredRelations(schema: SchemaMetadata): readonly RelationMetadata[] {
    if (!state.filter.trim()) {
      return schema.relations;
    }

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

  function metadataHeader(
    title: string,
    detail: string,
    isActive: boolean,
    leadingIcon: "database" | "chevron",
  ): HTMLElement {
    const header = element("div", "metadata-header");
    if (isActive) {
      header.classList.add("is-active");
    }
    const iconWrap = document.createElement("span");
    iconWrap.className = "metadata-icon";
    iconWrap.innerHTML = icon(leadingIcon === "database" ? "database" : "chevron-right", 14);
    header.append(iconWrap, textBlock(title, detail));
    return header;
  }

  function textBlock(title: string, detail: string): HTMLElement {
    const block = element("span", "text-block");
    const titleNode = document.createElement("strong");
    titleNode.textContent = title;
    const detailNode = document.createElement("small");
    detailNode.textContent = detail;
    block.append(titleNode, detailNode);
    return block;
  }

  function detailGrid(items: readonly (readonly [string, string])[]): HTMLElement {
    const grid = element("dl", "detail-grid");
    for (const [label, value] of items) {
      const dt = document.createElement("dt");
      dt.textContent = label;
      const dd = document.createElement("dd");
      dd.textContent = value;
      grid.append(dt, dd);
    }
    return grid;
  }

  function statCard(label: string, value: string): HTMLElement {
    const card = element("div", "stat-card");
    const span = document.createElement("span");
    span.textContent = label;
    const strong = document.createElement("strong");
    strong.textContent = value;
    card.append(span, strong);
    return card;
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

  function subheader(label: string): HTMLElement {
    const heading = document.createElement("h4");
    heading.className = "subheader";
    heading.textContent = label;
    return heading;
  }

  function countPill(count: number | undefined): HTMLElement {
    return badge(formatCount(count));
  }

  function badge(label: string): HTMLElement {
    const span = element("span", "badge badge-soft");
    span.textContent = label;
    return span;
  }

  function isSelectedRelation(relation: RelationMetadata): boolean {
    return (
      state.selectedRelation?.database === relation.database &&
      state.selectedRelation.schema === relation.schema &&
      state.selectedRelation.name === relation.name
    );
  }

  function formatCell(value: CellValue): string {
    if (value === null) {
      return "NULL";
    }

    return String(value);
  }

  function formatCount(value: number | undefined): string {
    return value === undefined ? "unknown" : new Intl.NumberFormat("en-US").format(value);
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

  function mustQuery<ElementType extends Element>(
    root: ParentNode,
    selector: string,
  ): ElementType {
    const node = root.querySelector<ElementType>(selector);
    if (!node) {
      throw new Error(`Missing required element: ${selector}`);
    }

    return node;
  }
}
