import "./styles.css";
import { PrototypeConsoleClient } from "./consoleClient";
import type {
  CellValue,
  DatabaseMetadata,
  ExplorerSnapshot,
  Protocol,
  QueryResult,
  RelationMetadata,
  SchemaMetadata,
} from "./domain";
import { countQuery, describeQuery, previewQuery, showSchemaTablesQuery } from "./queryTemplates";

interface ConsoleState {
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

const state: ConsoleState = {
  client: new PrototypeConsoleClient(),
  selectedDatabase: "postgres",
  selectedSchema: "public",
  protocol: "postgres",
  queryText: "SELECT *\nFROM \"postgres\".\"public\".\"fact_metrics\"\nLIMIT 100;",
  filter: "",
  isRunning: false,
  history: [],
};

const app = document.querySelector<HTMLDivElement>("#app");
if (!app) {
  throw new Error("Missing #app root element");
}

app.innerHTML = `
  <div class="console-shell">
    <aside class="product-rail" aria-label="Primary navigation">
      <div class="brand-lockup">
        <span class="brand-mark">A</span>
        <span>AnalyticsDB</span>
      </div>
      <nav class="rail-nav">
        <a class="rail-link is-active" href="#query-heading"><span>Q</span> Query editor</a>
        <a class="rail-link" href="#explorer-heading"><span>E</span> Explorer</a>
        <a class="rail-link" href="#results-heading"><span>R</span> Results</a>
        <a class="rail-link" href="#inspector-heading"><span>I</span> Inspector</a>
        <a class="rail-link is-disabled" href="#"><span>M</span> Metrics</a>
        <a class="rail-link is-disabled" href="#"><span>L</span> Logs</a>
      </nav>
      <div class="rail-footer">
        <span class="status-dot"></span>
        <div>
          <strong>Prototype UI</strong>
          <small>No live web gateway yet</small>
        </div>
      </div>
    </aside>

    <div class="app-frame">
      <header class="topbar">
        <button class="menu-button" type="button" aria-label="Open navigation">Menu</button>
        <div class="global-search" role="search">
          <span>Search resources, saved queries, jobs</span>
          <kbd>/</kbd>
        </div>
        <div class="topbar-actions">
          <span class="topbar-chip">PostgreSQL + Flight SQL</span>
          <span class="avatar" aria-label="Signed in administrator">JA</span>
        </div>
      </header>

      <section class="page-header" aria-label="Query workspace summary">
        <div>
          <p class="eyebrow">BigQuery-style workbench</p>
          <h1>SQL workspace</h1>
          <p class="page-copy">Explore catalog metadata, compose SQL, and inspect prototype result timing without leaving the query page.</p>
        </div>
        <div class="metric-strip">
          <article class="metric-card">
            <span>Selected database</span>
            <strong id="selected-database-metric">postgres</strong>
          </article>
          <article class="metric-card">
            <span>Selected schema</span>
            <strong id="selected-schema-metric">public</strong>
          </article>
          <article class="metric-card">
            <span>Client mode</span>
            <strong>Prototype</strong>
          </article>
        </div>
      </section>

      <section class="workspace-grid" aria-label="Admin console workspace">
        <aside class="explorer-card panel" aria-labelledby="explorer-heading">
          <div class="panel-heading">
            <div>
              <p class="eyebrow">Resources</p>
              <h2 id="explorer-heading">Explorer</h2>
            </div>
            <span class="badge">Preview</span>
          </div>
          <label class="field-label" for="explorer-filter">Filter metadata</label>
          <input id="explorer-filter" class="filter-input" type="search" placeholder="fact, reporting, pg_" />
          <div id="explorer-tree" class="explorer-tree" aria-live="polite"></div>
        </aside>

        <main class="editor-stack">
          <section class="query-card panel" aria-labelledby="query-heading">
            <div class="query-tabs" aria-label="Query tabs">
              <button class="query-tab is-active" type="button">Untitled query</button>
              <button class="query-tab" type="button">Scratchpad</button>
              <button class="query-tab add-tab" type="button">+</button>
            </div>
            <div class="panel-heading editor-heading">
              <div>
                <p class="eyebrow">SQL workbench</p>
                <h2 id="query-heading">Query editor</h2>
              </div>
              <div class="session-controls">
                <label>
                  <span>Protocol</span>
                  <select id="protocol-select">
                    <option value="postgres">PostgreSQL</option>
                    <option value="flight-sql">Arrow Flight SQL</option>
                  </select>
                </label>
                <label>
                  <span>Database</span>
                  <select id="database-select"></select>
                </label>
                <label>
                  <span>Schema</span>
                  <select id="schema-select"></select>
                </label>
              </div>
            </div>
            <div class="editor-frame">
              <div class="line-rail" aria-hidden="true">1<br />2<br />3<br />4<br />5<br />6<br />7<br />8</div>
              <textarea id="query-input" spellcheck="false" aria-label="SQL query editor"></textarea>
            </div>
            <div class="query-actions">
              <button id="run-query" class="primary-action" type="button">Run query</button>
              <button id="preview-relation" type="button">Preview selected</button>
              <button id="describe-relation" type="button">Describe</button>
              <button id="count-relation" type="button">Count rows</button>
            </div>
          </section>

          <section class="result-card panel" aria-labelledby="results-heading">
            <div class="panel-heading results-heading">
              <div>
                <p class="eyebrow">Execution</p>
                <h2 id="results-heading">Results</h2>
              </div>
              <span id="result-state" class="badge">Idle</span>
            </div>
            <div id="result-summary" class="result-summary"></div>
            <div id="result-grid" class="result-grid" aria-live="polite"></div>
            <div id="result-messages" class="message-list"></div>
          </section>
        </main>

        <aside class="inspector-card panel" aria-labelledby="inspector-heading">
          <div class="panel-heading">
            <div>
              <p class="eyebrow">Details</p>
              <h2 id="inspector-heading">Inspector</h2>
            </div>
          </div>
          <div id="relation-inspector"></div>
          <div class="history-block">
            <h3>Recent runs</h3>
            <div id="query-history"></div>
          </div>
        </aside>
      </section>
    </div>
  </div>
`;

const selectedDatabaseMetric = mustQuery<HTMLElement>("#selected-database-metric");
const selectedSchemaMetric = mustQuery<HTMLElement>("#selected-schema-metric");

const explorerFilter = mustQuery<HTMLInputElement>("#explorer-filter");
const explorerTree = mustQuery<HTMLDivElement>("#explorer-tree");
const queryInput = mustQuery<HTMLTextAreaElement>("#query-input");
const protocolSelect = mustQuery<HTMLSelectElement>("#protocol-select");
const databaseSelect = mustQuery<HTMLSelectElement>("#database-select");
const schemaSelect = mustQuery<HTMLSelectElement>("#schema-select");
const runQueryButton = mustQuery<HTMLButtonElement>("#run-query");
const previewButton = mustQuery<HTMLButtonElement>("#preview-relation");
const describeButton = mustQuery<HTMLButtonElement>("#describe-relation");
const countButton = mustQuery<HTMLButtonElement>("#count-relation");
const resultState = mustQuery<HTMLSpanElement>("#result-state");
const resultSummary = mustQuery<HTMLDivElement>("#result-summary");
const resultGrid = mustQuery<HTMLDivElement>("#result-grid");
const resultMessages = mustQuery<HTMLDivElement>("#result-messages");
const relationInspector = mustQuery<HTMLDivElement>("#relation-inspector");
const queryHistory = mustQuery<HTMLDivElement>("#query-history");

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
  selectedDatabaseMetric.textContent = state.selectedDatabase;
  selectedSchemaMetric.textContent = state.selectedSchema;
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
      .filter(({ schema, relations }) => matchesFilter(schema.name) || relations.length > 0 || matchesFilter(database.name));

    if (matchingSchemas.length === 0) {
      continue;
    }

    const databaseNode = element("section", "database-node");
    databaseNode.append(
      metadataHeader(database.name, `${database.schemas.length} schemas`, database.name === state.selectedDatabase),
    );

    for (const { schema, relations } of matchingSchemas) {
      const schemaNode = element("section", "schema-node");
      schemaNode.append(metadataHeader(schema.name, `${schema.relations.length} relations`, schema.name === state.selectedSchema));

      const relationList = element("div", "relation-list");
      for (const relation of relations) {
        const relationButton = element("button", "relation-row");
        if (isSelectedRelation(relation)) {
          relationButton.classList.add("is-selected");
        }
        relationButton.type = "button";
        relationButton.addEventListener("click", () => selectRelation(relation));
        relationButton.append(textBlock(relation.name, `${relation.kind} - ${relation.storage}`));
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
  runQueryButton.textContent = state.isRunning ? "Running..." : "Run query";
}

function renderInspector(): void {
  relationInspector.replaceChildren();

  const relation = state.selectedRelation;
  if (!relation) {
    relationInspector.append(emptyState("Select a table or view to inspect its columns."));
    return;
  }

  const summary = element("div", "relation-summary");
  summary.append(textBlock(relation.name, relation.description));
  summary.append(detailGrid([
    ["Database", relation.database],
    ["Schema", relation.schema],
    ["Kind", relation.kind],
    ["Storage", relation.storage],
    ["Rows", formatCount(relation.rowEstimate)],
  ]));

  const columnList = element("div", "column-list");
  for (const column of relation.columns) {
    const row = element("div", "column-row");
    row.append(textBlock(column.name, column.type));
    row.append(badge(column.nullable ? "nullable" : "required"));
    columnList.append(row);
  }

  relationInspector.append(summary, sectionTitle("Columns"), columnList);
}

function renderResult(): void {
  resultState.textContent = state.isRunning ? "Running" : state.latestResult ? state.latestResult.statementType : "Idle";
  resultSummary.replaceChildren();
  resultGrid.replaceChildren();
  resultMessages.replaceChildren();

  if (state.isRunning) {
    resultSummary.append(loadingCard());
    return;
  }

  const result = state.latestResult;
  if (!result) {
    resultSummary.append(emptyState("Run SQL from the editor to inspect rows, messages, query id, and timings."));
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
    messageNode.textContent = message.text;
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
    item.append(textBlock(result.queryId, `${result.statementType} - ${result.timings.totalMs} ms`));
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

function applySelectedRelationTemplate(template: (relation: RelationMetadata) => string): void {
  if (!state.selectedRelation) {
    return;
  }

  state.queryText = template(state.selectedRelation);
  renderEditor();
}

function renderOptions(select: HTMLSelectElement, options: readonly string[], selectedValue: string): void {
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
  return snapshot.databases.find((database) => database.name === state.selectedDatabase) ?? snapshot.databases[0];
}

function selectedSchema(): SchemaMetadata {
  const database = selectedDatabase();
  return database.schemas.find((schema) => schema.name === state.selectedSchema) ?? database.schemas[0];
}

function requireSnapshot(): ExplorerSnapshot {
  if (!state.snapshot) {
    throw new Error("Explorer metadata has not loaded yet");
  }

  return state.snapshot;
}

function metadataHeader(title: string, detail: string, isActive: boolean): HTMLElement {
  const header = element("div", "metadata-header");
  if (isActive) {
    header.classList.add("is-active");
  }
  header.append(textBlock(title, detail));
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
  card.textContent = "Submitting query to the prototype console client...";
  return card;
}

function emptyState(message: string): HTMLElement {
  const empty = element("div", "empty-state");
  empty.textContent = message;
  return empty;
}

function sectionTitle(title: string): HTMLElement {
  const heading = document.createElement("h3");
  heading.textContent = title;
  return heading;
}

function countPill(count: number | undefined): HTMLElement {
  return badge(formatCount(count));
}

function badge(label: string): HTMLElement {
  const span = element("span", "badge");
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

function mustQuery<ElementType extends Element>(selector: string): ElementType {
  const node = document.querySelector<ElementType>(selector);
  if (!node) {
    throw new Error(`Missing required element: ${selector}`);
  }

  return node;
}
