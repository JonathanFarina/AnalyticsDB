import type {
  AnalyticsConsoleClient,
  CellValue,
  ExplorerSnapshot,
  QueryRequest,
  QueryResult,
  QueryStatementType,
  QueryTiming,
  RelationMetadata,
} from "./domain";
import {
  allRelations,
  findDatabase,
  findRelation,
  findSchema,
  prototypeExplorerSnapshot,
  prototypeRowsByRelation,
  relationKey,
} from "./prototypeData";

export class PrototypeConsoleClient implements AnalyticsConsoleClient {
  private queryCounter = 0;

  async getExplorerSnapshot(): Promise<ExplorerSnapshot> {
    return structuredClone(prototypeExplorerSnapshot);
  }

  async executeQuery(request: QueryRequest): Promise<QueryResult> {
    const sql = request.sql.trim();
    const statementType = classifyStatement(sql);
    const queryId = this.nextQueryId();

    if (sql.length === 0) {
      return {
        queryId,
        statementType: "unknown",
        columns: [],
        rows: [],
        timings: timingFor(sql),
        messages: [{ level: "error", text: "No SQL was submitted." }],
      };
    }

    const metadataResult = executeMetadataQuery(request, queryId, statementType);
    if (metadataResult) {
      return metadataResult;
    }

    const relationResult = executeRelationQuery(request, queryId, statementType);
    if (relationResult) {
      return relationResult;
    }

    const scalarResult = executeScalarQuery(request, queryId, statementType);
    if (scalarResult) {
      return scalarResult;
    }

    return {
      queryId,
      statementType,
      columns: [],
      rows: [],
      affectedRows: statementType === "dml" || statementType === "ddl" ? 0 : undefined,
      timings: timingFor(sql),
      messages: [
        {
          level: statementType === "select" ? "warning" : "info",
          text:
            statementType === "select"
              ? "Prototype console client could not match this SELECT to sample metadata."
              : "Prototype console client accepted the statement for UI flow only; no engine mutation was persisted.",
        },
      ],
    };
  }

  private nextQueryId(): string {
    this.queryCounter += 1;
    return `web-proto-${String(this.queryCounter).padStart(6, "0")}`;
  }
}

export function classifyStatement(sql: string): QueryStatementType {
  const firstToken = sql.trim().split(/\s+/, 1)[0]?.toLowerCase() ?? "";

  if (firstToken === "select" || firstToken === "with") {
    return "select";
  }

  if (firstToken === "explain") {
    return "explain";
  }

  if (["show", "describe"].includes(firstToken)) {
    return "metadata";
  }

  if (["insert", "update", "delete", "truncate"].includes(firstToken)) {
    return "dml";
  }

  if (["create", "alter", "drop"].includes(firstToken)) {
    return "ddl";
  }

  return "unknown";
}

function executeMetadataQuery(
  request: QueryRequest,
  queryId: string,
  statementType: QueryStatementType,
): QueryResult | undefined {
  const normalized = normalizeSql(request.sql);

  if (normalized === "show databases") {
    return queryResult(request, queryId, statementType, ["database_name", "owner"], prototypeExplorerSnapshot.databases.map((database) => [
      database.name,
      database.owner,
    ]));
  }

  if (normalized.startsWith("show schemas")) {
    const database = parseFromTarget(request.sql) ?? request.database;
    const schemas = findDatabase(prototypeExplorerSnapshot, database)?.schemas ?? [];
    return queryResult(request, queryId, statementType, ["schema_name", "relation_count"], schemas.map((schema) => [
      schema.name,
      schema.relations.length,
    ]));
  }

  if (normalized.startsWith("show tables")) {
    const target = parseFromTarget(request.sql);
    const { database, schema } = resolveSchemaTarget(target, request.database, request.schema);
    const relations = findSchema(prototypeExplorerSnapshot, database, schema)?.relations ?? [];
    return queryResult(
      request,
      queryId,
      statementType,
      ["table_schema", "table_name", "storage", "row_estimate"],
      relations
        .filter((relation) => relation.kind === "table")
        .map((relation) => [relation.schema, relation.name, relation.storage, relation.rowEstimate ?? null]),
    );
  }

  if (normalized.startsWith("show views")) {
    const target = parseFromTarget(request.sql);
    const { database, schema } = resolveSchemaTarget(target, request.database, request.schema);
    const relations = findSchema(prototypeExplorerSnapshot, database, schema)?.relations ?? [];
    return queryResult(
      request,
      queryId,
      statementType,
      ["table_schema", "view_name", "storage", "row_estimate"],
      relations
        .filter((relation) => relation.kind === "view")
        .map((relation) => [relation.schema, relation.name, relation.storage, relation.rowEstimate ?? null]),
    );
  }

  if (normalized.startsWith("show columns") || normalized.startsWith("describe")) {
    const relation = relationFromMetadataStatement(request);
    if (!relation) {
      return undefined;
    }

    return queryResult(
      request,
      queryId,
      statementType,
      ["column_name", "data_type", "nullable"],
      relation.columns.map((column) => [column.name, column.type, column.nullable ? "YES" : "NO"]),
    );
  }

  return undefined;
}

function executeRelationQuery(
  request: QueryRequest,
  queryId: string,
  statementType: QueryStatementType,
): QueryResult | undefined {
  if (statementType === "explain") {
    return queryResult(request, queryId, statementType, ["plan"], [
      ["Prototype web-console plan preview"],
      [`Protocol: ${request.protocol}`],
      [`Session: ${request.database}.${request.schema}`],
      ["The production console will display the engine plan returned by AnalyticsDB."],
    ]);
  }

  if (statementType !== "select") {
    return undefined;
  }

  const relation = relationFromSelect(request);
  if (!relation) {
    return undefined;
  }

  const rows = prototypeRowsByRelation.get(relationKey(relation.database, relation.schema, relation.name)) ?? [];
  const lowerSql = request.sql.toLowerCase();

  if (/\bcount\s*\(\s*\*\s*\)/i.test(lowerSql)) {
    return queryResult(request, queryId, statementType, ["row_count"], [[relation.rowEstimate ?? rows.length]]);
  }

  return queryResult(
    request,
    queryId,
    statementType,
    relation.columns.map((column) => column.name),
    rows,
  );
}

function executeScalarQuery(
  request: QueryRequest,
  queryId: string,
  statementType: QueryStatementType,
): QueryResult | undefined {
  if (statementType !== "select") {
    return undefined;
  }

  const scalarMatch = request.sql.match(/^\s*select\s+(-?\d+)\s+(?:as\s+)?([a-z_][a-z0-9_]*)?\s*;?\s*$/i);
  if (!scalarMatch) {
    return undefined;
  }

  const value = Number(scalarMatch[1]);
  const columnName = scalarMatch[2] ?? "?column?";
  return queryResult(request, queryId, statementType, [columnName], [[value]]);
}

function queryResult(
  request: QueryRequest,
  queryId: string,
  statementType: QueryStatementType,
  columns: readonly string[],
  rows: readonly (readonly CellValue[])[],
): QueryResult {
  return {
    queryId,
    statementType,
    columns,
    rows,
    timings: timingFor(request.sql, rows.length),
    messages: [
      {
        level: "info",
        text: `Prototype result rendered locally for ${request.protocol}; no web execution gateway is wired yet.`,
      },
    ],
  };
}

function timingFor(sql: string, rowCount = 0): QueryTiming {
  const size = Math.max(sql.trim().length, 1);
  const queueMs = 2;
  const planMs = Math.min(120, 8 + Math.ceil(size / 18));
  const executeMs = Math.min(420, 14 + rowCount * 3 + Math.ceil(size / 9));
  const fetchMs = Math.min(160, 6 + rowCount * 2);

  return {
    queueMs,
    planMs,
    executeMs,
    fetchMs,
    totalMs: queueMs + planMs + executeMs + fetchMs,
  };
}

function normalizeSql(sql: string): string {
  return sql.trim().replace(/;$/, "").replace(/\s+/g, " ").toLowerCase();
}

function relationFromSelect(request: QueryRequest): RelationMetadata | undefined {
  const match = request.sql.match(/\bfrom\s+([a-z0-9_".]+)(?:\s|;|$)/i);
  if (!match) {
    return undefined;
  }

  const parts = splitIdentifierPath(match[1]);
  return resolveRelation(parts, request.database, request.schema);
}

function relationFromMetadataStatement(request: QueryRequest): RelationMetadata | undefined {
  const match = request.sql.match(/(?:from|describe)\s+([a-z0-9_".]+)(?:\s|;|$)/i);
  if (!match) {
    return undefined;
  }

  return resolveRelation(splitIdentifierPath(match[1]), request.database, request.schema);
}

function resolveRelation(
  parts: readonly string[],
  defaultDatabase: string,
  defaultSchema: string,
): RelationMetadata | undefined {
  if (parts.length === 1) {
    return findRelation(prototypeExplorerSnapshot, defaultDatabase, defaultSchema, parts[0]);
  }

  if (parts.length === 2) {
    return findRelation(prototypeExplorerSnapshot, defaultDatabase, parts[0], parts[1]);
  }

  if (parts.length === 3) {
    return findRelation(prototypeExplorerSnapshot, parts[0], parts[1], parts[2]);
  }

  return allRelations(prototypeExplorerSnapshot).find((relation) => relation.name === parts.at(-1));
}

function parseFromTarget(sql: string): string | undefined {
  return sql.match(/\bfrom\s+([a-z0-9_".]+)\s*;?\s*$/i)?.[1];
}

function resolveSchemaTarget(
  target: string | undefined,
  defaultDatabase: string,
  defaultSchema: string,
): { database: string; schema: string } {
  if (!target) {
    return { database: defaultDatabase, schema: defaultSchema };
  }

  const parts = splitIdentifierPath(target);
  if (parts.length === 1) {
    return { database: defaultDatabase, schema: parts[0] };
  }

  return { database: parts[0], schema: parts[1] };
}

function splitIdentifierPath(path: string): readonly string[] {
  return path
    .split(".")
    .map((part) => part.trim().replace(/^"|"$/g, "").replaceAll('""', '"'))
    .filter(Boolean);
}
