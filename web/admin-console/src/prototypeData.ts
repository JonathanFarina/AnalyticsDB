import type {
  CellValue,
  DatabaseMetadata,
  ExplorerSnapshot,
  RelationMetadata,
  SchemaMetadata,
} from "./domain";

const publicRelations: readonly RelationMetadata[] = [
  {
    database: "postgres",
    schema: "public",
    name: "fact_metrics",
    kind: "table",
    storage: "managed",
    rowEstimate: 1248,
    description: "Prototype managed table backed by local Parquet files in the current engine slice.",
    columns: [
      { name: "metric", type: "BIGINT", nullable: false },
      { name: "status", type: "TEXT", nullable: true },
      { name: "observed_at", type: "TIMESTAMP", nullable: true },
    ],
  },
  {
    database: "postgres",
    schema: "public",
    name: "daily_metrics",
    kind: "view",
    storage: "managed",
    rowEstimate: 31,
    description: "Persisted view example used by the console prototype.",
    columns: [
      { name: "metric_date", type: "DATE", nullable: false },
      { name: "row_count", type: "BIGINT", nullable: false },
      { name: "warning_count", type: "BIGINT", nullable: false },
    ],
  },
];

const analyticsRelations: readonly RelationMetadata[] = [
  {
    database: "analytics",
    schema: "reporting",
    name: "warehouse_events",
    kind: "table",
    storage: "external",
    rowEstimate: 9284421,
    description: "External table placeholder showing the target one-SQL-surface explorer shape.",
    columns: [
      { name: "event_id", type: "TEXT", nullable: false },
      { name: "tenant_id", type: "TEXT", nullable: false },
      { name: "event_type", type: "TEXT", nullable: false },
      { name: "event_time", type: "TIMESTAMP", nullable: false },
    ],
  },
  {
    database: "analytics",
    schema: "reporting",
    name: "query_latency_rollup",
    kind: "view",
    storage: "managed",
    rowEstimate: 1440,
    description: "Rollup view for the admin-console results grid and timing affordances.",
    columns: [
      { name: "minute", type: "TIMESTAMP", nullable: false },
      { name: "p50_ms", type: "DOUBLE PRECISION", nullable: true },
      { name: "p95_ms", type: "DOUBLE PRECISION", nullable: true },
      { name: "p99_ms", type: "DOUBLE PRECISION", nullable: true },
    ],
  },
];

const systemRelations: readonly RelationMetadata[] = [
  {
    database: "postgres",
    schema: "pg_catalog",
    name: "pg_tables",
    kind: "view",
    storage: "system",
    rowEstimate: 8,
    description: "Prototype PostgreSQL catalog-compatibility view exposed by the engine.",
    columns: [
      { name: "schemaname", type: "TEXT", nullable: false },
      { name: "tablename", type: "TEXT", nullable: false },
      { name: "tableowner", type: "TEXT", nullable: true },
    ],
  },
  {
    database: "postgres",
    schema: "information_schema",
    name: "columns",
    kind: "view",
    storage: "system",
    rowEstimate: 64,
    description: "Information schema column metadata surface for compatibility probes.",
    columns: [
      { name: "table_schema", type: "TEXT", nullable: false },
      { name: "table_name", type: "TEXT", nullable: false },
      { name: "column_name", type: "TEXT", nullable: false },
      { name: "data_type", type: "TEXT", nullable: false },
    ],
  },
];

const schemas: readonly SchemaMetadata[] = [
  {
    database: "postgres",
    name: "public",
    relations: publicRelations,
  },
  {
    database: "postgres",
    name: "pg_catalog",
    relations: systemRelations.filter((relation) => relation.schema === "pg_catalog"),
  },
  {
    database: "postgres",
    name: "information_schema",
    relations: systemRelations.filter((relation) => relation.schema === "information_schema"),
  },
  {
    database: "analytics",
    name: "reporting",
    relations: analyticsRelations,
  },
];

export const prototypeExplorerSnapshot: ExplorerSnapshot = {
  generatedAt: new Date("2026-04-30T00:00:00.000Z").toISOString(),
  databases: [
    {
      name: "postgres",
      owner: "postgres",
      schemas: schemas.filter((schema) => schema.database === "postgres"),
    },
    {
      name: "analytics",
      owner: "postgres",
      schemas: schemas.filter((schema) => schema.database === "analytics"),
    },
  ],
};

export const prototypeRowsByRelation = new Map<string, readonly (readonly CellValue[])[]>([
  [
    relationKey("postgres", "public", "fact_metrics"),
    [
      [11, "ok", "2026-04-29 08:15:00"],
      [12, "warn", "2026-04-29 08:16:00"],
      [13, "ok", "2026-04-29 08:17:00"],
    ],
  ],
  [
    relationKey("postgres", "public", "daily_metrics"),
    [
      ["2026-04-27", 2480, 19],
      ["2026-04-28", 2512, 14],
      ["2026-04-29", 2421, 22],
    ],
  ],
  [
    relationKey("analytics", "reporting", "warehouse_events"),
    [
      ["evt_001", "acme", "ingest.started", "2026-04-29 09:00:00"],
      ["evt_002", "acme", "query.completed", "2026-04-29 09:00:03"],
      ["evt_003", "northwind", "query.failed", "2026-04-29 09:00:07"],
    ],
  ],
  [
    relationKey("analytics", "reporting", "query_latency_rollup"),
    [
      ["2026-04-29 09:00:00", 18.4, 61.7, 94.8],
      ["2026-04-29 09:01:00", 20.2, 68.3, 109.5],
      ["2026-04-29 09:02:00", 17.9, 59.1, 90.6],
    ],
  ],
]);

export function allRelations(snapshot: ExplorerSnapshot = prototypeExplorerSnapshot): readonly RelationMetadata[] {
  return snapshot.databases.flatMap((database) =>
    database.schemas.flatMap((schema) => schema.relations),
  );
}

export function findDatabase(
  snapshot: ExplorerSnapshot,
  databaseName: string,
): DatabaseMetadata | undefined {
  return snapshot.databases.find((database) => equalsIdentifier(database.name, databaseName));
}

export function findSchema(
  snapshot: ExplorerSnapshot,
  databaseName: string,
  schemaName: string,
): SchemaMetadata | undefined {
  return findDatabase(snapshot, databaseName)?.schemas.find((schema) =>
    equalsIdentifier(schema.name, schemaName),
  );
}

export function findRelation(
  snapshot: ExplorerSnapshot,
  databaseName: string,
  schemaName: string,
  relationName: string,
): RelationMetadata | undefined {
  return findSchema(snapshot, databaseName, schemaName)?.relations.find((relation) =>
    equalsIdentifier(relation.name, relationName),
  );
}

export function relationKey(database: string, schema: string, relation: string): string {
  return `${database.toLowerCase()}.${schema.toLowerCase()}.${relation.toLowerCase()}`;
}

function equalsIdentifier(left: string, right: string): boolean {
  return left.localeCompare(right, undefined, { sensitivity: "accent" }) === 0;
}
