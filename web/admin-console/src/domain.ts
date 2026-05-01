export type RelationKind = "table" | "view";
export type RelationStorage = "managed" | "external" | "system";
export type Protocol = "postgres" | "flight-sql";
export type QueryMessageLevel = "info" | "warning" | "error";
export type QueryStatementType = "select" | "ddl" | "dml" | "explain" | "metadata" | "unknown";

export type CellValue = boolean | number | string | null;

export interface ColumnMetadata {
  readonly name: string;
  readonly type: string;
  readonly nullable: boolean;
}

export interface RelationMetadata {
  readonly database: string;
  readonly schema: string;
  readonly name: string;
  readonly kind: RelationKind;
  readonly storage: RelationStorage;
  readonly columns: readonly ColumnMetadata[];
  readonly rowEstimate?: number;
  readonly description: string;
}

export interface SchemaMetadata {
  readonly database: string;
  readonly name: string;
  readonly relations: readonly RelationMetadata[];
}

export interface DatabaseMetadata {
  readonly name: string;
  readonly owner: string;
  readonly schemas: readonly SchemaMetadata[];
}

export interface ExplorerSnapshot {
  readonly generatedAt: string;
  readonly databases: readonly DatabaseMetadata[];
}

export interface QueryRequest {
  readonly sql: string;
  readonly protocol: Protocol;
  readonly database: string;
  readonly schema: string;
}

export interface QueryTiming {
  readonly queueMs: number;
  readonly planMs: number;
  readonly executeMs: number;
  readonly fetchMs: number;
  readonly totalMs: number;
}

export interface QueryMessage {
  readonly level: QueryMessageLevel;
  readonly text: string;
}

export interface QueryResult {
  readonly queryId: string;
  readonly statementType: QueryStatementType;
  readonly columns: readonly string[];
  readonly rows: readonly (readonly CellValue[])[];
  readonly affectedRows?: number;
  readonly timings: QueryTiming;
  readonly messages: readonly QueryMessage[];
}

export interface AnalyticsConsoleClient {
  getExplorerSnapshot(): Promise<ExplorerSnapshot>;
  executeQuery(request: QueryRequest): Promise<QueryResult>;
}
