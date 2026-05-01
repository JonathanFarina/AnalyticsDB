import { describe, expect, it } from "vitest";
import { classifyStatement, PrototypeConsoleClient } from "./consoleClient";

describe("classifyStatement", () => {
  it("classifies the console statement families", () => {
    expect(classifyStatement("SELECT 1")).toBe("select");
    expect(classifyStatement("EXPLAIN SELECT 1")).toBe("explain");
    expect(classifyStatement("SHOW TABLES")).toBe("metadata");
    expect(classifyStatement("INSERT INTO demo VALUES (1)")).toBe("dml");
    expect(classifyStatement("CREATE TABLE demo AS SELECT 1")).toBe("ddl");
  });
});

describe("PrototypeConsoleClient", () => {
  it("returns explorer metadata for databases, schemas, tables, and views", async () => {
    const client = new PrototypeConsoleClient();
    const snapshot = await client.getExplorerSnapshot();

    expect(snapshot.databases.map((database) => database.name)).toEqual(["postgres", "analytics"]);
    expect(snapshot.databases[0].schemas[0].relations.map((relation) => relation.name)).toContain(
      "fact_metrics",
    );
  });

  it("executes a schema-scoped SHOW TABLES prototype query", async () => {
    const client = new PrototypeConsoleClient();
    const result = await client.executeQuery({
      sql: "SHOW TABLES FROM analytics.reporting;",
      protocol: "postgres",
      database: "postgres",
      schema: "public",
    });

    expect(result.columns).toEqual(["table_schema", "table_name", "storage", "row_estimate"]);
    expect(result.rows).toEqual([["reporting", "warehouse_events", "external", 9284421]]);
    expect(result.queryId).toBe("web-proto-000001");
  });

  it("renders sample relation rows for the query editor result grid", async () => {
    const client = new PrototypeConsoleClient();
    const result = await client.executeQuery({
      sql: 'SELECT * FROM "analytics"."reporting"."query_latency_rollup" LIMIT 100;',
      protocol: "flight-sql",
      database: "analytics",
      schema: "reporting",
    });

    expect(result.columns).toEqual(["minute", "p50_ms", "p95_ms", "p99_ms"]);
    expect(result.rows).toHaveLength(3);
    expect(result.messages[0].text).toContain("Prototype result rendered locally");
  });

  it("does not claim persistence for DDL or DML in the prototype web client", async () => {
    const client = new PrototypeConsoleClient();
    const result = await client.executeQuery({
      sql: "CREATE TABLE demo AS SELECT 1;",
      protocol: "postgres",
      database: "postgres",
      schema: "public",
    });

    expect(result.statementType).toBe("ddl");
    expect(result.affectedRows).toBe(0);
    expect(result.messages[0].text).toContain("no engine mutation was persisted");
  });
});
