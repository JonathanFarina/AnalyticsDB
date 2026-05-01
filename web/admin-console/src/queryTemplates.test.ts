import { describe, expect, it } from "vitest";
import { countQuery, describeQuery, previewQuery, quoteIdentifier } from "./queryTemplates";
import { prototypeExplorerSnapshot } from "./prototypeData";

const relation = prototypeExplorerSnapshot.databases[1].schemas[0].relations[1];

describe("queryTemplates", () => {
  it("quotes identifiers for PostgreSQL-compatible SQL snippets", () => {
    expect(quoteIdentifier('needs"quote')).toBe('"needs""quote"');
  });

  it("builds preview, describe, and count snippets for explorer relations", () => {
    expect(previewQuery(relation)).toBe(
      'SELECT *\nFROM "analytics"."reporting"."query_latency_rollup"\nLIMIT 100;',
    );
    expect(describeQuery(relation)).toBe('DESCRIBE "analytics"."reporting"."query_latency_rollup";');
    expect(countQuery(relation)).toBe(
      'SELECT COUNT(*) AS row_count\nFROM "analytics"."reporting"."query_latency_rollup";',
    );
  });
});
