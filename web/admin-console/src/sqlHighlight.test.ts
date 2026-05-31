import { describe, it, expect } from "vitest";
import { highlightSql } from "./sqlHighlight";

describe("highlightSql", () => {
  it("highlights keywords case-insensitively", () => {
    const html = highlightSql("select 1");
    expect(html).toContain('<span class="tok-keyword">select</span>');
    expect(html).toContain('<span class="tok-number">1</span>');
  });

  it("highlights strings and comments without tokenizing their contents", () => {
    const html = highlightSql("SELECT 'from where' -- FROM\n");
    expect(html).toContain('<span class="tok-string">\'from where\'</span>');
    expect(html).toContain('<span class="tok-comment">-- FROM</span>');
    // The keyword inside the string/comment must NOT be separately highlighted.
    expect(html).not.toContain('tok-keyword">FROM');
  });

  it("classifies types and functions", () => {
    const html = highlightSql("CAST(x AS BIGINT) COUNT(*)");
    expect(html).toContain('<span class="tok-type">BIGINT</span>');
    expect(html).toContain('<span class="tok-func">COUNT</span>');
  });

  it("escapes HTML so the overlay is injection-safe", () => {
    const html = highlightSql("SELECT '<script>' FROM t WHERE a < 2");
    expect(html).not.toContain("<script>");
    expect(html).toContain("&lt;script&gt;");
    expect(html).toContain("&lt;");
  });

  it("preserves the original text content exactly when tags are stripped", () => {
    const sql = "SELECT a, b\nFROM \"s\".\"t\"\nWHERE a >= 10; -- note";
    const stripped = highlightSql(sql).replace(/<[^>]+>/g, "");
    const decoded = stripped
      .replace(/&amp;/g, "&")
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">")
      .replace(/&quot;/g, '"');
    expect(decoded).toBe(sql);
  });
});
