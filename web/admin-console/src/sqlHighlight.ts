// Lightweight SQL syntax highlighter.
//
// Returns an HTML string in which SQL tokens are wrapped in <span> elements
// with `tok-*` classes. It is intentionally dependency-free so it can power the
// editor's highlight overlay and be unit-tested in isolation. All emitted text
// is HTML-escaped.

const KEYWORDS = new Set([
  "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "NULL", "IS", "IN", "LIKE",
  "ILIKE", "BETWEEN", "EXISTS", "GROUP", "BY", "ORDER", "HAVING", "LIMIT",
  "OFFSET", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "ON",
  "USING", "UNION", "ALL", "INTERSECT", "EXCEPT", "AS", "DISTINCT", "INSERT",
  "INTO", "VALUES", "UPDATE", "SET", "DELETE", "CREATE", "TABLE", "VIEW",
  "INDEX", "DATABASE", "SCHEMA", "DROP", "ALTER", "ADD", "COLUMN", "RENAME",
  "TO", "TRUNCATE", "WITH", "RECURSIVE", "CASE", "WHEN", "THEN", "ELSE", "END",
  "ASC", "DESC", "NULLS", "FIRST", "LAST", "PRIMARY", "KEY", "FOREIGN",
  "REFERENCES", "UNIQUE", "CHECK", "DEFAULT", "CONSTRAINT", "GRANT", "REVOKE",
  "ROLE", "USER", "GROUP", "SHOW", "DESCRIBE", "EXPLAIN", "ANALYZE", "VACUUM",
  "REINDEX", "CAST", "OVER", "PARTITION", "WINDOW", "FILTER", "RETURNING",
  "IF", "LATERAL", "FETCH", "ROWS", "ONLY", "TEMPORARY", "TEMP", "CASCADE",
]);

const TYPES = new Set([
  "INT", "INTEGER", "BIGINT", "SMALLINT", "DECIMAL", "NUMERIC", "REAL",
  "DOUBLE", "PRECISION", "FLOAT", "SERIAL", "BIGSERIAL", "MONEY", "BOOLEAN",
  "BOOL", "CHAR", "VARCHAR", "TEXT", "BYTEA", "DATE", "TIME", "TIMESTAMP",
  "TIMESTAMPTZ", "INTERVAL", "UUID", "JSON", "JSONB", "ARRAY",
]);

const FUNCTIONS = new Set([
  "COUNT", "SUM", "AVG", "MIN", "MAX", "COALESCE", "NULLIF", "GREATEST",
  "LEAST", "NOW", "CURRENT_DATE", "CURRENT_TIMESTAMP", "EXTRACT", "DATE_TRUNC",
  "LOWER", "UPPER", "LENGTH", "TRIM", "SUBSTRING", "REPLACE", "ROUND", "ABS",
  "CEIL", "FLOOR", "ROW_NUMBER", "RANK", "DENSE_RANK", "LAG", "LEAD",
]);

// Order matters: comments and strings first so their contents aren't tokenized.
const TOKEN_RE = new RegExp(
  [
    "(--[^\\n]*|/\\*[\\s\\S]*?\\*/)", // 1: comment
    "('(?:[^']|'')*'|\"(?:[^\"]|\"\")*\")", // 2: string / quoted identifier
    "(\\d[\\d_]*(?:\\.\\d+)?(?:[eE][+-]?\\d+)?)", // 3: number
    "([A-Za-z_][A-Za-z0-9_]*)", // 4: word (keyword / type / function / identifier)
    "([(),.;\\[\\]{}]|[-+*/<>=!|%~^&@:?]+)", // 5: punctuation / operator
  ].join("|"),
  "g",
);

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function span(cls: string, text: string): string {
  return `<span class="${cls}">${escapeHtml(text)}</span>`;
}

function classifyWord(word: string): string {
  const upper = word.toUpperCase();
  if (KEYWORDS.has(upper)) return "tok-keyword";
  if (TYPES.has(upper)) return "tok-type";
  if (FUNCTIONS.has(upper)) return "tok-func";
  return "tok-ident";
}

/** Returns HTML with SQL tokens wrapped in highlight spans. Input is escaped. */
export function highlightSql(sql: string): string {
  let out = "";
  let lastIndex = 0;
  for (const match of sql.matchAll(TOKEN_RE)) {
    const index = match.index ?? 0;
    // Emit any gap (whitespace or unmatched chars) as escaped plain text.
    if (index > lastIndex) {
      out += escapeHtml(sql.slice(lastIndex, index));
    }
    const [token, comment, string, number, word, punct] = match;
    if (comment !== undefined) {
      out += span("tok-comment", token);
    } else if (string !== undefined) {
      out += span(string.startsWith('"') ? "tok-ident" : "tok-string", token);
    } else if (number !== undefined) {
      out += span("tok-number", token);
    } else if (word !== undefined) {
      out += span(classifyWord(word), token);
    } else if (punct !== undefined) {
      out += span("tok-punct", token);
    } else {
      out += escapeHtml(token);
    }
    lastIndex = index + token.length;
  }
  if (lastIndex < sql.length) {
    out += escapeHtml(sql.slice(lastIndex));
  }
  return out;
}
