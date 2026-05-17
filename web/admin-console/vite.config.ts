import { defineConfig, type Plugin } from "vite";
import { promises as fs } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { IncomingMessage, ServerResponse } from "node:http";
import Database from "better-sqlite3";

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, "..", "..");
const CLUSTER_CONFIG_PATH = path.join(repoRoot, "cluster-config.json");
const LEGACY_CATALOG_JSON = path.join(repoRoot, "cluster-catalog.json");

function clusterAdminPlugin(): Plugin {
  return {
    name: "analyticsdb-cluster-admin-stub",
    configureServer(server) {
      server.middlewares.use("/api/cluster-config", (req, res) => {
        void handleClusterConfig(req, res);
      });
      server.middlewares.use("/api/cluster-catalog", (req, res) => {
        void handleClusterCatalog(req, res);
      });
    },
  };
}

async function handleClusterConfig(
  req: IncomingMessage,
  res: ServerResponse,
): Promise<void> {
  try {
    if (req.method === "GET") {
      const raw = await fs.readFile(CLUSTER_CONFIG_PATH, "utf8");
      respondJson(res, 200, {
        path: relativeFromRepo(CLUSTER_CONFIG_PATH),
        config: JSON.parse(raw),
      });
      return;
    }

    if (req.method === "PUT") {
      const body = await readBody(req);
      const parsed = JSON.parse(body);
      const formatted = `${JSON.stringify(parsed, null, 3)}\n`;
      await fs.writeFile(CLUSTER_CONFIG_PATH, formatted, "utf8");
      respondJson(res, 200, {
        path: relativeFromRepo(CLUSTER_CONFIG_PATH),
        config: parsed,
      });
      return;
    }

    res.statusCode = 405;
    res.setHeader("Allow", "GET, PUT");
    res.end();
  } catch (error) {
    respondJson(res, 500, {
      error: error instanceof Error ? error.message : String(error),
    });
  }
}

async function handleClusterCatalog(
  req: IncomingMessage,
  res: ServerResponse,
): Promise<void> {
  if (req.method !== "GET") {
    res.statusCode = 405;
    res.setHeader("Allow", "GET");
    res.end();
    return;
  }

  try {
    const envelope = await loadCatalog();
    respondJson(res, 200, envelope);
  } catch (error) {
    respondJson(res, 500, {
      error: error instanceof Error ? error.message : String(error),
    });
  }
}

async function loadCatalog(): Promise<{
  path: string;
  catalog: unknown;
  modifiedAtEpochMs: number;
  source: "sqlite" | "json-legacy";
}> {
  const sqlitePath = await resolveSqliteCatalogPath();
  if (sqlitePath) {
    const catalog = readSqliteCatalog(sqlitePath);
    const stat = await fs.stat(sqlitePath);
    return {
      path: relativeFromRepo(sqlitePath),
      catalog,
      modifiedAtEpochMs: stat.mtimeMs,
      source: "sqlite",
    };
  }

  const raw = await fs.readFile(LEGACY_CATALOG_JSON, "utf8");
  const stat = await fs.stat(LEGACY_CATALOG_JSON);
  return {
    path: relativeFromRepo(LEGACY_CATALOG_JSON),
    catalog: JSON.parse(raw),
    modifiedAtEpochMs: stat.mtimeMs,
    source: "json-legacy",
  };
}

async function resolveSqliteCatalogPath(): Promise<string | null> {
  let configRaw: string;
  try {
    configRaw = await fs.readFile(CLUSTER_CONFIG_PATH, "utf8");
  } catch {
    return null;
  }

  let configured: string | undefined;
  try {
    const parsed = JSON.parse(configRaw) as { catalog_path?: string };
    configured = parsed.catalog_path;
  } catch {
    return null;
  }

  const candidates: string[] = [];
  if (configured) {
    candidates.push(
      path.isAbsolute(configured)
        ? configured
        : path.resolve(repoRoot, configured),
    );
  }
  candidates.push(path.join(repoRoot, "cluster-catalog.db"));
  candidates.push(path.join(repoRoot, "analyticsdb-catalog.db"));

  for (const candidate of candidates) {
    if (isSqlitePath(candidate) && (await fileExists(candidate))) {
      return candidate;
    }
  }

  return null;
}

function isSqlitePath(filePath: string): boolean {
  return filePath.endsWith(".db") || filePath.endsWith(".sqlite");
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

interface CatalogShape {
  catalogue_version: number;
  databases: Record<string, unknown>;
  users: Record<string, unknown>;
  nodes: Record<string, unknown>;
  relations: Record<string, unknown>;
  aggregates: Record<string, unknown>;
  collations: Record<string, unknown>;
  conversions: Record<string, unknown>;
  functions: Record<string, unknown>;
  config?: unknown;
}

function readSqliteCatalog(sqlitePath: string): CatalogShape {
  const db = new Database(sqlitePath, { readonly: true, fileMustExist: true });
  try {
    const meta = readMeta(db);
    return {
      catalogue_version: Number(meta.catalogue_version ?? 0),
      config: meta.config,
      databases: readKeyValueTable(db, "databases", "name"),
      users: readKeyValueTable(db, "users", "name"),
      nodes: readKeyValueTable(db, "nodes", "id"),
      relations: readKeyValueTable(db, "relations", "key"),
      aggregates: readKeyValueTable(db, "aggregates", "key"),
      collations: readKeyValueTable(db, "collations", "key"),
      conversions: readKeyValueTable(db, "conversions", "key"),
      functions: readKeyValueTable(db, "functions", "key"),
    };
  } finally {
    db.close();
  }
}

function readMeta(db: Database.Database): { config?: unknown; catalogue_version?: number } {
  const tableExists = db
    .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='meta'")
    .get();
  if (!tableExists) {
    return {};
  }

  const rows = db.prepare("SELECT key, value FROM meta").all() as Array<{
    key: string;
    value: string;
  }>;
  const out: { config?: unknown; catalogue_version?: number } = {};
  for (const row of rows) {
    if (row.key === "catalogue_version") {
      const parsed = Number(row.value);
      if (Number.isFinite(parsed)) {
        out.catalogue_version = parsed;
      }
    } else if (row.key === "config") {
      try {
        out.config = JSON.parse(row.value);
      } catch {
        // ignore malformed config
      }
    }
  }
  return out;
}

function readKeyValueTable(
  db: Database.Database,
  table: string,
  keyColumn: string,
): Record<string, unknown> {
  const tableExists = db
    .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?")
    .get(table);
  if (!tableExists) {
    return {};
  }

  const rows = db.prepare(`SELECT ${keyColumn} AS key, data FROM ${table}`).all() as Array<{
    key: string;
    data: string;
  }>;
  const result: Record<string, unknown> = {};
  for (const row of rows) {
    try {
      result[row.key] = JSON.parse(row.data);
    } catch {
      result[row.key] = { raw: row.data };
    }
  }
  return result;
}

function respondJson(res: ServerResponse, status: number, body: unknown): void {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.end(JSON.stringify(body));
}

async function readBody(req: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString("utf8");
}

function relativeFromRepo(absolute: string): string {
  return path.relative(repoRoot, absolute);
}

export default defineConfig({
  plugins: [clusterAdminPlugin()],
  server: {
    host: "127.0.0.1",
    proxy: {
      // Proxy API requests to the gateway in development
      "/api": {
        target: "http://127.0.0.1:8080",
        changeOrigin: true,
        secure: false,
        rewrite: (path) => path.replace(/^\/api/, "/api"),
      },
    },
  },
});

