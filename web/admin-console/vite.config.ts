import { defineConfig, type Plugin } from "vite";
import { promises as fs } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { IncomingMessage, ServerResponse } from "node:http";

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, "..", "..");
const CLUSTER_CONFIG_PATH = path.join(repoRoot, "cluster-config.json");
const CLUSTER_CATALOG_PATH = path.join(repoRoot, "cluster-catalog.json");

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
    const raw = await fs.readFile(CLUSTER_CATALOG_PATH, "utf8");
    const stat = await fs.stat(CLUSTER_CATALOG_PATH);
    respondJson(res, 200, {
      path: relativeFromRepo(CLUSTER_CATALOG_PATH),
      catalog: JSON.parse(raw),
      modifiedAtEpochMs: stat.mtimeMs,
    });
  } catch (error) {
    respondJson(res, 500, {
      error: error instanceof Error ? error.message : String(error),
    });
  }
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
  },
});
