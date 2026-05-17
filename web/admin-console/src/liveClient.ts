import type {
  AnalyticsConsoleClient,
  CellValue,
  ExplorerSnapshot,
  QueryRequest,
  QueryResult,
  QueryResultChunk,
  QueryStatementType,
  QueryTiming,
  RelationMetadata,
  StreamingQueryResult,
} from "./domain";

const API_BASE_URL = import.meta.env.VITE_GATEWAY_URL ?? "http://localhost:8080/api";

interface LoginRequest {
  username: string;
  password: string;
}

interface LoginResponse {
  token: string;
  session: {
    sub: string;
    role: string;
    database: string;
    schema: string;
    exp: number;
  };
}

export class LiveConsoleClient implements AnalyticsConsoleClient {
  private token: string | null = null;

  constructor() {
    // Try to load token from localStorage
    this.token = localStorage.getItem("analyticsdb_token");
  }

  setToken(token: string): void {
    this.token = token;
    localStorage.setItem("analyticsdb_token", token);
  }

  clearToken(): void {
    this.token = null;
    localStorage.removeItem("analyticsdb_token");
  }

  isAuthenticated(): boolean {
    return this.token !== null;
  }

  private async request<T>(path: string, options?: RequestInit): Promise<T> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      ...(options?.headers as Record<string, string>),
    };

    if (this.token) {
      headers["Authorization"] = `Bearer ${this.token}`;
    }

    const response = await fetch(`${API_BASE_URL}${path}`, {
      ...options,
      headers,
    });

    if (!response.ok) {
      const error = await response.json().catch(() => ({
        error: `HTTP ${response.status}: ${response.statusText}`,
      }));
      throw new Error(error.error ?? `Request failed: ${response.status}`);
    }

    return response.json();
  }

  async login(username: string, password: string): Promise<LoginResponse> {
    const response = await this.request<LoginResponse>("/auth/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    });

    this.setToken(response.token);
    return response;
  }

  async logout(): Promise<void> {
    await this.request("/auth/logout", { method: "POST" });
    this.clearToken();
  }

  async getExplorerSnapshot(): Promise<ExplorerSnapshot> {
    return this.request<ExplorerSnapshot>("/explorer");
  }

  async executeQuery(request: QueryRequest): Promise<QueryResult> {
    return this.request<QueryResult>("/query", {
      method: "POST",
      body: JSON.stringify(request),
    });
  }

  executeQueryStreaming(request: QueryRequest): StreamingQueryResult {
    const queryId = `live-${Date.now()}`;
    const callbacks: Array<(chunk: QueryResultChunk) => void> = [];
    let completed = false;

    // Start the query and simulate streaming
    const queryPromise = this.request<QueryResult>("/query", {
      method: "POST",
      body: JSON.stringify(request),
    }).then((result) => {
      // Simulate streaming by calling the callback with the full result
      const chunk: QueryResultChunk = {
        columns: result.columns,
        rows: result.rows,
        isLast: true,
        timings: result.timings,
        messages: result.messages,
      };
      callbacks.forEach((cb) => cb(chunk));
      completed = true;
      return result;
    });

    return {
      queryId,
      statementType: "select", // Will be updated when result arrives
      onChunk(callback: (chunk: QueryResultChunk) => void) {
        callbacks.push(callback);
      },
      onComplete: () => queryPromise,
    };
  }

  async listDatabases(): Promise<Array<{ name: string; owner: string }>> {
    return this.request("/admin/databases");
  }

  async listUsers(): Promise<Array<{ name: string; role: string }>> {
    return this.request("/admin/users");
  }

  async getSystemMetrics(): Promise<{
    query_throughput_per_second: number;
    avg_latency_ms: number;
    error_rate_percent: number;
    active_connections: number;
    active_queries: number;
  }> {
    return this.request("/system/metrics");
  }

  async getQueryLog(limit?: number): Promise<Array<Record<string, unknown>>> {
    const params = limit ? `?limit=${limit}` : "";
    return this.request(`/system/query-log${params}`);
  }
}

// Singleton instance
export const liveClient = new LiveConsoleClient();
