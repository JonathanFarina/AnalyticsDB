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

export interface AdminUser {
  readonly name: string;
  readonly is_admin: boolean;
  readonly groups: readonly string[];
  readonly password_version?: number;
  readonly password_rotated_at_epoch_ms?: number | null;
}

export interface AdminGroup {
  readonly name: string;
  readonly members: readonly string[];
  readonly member_count: number;
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
  private user: string | null = null;
  private role: string | null = null;
  /** Invoked when a request is rejected as unauthorized (expired/invalid token). */
  onSessionExpired: (() => void) | null = null;

  constructor() {
    // Restore any persisted session from localStorage.
    this.token = localStorage.getItem("analyticsdb_token");
    this.user = localStorage.getItem("analyticsdb_user");
    this.role = localStorage.getItem("analyticsdb_role");
  }

  setToken(token: string): void {
    this.token = token;
    localStorage.setItem("analyticsdb_token", token);
  }

  clearToken(): void {
    this.token = null;
    this.user = null;
    this.role = null;
    localStorage.removeItem("analyticsdb_token");
    localStorage.removeItem("analyticsdb_user");
    localStorage.removeItem("analyticsdb_role");
  }

  isAuthenticated(): boolean {
    return this.token !== null;
  }

  /** The username of the signed-in account, or null when signed out. */
  currentUser(): string | null {
    return this.user;
  }

  /** The session role (e.g. "admin"), or null when signed out. */
  currentRole(): string | null {
    return this.role;
  }

  isAdmin(): boolean {
    return this.role === "admin";
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
      // A 401 on an authenticated request means the session is no longer valid;
      // clear it and notify the app so it can route back to the login screen.
      if (response.status === 401 && path !== "/auth/login") {
        this.clearToken();
        this.onSessionExpired?.();
      }
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
    this.user = response.session?.sub ?? username;
    this.role = response.session?.role ?? null;
    localStorage.setItem("analyticsdb_user", this.user);
    if (this.role) {
      localStorage.setItem("analyticsdb_role", this.role);
    }
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

  // --- Admin: users ---

  async listAdminUsers(): Promise<AdminUser[]> {
    return this.request<AdminUser[]>("/admin/users");
  }

  async createUser(
    name: string,
    password: string,
    groups: string[] = [],
  ): Promise<{ message: string }> {
    return this.request("/admin/users", {
      method: "POST",
      body: JSON.stringify({ name, password, groups }),
    });
  }

  async dropUser(name: string): Promise<{ message: string }> {
    return this.request(`/admin/users/${encodeURIComponent(name)}`, {
      method: "DELETE",
    });
  }

  /**
   * Resets a user's password. Pass an explicit `password` to set it directly,
   * or omit it to have the server generate a strong random one. When generated,
   * the plaintext is returned in `password`.
   */
  async resetUserPassword(
    name: string,
    password?: string,
  ): Promise<{ name: string; message: string; generated: boolean; password?: string }> {
    return this.request(
      `/admin/users/${encodeURIComponent(name)}/reset-password`,
      { method: "POST", body: JSON.stringify({ password: password ?? null }) },
    );
  }

  // --- Admin: groups ---

  async listGroups(): Promise<AdminGroup[]> {
    return this.request<AdminGroup[]>("/admin/groups");
  }

  async createGroup(name: string): Promise<{ message: string }> {
    return this.request("/admin/groups", {
      method: "POST",
      body: JSON.stringify({ name }),
    });
  }

  async dropGroup(name: string): Promise<{ message: string }> {
    return this.request(`/admin/groups/${encodeURIComponent(name)}`, {
      method: "DELETE",
    });
  }

  async addGroupMember(
    group: string,
    user: string,
  ): Promise<{ message: string }> {
    return this.request(
      `/admin/groups/${encodeURIComponent(group)}/members`,
      { method: "POST", body: JSON.stringify({ user }) },
    );
  }

  async removeGroupMember(
    group: string,
    user: string,
  ): Promise<{ message: string }> {
    return this.request(
      `/admin/groups/${encodeURIComponent(group)}/members/${encodeURIComponent(user)}`,
      { method: "DELETE" },
    );
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
