import "./styles.css";
import { icon, type IconName } from "./icons";
import { mountQueryView } from "./views/queryView";
import { mountUsersView } from "./views/usersView";
import { mountGroupsView } from "./views/groupsView";
import { mountSettingsView } from "./views/settingsView";
import { mountSystemView } from "./views/systemView";
import { liveClient } from "./liveClient";

interface RouteDefinition {
  readonly id: string;
  readonly label: string;
  readonly icon: IconName;
  readonly mount: (container: HTMLElement) => void;
  /** Restrict this route to administrators (Administrators-group members). */
  readonly adminOnly?: boolean;
}

const ROUTES: readonly RouteDefinition[] = [
  { id: "query", label: "SQL Query", icon: "database", mount: mountQueryView },
  { id: "users", label: "Users", icon: "users", mount: mountUsersView, adminOnly: true },
  { id: "groups", label: "Groups", icon: "user-group", mount: mountGroupsView, adminOnly: true },
  { id: "settings", label: "System Settings", icon: "settings", mount: mountSettingsView },
  { id: "system", label: "System Information", icon: "info", mount: mountSystemView },
];

const DEFAULT_ROUTE_ID = "query";
const GATEWAY_LABEL = import.meta.env.VITE_GATEWAY_URL ?? "http://localhost:8080";

const app = document.querySelector<HTMLDivElement>("#app");
if (!app) {
  throw new Error("Missing #app root element");
}
const root: HTMLDivElement = app;

// When the gateway rejects the session token, route back to the login screen.
liveClient.onSessionExpired = () => {
  renderApp();
};

window.addEventListener("hashchange", () => {
  // Only the in-app route changes need a re-render; ignore while logged out.
  if (liveClient.isAuthenticated()) {
    renderActiveRoute();
  }
});

renderApp();

/** Top-level render: choose between the login screen and the authenticated shell. */
function renderApp(): void {
  if (!liveClient.isAuthenticated()) {
    renderLoginView();
    return;
  }
  renderShell();
  renderActiveRoute();
}

// ── Login ─────────────────────────────────────────────────────────────────

function renderLoginView(): void {
  root.innerHTML = `
    <div class="login-screen">
      <form id="login-form" class="login-card" novalidate>
        <div class="login-brand">
          <span class="brand-mark brand-mark-lg">A</span>
          <div>
            <h1>AnalyticsDB</h1>
            <p>Admin Console</p>
          </div>
        </div>

        <p class="login-lead">Sign in to manage databases, users, and cluster settings.</p>

        <div class="form-group">
          <label class="form-label" for="username">Username</label>
          <input class="form-control" type="text" id="username" name="username"
            placeholder="analyticsdb_admin" required autocomplete="username" autofocus />
        </div>
        <div class="form-group">
          <label class="form-label" for="password">Password</label>
          <input class="form-control" type="password" id="password" name="password"
            required autocomplete="current-password" />
        </div>

        <div class="login-error message message-error" hidden>
          ${icon("x-circle", 14)}<span class="login-error-text"></span>
        </div>

        <button type="submit" class="btn btn-primary btn-block" id="login-submit">
          ${icon("log-in", 16)}<span>Sign in</span>
        </button>

        <div class="login-hint">
          <span class="text-muted">Connected to ${escapeHtml(GATEWAY_LABEL)}</span>
          <span class="text-muted">First run? Initialize with
            <code>analyticsdb-server --init-cluster</code> — it prints a one-time
            password for <code>analyticsdb_admin</code>. Lost it? Recover with
            <code>--reset-admin-password</code> or <code>--init-cluster --force</code>.</span>
        </div>
      </form>
    </div>
  `;

  const form = root.querySelector<HTMLFormElement>("#login-form")!;
  const submit = root.querySelector<HTMLButtonElement>("#login-submit")!;
  const errorBox = root.querySelector<HTMLDivElement>(".login-error")!;
  const errorText = root.querySelector<HTMLSpanElement>(".login-error-text")!;

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const username = (form.querySelector("#username") as HTMLInputElement).value.trim();
    const password = (form.querySelector("#password") as HTMLInputElement).value;

    if (!username || !password) {
      showError("Enter both a username and password.");
      return;
    }

    errorBox.hidden = true;
    submit.disabled = true;
    submit.classList.add("is-running");

    try {
      await liveClient.login(username, password);
      renderApp();
    } catch (error) {
      showError(loginErrorMessage(error));
      submit.disabled = false;
      submit.classList.remove("is-running");
    }
  });

  function showError(message: string): void {
    errorText.textContent = message;
    errorBox.hidden = false;
  }
}

/** Maps raw login errors to a clear, user-facing message. */
function loginErrorMessage(error: unknown): string {
  const raw = error instanceof Error ? error.message : String(error);
  // A failed fetch (gateway down / wrong URL) surfaces as a TypeError.
  if (/failed to fetch|networkerror|load failed/i.test(raw)) {
    return `Cannot reach the gateway at ${GATEWAY_LABEL}. Is the server running?`;
  }
  // The gateway returns 401 ("Authentication required") for bad credentials.
  if (/authentication required|unauthorized|invalid credentials/i.test(raw)) {
    return "Invalid username or password.";
  }
  return raw || "Login failed.";
}

// ── Authenticated shell ─────────────────────────────────────────────────────

function renderShell(): void {
  const user = liveClient.currentUser() ?? "user";
  const isAdmin = liveClient.isAdmin();
  const roleLabel = isAdmin ? "Administrator" : "Standard user";
  const visibleRoutes = ROUTES.filter((route) => !route.adminOnly || isAdmin);

  root.innerHTML = `
    <div class="page">
      <aside class="sidebar" aria-label="Primary navigation">
        <div class="sidebar-brand">
          <span class="brand-mark">A</span>
          <div class="brand-text">
            <strong>AnalyticsDB</strong>
            <small>Admin console</small>
          </div>
        </div>

        <nav class="sidebar-nav" aria-label="Console sections">
          <ul>
            ${visibleRoutes
              .map(
                (route) => `
              <li>
                <a class="nav-link" data-route="${route.id}" href="#/${route.id}">
                  <span class="nav-link-icon">${icon(route.icon, 18)}</span>
                  <span class="nav-link-label">${route.label}</span>
                </a>
              </li>`,
              )
              .join("")}
          </ul>
        </nav>

        <div class="sidebar-footer">
          <div class="cluster-card">
            <span class="cluster-dot"></span>
            <div>
              <strong>adb-prototype-01</strong>
              <small>Engine online · v0.1.0</small>
            </div>
          </div>
        </div>
      </aside>

      <div class="main">
        <header class="topbar">
          <button class="topbar-menu" type="button" aria-label="Toggle navigation">
            ${icon("menu", 18)}
          </button>
          <div class="topbar-search">
            ${icon("search", 16)}
            <input type="search" placeholder="Search tables, users, settings…" aria-label="Search the console" />
            <kbd>/</kbd>
          </div>
          <div class="topbar-actions">
            <span class="status-pill status-pill-success">
              <span class="status-dot"></span>
              Engine online
            </span>
            <div class="user-menu">
              <span class="avatar avatar-trigger" aria-hidden="true">${escapeHtml(initials(user))}</span>
              <div class="user-menu-meta">
                <strong>${escapeHtml(user)}</strong>
                <small>${escapeHtml(roleLabel)}</small>
              </div>
              <button class="btn btn-sm btn-logout" type="button" title="Sign out">
                ${icon("logout", 14)}<span>Sign out</span>
              </button>
            </div>
          </div>
        </header>

        <main id="view-outlet" class="view-outlet" tabindex="-1"></main>
      </div>
    </div>
  `;

  root.querySelector<HTMLButtonElement>(".btn-logout")?.addEventListener("click", async () => {
    try {
      await liveClient.logout();
    } catch {
      // Even if the network call fails, drop the local session.
      liveClient.clearToken();
    }
    window.location.hash = "";
    renderApp();
  });
}

function renderActiveRoute(): void {
  const outlet = root.querySelector<HTMLElement>("#view-outlet");
  if (!outlet) {
    return;
  }
  const route = resolveRoute();
  highlightActiveNav(route.id);
  outlet.replaceChildren();
  route.mount(outlet);
  outlet.scrollTo({ top: 0, behavior: "instant" });
}

function resolveRoute(): RouteDefinition {
  const id = window.location.hash.replace(/^#\/?/, "") || DEFAULT_ROUTE_ID;
  const isAdmin = liveClient.isAdmin();
  const match = ROUTES.find((route) => route.id === id);
  if (match && (!match.adminOnly || isAdmin)) {
    return match;
  }
  // Fall back to the default route (filtered for the current role).
  return ROUTES.find((route) => route.id === DEFAULT_ROUTE_ID) ?? ROUTES[0];
}

function highlightActiveNav(activeId: string): void {
  for (const link of document.querySelectorAll<HTMLAnchorElement>(".nav-link")) {
    const isActive = link.dataset.route === activeId;
    link.classList.toggle("is-active", isActive);
    if (isActive) {
      link.setAttribute("aria-current", "page");
    } else {
      link.removeAttribute("aria-current");
    }
  }
}

function initials(name: string): string {
  return name.replace(/[^a-zA-Z0-9]/g, "").slice(0, 2).toUpperCase() || "U";
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
