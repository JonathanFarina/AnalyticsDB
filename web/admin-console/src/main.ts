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
  readonly requiresAuth?: boolean;
}

const ROUTES: readonly RouteDefinition[] = [
  { id: "query", label: "SQL Query", icon: "database", mount: mountQueryView },
  { id: "users", label: "Users", icon: "users", mount: mountUsersView, requiresAuth: true },
  { id: "groups", label: "Groups", icon: "user-group", mount: mountGroupsView, requiresAuth: true },
  { id: "settings", label: "System Settings", icon: "settings", mount: mountSettingsView },
  { id: "system", label: "System Information", icon: "info", mount: mountSystemView },
];

const DEFAULT_ROUTE_ID = "query";
const LOGIN_ROUTE_ID = "login";

function isAuthenticated(): boolean {
  return liveClient.isAuthenticated();
}

function renderLoginView(container: HTMLElement): void {
  container.innerHTML = `
    <div class="login-view">
      <div class="login-card">
        <div class="login-brand">
          <span class="brand-mark">A</span>
          <h1>AnalyticsDB</h1>
          <p>Admin Console</p>
        </div>
        <form id="login-form" class="login-form">
          <div class="form-group">
            <label for="username">Username</label>
            <input type="text" id="username" name="username" required autocomplete="username" />
          </div>
          <div class="form-group">
            <label for="password">Password</label>
            <input type="password" id="password" name="password" required autocomplete="current-password" />
          </div>
          <div class="form-error" style="display: none;"></div>
          <button type="submit" class="btn btn-primary">Login</button>
        </form>
        <div class="login-info">
          <small>Gateway: ${import.meta.env.VITE_GATEWAY_URL ?? "http://localhost:8080"}</small>
        </div>
      </div>
    </div>
  `;

  const form = container.querySelector<HTMLFormElement>("#login-form")!;
  const errorDiv = container.querySelector<HTMLDivElement>(".form-error")!;

  form.addEventListener("submit", async (e) => {
    e.preventDefault();

    const username = (form.querySelector("#username") as HTMLInputElement).value;
    const password = (form.querySelector("#password") as HTMLInputElement).value;

    errorDiv.style.display = "none";
    errorDiv.textContent = "";

    try {
      await liveClient.login(username, password);
      renderActiveRoute();
    } catch (error) {
      errorDiv.style.display = "block";
      errorDiv.textContent = error instanceof Error ? error.message : "Login failed";
    }
  });
}

const app = document.querySelector<HTMLDivElement>("#app");
if (!app) {
  throw new Error("Missing #app root element");
}

app.innerHTML = `
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
          ${ROUTES.map(
            (route) => `
              <li>
                <a class="nav-link" data-route="${route.id}" href="#/${route.id}">
                  <span class="nav-link-icon">${icon(route.icon, 18)}</span>
                  <span class="nav-link-label">${route.label}</span>
                </a>
              </li>`,
          ).join("")}
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
        <button class="btn btn-sm btn-logout" type="button">Logout</button>
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
          <button class="avatar avatar-trigger" type="button" aria-label="Account menu">JA</button>
        </div>
      </header>

      <main id="view-outlet" class="view-outlet" tabindex="-1"></main>
    </div>
  </div>
`;

const viewOutlet = document.querySelector<HTMLElement>("#view-outlet");
if (!viewOutlet) {
  throw new Error("Missing #view-outlet element");
}
const outlet: HTMLElement = viewOutlet;

// Add logout handler
const logoutBtn = app.querySelector<HTMLButtonElement>(".btn-logout");
logoutBtn?.addEventListener("click", async () => {
  await liveClient.logout();
  renderActiveRoute();
});

window.addEventListener("hashchange", renderActiveRoute);
renderActiveRoute();

function renderActiveRoute(): void {
  // Check authentication
  if (!isAuthenticated()) {
    outlet.classList.add("auth-required");
    renderLoginView(outlet);
    return;
  }

  outlet.classList.remove("auth-required");

  const route = resolveRoute();
  highlightActiveNav(route.id);
  outlet.replaceChildren();
  route.mount(outlet);
  outlet.scrollTo({ top: 0, behavior: "instant" });
}

function resolveRoute(): RouteDefinition {
  const id = window.location.hash.replace(/^#\/?/, "") || DEFAULT_ROUTE_ID;
  return ROUTES.find((route) => route.id === id) ?? ROUTES[0];
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
