export function mountSettingsView(container: HTMLElement): void {
  container.innerHTML = `
    <div class="page-header">
      <div class="page-header-title">
        <p class="page-pretitle">Administration</p>
        <h2 class="page-title">System Settings</h2>
      </div>
      <div class="page-header-actions">
        <button class="btn" type="button">Discard changes</button>
        <button class="btn btn-primary" type="button">Save settings</button>
      </div>
    </div>

    <div class="page-body">
      <div class="settings-grid">
        <section class="card">
          <div class="card-header">
            <div>
              <h3 class="card-title">Engine</h3>
              <p class="card-subtitle">Tune query execution defaults applied across the cluster.</p>
            </div>
          </div>
          <div class="card-body">
            <div class="form-grid">
              <label class="form-field">
                <span class="form-label">Default query timeout</span>
                <input class="form-control" type="text" value="120s" />
                <span class="form-hint">Statements exceeding this duration are cancelled.</span>
              </label>
              <label class="form-field">
                <span class="form-label">Max concurrent queries</span>
                <input class="form-control" type="number" value="32" />
              </label>
              <label class="form-field">
                <span class="form-label">Result row cap</span>
                <input class="form-control" type="number" value="100000" />
              </label>
              <label class="form-field form-field-toggle">
                <span class="form-label">Enable EXPLAIN ANALYZE</span>
                <input type="checkbox" checked />
              </label>
            </div>
          </div>
        </section>

        <section class="card">
          <div class="card-header">
            <div>
              <h3 class="card-title">Catalog</h3>
              <p class="card-subtitle">Where AnalyticsDB persists database, schema, and table metadata.</p>
            </div>
          </div>
          <div class="card-body">
            <div class="form-grid">
              <label class="form-field">
                <span class="form-label">Backend</span>
                <select class="form-select">
                  <option selected>SQLite (default)</option>
                  <option>PostgreSQL</option>
                  <option>External (JSON file)</option>
                </select>
              </label>
              <label class="form-field">
                <span class="form-label">SQLite path</span>
                <input class="form-control" type="text" value="cluster-catalog.managed/catalog.sqlite" />
              </label>
              <label class="form-field form-field-toggle">
                <span class="form-label">Auto-vacuum on startup</span>
                <input type="checkbox" checked />
              </label>
            </div>
          </div>
        </section>

        <section class="card">
          <div class="card-header">
            <div>
              <h3 class="card-title">Authentication</h3>
              <p class="card-subtitle">How users sign into the admin console and connect over the wire.</p>
            </div>
          </div>
          <div class="card-body">
            <div class="form-grid">
              <label class="form-field">
                <span class="form-label">Authentication provider</span>
                <select class="form-select">
                  <option selected>Local users</option>
                  <option>OIDC</option>
                  <option>SAML</option>
                </select>
              </label>
              <label class="form-field">
                <span class="form-label">Session lifetime</span>
                <input class="form-control" type="text" value="8 hours" />
              </label>
              <label class="form-field form-field-toggle">
                <span class="form-label">Require TLS for client protocols</span>
                <input type="checkbox" checked />
              </label>
              <label class="form-field form-field-toggle">
                <span class="form-label">Enforce SSO for admin console</span>
                <input type="checkbox" />
              </label>
            </div>
          </div>
        </section>

        <section class="card">
          <div class="card-header">
            <div>
              <h3 class="card-title">Telemetry</h3>
              <p class="card-subtitle">Diagnostics, metrics, and structured engine logs.</p>
            </div>
          </div>
          <div class="card-body">
            <div class="form-grid">
              <label class="form-field">
                <span class="form-label">Log level</span>
                <select class="form-select">
                  <option>trace</option>
                  <option>debug</option>
                  <option selected>info</option>
                  <option>warn</option>
                  <option>error</option>
                </select>
              </label>
              <label class="form-field form-field-toggle">
                <span class="form-label">Export Prometheus metrics</span>
                <input type="checkbox" checked />
              </label>
              <label class="form-field form-field-toggle">
                <span class="form-label">Send anonymised usage statistics</span>
                <input type="checkbox" />
              </label>
            </div>
          </div>
        </section>
      </div>
    </div>
  `;
}
