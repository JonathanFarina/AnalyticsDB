# AnalyticsDB Admin Console

Prototype Vite and TypeScript web console for the AnalyticsDB admin surface.

Current scope:

- database explorer for prototype databases, schemas, tables, and views
- SQL query editor with PostgreSQL and Arrow Flight SQL session selectors
- result grid, engine-message area, query id, and timing cards
- swappable `AnalyticsConsoleClient` boundary for a future web execution gateway

This app does not yet execute SQL against a live AnalyticsDB server. The current `PrototypeConsoleClient` is a local UI harness with sample metadata and deterministic result rows so the console interactions can be designed and tested without inventing backend readiness.

## Commands

```bash
npm install
npm run dev
npm test
npm run build
```
