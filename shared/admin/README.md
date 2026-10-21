# Flop admin frontend

The admin uses React and `@cloudflare/kumo` for its UI components and themes.
Application layouts live in `src/layout.css`; the former custom control stylesheet
and runtime CDN imports have been removed.

Use Node.js 22 or newer to update the frontend:

```sh
cd shared/admin
npm ci
npm run build
cd ../..
go generate ./...
```

Commit the source, lockfile, built `shared/admin/*.html`, and matching embedded
`internal/server/*.html` together. The build produces self-contained HTML with
bundled JavaScript and CSS. Go library consumers do not need Node or a frontend
build step. CI rebuilds these assets and checks for drift.

- `src/admin.js`: database and service views, API calls, and live updates.
- `src/ui.jsx`: Kumo compositions, navigation, confirmations, and table library.
- `src/charts.jsx`: Kumo time-series charts with the required ECharts modules.
- `src/auth.jsx`: login and initial setup, including schema-driven fields.
- `src/layout.css`: responsive domain layouts using Kumo theme tokens.

Run the blog example as described in the root `VERIFY.md` to check browser
behavior. Verify login recovery, table search and scopes, row creation/editing,
archive/restore/delete confirmations, and service settings. Check phone and
desktop widths, keyboard navigation, dialogs, light/dark mode, and reduced motion.
The HTML template adapter only normalizes retained HTML attributes for React;
controls are explicit Kumo components.
