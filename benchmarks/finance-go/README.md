# Go Finance Benchmark

Finance benchmark server implemented in pure Go reducers/views (no QuickJS handler execution).

It exposes the same API contract as `benchmarks/finance`, so the same seed/dashboard flow works.

## Quick Start

1. Start the Go benchmark server:

```bash
cd go
GOCACHE=/tmp/go-build-cache go run ./cmd/go-finance --port=1985
```

2. Seed benchmark data:

```bash
deno run --allow-net benchmarks/go-finance/seed.ts --host=http://localhost:1985
```

3. Open the dashboard:

- Open `benchmarks/go-finance/index.html`
- Or serve it from any static server and set `?host=http://localhost:1985`
- Admin panel: `http://localhost:1985/_`
  - First run setup to create superadmin:
    - server log prints `Create superadmin: http://localhost:1985/_/setup?token=...`
  - Then login at `/_/login` with that superadmin account.

## Notes

- Data directory defaults to `benchmarks/go-finance/data`.
- Auth endpoints, reducers, views, and SSE routes are compatible with the existing finance seed script.
- This benchmark is intended for TypeScript vs pure-Go runtime comparison.
- Schema is defined in `appschema/` and generated models/tables live in `appschema/gen/`.
- Regenerate artifacts with:
  - `GOCACHE=/tmp/go-build-cache go run ./cmd/gen`
