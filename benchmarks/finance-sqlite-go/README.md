# SQLite Go Finance Benchmark

SQLite-backed finance benchmark server implemented in Go.

It exposes the same API contract as `benchmarks/finance`, so the same seed/dashboard flow works.

## Quick Start

1. Start the SQLite Go benchmark server:

```bash
GOCACHE=/tmp/go-build-cache go run ./cmd/sqlite-finance --port=1995
```

2. Seed benchmark data:

```bash
deno run --allow-net benchmarks/sqlite-go/seed.ts --host=http://localhost:1995
```

3. Open the dashboard:

- Open `benchmarks/sqlite-go/index.html`
- Or serve it from any static server and set `?host=http://localhost:1995`

## Notes

- Data defaults to `benchmarks/sqlite-go/data/finance.db`.
- Engine uses SQLite WAL mode.
