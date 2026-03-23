# SQLite Finance Benchmark

Same bank simulation as `benchmarks/finance`, but backed by SQLite (`node:sqlite` with WAL mode) instead of flop's custom binary storage.

Both benchmarks expose the same HTTP API, so the seed script, browser dashboard, and transfer throughput numbers are directly comparable.

## Quick Start

```bash
# 1. Start the SQLite server
deno run --allow-all benchmarks/sqlite-finance/app.ts

# 2. Seed data (in a second terminal)
deno run --allow-net benchmarks/sqlite-finance/seed.ts

# 3. Open the dashboard
open benchmarks/sqlite-finance/index.html
```

## Comparing with Flop

```bash
# Terminal 1 — Flop
deno task start benchmarks/finance/app.ts
# (create admin via setup URL, then seed)
deno run --allow-net benchmarks/finance/seed.ts

# Terminal 2 — SQLite (use a different port)
deno run --allow-all benchmarks/sqlite-finance/app.ts --port=1986
deno run --allow-net benchmarks/sqlite-finance/seed.ts --host=http://localhost:1986
```

## Architecture Differences

| Feature | Flop | SQLite |
|---------|------|--------|
| Storage | Custom 4KB page binary format | SQLite WAL mode |
| Auth | Built-in bcrypt + JWT | SHA-256 + JWT (simplified for benchmark) |
| Concurrency | Per-table async write lock | SQLite internal locking (WAL) |
| SSE | PubSub from write hooks | In-process pub/sub |
| Queries | Table scan + in-memory filter | SQL with indexes |
| Stats | `count()` O(1) + scan for aggregates | `COUNT(*)` + `SUM()` + `GROUP BY` |
