# PGlite Finance Benchmark

Finance benchmark server backed by PGlite (`@electric-sql/pglite`).

Run:

```bash
deno run --allow-all benchmarks/finance-pglite/app.ts --port=1990
```

Seed (optional):

```bash
deno run --allow-net benchmarks/compare/seed.ts --host=http://localhost:1990
```
