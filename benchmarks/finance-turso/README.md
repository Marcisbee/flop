# Turso Finance Benchmark

Finance benchmark server backed by Turso/libSQL (`@libsql/client` in local file mode).

Run:

```bash
deno run --allow-all benchmarks/finance-turso/app.ts --port=1989
```

Seed (optional):

```bash
deno run --allow-net benchmarks/compare/seed.ts --host=http://localhost:1989
```
