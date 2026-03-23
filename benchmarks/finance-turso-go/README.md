# Turso Go Finance Benchmark

Go benchmark server using the same API contract as the other finance benchmarks.
Current implementation runs a local SQLite-compatible baseline from Go.

Run:

```bash
GOCACHE=/tmp/go-build-cache go run . --port=1996
```

Seed:

```bash
deno run --allow-net benchmarks/compare/seed.ts --host=http://localhost:1996
```
