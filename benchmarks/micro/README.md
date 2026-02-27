# All-Engine Microbenchmark

Fast benchmark preset for comparing `flop` against other engines using the same
workload harness as `benchmarks/compare/run.ts`.

Default engines:

- `flop-ts`
- `flop-go`
- `sqlite-ts`
- `sqlite-go`
- `turso-ts`
- `pglite-ts`
- `turso-go`
- `mongodb-ts`
- `mongodb-go`

Run:

```bash
deno task bench:micro
```

Default shape (fast):

- scenarios: `high-load-rw`
- users: `80`
- setup concurrency: `12`
- workload concurrency: `40`
- duration: `4s`
- repeats: `1`

Examples:

```bash
# run all workload scenarios
deno task bench:micro --scenarios=high-load-rw,reads,writes,edits

# focus on core engines only
deno task bench:micro --engines=flop-ts,flop-go,sqlite-ts,sqlite-go

# stronger signal
deno task bench:micro --duration-sec=8 --repeats=2 --warmup-sec=2 --setup-retries=5

# machine-readable summary only
deno task bench:micro --json-only=1
```

Output includes a `BENCH_JSON:{...}` summary with:

- per-scenario engine ranking
- best engine per scenario
- overall leaderboard (`avg ops/s`)
- run metadata (`runId`, timestamp)
