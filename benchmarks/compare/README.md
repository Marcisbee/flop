# Unified Benchmark Matrix

Compare these engines with identical benchmark scenarios:

- `flop-ts` (`benchmarks/finance/app.ts`)
- `flop-go` (`go/cmd/go-finance`)
- `sqlite-ts` (`benchmarks/sqlite-finance/app.ts`)
- `sqlite-go` (`go/cmd/sqlite-finance`)

## Run Benchmarks

One command (from repository root):

```bash
deno task bench
```

If `sqlite-go` is included and dependencies are missing, bootstrap once:

```bash
cd go && go get modernc.org/sqlite@v1.39.1 && go mod tidy
```

Optional filters:

```bash
# only specific engines
deno run --allow-read --allow-write --allow-run --allow-env --allow-net benchmarks/compare/run.ts --engines=flop-ts,flop-go

# only specific scenarios
deno run --allow-read --allow-write --allow-run --allow-env --allow-net benchmarks/compare/run.ts --scenarios=high-load-rw,reads,writes,edits

# override size/shape without editing scenarios
deno run --allow-read --allow-write --allow-run --allow-env --allow-net benchmarks/compare/run.ts --scenarios=high-load-rw,reads --users=500 --duration-sec=20 --concurrency=200
```

Default scenarios:

- `high-load-rw`: high load read+write together
- `reads`: setup data, then read-only high concurrency load
- `writes`: setup data, then write-heavy load
- `edits`: setup data, then hot-row update/edit load

The orchestrator writes:

- Per-run artifact: `benchmarks/compare/results/runs/<run-id>.json`
- Rolling history: `benchmarks/compare/results/history.json`
- Static report snapshot (open directly):
  `benchmarks/compare/results/report.html`

## Report UI

Open generated static report directly after a run:

```bash
open benchmarks/compare/results/report.html
```

Report supports:

- all scenarios displayed at once as mini trend graphs
- each mini graph includes per-engine averages
- click `Expand` on any scenario to open full-size graph + min/max/avg/latest table
- focused workload metrics only (`ops/s`, `read ops/s`, `write ops/s`, `edit transfer ops/s`)

## Notes

- The runner resets benchmark data before each engine/scenario run.
- `sqlite-go` requires Go to download `modernc.org/sqlite` dependency the first
  time.
