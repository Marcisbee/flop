# Unified Benchmark Matrix

Compare these engines with identical benchmark scenarios:

- `flop-ts` (`benchmarks/finance-ts/app.ts`)
- `flop-go` (`benchmarks/finance-go`)
- `sqlite-ts` (`benchmarks/finance-sqlite/app.ts`)
- `sqlite-go` (`benchmarks/finance-sqlite-go`)
- `turso-ts` (`benchmarks/finance-turso/app.ts`)
- `pglite-ts` (`benchmarks/finance-pglite/app.ts`)
- `turso-go` (`benchmarks/finance-turso-go`, SQLite-compatible Go baseline)
- `mongodb-ts` (`benchmarks/finance-mongodb/app.ts`)
- `mongodb-go` (`benchmarks/finance-mongodb-go`)

## Run Benchmarks

One command (from repository root):

```bash
deno task bench
```

Reset benchmark artifacts:

```bash
deno task bench:clean
```

Also clears Mongo benchmark local data dirs: `benchmarks/finance-mongodb/data`
and `benchmarks/finance-mongodb-go/data`.

If Go engines are included and dependencies are missing, bootstrap once:

```bash
cd go && go get modernc.org/sqlite@v1.39.1 && go mod tidy
cd ../benchmarks/finance-mongodb-go && go mod tidy
```

If MongoDB engines are included, make sure `mongod` is installed and available
in your `PATH` (or pass `--mongod-bin` via engine app flags when running those
apps directly).

Quick local binary (downloaded into gitignored repo path):

```bash
./benchmarks/finance-mongodb/download-mongod.sh
```

After that, benchmarks auto-detect `benchmarks/.tools/mongodb/mongod`. Set
`MONGOD_BIN` only if you want to override that path.

Optional filters:

```bash
# profile controls benchmark size:
#   smoke (fastest), quick (default), full (largest)
# engine-set defaults to all engines unless overridden by --engines
deno run --allow-read --allow-write --allow-run --allow-env --allow-net benchmarks/compare/run.ts --profile=quick --engine-set=all

# stronger benchmark rigor controls
deno run --allow-read --allow-write --allow-run --allow-env --allow-net benchmarks/compare/run.ts --repeats=3 --min-repeats=2 --early-stop-rse=0.05 --warmup-sec=3 --shuffle-engines=1 --strict-setup=1 --setup-retries=8

# only specific engines
deno run --allow-read --allow-write --allow-run --allow-env --allow-net benchmarks/compare/run.ts --engines=flop-ts,flop-go,turso-ts,pglite-ts,turso-go,mongodb-ts,mongodb-go

# only specific scenarios
deno run --allow-read --allow-write --allow-run --allow-env --allow-net benchmarks/compare/run.ts --scenarios=high-load-rw,reads,writes,edits

# override size/shape without editing scenarios
deno run --allow-read --allow-write --allow-run --allow-env --allow-net benchmarks/compare/run.ts --scenarios=high-load-rw,reads --users=500 --accounts-per-user=3 --duration-sec=20 --concurrency=200
```

Engine sets:

- `all` (default): all available engines in the matrix
- `core`: `flop-ts, flop-go, sqlite-ts, sqlite-go`

Rigor flags:

- `--repeats=N`: run each engine/scenario N times and aggregate means
- `--min-repeats=N`: minimum repeats before early-stop is allowed
- `--early-stop-rse=X`: stop additional repeats for stable engines once relative
  standard error is <= X (`0` disables)
- `--warmup-sec=N`: warmup workload per run (discarded from score)
- `--workload-timeout-sec=N`: max time allowed for each workload subprocess
  before it is killed
- `--request-timeout-ms=N`: timeout for each HTTP request from workload client
  to engine
- `--shuffle-engines=1|0`: randomize engine order each repeat
- `--strict-setup=1|0`: fail run if setup counts differ from expected
- `--setup-retries=N`: retry setup API calls to reduce transient failures
- `--seed=N`: deterministic shuffle seed

Defaults:

- `full`: `repeats=3`, `min-repeats=2`, `early-stop-rse=0.05`, `setup-retries=8`
- `quick`: `repeats=2`, `setup-retries=6`
- `smoke`: `repeats=1`, `setup-retries=4`

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
- click `Expand` on any scenario to open full-size graph + min/max/avg/latest
  table
- focused workload metrics only (`ops/s`, `read ops/s`, `write ops/s`,
  `edit transfer ops/s`)
- memory efficiency tracking per engine (`avg RSS MB`, `peak RSS MB`,
  `ops/s per MB`)

## Notes

- The runner resets benchmark data before each engine/scenario run.
- `sqlite-go` requires Go to download `modernc.org/sqlite` dependency the first
  time.

## Micro Preset

For a faster all-engine run (flop vs others), use:

```bash
deno task bench:micro
```

This uses the same compare harness with small defaults (single scenario by
default) and still includes all engines unless you filter with `--engines=...`.
See `benchmarks/micro/README.md` for options.
