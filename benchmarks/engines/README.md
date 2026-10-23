# Embedded engine comparison

Compare Flop before/after, SQLite, and the **Turso Rust engine** on the same host.
This is separate from the HTTP finance harness: `finance-turso-go` uses SQLite,
and `finance-turso` uses libSQL; neither measures this Turso engine.

Requires Linux (RSS from `/proc`), Go 1.26.10, Python 3, a C compiler for SQLite,
and a real local filesystem. Do not use a memory-backed temporary directory for
durability measurements. Dependencies are pinned in this nested module, leaving
the engine's production module unchanged.

```sh
# From the repository root. Both revisions need the existing typed transaction API.
git worktree add /absolute/path/to/baseline 09023d0
benchmarks/engines/build.sh /absolute/path/to/baseline "$PWD/.quickbench/results/engines-before"
benchmarks/engines/build.sh "$PWD" "$PWD/.quickbench/results/engines-after"
python3 benchmarks/engines/collect.py \
  --baseline .quickbench/results/engines-before \
  --candidate .quickbench/results/engines-after \
  --output .quickbench/results/engines-comparison --repeats 5
python3 benchmarks/engines/report.py .quickbench/results/engines-comparison
```

Use a new output directory for every collection. `--smoke --repeats 1` exercises
all adapters quickly; it is not performance evidence. The builder temporarily
copies `adapter/engine_compare_test.go` into each checkout, compiles with an
alternate module file, and moves that source copy into the build output. It
refuses to overwrite an existing adapter. Build sequentially, then collect with
no competing builds, tests, or measurements. The same adapter is compiled against
both Flop revisions; engine code is not copied between revisions.

The collector retains every process log, JSON sample, engine binary hash,
revision, driver version, host, and filesystem. It rotates/reverses engine order
between repeats. A process failure stops collection; failed operations stay in
the data. The reporter requires a completed series and excludes failed or
incorrect cases from rankings. Five runs with separated min/max ranges provide
a descriptive signal, not a statistical confidence interval.

## Workloads and settings

- Flop: `SyncMode: "full"`, ordinary writes or `flop.Transaction`.
- SQLite: bundled SQLite through `go-sqlite3 v1.14.52`, WAL, FULL synchronous,
  `BEGIN IMMEDIATE` for explicit transactions.
- Turso: `tursogo v0.8.3`, MVCC journal, FULL synchronous, `BEGIN CONCURRENT`.
  Its `sqlite_version()` reports compatibility, not the Turso package version.
- Each worker owns its SQL connection or Flop reducer context. This is an
  in-process library comparison, including typed Flop / SQL driver overhead,
  without HTTP or network overhead. SQL statements are prepared by the driver's
  ordinary execution path; no extra application cache is added.
- Two tables contain string primary keys and eight-character string values.
  An explicit transaction updates one row in each table to the same value.
  Independent workers own different rows; shared workers use row `0`.
- Explicit transactions run at 1, 2, 4, 8, 16, 32, and 64 workers, both independent
  and shared, with scheduled arrivals and with saturation.
- Read, update, insert, delete, and 80% read / 20% update workloads run saturated
  at 1, 8, and 64 workers. Read/delete/mixed cases seed 1,024 rows per table;
  other cases seed 64. Inserts use unique new keys. Deletes consume the seeded
  rows and may end before two seconds. Mixed workers read/write their own rows.
- Scheduled workloads use 1,000 operations, Poisson arrivals at an expected
  1,000/s, and seed 42. Latency begins at scheduled arrival and includes queueing
  and retries. Throughput includes draining all queued work. This load can cap
  throughput; the separate two-second saturation runs have no arrival cap.
- SQL lock, write-write, and commit-dependency conflicts, and Flop's explicit
  retryable row conflicts, retry the whole operation after 1 ms for at most
  30 seconds. Errors such as missing rows are never silently retried. Retry time
  is included in latency; retries and terminal errors are recorded separately.
- Checkpoint diagnostics run update/transaction workloads at 8 and 32 workers.
  They request checkpoints every 100 ms and read row `0` every millisecond.
  Timers can coalesce. Calls may defer or do no persistence; reported checkpoint
  latency is **request latency**, not proof of an equivalent persistence job.
  Diagnostic reads that return a missing row count as errors.

All samples seed and checkpoint before timing. No warmup sample is discarded.
Row counts and final transaction values are checked before and after a clean
close/reopen. This does not replace the engine's crash-recovery tests. Results
with failed operations or verification failures are ineligible for speed
rankings. SQL error strings are classified narrowly because Turso's Go driver
returns its conflict errors as strings.

Latency histograms use fixed memory and have at most 1% bucket quantization
error. p99.9 from a small sample is coarse. RSS is Linux process high-water memory,
including the runtime, driver, seed, recorder, and reopen verification; it is not
isolated database-cache memory. Compact hot-key results do not establish winners
for large datasets, complex queries, joins, remote databases, or other hardware.

## Verification

```sh
go -C benchmarks/engines test -race ./...
go -C benchmarks/engines vet ./...
# Build both adapters, then run the collector's smoke profile above.
```

The tagged adapter is a measurement entry point, not a production API or normal
root test. The histogram test checks known independent percentiles; the smoke
profile exercises actual database implementations and reopen validation.
