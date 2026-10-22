# Durable concurrent writes

`BenchmarkConcurrentDurable` in the root Go package exercises the public typed
API. Each operation updates one fixed-width value in each of two tables, either
as two ordinary writes or as one explicit transaction. Writers use independent
keys or the same key. Full durability is always enabled.

Build once per revision, then run the same binary/settings for every sample:

```sh
mkdir -p "$PWD/.quickbench/results/concurrent/tmp"
export TMPDIR="$PWD/.quickbench/results/concurrent/tmp"
# Verify TMPDIR is on durable storage, not tmpfs, before interpreting fsync costs.
df -T "$TMPDIR"
go test -c -o .quickbench/results/concurrent/run.test .
FLOP_CONCURRENT_OPS=1000 FLOP_CONCURRENT_RATE=1000 \
  .quickbench/results/concurrent/run.test -test.run '^$' \
  -test.bench BenchmarkConcurrentDurable -test.benchtime=1x -test.count=5 \
  -test.timeout=30m > .quickbench/results/concurrent/run.log
```

Choose distinct output paths for baseline and candidate. Record their revisions,
source patches, Go version, CPU, filesystem, and environment; do not change
source or run competing workloads during collection. Repeat the baseline after
the candidate to check drift. Each sample creates fresh database files; `count`
repeats samples in the same process. For process-isolated memory comparisons,
invoke the binary separately for each sample and select one sub-benchmark.

The 28 cases cover 1, 2, 4, 8, 16, 32, and 64 writers, ordinary/explicit writes,
and independent/shared keys. `FLOP_CONCURRENT_OPS` defaults to 10,000 and
`FLOP_CONCURRENT_RATE` to 1,000 attempts/second. A deterministic Poisson schedule
(seed 42) is generated before measurement. Every write latency starts at its
scheduled arrival and includes waiting for an available writer. All scheduled
operations drain before throughput is calculated. This is a fixed offered-load
experiment, not a measurement of maximum sustainable throughput. An overloaded
case accumulates queueing latency; do not interpret it as single-commit cost.

Each `CONCURRENT` JSON record reports:

- `write_ms`: p50, p99, p99.9 of successful operations, in that order.
- `attempt_ms`: the same percentiles including failed attempts.
- `throughput`: successful two-write operations/second, including queue drain.
- `errors`: observed errors. Shared-key failures are logged and excluded from
  successful throughput/latency; they are not evidence of successful contention
  handling. Errors in independent-key cases fail the benchmark.
- `peak_heap_bytes`: peak Go heap allocation sampled every 10 ms, including the
  harness. This is neither process RSS nor total database/cache memory.

A 1,000-operation sample gives a coarse tail estimate. Report all five sample
ranges and medians; use longer runs for tail characterization. Non-overlapping
ranges are a screening signal, not a confidence interval. Never discard failed
or slow samples or compare different durability settings.

To examine checkpoints, set `FLOP_CONCURRENT_CHECKPOINT=1` with otherwise
identical settings. This requests a database checkpoint every 100 ms and a
lookup every millisecond. The report adds `checkpoint_ms` and `read_ms`
(p50/p99/p99.9). The lookup timer can coalesce when delayed; these are diagnostic
lookup latencies, including lookups that return no visible row, not an open-loop
read-throughput benchmark. Compare writes with and without this additional
load. Successful timing measurements do not establish checkpoint atomicity;
run recovery qualification separately.

For example, select eight independent writers with:

```sh
FLOP_CONCURRENT_OPS=1000 FLOP_CONCURRENT_CHECKPOINT=1 \
  .quickbench/results/concurrent/run.test -test.run '^$' \
  -test.bench 'BenchmarkConcurrentDurable/writers=8$/tx=.*/shared=false$' \
  -test.benchtime=1x -test.count=5 -test.timeout=10m
```
