# Query overhead after the mapped-index improvement

The index lookup was only part of the full query. A CPU profile of the 10-row
query at `da942630c83b03b38eb86a77f50d13a57367027a` attributed approximately 53%
of sampled CPU to `runtime.Stack`, called by request tracing to discover the
current goroutine ID. This happened even without any active trace collector.
The profile includes benchmark setup and is diagnostic, not a latency ratio.

The final change:

- Maintains an atomic indicator of whether the existing collector map is empty,
  updated under the same mutex as the map. No goroutine stack inspection or span
  construction is needed when the entire process has no active traces.
- Appends duration spans directly to the current collector, removing one layer
  from the stack being inspected. Duration and finish timestamps are still
  captured before collector lookup, preserving the timing boundary.
- Returns the mapped decoder's already-owned pointer snapshot directly when
  there are no overlay additions or deletions. This removes a second allocation
  and copy while preserving returned-slice independence. Overlay merges retain
  their existing behavior.

This does not disable tracing, change sampling, add a result cache, or alter
public APIs or index formats. An untraced goroutine must still identify itself
when another goroutine has a collector. Strike's view/reducer HTTP handlers start
collectors, so the fully traced benchmark is the relevant one for those paths.

## Measurements

Baseline is the first PR iteration, `da942630c83b03b38eb86a77f50d13a57367027a`.
Candidate is that revision plus this PR's final tracing and snapshot changes.
Both have identical expanded benchmark instrumentation. Host: Go 1.26.10,
Linux/amd64, Ryzen 9 9950X, GOMAXPROCS 32. No competing test/build workloads during
measurement. Each revision has five samples, followed by another five in reverse
revision order. The initial exploratory candidate was changed to preserve trace
timing boundaries and was excluded; all retained samples use the final code.

The fixture has 512 owners with 64 rows each, full durability, a clean close and
reopen, and queries returning 10 rows (or none). Seeding is outside the timer.
The benchmark checks the matching owner, row count, total and index use. Traced
queries include collector start/end per query; the other-request case has one
collector on another goroutine for the lifetime of the measurement.

First-series medians, lower is better:

| Case | Before | After | Latency reduction |
| --- | ---: | ---: | ---: |
| 10 rows, no active traces in process | 11.632 µs | 2.343 µs | 79.9% (4.96x faster) |
| No matches, no active traces in process | 9.109 µs | 0.164 µs | 98.2% |
| 10 rows, request traced | 16.095 µs | 14.806 µs | 8.0% |
| No matches, request traced | 13.371 µs | 12.194 µs | 8.8% |
| 10 rows, only another request traced | 11.720 µs | 10.511 µs | 10.3% |
| No matches, only another request traced | 9.150 µs | 7.894 µs | 13.7% |

The reverse series repeats the improvements: the 10-row query goes from
12.154 → 2.347 µs with no active traces (80.7% lower), 16.198 → 14.654 µs
with request tracing (9.5% lower), and 11.951 → 10.349 µs when only another
request is traced (13.4% lower). All six reverse-series ranges are also
non-overlapping. The other-request case isolates the presence of another
collector; it is not a concurrent HTTP throughput benchmark.

All six first-series latency ranges are non-overlapping. These are observed
min/max ranges, not confidence intervals. These synthetic measurements do not
establish production Strike endpoint latency improvements.

For the 10-row query, no-active-trace allocations fall from 6,227 B / 98
allocations to 5,459 B / 93 allocations. With request tracing, they fall from
7,411 B / 104 allocations to 6,899 B / 103 allocations. The mapped 64-posting
index hit itself now takes a median 271.9 ns with 512 B / 1 allocation; the first
iteration measured 482.7 ns and 1,024 B / 2 allocations (a separate earlier run).

[Raw samples](results/query-overhead-2026-10-26/) retain each observation.

## General database benchmark

Two five-process series per revision, in baseline/candidate then candidate/baseline
order, use 20,000 rows, 10,000 lookups, 1,000 searches, batch size 2,000,
search limit 8, seed 42, and full durability. These library calls have no active
request collectors, so the large lookup gains must not be projected onto traced
HTTP requests.

| Metric | First before → after | Reverse before → after |
| --- | ---: | ---: |
| Synchronous indexed lookup | 4.244 → 0.624 µs | 4.254 → 0.604 µs |
| Asynchronous-index-mode lookup | 5.614 → 0.701 µs | 5.687 → 0.692 µs |
| Synchronous full-text search | 675.160 → 666.940 µs | 671.311 → 667.065 µs |

The comparator reports non-overlapping ranges for these metrics. Result counts
are unchanged. Async search, startup and seed throughput remain inconclusive.
The raw comparator outputs include every metric, including those without gains.

## Reproduce

Apply the expanded `internal/engine/secondary_lookup_bench_test.go` to both
revisions, then run sequentially on the same host:

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkMappedSecondaryLookup$' -benchmem -benchtime=200ms -count=5
# Repeat candidate then baseline to check drift.
go test ./internal/storage -run '^$' -bench '^BenchmarkMultiIndexLookup$/postings=64/mapped=true' -benchmem -benchtime=200ms -count=5
```

The diagnostic CPU profile was captured before expanding the benchmark cases:

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkMappedSecondaryLookup/hit=true$' -benchtime=3s -cpuprofile=/tmp/query.cpu
go tool pprof -top -cum /tmp/query.cpu
```

On the expanded benchmark, select `trace=off/hit=true` instead.

For quickbench, run each revision with a new series directory, then compare:

```sh
make quickbench-series QB_SERIES_DIR=.quickbench/results/SIDE QB_SYNC_MODE=full QB_ROWS=20000 QB_LOOKUPS=10000 QB_SEARCHES=1000
go run ./cmd/quickbenchcmp -old /absolute/baseline-series -new /absolute/candidate-series
```

Production source hashes for the measured candidate (SHA-256):

```text
14b4affb6a1b3fe50c8e314fa7681345067deb3f9ea167ffdc04fa6a100277e0  internal/reqtrace/reqtrace.go
b3d4683577dd01d4a8e8726ca6eb7029258d39578d19922f6c74f89149f3f4d3  internal/storage/index.go
```

## Correctness and compatibility

The collector tests cover replacement, repeated `End`, inactive collection,
trace metadata, isolation between goroutines, and keeping one request active
when another request ends. Existing storage tests cover snapshot independence,
exact posting order, overlay changes and repeated persistence.

The final candidate passes Flop's full `make release-check`: formatting, vet,
all tests and race tests, reachable-vulnerability checks, 15 crash-recovery
scenarios, and the unchanged three-sample pillar gate. The focused race run
also passes for tracing, storage and engine packages.

The Strike baseline → candidate → baseline fixture was rerun successfully:
8 users, 256 notification IDs, receiver changes, deletion/insertion, authenticated
unread counts, and clean closes/reopens. It waits for secondary indexes to be
ready. There is no on-disk format or public API change. Strike's tracked files
and production deployment remain unchanged.

Strike's final `deno task test:full` passed against the candidate through an
external Go workspace: all Go packages, frontend tests, API contracts,
production asset/server builds, 199 production browser cases (3 configured
skips), and 4 preview cases (1 configured skip). The baseline full run recorded
in the first-iteration report used the same Strike revision and passed with the
same case counts. No tests were weakened or disabled for this change.
