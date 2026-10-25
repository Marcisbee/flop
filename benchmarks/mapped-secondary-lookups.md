# Mapped secondary-index lookup comparison (first iteration)

This report records the first iteration at `da942630c83b03b38eb86a77f50d13a57367027a`.
The [follow-up profile and comparison](query-tracing-overhead.md) cover the final
query-overhead improvements in this PR.

Flop now searches persisted non-unique index keys before decoding the matching
row-pointer list. Previously, every binary-search probe allocated and decoded the
postings of an unrelated key. A miss did this work without returning any rows.
The change reuses the existing header-only search used by `HasKey` and
`HasPointer`. It preserves pointer order, snapshot ownership, overlay handling,
locking, public APIs, and the version-1 mapped index file format.

## Research and scope

The useful principle from [bbtree's design](https://github.com/XiangpengHao/bbtree/blob/main/design/14-blogpost.md)
is to minimize dependent memory access and separate key comparisons from value
loads. This is a direct application to Flop's existing sorted mapped indexes.

A Rust B-tree replacement is not justified by bbtree's Rust BTreeMap/skiplist
comparisons: Flop uses Go hash maps and mapped indexes. New ordered indexes would
also require ordered key encoding and explicit query integration. Batching and
optimistic concurrency remain separate possibilities, without evidence here that
they should replace Flop's current API or locking. No cache, extra index copy,
new dependency, schema migration, or Rust integration is introduced.

Strike uses non-unique indexes for notifications by receiver, posts/comments by
user, leaderboard entries by game, and playtime by member. Those reads use this
path when their secondary indexes have been persisted and reopened.

## Environment and method

- Baseline Flop: `d27e88a216b631d2d84d39501bdf58ce21622bfc`.
- Candidate: `da942630c83b03b38eb86a77f50d13a57367027a`.
- Strike: `07b5071ababf3b692a6afb518603404f8bc4d097`, which pins that exact baseline Flop version.
- Go 1.26.10, Linux/amd64, AMD Ryzen 9 9950X, default GOMAXPROCS (32).
- Identical measurement-only benchmark files on both revisions.
- Five samples per revision. First baseline then candidate; a second affected-path
  series runs candidate then baseline to check drift. No competing test/build
  workloads during measurement. The initial series overlapped the Strike repository
  download; the reverse series ran after that completed.
- Storage: 4,096 keys with common prefixes, 1 or 64 postings per key, varied
  hits/misses, mapped reopen and in-memory control. Public `MultiIndex.GetAll`
  includes the result-copy cost.
- Engine: 512 owners with 64 rows each (32,768 rows), durable `full` sync mode,
  close/reopen, `LookupByField` returning up to 10 decoded rows; results, owner,
  total count and index use checked on every operation. Setup excluded from timing.
- These are warm local synthetic workloads, not production Strike request latency.

## Results

Medians from the first five-sample series; lower time/bytes/allocations is better.

| Workload | Before | After | Difference |
| --- | ---: | ---: | ---: |
| Mapped index hit, 64 postings | 2,271 ns | 482.7 ns | 4.70x faster |
| Mapped index miss, 64 postings/key | 2,202 ns | 100.4 ns | 21.93x faster |
| Mapped index hit, 1 posting | 176.3 ns | 93.86 ns | 1.88x faster |
| Mapped index miss, 1 posting/key | 186.6 ns | 92.89 ns | 2.01x faster |
| Engine query, 10 returned rows | 12,458 ns | 11,994 ns | 3.72% lower median |
| Engine query, missing owner | 10,254 ns | 9,194 ns | 10.34% lower median |
| Index hit allocation, 64 postings | 6,145 B / 12 allocs | 1,024 B / 2 allocs | ~83% fewer bytes |
| Index miss allocation, 64 postings/key | 6,144 B / 12 allocs | 0 B / 0 allocs | eliminated |
| Engine hit allocation | 9,821 B / 105 allocs | 6,227 B / 98 allocs | ~37% fewer bytes |
| Engine miss allocation | 4,881 B / 14 allocs | 272 B / 5 allocs | ~94% fewer bytes |

The first engine-hit ranges slightly overlap (baseline 12,325–13,054 ns,
candidate 11,989–12,359 ns), so that first latency signal alone is inconclusive.
In reverse order, the engine-hit medians are 12,565 → 11,761 ns (6.40% lower),
with non-overlapping observed ranges: 12,489–13,155 vs 11,719–12,165 ns.
The reverse index series repeats the strong signals: 2,323 → 496.1 ns for hits
(4.68x), and 2,356 → 98.37 ns for misses (23.95x). Observed ranges are not
confidence intervals. In-memory control allocations are unchanged, with only
small timing variation.

The five-process general quickbench series used 20,000 rows, 10,000 lookups,
1,000 searches, batch size 2,000, search limit 8, seed 42, and `full` durability.
It reports no conclusive latency/throughput change; result counts are identical.
This does not support a claim of a broad database or Strike speedup.

[Raw benchmark samples and quickbench comparison](results/mapped-secondary-2026-10-25/)
are retained with this report.

## Reproduce

Run on each revision with the same benchmark files:

```sh
go test ./internal/storage -run '^$' -bench '^BenchmarkMultiIndexLookup$' -benchmem -count=5
go test ./internal/engine -run '^$' -bench '^BenchmarkMappedSecondaryLookup$' -benchmem -benchtime=200ms -count=5
```

For the reverse storage series use
`-bench '^BenchmarkMultiIndexLookup$/postings=64/mapped=true' -benchtime=200ms`.
Run candidate then baseline in isolation.

For the broader comparison, use a fresh directory per side:

```sh
make quickbench-series QB_SERIES_DIR=.quickbench/results/baseline-series QB_SYNC_MODE=full QB_ROWS=20000 QB_LOOKUPS=10000 QB_SEARCHES=1000
# Repeat with the candidate and a new candidate-series directory.
go run ./cmd/quickbenchcmp -old .quickbench/results/baseline-series -new .quickbench/results/candidate-series
```

## Strike compatibility fixture

[The standalone fixture](strike-index-compat.go.txt) uses Strike's actual schema
and `BuildAuthMePayload`, waiting for secondary indexes to be ready and checking
all notification IDs, receiver ownership,
unread counts, and missing-user lookups. It seeds 8 users and 256 notifications
with baseline Flop; candidate Flop reopens that database and changes receiver,
deletes, inserts, and checkpoints; baseline Flop reopens the resulting database.

Copy it to a `.go` file under Strike's ignored `.tmp/` directory. Use separate
external Go workspace files to select baseline/candidate Flop alongside Strike,
without changing either module's dependency declarations. Run from Strike:

```sh
GOWORK=/absolute/baseline.work go run ./.tmp/compat/main.go .tmp/compat-data seed
GOWORK=/absolute/candidate.work go run ./.tmp/compat/main.go .tmp/compat-data mutate
GOWORK=/absolute/baseline.work go run ./.tmp/compat/main.go .tmp/compat-data verify
```

Use a fresh disposable directory. No production data or external credentials are
needed. The fixture refuses to reseed an existing manifest.

## Qualification

- Flop `make release-check`: passed formatting, vet, full tests, full race tests,
  reachable-vulnerability checking, all 15 crash-recovery scenarios, and the
  unchanged three-sample pillar gate.
- Strike `deno task test:full` passed on both baseline and candidate: all Go
  packages, frontend tests, API contracts, production asset/server builds,
  199 production browser cases (3 configured skips), and 4 seasonal-preview
  cases (1 configured skip). Strike's tracked files remain unchanged.
- Storage coverage checks exact pointer order across shared-prefix, empty, binary
  and Unicode keys; misses; overlay deletion/insertion; repeated persistence;
  and returned-slice ownership for both in-memory and mapped indexes.
- The Strike baseline → candidate → baseline fixture passed with ready indexes,
  checking 8 users and 256 notification IDs and auth unread counts before and
  after mutations. Strike needs no application-code or schema changes.
  Its pinned Flop dependency must be advanced after merge to adopt this change;
  local validation selected the candidate through an external Go workspace.
  No production deployment was performed.

Qualification is local with disposable data; it does not claim that every
production workload or external authentication provider has been exercised.
