# Development and verification

Start here for setup, a baseline, and a repeatable change/check loop. Run commands
from the repository root unless a command explicitly changes directories.
`Makefile` and `.github/workflows/ci.yml` are the canonical qualification commands.

## Setup and run

Install Git, Make, a POSIX shell, and Go. `go.mod` declares the module minimum;
use the latest patched Go **1.26.x**, as CI does, for release qualification.
The Go installation must put both `go` and `gofmt` on `PATH`. Check:

```sh
go version
gofmt -h
go mod download
make test
```

The engine is a library in the root module, not a standalone root server.
For a local application with an API and admin panel:

```sh
make -C examples/blog-go-react dev
# In another terminal; expect HTTP 200 and the demo page:
curl --fail --max-time 5 http://localhost:1985/
```

Stop with Ctrl-C. The demo keeps local data in `examples/blog-go-react/data`;
use disposable local data for experiments. See its README for routes and generated
artifacts. Movies and Twitter demos have their own Makefiles and module files.
Root `go test ./...` does **not** traverse these nested modules.

Deno is optional for the HTTP comparison harness in `benchmarks/compare` and
`benchmarks/micro`; it is not required to build or qualify the engine. React demo
source changes may need that demo's frontend toolchain. Consult the affected
demo's configuration rather than installing a root JavaScript runtime.

## Choose the closest check

| Changed behavior | Baseline and iteration seam |
| --- | --- |
| Engine queries, indexes, transactions, recovery | `go test ./internal/engine ./internal/storage -count=1`; select existing tests with `-run` while iterating, then rerun both packages and the affected public root tests |
| Public database/API, auth, policies, uploads, workflows | `go test . -count=1 -run 'TestNameOrPattern'`; discover names in root `*_test.go`, then run `go test . -count=1` |
| HTTP filters, JSON, request analytics, security | `go test ./internal/server ./internal/reqtrace -count=1`, plus affected root API/security tests |
| Concurrent access or lifecycle | Closest packages with `go test -race ... -count=1`, plus relevant transaction/recovery tests |
| Benchmark tooling | `go test ./cmd/quickbenchcmp ./cmd/pillargate`; exercise the real quickbench series below |
| Generators or shared admin source | `go generate ./...`, inspect generated diffs, run affected Go tests; validate browser behavior in a maintained demo for UI changes |
| A demo or benchmark server | Run tests in its own module, e.g. `(cd examples/blog-go-react && go test ./...)`; launch it and exercise the affected HTTP/browser behavior |

Use `go test <package> -list '<pattern>'` to confirm a filter actually selects
tests. A passing command with no selected tests is not verification. The main
source areas are root public APIs, `internal/engine`, `internal/storage`,
`internal/server`, `internal/schema`, `cmd/flop-gen`, and `shared` admin assets.

Before editing, state the observable result and select a check that can detect
its absence. Record the base commit, toolchain, exact command, and result in your
work notes. For bugs, reproduce the failure first when practical. After each
change rerun the same check with the same inputs, inspect the result, and iterate.
For a pre-existing failure, retain the baseline evidence and identify whether the
candidate changes it. Before handoff review the diff and rerun the affected suite
on the final revision. Do not equate a benchmark improvement with correctness.

## Engine performance loop

Choose the affected metric and a useful target **before** measuring (for example,
lookup latency for the reported workload). There is no universal percentage that
makes an optimization valuable. Keep public results, durability, and semantics
unchanged. `AGENTS.md` describes the engine's optimization constraints.

Quickbench measures seeding throughput, synchronous/asynchronous startup, indexed
lookups, full-text search, warmup, and result counts. It uses a deterministic seed
and fresh temporary databases. The default `normal` sync mode is an exploratory
profile; use `QB_SYNC_MODE=full` for durable-mode measurements. Never compare modes.

Capture an explicit baseline before implementation, then a candidate after it:

```sh
make quickbench-series QB_SERIES_DIR=.quickbench/results/baseline QB_SYNC_MODE=full
# Make the change, then use exactly the same settings:
make quickbench-series QB_SERIES_DIR=.quickbench/results/candidate QB_SYNC_MODE=full
go run ./cmd/quickbenchcmp -old .quickbench/results/baseline -new .quickbench/results/candidate
```

These commands build once per series and run five fresh processes/databases per
side. Set `QB_RUNS` to a larger repeat count when needed (minimum five). Set
`QB_ROWS`, `QB_LOOKUPS`, `QB_SEARCHES`, `QB_BATCH`, `QB_SEARCH_LIMIT`, `QB_SEED`,
and `QB_WARM_TIMEOUT` to match the relevant workload, identically on both sides.
Each directory must be new; it cannot silently overwrite or mix old results.
A failed sample stops collection and leaves its log for diagnosis. Recapture into
a new directory after fixing the failure; do not cherry-pick successful samples.
Reports and logs under `.quickbench/results` are ignored by Git.

Keep the same host, Go version, CPU allocation, build flags, filesystem, available
memory, and power mode. Avoid competing builds/tests/benchmarks. Reports record
workload settings, host, OS/architecture, CPU count, GOMAXPROCS, temporary directory,
GOFLAGS/GODEBUG, Go version, commit and dirty state. They cannot detect every change
in host load or storage configuration; record these conditions yourself. Do not
edit during collection. For an uncommitted candidate, keep its patch alongside
your work notes because a dirty flag alone does not identify its source.

The comparator rejects mismatched settings, mixed revisions within a series,
missing/changed metrics or units, index warmup timeouts, and changed result counts.
It prints median, min/max, and percentage change per metric. At least five samples
per side and non-overlapping observed ranges produce an improvement/regression
**signal**; overlap or fewer samples is **inconclusive**, even if the median moves.
Ranges are not confidence intervals and this conservative screen is not a
statistical significance test. Repeat a promising result in reverse revision order
on the same host to check drift; investigate wide ranges instead of dropping
outliers. Ratios are descriptive, and row counts are correctness observations.
Zero baseline measurements have no percentage; increase workload size when timing
resolution is too coarse. A command exit of zero means comparable reports, not
that the candidate improved or is fit for release.

For two revisions that predate this workflow, apply the same measurement-only
instrumentation to both before collecting. Legacy reports without comparison
metadata must be recaptured. Single-report `quickbench-save` and
`quickbench-compare-last` remain exploratory helpers; use explicit series for a
handoff so a moving “latest” file cannot replace the intended baseline.

For HTTP JSON or SSE hot paths, use Go microbenchmarks at their actual seam:

```sh
go test ./internal/server -run '^$' -bench 'BenchmarkHTTPJSON|BenchmarkSSE' -benchmem -count=10
go test . -run '^$' -bench '^BenchmarkAPISSE$' -benchmem -count=10
```

Save both revisions' output with identical flags and compare latency, bytes/op,
and allocs/op across all samples, using benchstat if available. For end-to-end
HTTP workloads, see [the comparison harness](benchmarks/compare/README.md):
`deno task bench:dev` is a smoke path; repeated workloads with explicit warmup,
seed and strict setup are the measurement path. Select `flop-go` unless comparing
other engines is part of the question. Install dependencies in the affected
nested module, not by changing root `go.mod` for a benchmark helper.

## Production qualification and handoff

```sh
make release-check
```

This retains formatting, vet, tests, race tests, reachable-vulnerability checks,
and `pillar-gate`. The gate uses three fresh baseline samples and the existing
median thresholds, with one crash-recovery matrix. `PB_GATE_*` settings in the
Makefile are the source of truth. Do not loosen thresholds, disable checks, or
change `full` durability to get a green result. Quickbench signals do not replace
this gate. Qualification needs network access for module/vulnerability data.

For tooling-only changes, a fast end-to-end plumbing check is:

```sh
make quickbench-series QB_SERIES_DIR=.quickbench/results/smoke-a QB_ROWS=200 QB_LOOKUPS=100 QB_SEARCHES=20 QB_BATCH=100
make quickbench-series QB_SERIES_DIR=.quickbench/results/smoke-b QB_ROWS=200 QB_LOOKUPS=100 QB_SEARCHES=20 QB_BATCH=100
go run ./cmd/quickbenchcmp -old .quickbench/results/smoke-a -new .quickbench/results/smoke-b
```

This tiny workload validates collection/comparison, not engine performance.
Include the relevant correctness result, benchmark commands/settings, both
revisions, repeat count, observed variation, and any qualification failures in the
handoff. If results change, a regression appears, or qualification fails, stop the
release handoff and investigate or revert the candidate optimization. Report an
unresolved baseline/environment blocker instead of claiming qualification.
