# JSON v2 reliability follow-up

The remaining struct-encoding regression has not been safely removed. Two experiments were discarded because they made that case slower. The existing object optimization is unchanged. This follow-up fixes a reproduced, pre-existing oversized-update corruption risk and strengthens validation; it is not a claim that the engine cannot fail.

## Reproduced row-size bug

The fast update path compared `uint16(len(serialized))` with the previous slot size. `Page.UpdateRow` also narrowed the new payload length before comparing it. A 65,536-byte field could wrap the comparison and be accepted as an in-place update instead of returning `ErrRowTooLarge`. The defect exists in the pre-migration source at `df1fa8a`; JSON v2 did not introduce it.

The extended engine regression failed before the fix with `expected ErrRowTooLarge, got <nil>` for a 65,536-byte field. The new page-level regression separately failed with `accepted oversized update whose length wraps to zero`. The fix validates serialized size before constructing a WAL record or changing a page, and compares full-width lengths in both the engine and page writer.

Both regressions pass after the fix. Engine checks verify that the target and neighboring records remain unchanged immediately and after close/reopen. Page checks verify that rejected writes do not alter any page bytes. Ordinary oversized inputs remain covered. Existing JSON-write tests now also exercise malformed values nested inside the optimized map path, verifying rejection and unchanged persisted records.

## JSON validation and security scope

The object encoder continues to use the standard validating JSON writer and standard-v2 fallbacks. Tests cover invalid UTF-8, duplicate raw members, non-finite numbers, unsupported types, custom marshalers, exact large integers, float32, negative zero, and cycles. A 45-second differential fuzz run completed 4,404,184 executions without a failure. Fuzz inputs include arbitrary bytes; invalid JSON inputs are rejected before the object comparison. This count does not mean four million distinct valid objects or exhaustive coverage.

The focused review checked JSON serialization failure propagation before writes, row length boundaries, strict request decoding, and the existing request-body limit and security regression coverage. It is not an independent security audit or a proof of no vulnerabilities. Callers must still honor engine/API contracts; hardware failure, disk exhaustion, process termination, and untested concurrency or input combinations remain possible failure modes.

The v2 migration is still behaviorally breaking. In particular, stricter reading can reject previously accepted stored JSON (for example duplicate members originally emitted by a custom marshaler). Consumer databases and application compatibility have not been qualified. Keep the migration draft until upgrade/recovery checks against a representative database copy succeed. No production database was opened or modified during this investigation.

## Performance experiments

Go 1.27.3, Linux/amd64, AMD Ryzen 9 9950X, GOMAXPROCS=32. Seven 300ms samples per exploratory variant. Median struct encode: existing implementation 250.7 ns/op; consolidated marshal dispatch 258.8 ns/op; direct row-buffer streaming 258.6 ns/op. The streaming experiment reduced allocated bytes from 336 to 312 for structs and improved object encoding, but did not achieve the requested struct speedup. Both experiments were removed. Raw experimental outputs are retained and are explicitly not the delivered code.

## Retained version versus prior PR

Baseline is `1056a8cc222335f2fa362978d8c01b92ab112be2`. Candidate is the accompanying change, recorded in `candidate.patch`. The four added/extended test files were copied into the baseline so both benchmark binaries use identical test instrumentation. Ten samples per workload/revision, five in each order, 200ms per sample, same machine/toolchain with no competing build/test jobs. Medians and observed ranges are descriptive; overlapping ranges are inconclusive, not evidence of equivalence. No samples were discarded. These benchmarks cover row encoding and the update path affected by the safety checks.

| Workload | Before ns/op [min–max] | After ns/op [min–max] | Change | B/op before → after | allocs/op before → after | Range signal |
|---|---:|---:|---:|---:|---:|---|
| ArrayRowCodec/Encode | 977.3 [962.9–988.1] | 1,016.0 [1,000.0–1,033.0] | +4.0% | 3072 → 3072 | 4 → 4 | regression |
| ArrayRowCodec/Decode | 1,863.0 [1,826.0–1,904.0] | 1,860.5 [1,824.0–1,979.0] | -0.1% | 3581 → 3581 | 134 → 134 | overlap |
| JSONRowCodec/Encode | 384.1 [378.6–388.3] | 386.8 [380.8–389.4] | +0.7% | 344 → 344 | 3 → 3 | overlap |
| JSONFieldShapes/TopStruct | 263.2 [260.1–266.8] | 262.4 [260.5–265.7] | -0.3% | 336 → 336 | 3 → 3 | overlap |
| RowIO/Scalar/Get | 215.4 [213.1–261.2] | 215.2 [208.6–262.8] | -0.1% | 421 → 421 | 7 → 7 | overlap |
| RowIO/Scalar/Insert | 6,475.5 [6,414.0–7,102.0] | 6,462.0 [6,397.0–6,615.0] | -0.2% | 1778 → 1776.5 | 23 → 23 | overlap |
| RowIO/Scalar/Update | 3,580.5 [3,529.0–3,799.0] | 3,525.0 [3,454.0–3,807.0] | -1.6% | 1953 → 1953 | 29 → 29 | overlap |
| RowIO/Arrays/Get | 2,194.0 [2,045.0–2,697.0] | 2,098.0 [2,059.0–2,655.0] | -4.4% | 4637 → 4637 | 137 → 137 | overlap |
| RowIO/Arrays/Insert | 6,450.0 [6,337.0–6,651.0] | 6,477.0 [6,258.0–6,810.0] | +0.4% | 7790.5 → 7786.5 | 26 → 26 | overlap |
| RowIO/Arrays/Update | 8,471.0 [7,871.0–10,221.0] | 8,049.0 [7,797.0–9,524.0] | -5.0% | 12889 → 12889 | 162 → 162 | overlap |

Raw data and the source snapshot are in [results/json-v2-reliability](results/json-v2-reliability). Reproduce each side, then repeat in reverse order:

```sh
GOTOOLCHAIN=go1.27.3 go test ./internal/storage ./internal/engine -run '^$' -bench '^BenchmarkJSONFieldShapes/TopStruct$|^BenchmarkJSONRowCodec/Encode$|^BenchmarkRowIO$|^BenchmarkArrayRowCodec$' -benchmem -benchtime=200ms -count=5
GOTOOLCHAIN=go1.27.3 go test ./internal/jsonx -run '^$' -fuzz '^FuzzMarshalObjectMatchesV2$' -fuzztime=45s -parallel=4
GOTOOLCHAIN=go1.27.3 make release-check
```

The original migration and object-encoding improvements remain documented in [the migration report](json-v2.md) and [the optimization report](json-v2-optimization.md).

## Qualification and remaining signals

`make release-check` passes on Go 1.27.3: formatting, vet, full tests, race tests, reachable-vulnerability scanning, and the three-sample durability/crash-recovery gate. The scan reported no known reachable vulnerabilities; it is not a general security certification. See [qualification output](results/json-v2-reliability/qualification.txt). The same regression tests [fail on the previous revision](results/json-v2-reliability/regression-before.txt) and [pass with the fix](results/json-v2-reliability/regression-fixed.txt).

The matched run does not establish any struct-encoding improvement (263.2 → 262.4 ns/op, overlapping ranges). Binary-array encoding shows a +4.0% regression signal with unchanged allocations, although its serialization implementation is unchanged and the added size checks are in the update/page writer paths. The cause is not established. Earlier runs with different test instrumentation showed different control timings, so small benchmark movements must not be attributed to runtime work without further evidence. The safety fix is retained; this remains a draft, not a performance or production-readiness certification.
