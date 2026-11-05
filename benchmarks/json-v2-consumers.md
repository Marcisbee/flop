# Consumer HTTP compatibility follow-up

Strike integration tests exposed a response-contract regression in the JSON v2 migration: a typed nil row filtered by access policy became `{}` rather than `null`. Tests did not observe private message content escaping the policy, but clients must retain the missing-row distinction. Repeated yearly snapshot responses also changed map ordering, violating the application's stable-response checks.

The shared HTTP writer still uses JSON v2, with explicit options preserving nil-map and nil-slice nulls, legacy zero-value omission, HTML/JavaScript escaping and deterministic map ordering. Internal storage and typed conversion semantics remain documented separately. The new writer regression failed before the change and passes afterward; all five affected Strike integration tests pass with the correction. Consumer PRs must pin this corrected revision, not the earlier `d476cba` candidate.

`make release-check` passes on Go 1.27.3, including race, vulnerability and crash-recovery checks. This is not approval to deploy unqualified consumer databases. Strike and Baby qualification is being carried out in their own repositories with old-binary fixtures; actual production backups were not supplied.

## Performance cost of preserving the HTTP contract

Before: `d476cba9689cde2ca53dccae8677cd9bd915e293`; after: this correction. Same Go 1.27.3, Linux/amd64, Ryzen 9 9950X, GOMAXPROCS=32. Five 200ms samples per revision in each order, ten total. No competing test/build jobs ran during collection. Ranges are observed extrema, not confidence intervals; overlap is inconclusive. The older reports are historical and do not represent this corrected HTTP configuration.

| Workload | Before ns/op [min–max] | After ns/op [min–max] | Change | B/op before → after | allocs/op before → after | Range signal |
|---|---:|---:|---:|---:|---:|---|
| HTTPJSONRoundTripSmall | 3,608.5 [3,505.0–3,644.0] | 3,682.5 [3,589.0–3,732.0] | +2.1% | 6,726 → 6,772 | 33 → 35 | overlap |
| HTTPJSONRoundTripMedium | 89,345.5 [87,784.0–90,304.0] | 98,623.0 [98,269.0–99,894.0] | +10.4% | 64,874 → 70,150 | 1835 → 2157 | regression |
| SSEChangeBatchMarshal | 112,697.5 [109,813.0–113,346.0] | 111,681.0 [109,988.0–114,318.0] | -0.9% | 76,009 → 76,012 | 1170 → 1170 | overlap |
| SSEChangeBatchMarshalOnly | 91,279.0 [90,690.0–91,934.0] | 90,349.5 [89,126.0–91,826.0] | -1.0% | 31,795 → 31,794 | 1152 → 1152 | overlap |
| SSEChangeBatchWriteOnly | 15,258.5 [14,558.0–17,259.0] | 14,554.0 [13,683.0–15,286.0] | -4.6% | 44,120 → 44,120 | 18 → 18 | overlap |
| SSEEngineChangeEventMarshalOnly | 182,740.5 [181,036.0–185,495.0] | 182,641.5 [179,666.0–185,620.0] | -0.1% | 82,060 → 82,058 | 2048 → 2048 | overlap |
| SSEEngineChangeEventMarshal | 223,797.0 [220,374.0–225,161.0] | 221,538.0 [218,086.0–223,530.0] | -1.0% | 181,018 → 181,016 | 2066 → 2066 | overlap |

Raw measurements and qualification output are in [results/json-v2-consumers](results/json-v2-consumers). Reproduce in both checkouts, then repeat in reverse order:

```sh
GOTOOLCHAIN=go1.27.3 go test ./internal/server -run '^$' -bench 'BenchmarkHTTPJSON|BenchmarkSSE' -benchmem -benchtime=200ms -count=5
GOTOOLCHAIN=go1.27.3 make release-check
```
