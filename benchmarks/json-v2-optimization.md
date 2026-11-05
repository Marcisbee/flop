# JSON v2 object encoding optimization

This follow-up removes reflection copies from plain JSON-object encoding. It improves embedded object writes and typed-row reads, but **does not establish a universally faster Flop**. The tables include slower cases and unchanged controls.

Baseline: initial v2 migration `6da073295f6609ca0e1c76d55dd1ae2177c900d3`, Go 1.27.3. Candidate: the source accompanying this report, captured in [candidate.patch](results/json-v2-optimization/candidate.patch). Original v1 comparison: `df1fa8af1a47910f9f490e150c868dc446301a3a`, Go 1.26.10. Same Linux/amd64 host, AMD Ryzen 9 9950X, GOMAXPROCS=32, default build flags. No concurrent benchmark/build/test jobs were run during collection.

The measured JSON-row encode drops from 780.5 to 392.4 ns/op (49.7% less time), with 11 → 3 allocations. Typed reads drop from 2,200.5 to 1,817.5 ns/op (17.4%). Against the original Go 1.26/v1 build, these paths take 30.2% and 40.3% less time, respectively.

Struct-valued JSON is the remaining regression signal: 241.8 → 254.2 ns/op (+5.1%, about 12 ns), with unchanged allocations. The extra map-selection path adds work even when it falls back to standard encoding. Binary-array and SSE slowdowns seen in exploratory runs did not retain non-overlapping ranges in the final forward/reverse sweep; their causes are not established. This stays a draft for weighing the fallback cost and the migration’s compatibility changes.

## Implementation and compatibility

Stored JSON fields and typed-row map conversion specialize exact `map[string]any` objects. The standard `jsontext` writer validates syntax, UTF-8, duplicate names and nesting. Named types, custom marshalers, raw JSON, byte slices, float32 and unusual values go through standard v2. Deep traversal falls back to v2, retaining cyclic-reference rejection. Empty maps and single-map envelopes use the standard path. No persistent caches or pooled application objects are added.

The public API is unchanged by this optimization. Standard-v2 differential tests cover values, custom marshaling, integer precision, negative zero, invalid input, and cycles. The earlier migration's breaking JSON semantics and Go 1.27.3 minimum still apply; this optimization does not restore v1 compatibility.

## Initial v2 → optimized v2

Each microbenchmark has ten samples per side: five baseline then candidate, followed by five candidate then baseline. The table pools both orders without discarding samples. Negative elapsed-time change is faster. Ranges are observed min/max, not confidence intervals; overlapping ranges are inconclusive. Non-overlapping ranges are a conservative signal, not a statistical significance test. B/op is allocated bytes, not resident memory.

HTTP uses httptest without database/network latency. SSE uses 128-event batches. Typed conversion excludes database IO. JSON shapes use actual row serialization. Scalar/array RowIO uses full durability. Archive benchmarks exclude fixture creation/copying and verify recovered records.

| Benchmark | Before ns/op [min–max] | After ns/op [min–max] | Time change | B/op before → after | allocs/op before → after | Range signal |
|---|---:|---:|---:|---:|---:|---|
| JSONFieldShapes/EmptyObject | 153.9 [153.2–158.5] | 156.4 [155.3–160.8] | +1.6% | 272 → 272 | 3 → 3 | overlap |
| JSONFieldShapes/EmptyArray | 158.6 [155.5–160.8] | 159.2 [156.9–161.9] | +0.4% | 288 → 288 | 3 → 3 | overlap |
| JSONFieldShapes/Array1 | 212.2 [210.0–213.3] | 215.1 [212.2–220.1] | +1.4% | 304 → 304 | 4 → 4 | overlap |
| JSONFieldShapes/Array4 | 348.5 [344.9–353.9] | 351.0 [346.2–359.2] | +0.7% | 344 → 344 | 7 → 7 | overlap |
| JSONFieldShapes/Array8 | 711.5 [706.2–716.9] | 733.9 [712.1–759.7] | +3.1% | 432 → 432 | 19 → 19 | overlap |
| JSONFieldShapes/Scalars | 692.0 [686.8–697.1] | 361.1 [355.9–366.0] | -47.8% | 408 → 328 | 10 → 3 | improvement |
| JSONFieldShapes/Rows64 | 15,531.5 [15,376.0–15,807.0] | 15,270.0 [15,149.0–15,376.0] | -1.7% | 11,632 → 11,065 | 71 → 4 | overlap |
| JSONFieldShapes/Array64 | 18,038.5 [17,841.0–18,388.0] | 17,718.0 [17,501.0–18,080.0] | -1.8% | 12,106 → 12,103 | 132 → 132 | overlap |
| JSONFieldShapes/TypedLeaf | 505.1 [498.2–514.7] | 422.5 [412.3–434.0] | -16.3% | 424 → 360 | 7 → 4 | improvement |
| JSONFieldShapes/RawLeaf | 551.1 [546.6–574.8] | 447.2 [430.6–459.2] | -18.8% | 408 → 352 | 7 → 4 | improvement |
| JSONFieldShapes/Deep | 6,188.5 [6,154.0–6,599.0] | 6,191.0 [6,135.0–6,246.0] | +0.0% | 3,890 → 3,889 | 15 → 15 | overlap |
| JSONFieldShapes/TopStruct | 241.8 [238.6–245.0] | 254.2 [252.3–258.5] | +5.1% | 336 → 336 | 3 → 3 | regression |
| JSONFieldShapes/TopString | 172.7 [170.7–176.4] | 168.7 [165.3–171.1] | -2.3% | 296 → 296 | 3 → 3 | overlap |
| RowIO/Scalar/Get | 218.1 [212.1–263.0] | 220.6 [216.1–263.6] | +1.1% | 421 → 421 | 7 → 7 | overlap |
| RowIO/Scalar/Insert | 6,586.0 [6,443.0–6,905.0] | 6,538.5 [6,422.0–6,680.0] | -0.7% | 1,782 → 1,778 | 23 → 23 | overlap |
| RowIO/Scalar/Update | 3,582.5 [3,429.0–3,780.0] | 3,508.0 [3,472.0–3,708.0] | -2.1% | 1,953 → 1,953 | 29 → 29 | overlap |
| RowIO/Arrays/Get | 2,111.0 [2,011.0–2,623.0] | 2,040.5 [1,995.0–2,536.0] | -3.3% | 4,637 → 4,637 | 137 → 137 | overlap |
| RowIO/Arrays/Insert | 6,475.0 [6,367.0–6,845.0] | 6,617.5 [6,356.0–7,031.0] | +2.2% | 7,792 → 7,788 | 26 → 26 | overlap |
| RowIO/Arrays/Update | 8,266.0 [7,830.0–9,997.0] | 8,265.0 [7,927.0–11,219.0] | -0.0% | 12,889 → 12,889 | 162 → 162 | overlap |
| ArrayRowCodec/Encode | 1,021.0 [988.2–1,043.0] | 1,037.0 [985.4–1,073.0] | +1.6% | 3,072 → 3,072 | 4 → 4 | overlap |
| ArrayRowCodec/Decode | 1,876.0 [1,842.0–1,901.0] | 1,877.0 [1,868.0–1,918.0] | +0.1% | 3,581 → 3,581 | 134 → 134 | overlap |
| HTTPJSONRoundTripSmall | 3,615.5 [3,527.0–3,652.0] | 3,587.0 [3,547.0–3,702.0] | -0.8% | 6,726 → 6,726 | 33 → 33 | overlap |
| HTTPJSONRoundTripMedium | 89,470.0 [88,609.0–89,888.0] | 90,029.0 [89,070.0–91,797.0] | +0.6% | 64,869 → 64,860 | 1835 → 1835 | overlap |
| SSEChangeBatchMarshal | 110,797.5 [109,999.0–112,854.0] | 112,615.0 [110,729.0–114,587.0] | +1.6% | 76,010 → 76,009 | 1170 → 1170 | overlap |
| SSEChangeBatchMarshalOnly | 90,048.5 [89,560.0–90,750.0] | 91,087.5 [90,040.0–93,093.0] | +1.2% | 31,794 → 31,794 | 1152 → 1152 | overlap |
| SSEChangeBatchWriteOnly | 15,045.0 [14,128.0–15,740.0] | 15,114.0 [14,764.0–16,654.0] | +0.5% | 44,120 → 44,120 | 18 → 18 | overlap |
| SSEEngineChangeEventMarshalOnly | 182,338.5 [180,622.0–184,197.0] | 184,101.0 [182,669.0–185,954.0] | +1.0% | 82,056 → 82,058 | 2048 → 2048 | overlap |
| SSEEngineChangeEventMarshal | 220,322.5 [216,404.0–224,320.0] | 221,748.0 [219,057.0–223,543.0] | +0.6% | 181,012 → 181,014 | 2066 → 2066 | overlap |
| ArchiveRecovery/archive/100 | 945,470.0 [892,818.0–1,051,408.0] | 982,798.0 [845,344.0–1,087,051.0] | +3.9% | 1,442,654 → 1,444,578 | 1036.5 → 1052.5 | overlap |
| ArchiveRecovery/archive/10000 | 7,205,863.5 [6,950,863.0–7,413,879.0] | 7,037,924.0 [6,715,480.0–7,251,371.0] | -2.3% | 7,382,612 → 7,383,300 | 50715 → 50715 | overlap |
| ArchiveRecovery/restore/100 | 966,445.0 [851,942.0–1,085,597.0] | 954,114.0 [836,480.0–1,139,647.0] | -1.3% | 1,446,452 → 1,445,642 | 1076.5 → 1069 | overlap |
| ArchiveRecovery/restore/10000 | 7,118,468.5 [6,716,216.0–7,444,639.0] | 6,941,335.0 [6,683,953.0–7,174,616.0] | -2.5% | 7,381,958 → 7,384,960 | 50743 → 50745.5 | overlap |
| TypedJSONConversion/Read | 2,200.5 [2,157.0–2,235.0] | 1,817.5 [1,804.0–1,823.0] | -17.4% | 884 → 771 | 20 → 12 | improvement |
| TypedJSONConversion/Write | 2,395.5 [2,356.0–2,435.0] | 2,450.0 [2,434.0–2,472.0] | +2.3% | 1,398 → 1,398 | 31 → 31 | overlap |
| JSONRowCodec/Encode | 780.5 [776.6–787.9] | 392.4 [389.8–397.0] | -49.7% | 463 → 344 | 11 → 3 | improvement |
| JSONRowCodec/Decode | 816.4 [808.3–826.0] | 811.8 [802.4–821.8] | -0.6% | 954 → 950 | 20 → 20 | overlap |

## Original v1 → optimized v2

Five freshly collected original-Go samples compared with the ten candidate samples above. This comparison includes the toolchain change; it is not an isolated codec experiment.

| Benchmark | Before ns/op [min–max] | After ns/op [min–max] | Time change | B/op before → after | allocs/op before → after | Range signal |
|---|---:|---:|---:|---:|---:|---|
| TypedJSONConversion/Read | 3,045.0 [3,025.0–3,054.0] | 1,817.5 [1,804.0–1,823.0] | -40.3% | 1,779 → 771 | 48 → 12 | improvement |
| TypedJSONConversion/Write | 2,624.0 [2,610.0–2,641.0] | 2,450.0 [2,434.0–2,472.0] | -6.6% | 1,827 → 1,398 | 53 → 31 | improvement |
| JSONRowCodec/Encode | 562.2 [559.7–566.5] | 392.4 [389.8–397.0] | -30.2% | 625 → 344 | 11 → 3 | improvement |
| JSONRowCodec/Decode | 1,006.0 [1,001.0–1,015.0] | 811.8 [802.4–821.8] | -19.3% | 1,144 → 950 | 28 → 20 | improvement |

## Fresh database workload

Five fresh processes/databases per side; 10,000 rows, 5,000 indexed lookups, 1,000 searches, batch 1,000, search limit 8, seed 42, full durability, warm timeout 45s. These scalar/index workloads do not isolate JSON encoding. Small timings and overlapping ranges do not establish an engine-wide gain.

| Metric | Unit | Before median [min–max] | After median [min–max] |
|---|---|---:|---:|
| async_warmup_ms | ms | 0.000 [0.000–0.000] | 0.000 [0.000–0.000] |
| lookup_async_avg_us | us/op | 0.707 [0.533–0.767] | 0.611 [0.413–0.667] |
| lookup_async_first_us | us | 1.000 [1.000–1.000] | 1.000 [1.000–1.000] |
| lookup_async_total_ms | ms | 3.000 [2.000–3.000] | 3.000 [2.000–3.000] |
| lookup_sync_avg_us | us/op | 0.609 [0.511–0.787] | 0.553 [0.423–0.701] |
| lookup_sync_over_async_ratio | x | 0.861 [0.775–1.475] | 1.026 [0.693–1.273] |
| lookup_sync_total_ms | ms | 3.000 [2.000–3.000] | 2.000 [2.000–3.000] |
| open_async_ms | ms | 8.000 [8.000–9.000] | 8.000 [7.000–9.000] |
| open_sync_ms | ms | 8.000 [8.000–9.000] | 8.000 [8.000–9.000] |
| open_sync_over_async_ratio | x | 1.003 [0.951–1.086] | 1.089 [0.872–1.123] |
| search_async_avg_us | us/op | 341.606 [340.517–345.108] | 341.372 [340.318–342.596] |
| search_async_first_hits | rows | 8.000 [8.000–8.000] | 8.000 [8.000–8.000] |
| search_async_first_us | us | 621.000 [601.000–646.000] | 606.000 [596.000–621.000] |
| search_async_total_hits | rows | 8,000.000 [8,000.000–8,000.000] | 8,000.000 [8,000.000–8,000.000] |
| search_async_total_ms | ms | 341.000 [340.000–345.000] | 341.000 [340.000–342.000] |
| search_sync_avg_us | us/op | 343.331 [340.377–345.948] | 344.727 [340.290–345.497] |
| search_sync_over_async_ratio | x | 1.003 [0.991–1.016] | 1.010 [0.999–1.013] |
| search_sync_total_hits | rows | 8,000.000 [8,000.000–8,000.000] | 8,000.000 [8,000.000–8,000.000] |
| search_sync_total_ms | ms | 343.000 [340.000–345.000] | 344.000 [340.000–345.000] |
| seed_insert_ms | ms | 60.000 [58.000–63.000] | 59.000 [57.000–61.000] |
| seed_insert_rows_per_sec | rows/s | 166,305.894 [157,760.408–171,661.434] | 168,646.324 [163,199.445–174,032.412] |
| seed_open_ms | ms | 0.000 [0.000–0.000] | 0.000 [0.000–0.000] |

## Reproduction and qualification

Raw outputs and source snapshot are in [results/json-v2-optimization](results/json-v2-optimization). Copy `internal/storage/row_json_shapes_bench_test.go` into the initial-v2 baseline checkout; all other fixtures already exist there. For the original v1 checkout use the compatibility fixtures in [the original report](json-v2.md). Run each command in both checkouts, then repeat in reverse order, with no competing workloads:

```sh
GOTOOLCHAIN=go1.27.3 go test . ./internal/storage -run '^$' -bench 'BenchmarkTypedJSONConversion|BenchmarkJSONRowCodec' -benchmem -benchtime=300ms -count=5
GOTOOLCHAIN=go1.27.3 go test ./internal/storage -run '^$' -bench '^BenchmarkJSONFieldShapes' -benchmem -benchtime=200ms -count=5
GOTOOLCHAIN=go1.27.3 go test ./internal/server -run '^$' -bench 'BenchmarkHTTPJSON|BenchmarkSSE' -benchmem -benchtime=300ms -count=5
GOTOOLCHAIN=go1.27.3 go test ./internal/engine ./internal/storage -run '^$' -bench 'BenchmarkRowIO|BenchmarkArrayRowCodec' -benchmem -benchtime=200ms -count=5
GOTOOLCHAIN=go1.27.3 go test ./internal/engine -run '^$' -bench '^BenchmarkArchiveRecovery$' -benchmem -benchtime=5x -count=5
GOTOOLCHAIN=go1.27.3 make quickbench-series QB_SERIES_DIR=NEW_DIRECTORY QB_ROWS=10000 QB_LOOKUPS=5000 QB_SEARCHES=1000 QB_BATCH=1000 QB_SYNC_MODE=full
```

Use Go 1.26.10 for the original comparison. Collection settings are also recorded in `collection.py.txt` (its scratch-directory paths must be adapted to your checkouts). These are local measurements, not production benchmarks of dependency users.

`make release-check` passes on the retained version with Go 1.27.3: formatting, vet, full tests, race tests, reachable-vulnerability checks, and the three-sample durability/crash-recovery gate. See [qualification output](results/json-v2-optimization/qualification.txt). The [database comparator](results/json-v2-optimization/quick-comparison.txt) accepted matching configurations and result counts; its timing signals are inconclusive. The initial migration’s seven embedding-module checks remain recorded in the original report. This optimization changes no module requirements or public APIs.
