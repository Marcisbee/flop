# Flop JSON v2 measurements

Baseline: `df1fa8af1a47910f9f490e150c868dc446301a3a`. Candidate: the JSON v2 migration accompanying this report. No benchmark is a claim about Strike, Kodē, or Baby production latency.

Three configurations: original Flop on Go 1.26.10 (`legacy`); unchanged codec on Go 1.27.3 (`baseline`); migrated v2 on Go 1.27.3 (`final`). The original code was measured in a separate checkout. Linux/amd64, AMD Ryzen 9 9950X, GOMAXPROCS=32. Default build tags; Sonic was not enabled.

Server/SSE and typed/JSON-row microbenchmarks have ten samples per configuration, five before and five in reverse configuration order. Other microbenchmarks have five samples. Values below are medians; ranges are observed min/max, not confidence intervals. Overlapping ranges are not a demonstrated gain or regression. No outliers were discarded.

HTTP measurements exercise request decoding and response serialization with httptest; they exclude database and network latency. SSE measurements encode/write batches of 128 events, not live subscriber delivery. Typed conversion benchmarks call the actual conversion helpers, excluding database IO. Row codecs exercise actual binary serialization, including embedded JSON. Scalar/array RowIO uses full durability. Archive recovery excludes fixture creation/copying and verifies recovered records/counts.

## Total change: current Go 1.26 / v1 → Go 1.27 / v2

Negative time change means less elapsed time. B/op means allocated bytes per operation, not peak resident memory.

| Benchmark | Before ns/op [min–max] | After ns/op [min–max] | Time change | B/op before → after | allocs/op before → after | Range signal |
|---|---:|---:|---:|---:|---:|---|
| HTTPJSONRoundTripSmall | 4,091.5 [4,013.0–4,183.0] | 3,634.5 [3,598.0–3,730.0] | -11.2% | 7,718 → 6,726 | 45 → 33 | improvement |
| HTTPJSONRoundTripMedium | 111,802.5 [110,124.0–115,367.0] | 90,922.5 [89,820.0–92,372.0] | -18.7% | 112,115 → 64,880 | 2356 → 1835 | improvement |
| SSEChangeBatchMarshal | 135,100.0 [131,526.0–145,967.0] | 114,735.5 [110,915.0–117,425.0] | -15.1% | 136,530 → 76,011 | 2450 → 1170 | improvement |
| SSEChangeBatchMarshalOnly | 115,325.0 [112,295.0–123,948.0] | 93,519.0 [90,753.0–97,128.0] | -18.9% | 92,307 → 31,794 | 2432 → 1152 | improvement |
| SSEChangeBatchWriteOnly | 15,274.0 [13,794.0–16,780.0] | 14,915.5 [13,780.0–15,460.0] | -2.3% | 44,120 → 44,120 | 18 → 18 | overlap |
| SSEEngineChangeEventMarshalOnly | 197,793.5 [194,351.0–201,548.0] | 189,461.5 [181,663.0–195,456.0] | -4.2% | 155,904 → 82,057 | 3328 → 2048 | overlap |
| SSEEngineChangeEventMarshal | 237,092.5 [233,936.0–239,437.0] | 226,696.5 [219,515.0–233,791.0] | -4.4% | 254,852 → 181,012 | 3346 → 2066 | improvement |
| ArchiveRecovery/archive/100 | 1,187,792.0 [974,581.0–1,231,967.0] | 1,024,314.0 [860,028.0–1,081,537.0] | -13.8% | 1,463,790 → 1,443,281 | 1514 → 1055 | overlap |
| ArchiveRecovery/archive/10000 | 13,349,770.0 [12,839,597.0–13,412,297.0] | 7,053,388.0 [6,908,311.0–7,086,292.0] | -47.2% | 9,376,756 → 7,382,140 | 90735 → 50713 | improvement |
| ArchiveRecovery/restore/100 | 1,155,359.0 [874,130.0–1,163,972.0] | 941,929.0 [852,820.0–1,060,387.0] | -18.5% | 1,466,129 → 1,445,395 | 1538 → 1069 | overlap |
| ArchiveRecovery/restore/10000 | 12,824,221.0 [12,490,246.0–13,051,019.0] | 7,085,552.0 [6,809,227.0–7,277,626.0] | -44.7% | 9,377,708 → 7,382,075 | 90761 → 50741 | improvement |
| RowIO/Scalar/Get | 230.2 [228.8–279.2] | 214.1 [210.7–265.3] | -7.0% | 421 → 421 | 7 → 7 | overlap |
| RowIO/Scalar/Insert | 6,524.0 [6,446.0–6,695.0] | 6,541.0 [6,512.0–6,714.0] | +0.3% | 1,781 → 1,782 | 23 → 23 | overlap |
| RowIO/Scalar/Update | 3,548.0 [3,534.0–3,748.0] | 3,599.0 [3,533.0–3,788.0] | +1.4% | 1,953 → 1,953 | 29 → 29 | overlap |
| RowIO/Arrays/Get | 2,282.0 [2,222.0–2,774.0] | 2,125.0 [2,061.0–2,672.0] | -6.9% | 4,637 → 4,637 | 137 → 137 | overlap |
| RowIO/Arrays/Insert | 6,602.0 [6,454.0–6,701.0] | 6,514.0 [6,342.0–6,747.0] | -1.3% | 7,789 → 7,786 | 26 → 26 | overlap |
| RowIO/Arrays/Update | 8,458.0 [8,339.0–10,064.0] | 9,203.0 [8,782.0–10,006.0] | +8.8% | 12,889 → 12,889 | 162 → 162 | overlap |
| ArrayRowCodec/Encode | 1,057.0 [1,048.0–1,105.0] | 970.4 [969.2–977.6] | -8.2% | 3,072 → 3,072 | 4 → 4 | improvement |
| ArrayRowCodec/Decode | 2,096.0 [2,062.0–2,116.0] | 1,864.0 [1,850.0–1,893.0] | -11.1% | 3,581 → 3,581 | 134 → 134 | improvement |
| TypedJSONConversion/Read | 3,091.5 [3,034.0–3,150.0] | 2,225.0 [2,201.0–2,272.0] | -28.0% | 1,779 → 884 | 48 → 20 | improvement |
| TypedJSONConversion/Write | 2,697.5 [2,611.0–2,779.0] | 2,426.0 [2,408.0–2,450.0] | -10.1% | 1,827 → 1,398 | 53 → 31 | improvement |
| JSONRowCodec/Encode | 570.8 [560.4–589.9] | 787.2 [778.1–792.8] | +37.9% | 625 → 463 | 11 → 11 | regression |
| JSONRowCodec/Decode | 1,007.5 [997.8–1,023.0] | 827.0 [808.3–833.2] | -17.9% | 1,144 → 950 | 28 → 20 | improvement |

## Codec change only: Go 1.27 / v1 semantics → Go 1.27 / v2

Negative time change means less elapsed time. B/op means allocated bytes per operation, not peak resident memory.

| Benchmark | Before ns/op [min–max] | After ns/op [min–max] | Time change | B/op before → after | allocs/op before → after | Range signal |
|---|---:|---:|---:|---:|---:|---|
| TypedJSONConversion/Read | 2,673.0 [2,595.0–2,703.0] | 2,225.0 [2,201.0–2,272.0] | -16.8% | 1,007 → 884 | 30 → 20 | improvement |
| TypedJSONConversion/Write | 3,200.5 [3,153.0–3,266.0] | 2,426.0 [2,408.0–2,450.0] | -24.2% | 1,610 → 1,398 | 47 → 31 | improvement |
| JSONRowCodec/Encode | 874.5 [865.0–884.3] | 787.2 [778.1–792.8] | -10.0% | 529 → 463 | 15 → 11 | improvement |
| JSONRowCodec/Decode | 1,485.5 [1,472.0–1,503.0] | 827.0 [808.3–833.2] | -44.3% | 1,058 → 950 | 27 → 20 | improvement |
| HTTPJSONRoundTripSmall | 4,137.5 [4,050.0–4,217.0] | 3,634.5 [3,598.0–3,730.0] | -12.2% | 7,269 → 6,726 | 42 → 33 | improvement |
| HTTPJSONRoundTripMedium | 165,150.5 [164,143.0–168,560.0] | 90,922.5 [89,820.0–92,372.0] | -44.9% | 91,152 → 64,880 | 2684 → 1835 | improvement |
| SSEChangeBatchMarshal | 140,173.0 [139,004.0–140,556.0] | 114,735.5 [110,915.0–117,425.0] | -18.1% | 84,353 → 76,011 | 1682 → 1170 | improvement |
| SSEChangeBatchMarshalOnly | 119,011.0 [118,279.0–120,396.0] | 93,519.0 [90,753.0–97,128.0] | -21.4% | 40,067 → 31,794 | 1664 → 1152 | improvement |
| SSEChangeBatchWriteOnly | 15,015.0 [14,285.0–16,586.0] | 14,915.5 [13,780.0–15,460.0] | -0.7% | 44,120 → 44,120 | 18 → 18 | overlap |
| SSEEngineChangeEventMarshalOnly | 230,313.5 [226,609.0–236,633.0] | 189,461.5 [181,663.0–195,456.0] | -17.7% | 98,642 → 82,057 | 3072 → 2048 | improvement |
| SSEEngineChangeEventMarshal | 268,463.5 [264,042.0–274,157.0] | 226,696.5 [219,515.0–233,791.0] | -15.6% | 197,759 → 181,012 | 3091 → 2066 | improvement |
| RowIO/Scalar/Get | 214.2 [213.5–264.1] | 214.1 [210.7–265.3] | -0.0% | 421 → 421 | 7 → 7 | overlap |
| RowIO/Scalar/Insert | 6,544.0 [6,429.0–6,668.0] | 6,541.0 [6,512.0–6,714.0] | -0.0% | 1,782 → 1,782 | 23 → 23 | overlap |
| RowIO/Scalar/Update | 3,484.0 [3,451.0–3,924.0] | 3,599.0 [3,533.0–3,788.0] | +3.3% | 1,953 → 1,953 | 29 → 29 | overlap |
| RowIO/Arrays/Get | 2,085.0 [2,050.0–2,628.0] | 2,125.0 [2,061.0–2,672.0] | +1.9% | 4,637 → 4,637 | 137 → 137 | overlap |
| RowIO/Arrays/Insert | 6,451.0 [6,336.0–6,752.0] | 6,514.0 [6,342.0–6,747.0] | +1.0% | 7,790 → 7,786 | 26 → 26 | overlap |
| RowIO/Arrays/Update | 8,041.0 [7,918.0–11,209.0] | 9,203.0 [8,782.0–10,006.0] | +14.5% | 12,890 → 12,889 | 162 → 162 | overlap |
| ArrayRowCodec/Encode | 1,016.0 [1,013.0–1,026.0] | 970.4 [969.2–977.6] | -4.5% | 3,072 → 3,072 | 4 → 4 | improvement |
| ArrayRowCodec/Decode | 1,864.0 [1,852.0–1,898.0] | 1,864.0 [1,850.0–1,893.0] | +0.0% | 3,581 → 3,581 | 134 → 134 | overlap |
| ArchiveRecovery/archive/100 | 1,004,746.0 [969,667.0–1,042,680.0] | 1,024,314.0 [860,028.0–1,081,537.0] | +1.9% | 1,444,689 → 1,443,281 | 1022 → 1055 | overlap |
| ArchiveRecovery/archive/10000 | 8,058,916.0 [7,787,253.0–8,370,583.0] | 7,053,388.0 [6,908,311.0–7,086,292.0] | -12.5% | 7,240,011 → 7,382,140 | 50710 → 50713 | improvement |
| ArchiveRecovery/restore/100 | 975,322.0 [861,604.0–1,085,008.0] | 941,929.0 [852,820.0–1,060,387.0] | -3.4% | 1,448,060 → 1,445,395 | 1075 → 1069 | overlap |
| ArchiveRecovery/restore/10000 | 7,876,931.0 [7,825,133.0–8,086,006.0] | 7,085,552.0 [6,809,227.0–7,277,626.0] | -10.0% | 7,241,734 → 7,382,075 | 50740 → 50741 | improvement |

## Database workload: all reported metrics

Five fresh processes/databases per configuration, 10,000 rows, 5,000 indexed lookups, 1,000 full-text searches, batch 1,000, search limit 8, seed 42, sync mode full, 45s warm timeout. All returned hit counts agree. Small millisecond totals and tiny differences are not evidence of meaningful engine speedups. This workload is primarily scalar/index work rather than JSON processing.

| Metric | Unit | Go 1.26/v1 median [min–max] | Go 1.27/v1 median [min–max] | Go 1.27/v2 median [min–max] |
|---|---|---:|---:|---:|
| async_warmup_ms | ms | 0.000 [0.000–0.000] | 0.000 [0.000–0.000] | 0.000 [0.000–0.000] |
| lookup_async_avg_us | us/op | 0.701 [0.548–0.713] | 0.619 [0.514–0.713] | 0.629 [0.575–0.738] |
| lookup_async_first_us | us | 1.000 [1.000–1.000] | 1.000 [1.000–1.000] | 1.000 [1.000–2.000] |
| lookup_async_total_ms | ms | 3.000 [2.000–3.000] | 3.000 [2.000–3.000] | 3.000 [2.000–3.000] |
| lookup_sync_avg_us | us/op | 0.513 [0.451–0.699] | 0.598 [0.497–0.717] | 0.576 [0.491–0.821] |
| lookup_sync_over_async_ratio | x | 0.790 [0.662–1.263] | 0.966 [0.764–1.302] | 0.938 [0.699–1.428] |
| lookup_sync_total_ms | ms | 2.000 [2.000–3.000] | 2.000 [2.000–3.000] | 2.000 [2.000–4.000] |
| open_async_ms | ms | 9.000 [8.000–9.000] | 8.000 [7.000–8.000] | 8.000 [8.000–9.000] |
| open_sync_ms | ms | 8.000 [8.000–8.000] | 8.000 [7.000–9.000] | 8.000 [7.000–9.000] |
| open_sync_over_async_ratio | x | 0.961 [0.884–1.019] | 1.033 [1.002–1.243] | 0.962 [0.857–1.092] |
| search_async_avg_us | us/op | 342.684 [340.901–343.493] | 342.716 [340.443–345.271] | 341.310 [339.411–342.112] |
| search_async_first_hits | rows | 8.000 [8.000–8.000] | 8.000 [8.000–8.000] | 8.000 [8.000–8.000] |
| search_async_first_us | us | 606.000 [597.000–614.000] | 612.000 [589.000–626.000] | 614.000 [595.000–804.000] |
| search_async_total_hits | rows | 8,000.000 [8,000.000–8,000.000] | 8,000.000 [8,000.000–8,000.000] | 8,000.000 [8,000.000–8,000.000] |
| search_async_total_ms | ms | 342.000 [340.000–343.000] | 342.000 [340.000–345.000] | 341.000 [339.000–342.000] |
| search_sync_avg_us | us/op | 341.598 [338.593–342.870] | 343.060 [342.239–345.413] | 341.696 [339.308–342.257] |
| search_sync_over_async_ratio | x | 0.994 [0.986–1.006] | 1.001 [0.991–1.011] | 1.001 [0.997–1.002] |
| search_sync_total_hits | rows | 8,000.000 [8,000.000–8,000.000] | 8,000.000 [8,000.000–8,000.000] | 8,000.000 [8,000.000–8,000.000] |
| search_sync_total_ms | ms | 341.000 [338.000–342.000] | 343.000 [342.000–345.000] | 341.000 [339.000–342.000] |
| seed_insert_ms | ms | 60.000 [58.000–61.000] | 58.000 [56.000–61.000] | 58.000 [57.000–59.000] |
| seed_insert_rows_per_sec | rows/s | 165,691.717 [163,038.926–169,907.387] | 170,420.790 [161,422.119–175,532.811] | 169,772.277 [168,972.516–172,641.513] |
| seed_open_ms | ms | 0.000 [0.000–0.000] | 0.000 [0.000–0.000] | 0.000 [0.000–0.000] |

## Reproduction

Use separate baseline and candidate checkouts. Add the two baseline benchmark fixtures from `results/json-v2/baseline-*_test.go.txt` to their original paths. Existing SSE benchmarks on the candidate call the same migrated codec as production; baseline calls the original codec. Fixture payloads and iteration settings are unchanged. Repeat in reverse configuration order. Run no competing tests/builds during measurement.

```sh
GOTOOLCHAIN=go1.27.3 go test ./internal/server -run '^$' -bench 'BenchmarkHTTPJSON|BenchmarkSSE' -benchmem -benchtime=300ms -count=5
GOTOOLCHAIN=go1.27.3 go test . ./internal/storage -run '^$' -bench 'BenchmarkTypedJSONConversion|BenchmarkJSONRowCodec' -benchmem -benchtime=300ms -count=5
GOTOOLCHAIN=go1.27.3 go test ./internal/engine ./internal/storage -run '^$' -bench 'BenchmarkRowIO|BenchmarkArrayRowCodec' -benchmem -benchtime=200ms -count=5
GOTOOLCHAIN=go1.27.3 go test ./internal/engine -run '^$' -bench '^BenchmarkArchiveRecovery$' -benchmem -benchtime=5x -count=5
GOTOOLCHAIN=go1.27.3 make quickbench-series QB_SERIES_DIR=NEW_DIRECTORY QB_ROWS=10000 QB_LOOKUPS=5000 QB_SEARCHES=1000 QB_BATCH=1000 QB_SYNC_MODE=full
```

Use Go 1.26.10 for the legacy configuration. Raw samples and all quickbench reports are in [results/json-v2](results/json-v2). These are exploratory measurements, not production consumer benchmarks.

## Qualification and compatibility

`make release-check` passes on Go 1.27.3: formatting, vet, the full test suite,
race detection, reachable-vulnerability checking, and the three-sample
production gate including crash recovery. All seven nested modules embedding
Flop pass `go test ./...`. Raw results are in
[qualification.txt](results/json-v2/qualification.txt) and
[embedding-module-tests.txt](results/json-v2/embedding-module-tests.txt).

The embedded JSON write regression fails when serialization errors are deliberately
discarded and passes with the migration's error propagation. No invalid insert or
update is persisted by that regression, including after closing and reopening.

This migration intentionally changes consumer JSON semantics and raises the minimum
Go version to 1.27.3. See the README migration notes. Consumer applications outside
this repository have not been qualified. The JSON-row encoding regression is real
in these samples; benchmark improvements elsewhere do not make this a universally
faster database or establish production readiness for every consumer.
