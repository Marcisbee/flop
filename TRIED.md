# Performance Optimization Attempts

Baseline: **~237 tx/s** (initial Go port)
Current: **~2,550 tx/s** (after all optimizations below)
Reference: **~4,900 tx/s** (TS/Deno implementation)

Benchmark: finance — 50K random transfers, 50 concurrent connections, 1K users, 3K accounts.

---

## Worked

### VM pool (237 → ~800 tx/s)
Single QuickJS VM was a bottleneck — all requests serialized through one mutex. Created a pool of VMs (`NumCPU * N`), each with its own Bridge. Requests grab a VM from the channel, execute, and return it.

### Bytecode pre-compilation (~800 → ~1,200 tx/s)
Compiling the handler call script from source on every request was expensive. Pre-compile to bytecode once at startup, then `EvalBytecode()` on each call. Avoids repeated parsing.

### Transaction batching / group commit (~1,200 → ~1,500 tx/s)
Individual fsync per write was killing throughput. Buffer WAL records in memory during a transaction, then batch-commit with a single fsync. The `EnqueueCommit` path groups multiple transactions into one disk write.

### Parallel fsync + ID generation (~1,500 → ~1,800 tx/s)
- Switched from UUID to a fast random ID generator (base36, no crypto/rand per call).
- Parallelized WAL fsync operations where possible.
- syncMode passthrough from user config.

### Async stripping + sync-first bytecode (~1,800 → ~2,550 tx/s)
The biggest single win. Since all Go host functions (db.insert, db.get, db.update, etc.) are synchronous, the `async`/`await` in user handlers is unnecessary. It forces QuickJS into the expensive module evaluation path with `js_std_await` microtask pumping on every call.

**What we did:**
1. `StripAsync()` — regex post-processing of esbuild output to remove `async` and `await` keywords.
2. Compile a **global** bytecode (IIFE) for the fast sync path — no module resolution, no `js_std_await`.
3. If a handler still returns a Promise (e.g. `Promise.all` in blog's `list_posts`), detect it and fall back to a pre-compiled **module** bytecode with `js_std_await`.
4. Replace `qjs.Call("funcName", ...)` with `qjs.Eval(assignment)` — `Call()` was evaluating the function name string as JavaScript on every invocation.
5. Reuse pre-built `ctx` objects per handler type (view/reducer) instead of creating new ones per request.
6. Increased pool size from `NumCPU * 4` to `NumCPU * 8`.

---

## Did not work

### V8 migration (evaluated, rejected)
Evaluated `tommie/v8go` as a replacement for QuickJS. **Rejected** because:
- No ES module support (needed for `await` fallback path)
- No memory limit API
- No `SetEvalTimeout` equivalent
- Requires CGo with heavy C++ dependency
- Drops Windows/ARM support
- High migration cost with unclear gains — the bottleneck is JS interpretation speed regardless of engine, and V8's JIT advantage is reduced for short-lived handler calls.

### RWMutex patch on QuickJS `goFuncsMu` (~1,750 tx/s, no change)
CPU profiling showed ~31% in mutex contention (`runtime.usleep`, `sync.Mutex.Unlock`). The QuickJS Go binding has a package-level `sync.Mutex` (`goFuncsMu`) that's acquired on every Go callback from JS. With 32 VMs × ~8 Go function calls per transfer = ~14,400 lock acquisitions/s.

Created a local fork of `modernc.org/quickjs`, changed `goFuncsMu` from `sync.Mutex` to `sync.RWMutex`, changed `callGo` to use `RLock/RUnlock`. **No measurable improvement.** The contention was actually Go runtime scheduling overhead (goroutine parking/waking), not the specific lock.

### Global bytecode + `.then()` + `js_std_loop()` flush (~16,800 ops/s in micro-bench, slower than module path)
Tried avoiding module bytecode entirely by using global bytecode with `.then()` chaining and flushing the event loop with `js_std_loop()`. Turned out slower than the module path in micro-benchmarks (16,858/s vs 18,846/s). The `.then()` chaining + manual flush has its own overhead.

### Single module bytecode without async stripping (~1,770 tx/s, no change from baseline)
Simplified to a single module bytecode path with Eval-based arg setting (eliminating `qjs.Call()` overhead). Without stripping async/await, still hitting `js_std_await` on every call. The arg-passing optimization alone was negligible — the `js_std_await` dominates.

---

## Remaining bottleneck

CPU profile breakdown at ~2,550 tx/s (estimated):
- **JS interpretation** (`_JS_CallInternal`): ~40% — inherent cost of running JS in QuickJS's tree-walking interpreter.
- **JS memory allocation** (`_js_def_malloc`): ~20% — object creation during handler execution (JSON.parse, proxy access, etc.).
- **Go runtime scheduling**: ~15% — goroutine context switching with 50 concurrent connections across the VM pool.
- **Disk I/O** (WAL fsync): ~10% — irreducible for durable writes.

The gap to the TS/Deno reference (~4,900 tx/s) is primarily V8's JIT compiler vs QuickJS's interpreter. V8 compiles hot handler functions to native code; QuickJS interprets every bytecode instruction. This is a fundamental engine limitation, not something optimizable from the Go side.
