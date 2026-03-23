/**
 * Unified workload benchmark orchestrator for:
 * - flop-go
 * - sqlite-ts
 * - sqlite-go
 * - turso-ts
 * - pglite-ts
 * - turso-go
 * - mongodb-ts
 * - mongodb-go
 *
 * Scenarios are workload-only:
 * - high-load-rw
 * - reads
 * - writes
 * - edits
 */

import { fromFileUrl, resolve } from "@std/path";

type EngineID =
  | "flop-go"
  | "sqlite-ts"
  | "sqlite-go"
  | "turso-ts"
  | "pglite-ts"
  | "turso-go"
  | "mongodb-ts"
  | "mongodb-go";
type BenchmarkProfile = "smoke" | "quick" | "full";
type EngineSet = "core" | "all";
type ScenarioKind = "reads" | "writes" | "edits" | "mixed";

type Scenario = {
  name: string;
  kind: ScenarioKind;
  users: number;
  setupConcurrency: number;
  concurrency: number;
  durationSec: number;
  readShare?: number;
  accountsPerUser?: number;
};

type WorkloadBenchResult = {
  kind: "workload";
  mode: ScenarioKind;
  host: string;
  usersRequested: number;
  accountsPerUser: number;
  setupConcurrency: number;
  concurrency: number;
  durationSecRequested: number;
  startedAt: string;
  finishedAt: string;
  totalMs: number;
  setup: {
    register: {
      durationMs: number;
      attempted: number;
      success: number;
      ratePerSec: number;
    };
    createAccounts: {
      durationMs: number;
      attempted: number;
      success: number;
      ratePerSec: number;
    };
    deposit: {
      durationMs: number;
      attempted: number;
      success: number;
      ratePerSec: number;
    };
  };
  workload: {
    durationMs: number;
    attempted: number;
    success: number;
    failed: number;
    opsPerSec: number;
    readOps: number;
    writeOps: number;
    readOpsPerSec: number;
    writeOpsPerSec: number;
    transferOpsPerSec: number;
    byOp: Record<string, number>;
  };
  stats?: Record<string, unknown>;
};

type ScenarioResult = {
  engine: EngineID;
  ok: boolean;
  elapsedMs: number;
  metrics?: WorkloadBenchResult;
  memory?: MemoryProfile;
  error?: string;
  repeatCount?: number;
  successfulRepeats?: number;
  repeats?: ScenarioResult[];
};

type MemoryProfile = {
  supported: boolean;
  sampleCount: number;
  rssAvgMB?: number;
  rssPeakMB?: number;
  rssMinMB?: number;
  opsPerSecPerAvgMB?: number;
};

type RunRecord = {
  schemaVersion: 3;
  runId: string;
  createdAt: string;
  git: { sha: string; branch: string; dirty: boolean };
  host: { os: string; arch: string; deno: string };
  scenarios: Array<{
    name: string;
    config: Scenario;
    scoreLabel: "ops/s";
    results: ScenarioResult[];
  }>;
};

type HistoryFile = {
  schemaVersion: number;
  updatedAt: string;
  runs: RunRecord[];
};

let STOP_REQUESTED = false;

const COMPARE_DIR = fromFileUrl(new URL(".", import.meta.url));
const ROOT = resolve(COMPARE_DIR, "..", "..");
const RESULTS_DIR = resolve(COMPARE_DIR, "results");
const RUNS_DIR = resolve(RESULTS_DIR, "runs");
const HISTORY_PATH = resolve(RESULTS_DIR, "history.json");
const REPORT_TEMPLATE_PATH = resolve(COMPARE_DIR, "report", "index.html");
const REPORT_SNAPSHOT_PATH = resolve(RESULTS_DIR, "report.html");
const FALLBACK_REPORT_TEMPLATE = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>Benchmark Report</title>
<style>
  body{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;padding:20px;line-height:1.4}
  h1{margin:0 0 6px} p{margin:0 0 14px;color:#4b5563}
  table{border-collapse:collapse;width:100%;max-width:920px}
  th,td{border:1px solid #d1d5db;padding:8px;text-align:left}
  th{background:#f3f4f6}
  td.num{text-align:right;font-variant-numeric:tabular-nums}
</style>
</head>
<body>
  <h1>Benchmark Report (Fallback)</h1>
  <p>Template missing at <code>benchmarks/compare/report/index.html</code>. Restore it for the full UI.</p>
  <table id="tbl"></table>
  <script>
    const h = window.__BENCH_HISTORY__ || { runs: [] };
    const latest = h.runs?.[0];
    const rows = (latest?.scenarios || []).map(s => {
      const best = (s.results || []).filter(r => r.ok && r.metrics)
        .sort((a,b)=> (b.metrics.workload.opsPerSec||0) - (a.metrics.workload.opsPerSec||0))[0];
      return {
        scenario: s.name,
        engine: best?.engine || "-",
        ops: best?.metrics?.workload?.opsPerSec || 0,
      };
    });
    document.getElementById("tbl").innerHTML = \`
      <thead><tr><th>Scenario</th><th>Best Engine</th><th class="num">Ops/s</th></tr></thead>
      <tbody>\${rows.map(r => \`<tr><td>\${r.scenario}</td><td>\${r.engine}</td><td class="num">\${Math.round(r.ops)}</td></tr>\`).join("")}</tbody>
    \`;
  </script>
</body>
</html>`;

const ACTIVE_PROCS = new Set<Deno.ChildProcess>();

const FULL_SCENARIOS: Scenario[] = [
  {
    name: "high-load-rw",
    kind: "mixed",
    users: 600,
    setupConcurrency: 50,
    concurrency: 220,
    durationSec: 15,
    readShare: 0.55,
    accountsPerUser: 3,
  },
  {
    name: "reads",
    kind: "reads",
    users: 600,
    setupConcurrency: 50,
    concurrency: 220,
    durationSec: 15,
    accountsPerUser: 3,
  },
  {
    name: "writes",
    kind: "writes",
    users: 600,
    setupConcurrency: 50,
    concurrency: 160,
    durationSec: 15,
    accountsPerUser: 3,
  },
  {
    name: "edits",
    kind: "edits",
    users: 600,
    setupConcurrency: 50,
    concurrency: 160,
    durationSec: 15,
    accountsPerUser: 3,
  },
];

const QUICK_SCENARIOS: Scenario[] = [
  {
    name: "high-load-rw",
    kind: "mixed",
    users: 80,
    setupConcurrency: 40,
    concurrency: 80,
    durationSec: 4,
    readShare: 0.55,
    accountsPerUser: 3,
  },
  {
    name: "reads",
    kind: "reads",
    users: 80,
    setupConcurrency: 40,
    concurrency: 80,
    durationSec: 4,
    accountsPerUser: 3,
  },
  {
    name: "writes",
    kind: "writes",
    users: 80,
    setupConcurrency: 40,
    concurrency: 80,
    durationSec: 4,
    accountsPerUser: 3,
  },
  {
    name: "edits",
    kind: "edits",
    users: 80,
    setupConcurrency: 40,
    concurrency: 80,
    durationSec: 4,
    accountsPerUser: 3,
  },
];

const SMOKE_SCENARIOS: Scenario[] = [
  {
    name: "high-load-rw",
    kind: "mixed",
    users: 120,
    setupConcurrency: 20,
    concurrency: 60,
    durationSec: 5,
    readShare: 0.55,
    accountsPerUser: 3,
  },
  {
    name: "reads",
    kind: "reads",
    users: 120,
    setupConcurrency: 20,
    concurrency: 60,
    durationSec: 5,
    accountsPerUser: 3,
  },
  {
    name: "writes",
    kind: "writes",
    users: 120,
    setupConcurrency: 20,
    concurrency: 50,
    durationSec: 5,
    accountsPerUser: 3,
  },
  {
    name: "edits",
    kind: "edits",
    users: 120,
    setupConcurrency: 20,
    concurrency: 50,
    durationSec: 5,
    accountsPerUser: 3,
  },
];

const SCENARIOS_BY_PROFILE: Record<BenchmarkProfile, Scenario[]> = {
  smoke: SMOKE_SCENARIOS,
  quick: QUICK_SCENARIOS,
  full: FULL_SCENARIOS,
};

const SCENARIO_CATALOG: Scenario[] = [...FULL_SCENARIOS];

const ENGINE_ORDER: EngineID[] = [
  "flop-go",
  "sqlite-ts",
  "sqlite-go",
  "turso-ts",
  "pglite-ts",
  "turso-go",
  "mongodb-ts",
  "mongodb-go",
];
const ENGINE_BASE_PORT: Record<EngineID, number> = {
  "flop-go": 41086,
  "sqlite-ts": 41087,
  "sqlite-go": 41088,
  "turso-ts": 41089,
  "pglite-ts": 41090,
  "turso-go": 41091,
  "mongodb-ts": 41092,
  "mongodb-go": 41093,
};
const ENGINES_BY_SET: Record<EngineSet, EngineID[]> = {
  core: ["flop-go", "sqlite-ts", "sqlite-go"],
  all: [...ENGINE_ORDER],
};
const MEMORY_SAMPLE_INTERVAL_MS = 250;

function arg(name: string, fallback = ""): string {
  const prefix = `--${name}=`;
  const found = Deno.args.find((a) => a.startsWith(prefix));
  return found ? found.slice(prefix.length) : fallback;
}

function toMean(values: number[]): number {
  if (!values.length) return 0;
  return values.reduce((a, b) => a + b, 0) / values.length;
}

function relativeStdErr(values: number[]): number | null {
  if (values.length < 2) return null;
  const mean = toMean(values);
  if (!Number.isFinite(mean) || mean <= 0) return null;
  let sumSq = 0;
  for (const value of values) {
    const diff = value - mean;
    sumSq += diff * diff;
  }
  const variance = sumSq / (values.length - 1);
  const stddev = Math.sqrt(Math.max(0, variance));
  return stddev / mean / Math.sqrt(values.length);
}

function isFiniteNumber(v: unknown): v is number {
  return typeof v === "number" && Number.isFinite(v);
}

function shuffleWithRng<T>(arr: T[], next: () => number): T[] {
  const out = [...arr];
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(next() * (i + 1));
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

function seededRng(seed: number): () => number {
  let t = seed >>> 0;
  return () => {
    t += 0x6D2B79F5;
    let x = Math.imul(t ^ (t >>> 15), 1 | t);
    x ^= x + Math.imul(x ^ (x >>> 7), 61 | x);
    return ((x ^ (x >>> 14)) >>> 0) / 4294967296;
  };
}

function parseRepeats(profile: BenchmarkProfile): number {
  const raw = Number(arg("repeats", "0"));
  if (raw >= 1) return Math.floor(raw);
  if (profile === "full") return 3;
  return 1;
}

function parseMinRepeats(
  profile: BenchmarkProfile,
  repeatsMax: number,
): number {
  const raw = Number(arg("min-repeats", "0"));
  if (raw >= 1) return Math.min(Math.floor(raw), repeatsMax);
  if (profile === "full" && repeatsMax >= 2) return 2;
  return Math.min(1, repeatsMax);
}

function parseEarlyStopRse(profile: BenchmarkProfile): number {
  const raw = Number(arg("early-stop-rse", "-1"));
  if (Number.isFinite(raw) && raw >= 0) return raw;
  if (profile === "full") return 0.05;
  return 0;
}

function parseWarmupSec(profile: BenchmarkProfile): number {
  const raw = Number(arg("warmup-sec", "-1"));
  if (raw >= 0) return raw;
  if (profile === "full") return 3;
  return 0;
}

function parseWorkloadTimeoutSec(profile: BenchmarkProfile): number {
  const raw = Number(arg("workload-timeout-sec", "0"));
  if (raw > 0 && Number.isFinite(raw)) return Math.floor(raw);
  if (profile === "full") return 600;
  if (profile === "quick") return 240;
  return 120;
}

function parseRequestTimeoutMs(profile: BenchmarkProfile): number {
  const raw = Number(arg("request-timeout-ms", "0"));
  if (raw > 0 && Number.isFinite(raw)) return Math.floor(raw);
  if (profile === "full") return 15000;
  if (profile === "quick") return 12000;
  return 8000;
}

function parseShuffleEngines(): boolean {
  const raw = arg("shuffle-engines", "1").trim().toLowerCase();
  return !(raw === "0" || raw === "false" || raw === "no");
}

function parseStrictSetup(): boolean {
  const raw = arg("strict-setup", "1").trim().toLowerCase();
  return !(raw === "0" || raw === "false" || raw === "no");
}

function parseSetupRetries(profile: BenchmarkProfile): number {
  const rawArg = arg("setup-retries", "");
  if (rawArg.trim() !== "") {
    const raw = Number(rawArg);
    if (!Number.isFinite(raw)) return 4;
    return Math.max(1, Math.floor(raw));
  }
  if (profile === "full") return 8;
  if (profile === "quick") return 6;
  return 4;
}

function parseSeed(): number {
  const raw = Number(arg("seed", "0"));
  if (Number.isFinite(raw) && raw > 0) return Math.floor(raw);
  const now = Date.now() % 2147483647;
  return now > 0 ? now : 1;
}

function parseProfile(): BenchmarkProfile {
  const raw = arg("profile", "quick").trim().toLowerCase();
  if (raw === "smoke" || raw === "full" || raw === "quick") return raw;
  return "quick";
}

function parseEngineSet(): EngineSet {
  const raw = arg("engine-set", "all").trim().toLowerCase();
  if (raw === "all" || raw === "core") return raw;
  return "all";
}

function parseEngineList(engineSet: EngineSet): EngineID[] {
  const raw = arg("engines", ENGINES_BY_SET[engineSet].join(","));
  const values = raw.split(",").map((x) => x.trim()).filter(
    Boolean,
  ) as EngineID[];
  const out: EngineID[] = [];
  for (const id of values) {
    if (ENGINE_ORDER.includes(id) && !out.includes(id)) out.push(id);
  }
  return out.length ? out : [...ENGINES_BY_SET[engineSet]];
}

function parseScenarioList(profile: BenchmarkProfile): Scenario[] {
  const selected = arg("scenarios", "").trim();
  const defaults = SCENARIOS_BY_PROFILE[profile] ?? SCENARIOS_BY_PROFILE.quick;
  const picked = !selected
    ? [...defaults]
    : selected.split(",").map((x) => x.trim()).filter(Boolean).map((n) =>
      defaults.find((s) => s.name === n) ??
        SCENARIO_CATALOG.find((s) => s.name === n)
    ).filter((x): x is Scenario => Boolean(x));

  const usersOverride = Number(arg("users", "0"));
  const setupOverride = Number(arg("setup-concurrency", "0"));
  const concOverride = Number(arg("concurrency", "0"));
  const durationOverride = Number(arg("duration-sec", "0"));
  const accountsOverride = Number(arg("accounts-per-user", "0"));
  const readShareOverride = Number(arg("read-share", "-1"));

  return picked.map((s) => ({
    ...s,
    ...(usersOverride > 0 ? { users: usersOverride } : {}),
    ...(accountsOverride > 0 ? { accountsPerUser: accountsOverride } : {}),
    ...(setupOverride > 0 ? { setupConcurrency: setupOverride } : {}),
    ...(concOverride > 0 ? { concurrency: concOverride } : {}),
    ...(durationOverride > 0 ? { durationSec: durationOverride } : {}),
    ...(s.kind === "mixed" && readShareOverride >= 0 && readShareOverride <= 1
      ? { readShare: readShareOverride }
      : {}),
  }));
}

async function ensureDirs() {
  await Deno.mkdir(RUNS_DIR, { recursive: true });
}

async function safeRemove(path: string) {
  try {
    await Deno.remove(path, { recursive: true });
  } catch {
    // ignore
  }
}

async function prepareData(
  engine: EngineID,
  runId: string,
  scenario: string,
): Promise<string | null> {
  if (engine === "sqlite-ts") {
    const dir = `${ROOT}/benchmarks/finance-sqlite/data`;
    await Deno.mkdir(dir, { recursive: true });
    await safeRemove(`${dir}/finance.db`);
    await safeRemove(`${dir}/finance.db-shm`);
    await safeRemove(`${dir}/finance.db-wal`);
    return null;
  }
  if (engine === "turso-ts") {
    const dir = `${ROOT}/benchmarks/finance-turso/data`;
    await Deno.mkdir(dir, { recursive: true });
    await safeRemove(`${dir}/finance.db`);
    await safeRemove(`${dir}/finance.db-shm`);
    await safeRemove(`${dir}/finance.db-wal`);
    return null;
  }
  if (engine === "pglite-ts") {
    const dir = `${ROOT}/benchmarks/finance-pglite/data`;
    await safeRemove(dir);
    await Deno.mkdir(dir, { recursive: true });
    return null;
  }
  if (engine === "mongodb-ts") {
    const dir = `${ROOT}/benchmarks/finance-mongodb/data`;
    await safeRemove(dir);
    await Deno.mkdir(dir, { recursive: true });
    return null;
  }
  if (engine === "flop-go") {
    const dir =
      `${ROOT}/benchmarks/compare/results/tmp/${runId}/${scenario}/flop-go`;
    await safeRemove(dir);
    await Deno.mkdir(dir, { recursive: true });
    return `${dir}/data`;
  }
  if (engine === "sqlite-go") {
    const dir =
      `${ROOT}/benchmarks/compare/results/tmp/${runId}/${scenario}/sqlite-go`;
    await safeRemove(dir);
    await Deno.mkdir(dir, { recursive: true });
    return `${dir}/finance.db`;
  }
  if (engine === "mongodb-go") {
    const dir = `${ROOT}/benchmarks/finance-mongodb-go/data`;
    await safeRemove(dir);
    await Deno.mkdir(dir, { recursive: true });
    return dir;
  }
  const dir =
    `${ROOT}/benchmarks/compare/results/tmp/${runId}/${scenario}/turso-go`;
  await safeRemove(dir);
  await Deno.mkdir(dir, { recursive: true });
  return `${dir}/finance.db`;
}

function commandFor(
  engine: EngineID,
  port: number,
  dataPath: string | null,
  goBins: Partial<Record<EngineID, string>>,
): { cmd: string; args: string[]; cwd?: string; env?: Record<string, string> } {
  if (engine === "sqlite-ts") {
    return {
      cmd: "deno",
      args: [
        "run",
        "--allow-all",
        "benchmarks/finance-sqlite/app.ts",
        `--port=${port}`,
      ],
      cwd: ROOT,
    };
  }

  if (engine === "turso-ts") {
    return {
      cmd: "deno",
      args: [
        "run",
        "--allow-all",
        "benchmarks/finance-turso/app.ts",
        `--port=${port}`,
      ],
      cwd: ROOT,
    };
  }

  if (engine === "pglite-ts") {
    return {
      cmd: "deno",
      args: [
        "run",
        "--allow-all",
        "benchmarks/finance-pglite/app.ts",
        `--port=${port}`,
      ],
      cwd: ROOT,
    };
  }

  if (engine === "mongodb-ts") {
    return {
      cmd: "deno",
      args: [
        "run",
        "--allow-all",
        "benchmarks/finance-mongodb/app.ts",
        `--port=${port}`,
      ],
      cwd: ROOT,
    };
  }

  if (engine === "flop-go") {
    const bin = goBins["flop-go"];
    if (!bin) throw new Error("missing built binary for flop-go");
    return {
      cmd: bin,
      args: [`--port=${port}`, ...(dataPath ? [`--data=${dataPath}`] : [])],
      cwd: `${ROOT}/go`,
    };
  }

  if (engine === "sqlite-go") {
    const bin = goBins["sqlite-go"];
    if (!bin) throw new Error("missing built binary for sqlite-go");
    return {
      cmd: bin,
      args: [`--port=${port}`, ...(dataPath ? [`--data=${dataPath}`] : [])],
      cwd: `${ROOT}/go`,
    };
  }

  if (engine === "mongodb-go") {
    const bin = goBins["mongodb-go"];
    if (!bin) throw new Error("missing built binary for mongodb-go");
    return {
      cmd: bin,
      args: [
        `--port=${port}`,
        ...(dataPath ? [`--mongo-dir=${dataPath}`] : []),
      ],
      cwd: `${ROOT}/go`,
    };
  }

  const bin = goBins["turso-go"];
  if (!bin) throw new Error("missing built binary for turso-go");
  return {
    cmd: bin,
    args: [`--port=${port}`, ...(dataPath ? [`--data=${dataPath}`] : [])],
    cwd: `${ROOT}/go`,
  };
}

async function waitForServer(host: string, timeoutMs = 30000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(`${host}/api/view/get_stats`);
      if (res.ok || res.status === 401) return;
    } catch {
      // wait
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(`timeout waiting for server ${host}`);
}

async function findFreePort(start: number, maxScan = 200): Promise<number> {
  for (let i = 0; i < maxScan; i++) {
    const port = start + i;
    let listener: Deno.Listener | null = null;
    try {
      // Probe loopback only; benchmark traffic is local.
      listener = Deno.listen({ hostname: "127.0.0.1", port });
      return port;
    } catch (err) {
      // Keep scanning only for true port conflicts.
      if (err instanceof Deno.errors.AddrInUse) continue;
      const message = err instanceof Error ? err.message : String(err);
      throw new Error(`cannot probe port ${port}: ${message}`);
    } finally {
      listener?.close();
    }
  }
  throw new Error(`No free port found from ${start} to ${start + maxScan - 1}`);
}

async function stopProcess(proc: Deno.ChildProcess) {
  const waitForExit = async (ms: number): Promise<boolean> => {
    const done = proc.status.then(() => true).catch(() => true);
    const timeout = new Promise<boolean>((resolve) =>
      setTimeout(() => resolve(false), ms)
    );
    return await Promise.race([done, timeout]);
  };

  if (await waitForExit(10)) return;

  for (
    const [signal, waitMs] of [
      ["SIGINT", 2000],
      ["SIGTERM", 3000],
      ["SIGKILL", 3000],
    ] as const
  ) {
    try {
      proc.kill(signal);
    } catch {
      // ignore
    }
    if (await waitForExit(waitMs)) return;
  }
}

function trackProcess(proc: Deno.ChildProcess): Deno.ChildProcess {
  ACTIVE_PROCS.add(proc);
  proc.status.finally(() => ACTIVE_PROCS.delete(proc));
  return proc;
}

async function stopAllProcesses() {
  const procs = [...ACTIVE_PROCS];
  if (procs.length === 0) return;
  await Promise.allSettled(procs.map((p) => stopProcess(p)));
}

async function readRSSBytes(pid: number): Promise<number | null> {
  if (Deno.build.os === "windows") return null;
  try {
    const result = await new Deno.Command("ps", {
      args: ["-o", "rss=", "-p", String(pid)],
      stdout: "piped",
      stderr: "null",
    }).output();
    if (!result.success) return null;

    const raw = new TextDecoder().decode(result.stdout).trim();
    if (!raw) return null;
    const kb = Number.parseInt(raw.split(/\s+/)[0], 10);
    if (!Number.isFinite(kb) || kb <= 0) return null;
    return kb * 1024;
  } catch {
    return null;
  }
}

function round2(v: number): number {
  return Math.round(v * 100) / 100;
}

function bytesToMB(v: number): number {
  return v / (1024 * 1024);
}

function startMemorySampler(pid: number) {
  let running = true;
  const samples: number[] = [];
  const loop = (async () => {
    while (running) {
      try {
        const rss = await readRSSBytes(pid);
        if (rss != null) samples.push(rss);
      } catch {
        // keep loop alive even if memory sampling fails
      }
      await new Promise((r) => setTimeout(r, MEMORY_SAMPLE_INTERVAL_MS));
    }
  })();

  return {
    async stop(opsPerSec?: number): Promise<MemoryProfile> {
      running = false;
      await loop;
      if (samples.length === 0) {
        return { supported: Deno.build.os !== "windows", sampleCount: 0 };
      }

      let sum = 0;
      let min = Number.POSITIVE_INFINITY;
      let max = 0;
      for (const v of samples) {
        sum += v;
        if (v < min) min = v;
        if (v > max) max = v;
      }
      const avg = sum / samples.length;
      const avgMB = round2(bytesToMB(avg));
      const out: MemoryProfile = {
        supported: true,
        sampleCount: samples.length,
        rssAvgMB: avgMB,
        rssPeakMB: round2(bytesToMB(max)),
        rssMinMB: round2(bytesToMB(min)),
      };
      if (typeof opsPerSec === "number" && opsPerSec > 0 && avgMB > 0) {
        out.opsPerSecPerAvgMB = round2(opsPerSec / avgMB);
      }
      return out;
    },
  };
}

async function runWorkload(
  s: Scenario,
  host: string,
  opts: {
    warmupSec: number;
    strictSetup: boolean;
    setupRetries: number;
    workloadTimeoutSec: number;
    requestTimeoutMs: number;
  },
): Promise<WorkloadBenchResult> {
  const cmd = new Deno.Command("deno", {
    cwd: ROOT,
    args: [
      "run",
      "--allow-net",
      "benchmarks/compare/workload.ts",
      `--host=${host}`,
      `--mode=${s.kind}`,
      `--users=${s.users}`,
      `--accounts-per-user=${s.accountsPerUser ?? 3}`,
      `--setup-concurrency=${s.setupConcurrency}`,
      `--concurrency=${s.concurrency}`,
      `--duration-sec=${s.durationSec}`,
      `--warmup-sec=${opts.warmupSec}`,
      `--strict-setup=${opts.strictSetup ? "1" : "0"}`,
      `--setup-retries=${opts.setupRetries}`,
      `--request-timeout-ms=${opts.requestTimeoutMs}`,
      `--read-share=${s.readShare ?? 0.6}`,
      "--json-only=1",
    ],
    stdout: "piped",
    stderr: "piped",
  });
  const proc = cmd.spawn();
  let timer: number | undefined;
  const timeoutMs = Math.max(1, opts.workloadTimeoutSec) * 1000;
  const timeout = new Promise<Deno.CommandOutput>((_, reject) => {
    timer = setTimeout(() => {
      try {
        proc.kill("SIGTERM");
      } catch {
        // ignore
      }
      setTimeout(() => {
        try {
          proc.kill("SIGKILL");
        } catch {
          // ignore
        }
      }, 1000);
      reject(
        new Error(
          `workload timed out after ${opts.workloadTimeoutSec}s`,
        ),
      );
    }, timeoutMs);
  });

  let result: Deno.CommandOutput;
  try {
    result = await Promise.race([proc.output(), timeout]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }

  const out = new TextDecoder().decode(result.stdout);
  const err = new TextDecoder().decode(result.stderr);
  if (!result.success) throw new Error(`workload failed: ${err || out}`);

  const marker = out.split("\n").find((line) => line.startsWith("BENCH_JSON:"));
  if (!marker) {
    throw new Error(`workload output missing BENCH_JSON marker: ${out}`);
  }
  return JSON.parse(marker.slice("BENCH_JSON:".length)) as WorkloadBenchResult;
}

function aggregateMemoryProfiles(samples: MemoryProfile[]): MemoryProfile {
  if (!samples.length) return { supported: false, sampleCount: 0 };
  const supported = samples.every((s) => s.supported);
  const out: MemoryProfile = {
    supported,
    sampleCount: samples.reduce((n, s) => n + s.sampleCount, 0),
  };
  const avg = samples.map((s) => s.rssAvgMB).filter(isFiniteNumber);
  const peak = samples.map((s) => s.rssPeakMB).filter(isFiniteNumber);
  const min = samples.map((s) => s.rssMinMB).filter(isFiniteNumber);
  const eff = samples.map((s) => s.opsPerSecPerAvgMB).filter(isFiniteNumber);
  if (avg.length) out.rssAvgMB = round2(toMean(avg));
  if (peak.length) out.rssPeakMB = round2(toMean(peak));
  if (min.length) out.rssMinMB = round2(toMean(min));
  if (eff.length) out.opsPerSecPerAvgMB = round2(toMean(eff));
  return out;
}

function aggregateStats(
  all: Array<Record<string, unknown> | undefined>,
): Record<string, unknown> | undefined {
  const rows = all.filter((x): x is Record<string, unknown> => Boolean(x));
  if (!rows.length) return undefined;
  const out: Record<string, unknown> = {};
  const keys = new Set<string>();
  for (const row of rows) {
    for (const k of Object.keys(row)) keys.add(k);
  }
  for (const key of keys) {
    const vals = rows.map((r) => r[key]).filter((v) => v !== undefined);
    const nums = vals.filter(isFiniteNumber);
    if (vals.length && nums.length === vals.length) {
      out[key] = round2(toMean(nums));
    } else if (vals.length) {
      out[key] = vals[vals.length - 1];
    }
  }
  return out;
}

function aggregateWorkloadResults(
  samples: WorkloadBenchResult[],
): WorkloadBenchResult {
  if (samples.length === 1) return samples[0];
  const first = samples[0];
  const last = samples[samples.length - 1];

  const stats = aggregateStats(samples.map((s) => s.stats));

  return {
    kind: "workload",
    mode: first.mode,
    host: first.host,
    usersRequested: first.usersRequested,
    accountsPerUser: first.accountsPerUser,
    setupConcurrency: first.setupConcurrency,
    concurrency: first.concurrency,
    durationSecRequested: first.durationSecRequested,
    startedAt: first.startedAt,
    finishedAt: last.finishedAt,
    totalMs: toMean(samples.map((s) => s.totalMs)),
    setup: {
      register: {
        durationMs: toMean(samples.map((s) => s.setup.register.durationMs)),
        attempted: toMean(samples.map((s) => s.setup.register.attempted)),
        success: toMean(samples.map((s) => s.setup.register.success)),
        ratePerSec: toMean(samples.map((s) => s.setup.register.ratePerSec)),
      },
      createAccounts: {
        durationMs: toMean(
          samples.map((s) => s.setup.createAccounts.durationMs),
        ),
        attempted: toMean(samples.map((s) => s.setup.createAccounts.attempted)),
        success: toMean(samples.map((s) => s.setup.createAccounts.success)),
        ratePerSec: toMean(
          samples.map((s) => s.setup.createAccounts.ratePerSec),
        ),
      },
      deposit: {
        durationMs: toMean(samples.map((s) => s.setup.deposit.durationMs)),
        attempted: toMean(samples.map((s) => s.setup.deposit.attempted)),
        success: toMean(samples.map((s) => s.setup.deposit.success)),
        ratePerSec: toMean(samples.map((s) => s.setup.deposit.ratePerSec)),
      },
    },
    workload: {
      durationMs: toMean(samples.map((s) => s.workload.durationMs)),
      attempted: toMean(samples.map((s) => s.workload.attempted)),
      success: toMean(samples.map((s) => s.workload.success)),
      failed: toMean(samples.map((s) => s.workload.failed)),
      opsPerSec: toMean(samples.map((s) => s.workload.opsPerSec)),
      readOps: toMean(samples.map((s) => s.workload.readOps)),
      writeOps: toMean(samples.map((s) => s.workload.writeOps)),
      readOpsPerSec: toMean(samples.map((s) => s.workload.readOpsPerSec)),
      writeOpsPerSec: toMean(samples.map((s) => s.workload.writeOpsPerSec)),
      transferOpsPerSec: toMean(
        samples.map((s) => s.workload.transferOpsPerSec),
      ),
      byOp: {},
    },
    ...(stats ? { stats } : {}),
  };
}

function aggregateByOp(samples: WorkloadBenchResult[]): Record<string, number> {
  const allKeys = new Set<string>();
  for (const sample of samples) {
    for (const key of Object.keys(sample.workload.byOp)) allKeys.add(key);
  }
  const out: Record<string, number> = {};
  for (const key of allKeys) {
    out[key] = toMean(samples.map((s) => s.workload.byOp[key] ?? 0));
  }
  return out;
}

function summarizeRepeats(
  engine: EngineID,
  repeats: ScenarioResult[],
): ScenarioResult {
  const successes = repeats.filter((r) => r.ok && r.metrics) as Array<
    ScenarioResult & { metrics: WorkloadBenchResult }
  >;
  if (successes.length === 0) {
    const first = repeats[0];
    return {
      engine,
      ok: false,
      elapsedMs: toMean(repeats.map((r) => r.elapsedMs)),
      error: first?.error ?? "all repeats failed",
      repeatCount: repeats.length,
      successfulRepeats: 0,
      repeats,
    };
  }
  const metrics = aggregateWorkloadResults(successes.map((s) => s.metrics));
  metrics.workload.byOp = aggregateByOp(successes.map((s) => s.metrics));
  const memory = aggregateMemoryProfiles(
    successes.map((s) => s.memory).filter((m): m is MemoryProfile =>
      Boolean(m)
    ),
  );
  return {
    engine,
    ok: true,
    elapsedMs: toMean(successes.map((r) => r.elapsedMs)),
    metrics,
    memory,
    repeatCount: repeats.length,
    successfulRepeats: successes.length,
    ...(successes.length < repeats.length
      ? { error: `${repeats.length - successes.length} repeats failed` }
      : {}),
    repeats,
  };
}

function shouldEarlyStop(
  repeats: ScenarioResult[],
  minRepeats: number,
  targetRse: number,
): boolean {
  if (targetRse <= 0) return false;
  if (repeats.length < minRepeats) return false;
  if (repeats.some((r) => !r.ok || !r.metrics)) return false;
  const ops = repeats.map((r) => r.metrics?.workload.opsPerSec).filter(
    (v): v is number => typeof v === "number" && Number.isFinite(v),
  );
  const rse = relativeStdErr(ops);
  return rse != null && rse <= targetRse;
}

async function runShell(cmd: string, args: string[]): Promise<string> {
  const result = await new Deno.Command(cmd, {
    args,
    stdout: "piped",
    stderr: "null",
  }).output();
  if (!result.success) return "";
  return new TextDecoder().decode(result.stdout).trim();
}

async function ensureSQLiteGoDeps(_engines: EngineID[]) {
  // sqlite-go now has its own go.mod with the sqlite dependency included.
  // No bootstrapping needed.
}

async function ensureMongoGoDeps(engines: EngineID[]) {
  if (!engines.includes("mongodb-go")) return;

  const modDir = `${ROOT}/benchmarks/finance-mongodb-go`;
  const sumPath = `${modDir}/go.sum`;
  try {
    await Deno.stat(sumPath);
    return;
  } catch (err) {
    if (!(err instanceof Deno.errors.NotFound)) throw err;
  }

  console.log("mongodb-go: bootstrapping Go deps (go mod tidy)...");
  const tidy = await new Deno.Command("go", {
    cwd: modDir,
    args: ["mod", "tidy"],
    env: { GOCACHE: "/tmp/go-build-cache" },
    stdout: "inherit",
    stderr: "inherit",
  }).output();
  if (!tidy.success) {
    throw new Error(
      "failed to bootstrap mongodb-go deps. Run manually: cd benchmarks/finance-mongodb-go && go mod tidy",
    );
  }
}

async function buildGoBinaries(
  engines: EngineID[],
  runId: string,
): Promise<Partial<Record<EngineID, string>>> {
  const out: Partial<Record<EngineID, string>> = {};
  const needFlopGo = engines.includes("flop-go");
  const needSQLiteGo = engines.includes("sqlite-go");
  const needTursoGo = engines.includes("turso-go");
  const needMongoGo = engines.includes("mongodb-go");
  if (!needFlopGo && !needSQLiteGo && !needTursoGo && !needMongoGo) return out;

  const binDir = `${RESULTS_DIR}/tmp/${runId}/bin`;
  await Deno.mkdir(binDir, { recursive: true });

  // Ensure shared admin HTML is copied into Go source tree for go:embed (only needed for flop-go/sqlite-go/turso-go).
  if (needFlopGo || needSQLiteGo || needTursoGo) {
    const gen = await new Deno.Command("go", {
      cwd: `${ROOT}/go`,
      args: ["generate", "./..."],
      env: { GOCACHE: "/tmp/go-build-cache" },
      stdout: "inherit",
      stderr: "inherit",
    }).output();
    if (!gen.success) throw new Error("go generate failed");
  }

  if (needFlopGo) {
    const binPath = `${binDir}/go-finance`;
    const build = await new Deno.Command("go", {
      cwd: `${ROOT}/benchmarks/finance-go`,
      args: ["build", "-o", binPath, "."],
      env: { GOCACHE: "/tmp/go-build-cache" },
      stdout: "inherit",
      stderr: "inherit",
    }).output();
    if (!build.success) throw new Error("failed to build flop-go binary");
    out["flop-go"] = binPath;
  }

  if (needSQLiteGo) {
    const binPath = `${binDir}/sqlite-finance`;
    const build = await new Deno.Command("go", {
      cwd: `${ROOT}/benchmarks/finance-sqlite-go`,
      args: ["build", "-o", binPath, "."],
      env: { GOCACHE: "/tmp/go-build-cache" },
      stdout: "inherit",
      stderr: "inherit",
    }).output();
    if (!build.success) throw new Error("failed to build sqlite-go binary");
    out["sqlite-go"] = binPath;
  }

  if (needTursoGo) {
    const binPath = `${binDir}/turso-finance`;
    const build = await new Deno.Command("go", {
      cwd: `${ROOT}/benchmarks/finance-turso-go`,
      args: ["build", "-o", binPath, "."],
      env: { GOCACHE: "/tmp/go-build-cache" },
      stdout: "inherit",
      stderr: "inherit",
    }).output();
    if (!build.success) throw new Error("failed to build turso-go binary");
    out["turso-go"] = binPath;
  }

  if (needMongoGo) {
    const binPath = `${binDir}/mongo-finance`;
    const build = await new Deno.Command("go", {
      cwd: `${ROOT}/benchmarks/finance-mongodb-go`,
      args: ["build", "-o", binPath, "."],
      env: { GOCACHE: "/tmp/go-build-cache" },
      stdout: "inherit",
      stderr: "inherit",
    }).output();
    if (!build.success) throw new Error("failed to build mongodb-go binary");
    out["mongodb-go"] = binPath;
  }

  return out;
}

async function gitInfo() {
  const sha = await runShell("git", ["rev-parse", "--short", "HEAD"]);
  const branch = await runShell("git", ["rev-parse", "--abbrev-ref", "HEAD"]);
  const dirty = (await runShell("git", ["status", "--porcelain"])).length > 0;
  return { sha: sha || "unknown", branch: branch || "unknown", dirty };
}

function utcStamp(d = new Date()): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${
    pad(d.getUTCDate())
  }-${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}`;
}

async function readHistory(): Promise<HistoryFile> {
  try {
    const raw = await Deno.readTextFile(HISTORY_PATH);
    const parsed = JSON.parse(raw) as HistoryFile;
    if (!Array.isArray(parsed.runs)) throw new Error("invalid history");
    parsed.runs = parsed.runs.filter((r: any) =>
      Number(r?.schemaVersion) === 3
    );
    return parsed;
  } catch {
    return { schemaVersion: 3, updatedAt: new Date().toISOString(), runs: [] };
  }
}

async function writeHistory(run: RunRecord): Promise<HistoryFile> {
  const history = await readHistory();
  history.runs.unshift(run);
  history.runs = history.runs.slice(0, 300);
  history.schemaVersion = 3;
  history.updatedAt = new Date().toISOString();
  await Deno.writeTextFile(HISTORY_PATH, JSON.stringify(history, null, 2));
  return history;
}

async function writeStaticReport(history: HistoryFile) {
  let template = FALLBACK_REPORT_TEMPLATE;
  try {
    template = await Deno.readTextFile(REPORT_TEMPLATE_PATH);
  } catch (err) {
    if (err instanceof Deno.errors.NotFound) {
      console.warn(
        `warning: report template missing at ${REPORT_TEMPLATE_PATH}; using fallback template`,
      );
    } else {
      throw err;
    }
  }
  const safeJson = JSON.stringify(history).replaceAll("</script", "<\\/script");
  const boot = `<script>window.__BENCH_HISTORY__ = ${safeJson};</script>`;
  const injected = template.includes("</head>")
    ? template.replace("</head>", `${boot}\n</head>`)
    : `${boot}\n${template}`;
  await Deno.writeTextFile(REPORT_SNAPSHOT_PATH, injected);
}

async function main() {
  let signalHooked = false;
  const onSignal = async () => {
    if ((onSignal as { _running?: boolean })._running) return;
    (onSignal as { _running?: boolean })._running = true;
    STOP_REQUESTED = true;
    console.error("\nInterrupted, stopping benchmark subprocesses...");
    await stopAllProcesses();
    Deno.exit(130);
  };
  if (Deno.build.os !== "windows") {
    Deno.addSignalListener("SIGINT", onSignal);
    Deno.addSignalListener("SIGTERM", onSignal);
    signalHooked = true;
  }

  try {
    const profile = parseProfile();
    const engineSet = parseEngineSet();
    const repeatsMax = parseRepeats(profile);
    const minRepeats = parseMinRepeats(profile, repeatsMax);
    const earlyStopRse = parseEarlyStopRse(profile);
    const warmupSec = parseWarmupSec(profile);
    const workloadTimeoutSec = parseWorkloadTimeoutSec(profile);
    const requestTimeoutMs = parseRequestTimeoutMs(profile);
    const shuffleEngines = parseShuffleEngines();
    const strictSetup = parseStrictSetup();
    const setupRetries = parseSetupRetries(profile);
    const seed = parseSeed();
    const nextRand = seededRng(seed);
    const engines = parseEngineList(engineSet);
    const scenarios = parseScenarioList(profile);
    if (scenarios.length === 0) {
      throw new Error(
        `No scenarios selected. Available: ${
          SCENARIO_CATALOG.map((s) => s.name).join(", ")
        }`,
      );
    }

    await ensureDirs();
    await ensureSQLiteGoDeps(engines);
    await ensureMongoGoDeps(engines);

    const now = new Date();
    const git = await gitInfo();
    const runId = `${utcStamp(now)}-${git.sha}`;
    const goBins = await buildGoBinaries(engines, runId);

    const runRecord: RunRecord = {
      schemaVersion: 3,
      runId,
      createdAt: now.toISOString(),
      git,
      host: {
        os: Deno.build.os,
        arch: Deno.build.arch,
        deno: Deno.version.deno,
      },
      scenarios: [],
    };

    console.log(`Run ID: ${runId}`);
    console.log(`Profile: ${profile}`);
    console.log(`Engine set: ${engineSet}`);
    console.log(`Repeats max: ${repeatsMax}`);
    console.log(`Min repeats: ${minRepeats}`);
    console.log(`Early-stop RSE: ${earlyStopRse}`);
    console.log(`Warmup sec: ${warmupSec}`);
    console.log(`Workload timeout sec: ${workloadTimeoutSec}`);
    console.log(`Request timeout ms: ${requestTimeoutMs}`);
    console.log(`Shuffle engines: ${shuffleEngines}`);
    console.log(`Strict setup: ${strictSetup}`);
    console.log(`Setup retries: ${setupRetries}`);
    console.log(`Seed: ${seed}`);
    console.log(`Engines: ${engines.join(", ")}`);
    console.log(
      `Scenarios: ${scenarios.map((s) => `${s.name}:${s.kind}`).join(", ")}`,
    );

    for (const [scenarioIndex, scenario] of scenarios.entries()) {
      if (STOP_REQUESTED) break;
      console.log(
        `\nScenario: ${scenario.name} [${scenario.kind}] (users=${scenario.users}, setup=${scenario.setupConcurrency}, conc=${scenario.concurrency}, duration=${scenario.durationSec}s)`,
      );
      const repeatResults = new Map<EngineID, ScenarioResult[]>();
      for (const engine of engines) repeatResults.set(engine, []);

      for (let repeatIndex = 0; repeatIndex < repeatsMax; repeatIndex++) {
        if (STOP_REQUESTED) break;
        const activeEngines = engines.filter((engine) =>
          !shouldEarlyStop(
            repeatResults.get(engine) ?? [],
            minRepeats,
            earlyStopRse,
          )
        );
        if (activeEngines.length === 0) {
          console.log(
            `  Early stop: all engines reached stability target after ${repeatIndex} repeats`,
          );
          break;
        }
        const orderedEngines = shuffleEngines
          ? shuffleWithRng(activeEngines, nextRand)
          : [...activeEngines];
        console.log(
          `  Repeat ${repeatIndex + 1}/${repeatsMax}: ${
            orderedEngines.join(", ")
          }`,
        );

        for (const engine of orderedEngines) {
          if (STOP_REQUESTED) break;
          const preferredPort = ENGINE_BASE_PORT[engine] + scenarioIndex * 20;
          const port = await findFreePort(preferredPort);
          const host = `http://localhost:${port}`;
          const started = performance.now();
          console.log(`  -> ${engine} on ${host}`);

          const dataPath = await prepareData(engine, runId, scenario.name);
          const cmd = commandFor(engine, port, dataPath, goBins);

          const proc = new Deno.Command(cmd.cmd, {
            args: cmd.args,
            cwd: cmd.cwd,
            env: cmd.env,
            stdout: "inherit",
            stderr: "inherit",
          }).spawn();
          trackProcess(proc);
          const mem = startMemorySampler(proc.pid);
          let memory: MemoryProfile | undefined;

          try {
            await waitForServer(host);
            const metrics = await runWorkload(scenario, host, {
              warmupSec,
              strictSetup,
              setupRetries,
              workloadTimeoutSec,
              requestTimeoutMs,
            });
            const elapsedMs = performance.now() - started;
            memory = await mem.stop(metrics.workload.opsPerSec);
            repeatResults.get(engine)!.push({
              engine,
              ok: true,
              elapsedMs,
              metrics,
              memory,
            });
            const memLabel = memory.sampleCount > 0
              ? ` mem(avg/peak)=${Math.round(memory.rssAvgMB ?? 0)}/${
                Math.round(memory.rssPeakMB ?? 0)
              }MB eff=${Math.round(memory.opsPerSecPerAvgMB ?? 0)} ops/s/MB`
              : "";
            console.log(
              `     ok: ${Math.round(metrics.workload.opsPerSec)} ops/s total=${
                (metrics.totalMs / 1000).toFixed(2)
              }s${memLabel}`,
            );
          } catch (err) {
            const elapsedMs = performance.now() - started;
            memory = await mem.stop();
            repeatResults.get(engine)!.push({
              engine,
              ok: false,
              elapsedMs,
              memory,
              error: err instanceof Error ? err.message : String(err),
            });
            console.error(`     failed: ${err}`);
          } finally {
            if (!memory) await mem.stop();
            await stopProcess(proc);
          }
        }
      }

      const scenarioResults = engines.map((engine) =>
        summarizeRepeats(engine, repeatResults.get(engine) ?? [])
      );

      runRecord.scenarios.push({
        name: scenario.name,
        config: scenario,
        scoreLabel: "ops/s",
        results: scenarioResults,
      });
    }

    const runPath = `${RUNS_DIR}/${runId}.json`;
    await Deno.writeTextFile(runPath, JSON.stringify(runRecord, null, 2));
    const history = await writeHistory(runRecord);
    await Deno.writeTextFile(
      `${RESULTS_DIR}/latest.json`,
      JSON.stringify(runRecord, null, 2),
    );
    await writeStaticReport(history);

    console.log("\nSummary");
    for (const s of runRecord.scenarios) {
      const ok = s.results.filter((r) => r.ok && r.metrics);
      ok.sort((a, b) =>
        b.metrics!.workload.opsPerSec - a.metrics!.workload.opsPerSec
      );
      if (!ok.length) {
        console.log(`  ${s.name}: no successful runs`);
        continue;
      }
      const best = ok[0];
      console.log(
        `  ${s.name}: ${best.engine} (${
          Math.round(best.metrics!.workload.opsPerSec)
        } ops/s)`,
      );
    }

    console.log(`\nSaved run: ${runPath}`);
    console.log(`Updated history: ${HISTORY_PATH}`);
    console.log(`Report (static): ${REPORT_SNAPSHOT_PATH}`);
    console.log("Report (server): benchmarks/compare/report/index.html");
  } finally {
    if (signalHooked && Deno.build.os !== "windows") {
      Deno.removeSignalListener("SIGINT", onSignal);
      Deno.removeSignalListener("SIGTERM", onSignal);
    }
    await stopAllProcesses();
  }
}

if (import.meta.main) {
  main().catch((err) => {
    console.error(err);
    Deno.exit(1);
  });
}
