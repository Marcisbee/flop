/**
 * Unified workload benchmark orchestrator for:
 * - flop-ts
 * - flop-go
 * - sqlite-ts
 * - sqlite-go
 *
 * Scenarios are workload-only:
 * - high-load-rw
 * - reads
 * - writes
 * - edits
 */

import { fromFileUrl, resolve } from "@std/path";

type EngineID = "flop-ts" | "flop-go" | "sqlite-ts" | "sqlite-go";
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

const DEFAULT_SCENARIOS: Scenario[] = [
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

const SCENARIO_CATALOG: Scenario[] = [...DEFAULT_SCENARIOS];

const ENGINE_ORDER: EngineID[] = [
  "flop-ts",
  "flop-go",
  "sqlite-ts",
  "sqlite-go",
];
const ENGINE_BASE_PORT: Record<EngineID, number> = {
  "flop-ts": 41085,
  "flop-go": 41086,
  "sqlite-ts": 41087,
  "sqlite-go": 41088,
};
const MEMORY_SAMPLE_INTERVAL_MS = 250;

function arg(name: string, fallback = ""): string {
  const prefix = `--${name}=`;
  const found = Deno.args.find((a) => a.startsWith(prefix));
  return found ? found.slice(prefix.length) : fallback;
}

function parseEngineList(): EngineID[] {
  const raw = arg("engines", ENGINE_ORDER.join(","));
  const values = raw.split(",").map((x) => x.trim()).filter(
    Boolean,
  ) as EngineID[];
  const out: EngineID[] = [];
  for (const id of values) {
    if (ENGINE_ORDER.includes(id) && !out.includes(id)) out.push(id);
  }
  return out.length ? out : [...ENGINE_ORDER];
}

function parseScenarioList(): Scenario[] {
  const selected = arg("scenarios", "").trim();
  const picked = !selected
    ? [...DEFAULT_SCENARIOS]
    : selected.split(",").map((x) => x.trim()).filter(Boolean).map((n) =>
      SCENARIO_CATALOG.find((s) => s.name === n)
    ).filter((x): x is Scenario => Boolean(x));

  const usersOverride = Number(arg("users", "0"));
  const setupOverride = Number(arg("setup-concurrency", "0"));
  const concOverride = Number(arg("concurrency", "0"));
  const durationOverride = Number(arg("duration-sec", "0"));
  const readShareOverride = Number(arg("read-share", "-1"));

  return picked.map((s) => ({
    ...s,
    ...(usersOverride > 0 ? { users: usersOverride } : {}),
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
  if (engine === "flop-ts") {
    const dir = `${ROOT}/benchmarks/finance/data`;
    await safeRemove(dir);
    await Deno.mkdir(dir, { recursive: true });
    return null;
  }
  if (engine === "sqlite-ts") {
    const dir = `${ROOT}/benchmarks/sqlite-finance/data`;
    await Deno.mkdir(dir, { recursive: true });
    await safeRemove(`${dir}/finance.db`);
    await safeRemove(`${dir}/finance.db-shm`);
    await safeRemove(`${dir}/finance.db-wal`);
    return null;
  }
  if (engine === "flop-go") {
    const dir =
      `${ROOT}/benchmarks/compare/results/tmp/${runId}/${scenario}/flop-go`;
    await safeRemove(dir);
    await Deno.mkdir(dir, { recursive: true });
    return `${dir}/data`;
  }
  const dir =
    `${ROOT}/benchmarks/compare/results/tmp/${runId}/${scenario}/sqlite-go`;
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
  if (engine === "flop-ts") {
    return {
      cmd: "deno",
      args: [
        "run",
        "--allow-read",
        "--allow-write",
        "--allow-net",
        "--allow-env",
        "main.ts",
        "benchmarks/finance/app.ts",
      ],
      cwd: ROOT,
      env: { FLOP_PORT: String(port) },
    };
  }

  if (engine === "sqlite-ts") {
    return {
      cmd: "deno",
      args: [
        "run",
        "--allow-all",
        "benchmarks/sqlite-finance/app.ts",
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

  const bin = goBins["sqlite-go"];
  if (!bin) throw new Error("missing built binary for sqlite-go");
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
      listener = Deno.listen({ hostname: "0.0.0.0", port });
      return port;
    } catch {
      // next
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
      const rss = await readRSSBytes(pid);
      if (rss != null) samples.push(rss);
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
): Promise<WorkloadBenchResult> {
  const result = await new Deno.Command("deno", {
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
      `--read-share=${s.readShare ?? 0.6}`,
      "--json-only=1",
    ],
    stdout: "piped",
    stderr: "piped",
  }).output();

  const out = new TextDecoder().decode(result.stdout);
  const err = new TextDecoder().decode(result.stderr);
  if (!result.success) throw new Error(`workload failed: ${err || out}`);

  const marker = out.split("\n").find((line) => line.startsWith("BENCH_JSON:"));
  if (!marker) {
    throw new Error(`workload output missing BENCH_JSON marker: ${out}`);
  }
  return JSON.parse(marker.slice("BENCH_JSON:".length)) as WorkloadBenchResult;
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

async function ensureSQLiteGoDeps(engines: EngineID[]) {
  if (!engines.includes("sqlite-go")) return;
  const goCwd = `${ROOT}/go`;
  const check = await new Deno.Command("go", {
    cwd: goCwd,
    args: ["list", "-m", "modernc.org/sqlite"],
    stdout: "null",
    stderr: "null",
  }).output();
  if (check.success) return;

  console.log("Bootstrapping sqlite-go dependency: modernc.org/sqlite");
  const get = await new Deno.Command("go", {
    cwd: goCwd,
    args: ["get", "modernc.org/sqlite@v1.39.1"],
    env: { GOCACHE: "/tmp/go-build-cache" },
    stdout: "inherit",
    stderr: "inherit",
  }).output();
  if (!get.success) {
    throw new Error(
      "Failed to download modernc.org/sqlite. Run: cd go && go get modernc.org/sqlite@v1.39.1 && go mod tidy",
    );
  }

  const tidy = await new Deno.Command("go", {
    cwd: goCwd,
    args: ["mod", "tidy"],
    env: { GOCACHE: "/tmp/go-build-cache" },
    stdout: "inherit",
    stderr: "inherit",
  }).output();
  if (!tidy.success) {
    throw new Error("go mod tidy failed after sqlite dependency bootstrap");
  }
}

async function buildGoBinaries(
  engines: EngineID[],
  runId: string,
): Promise<Partial<Record<EngineID, string>>> {
  const out: Partial<Record<EngineID, string>> = {};
  const needFlopGo = engines.includes("flop-go");
  const needSQLiteGo = engines.includes("sqlite-go");
  if (!needFlopGo && !needSQLiteGo) return out;

  const goCwd = `${ROOT}/go`;
  const binDir = `${RESULTS_DIR}/tmp/${runId}/bin`;
  await Deno.mkdir(binDir, { recursive: true });

  if (needFlopGo) {
    const binPath = `${binDir}/go-finance`;
    const build = await new Deno.Command("go", {
      cwd: goCwd,
      args: ["build", "-o", binPath, "./cmd/go-finance"],
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
      cwd: goCwd,
      args: ["build", "-o", binPath, "./cmd/sqlite-finance"],
      env: { GOCACHE: "/tmp/go-build-cache" },
      stdout: "inherit",
      stderr: "inherit",
    }).output();
    if (!build.success) throw new Error("failed to build sqlite-go binary");
    out["sqlite-go"] = binPath;
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
    const engines = parseEngineList();
    const scenarios = parseScenarioList();
    if (scenarios.length === 0) {
      throw new Error(
        `No scenarios selected. Available: ${
          SCENARIO_CATALOG.map((s) => s.name).join(", ")
        }`,
      );
    }

    await ensureDirs();
    await ensureSQLiteGoDeps(engines);

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
    console.log(`Engines: ${engines.join(", ")}`);
    console.log(
      `Scenarios: ${scenarios.map((s) => `${s.name}:${s.kind}`).join(", ")}`,
    );

    for (const [scenarioIndex, scenario] of scenarios.entries()) {
      console.log(
        `\nScenario: ${scenario.name} [${scenario.kind}] (users=${scenario.users}, setup=${scenario.setupConcurrency}, conc=${scenario.concurrency}, duration=${scenario.durationSec}s)`,
      );
      const scenarioResults: ScenarioResult[] = [];

      for (const engine of engines) {
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
          const metrics = await runWorkload(scenario, host);
          const elapsedMs = performance.now() - started;
          memory = await mem.stop(metrics.workload.opsPerSec);
          scenarioResults.push({ engine, ok: true, elapsedMs, metrics, memory });
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
          scenarioResults.push({
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
