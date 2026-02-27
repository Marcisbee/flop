/**
 * Fast all-engine benchmark preset.
 *
 * This script wraps benchmarks/compare/run.ts with small defaults so we can
 * quickly compare flop against all other engines.
 *
 * Emits:
 *   BENCH_JSON:{...}
 */

import { fromFileUrl, resolve } from "@std/path";

type EngineID =
  | "flop-ts"
  | "flop-go"
  | "sqlite-ts"
  | "sqlite-go"
  | "turso-ts"
  | "pglite-ts"
  | "turso-go"
  | "mongodb-ts"
  | "mongodb-go";

type ScenarioName = "high-load-rw" | "reads" | "writes" | "edits";

type RunScenarioResult = {
  engine: string;
  ok: boolean;
  metrics?: { workload?: { opsPerSec?: number } };
  repeatCount?: number;
  successfulRepeats?: number;
  error?: string;
};

type RunRecord = {
  runId: string;
  createdAt: string;
  scenarios: Array<{
    name: string;
    results: RunScenarioResult[];
  }>;
};

type EngineScore = {
  engine: string;
  ok: boolean;
  opsPerSec: number | null;
  repeatCount: number;
  successfulRepeats: number;
  error?: string;
};

type ScenarioSummary = {
  name: string;
  bestEngine: string | null;
  bestOpsPerSec: number | null;
  scores: EngineScore[];
};

type LeaderboardRow = {
  engine: string;
  avgOpsPerSec: number;
  scenarioCount: number;
};

type Output = {
  kind: "engine-micro";
  createdAt: string;
  config: {
    engines: EngineID[];
    scenarios: ScenarioName[];
    users: number;
    accountsPerUser: number;
    setupConcurrency: number;
    concurrency: number;
    durationSec: number;
    repeats: number;
    warmupSec: number;
    setupRetries: number;
    readShare: number;
    shuffleEngines: boolean;
    strictSetup: boolean;
    seed: number | null;
  };
  run: {
    runId: string;
    createdAt: string;
  };
  scenarios: ScenarioSummary[];
  leaderboard: LeaderboardRow[];
};

const ROOT = resolve(fromFileUrl(new URL(".", import.meta.url)), "..", "..");
const LATEST_RUN_PATH = resolve(
  ROOT,
  "benchmarks",
  "compare",
  "results",
  "latest.json",
);

const ENGINE_ORDER: EngineID[] = [
  "flop-ts",
  "flop-go",
  "sqlite-ts",
  "sqlite-go",
  "turso-ts",
  "pglite-ts",
  "turso-go",
  "mongodb-ts",
  "mongodb-go",
];
const SCENARIO_ORDER: ScenarioName[] = [
  "high-load-rw",
  "reads",
  "writes",
  "edits",
];

const JSON_ONLY = boolArg("json-only", false);
const ENGINES = parseEngineList();
const SCENARIOS = parseScenarioList();
const USERS = numberArg("users", 80, 1);
const ACCOUNTS_PER_USER = numberArg("accounts-per-user", 3, 1);
const SETUP_CONCURRENCY = numberArg("setup-concurrency", 12, 1);
const CONCURRENCY = numberArg("concurrency", 40, 1);
const DURATION_SEC = numberArg("duration-sec", 4, 1);
const REPEATS = numberArg("repeats", 1, 1);
const WARMUP_SEC = numberArg("warmup-sec", 0, 0);
const SETUP_RETRIES = numberArg("setup-retries", 4, 1);
const READ_SHARE = clamp(numberArgRaw("read-share", 0.55), 0, 1);
const SHUFFLE_ENGINES = boolArg("shuffle-engines", true);
const STRICT_SETUP = boolArg("strict-setup", true);
const SEED = optionalPositiveIntArg("seed");

function arg(name: string): string | undefined {
  const prefix = `--${name}=`;
  const found = Deno.args.find((a) => a.startsWith(prefix));
  return found ? found.slice(prefix.length) : undefined;
}

function log(msg: string) {
  if (!JSON_ONLY) console.log(msg);
}

function boolArg(name: string, fallback: boolean): boolean {
  const raw = arg(name);
  if (raw == null) return fallback;
  const v = raw.trim().toLowerCase();
  return !(v === "0" || v === "false" || v === "no");
}

function numberArgRaw(name: string, fallback: number): number {
  const raw = arg(name);
  if (raw == null) return fallback;
  const value = Number(raw);
  return Number.isFinite(value) ? value : fallback;
}

function numberArg(name: string, fallback: number, min: number): number {
  return Math.max(min, Math.floor(numberArgRaw(name, fallback)));
}

function optionalPositiveIntArg(name: string): number | null {
  const raw = arg(name);
  if (!raw) return null;
  const value = Math.floor(Number(raw));
  return Number.isFinite(value) && value > 0 ? value : null;
}

function clamp(v: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, v));
}

function parseEngineList(): EngineID[] {
  const raw = arg("engines") ?? ENGINE_ORDER.join(",");
  const out: EngineID[] = [];
  for (const val of raw.split(",").map((x) => x.trim()).filter(Boolean)) {
    if (
      ENGINE_ORDER.includes(val as EngineID) && !out.includes(val as EngineID)
    ) {
      out.push(val as EngineID);
    }
  }
  return out.length ? out : [...ENGINE_ORDER];
}

function parseScenarioList(): ScenarioName[] {
  const raw = arg("scenarios") ?? "high-load-rw";
  const out: ScenarioName[] = [];
  for (const val of raw.split(",").map((x) => x.trim()).filter(Boolean)) {
    if (
      SCENARIO_ORDER.includes(val as ScenarioName) &&
      !out.includes(val as ScenarioName)
    ) {
      out.push(val as ScenarioName);
    }
  }
  return out.length ? out : ["high-load-rw"];
}

function ops(result: RunScenarioResult): number | null {
  const value = result.metrics?.workload?.opsPerSec;
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function summarizeScenario(
  scenarioName: string,
  results: RunScenarioResult[],
): ScenarioSummary {
  const scores = results.map((r) => {
    const points = ops(r);
    return {
      engine: r.engine,
      ok: r.ok && points !== null,
      opsPerSec: points,
      repeatCount: Math.max(1, r.repeatCount ?? 1),
      successfulRepeats: Math.max(0, r.successfulRepeats ?? (r.ok ? 1 : 0)),
      ...(r.error ? { error: r.error } : {}),
    } satisfies EngineScore;
  }).sort((a, b) => (b.opsPerSec ?? -1) - (a.opsPerSec ?? -1));

  const best = scores.find((s) => s.ok && s.opsPerSec != null);
  return {
    name: scenarioName,
    bestEngine: best?.engine ?? null,
    bestOpsPerSec: best?.opsPerSec ?? null,
    scores,
  };
}

function buildLeaderboard(scenarios: ScenarioSummary[]): LeaderboardRow[] {
  const map = new Map<string, number[]>();
  for (const scenario of scenarios) {
    for (const score of scenario.scores) {
      if (score.opsPerSec == null) continue;
      const arr = map.get(score.engine) ?? [];
      arr.push(score.opsPerSec);
      map.set(score.engine, arr);
    }
  }
  const rows: LeaderboardRow[] = [];
  for (const [engine, points] of map) {
    const avg = points.reduce((a, b) => a + b, 0) / points.length;
    rows.push({
      engine,
      avgOpsPerSec: avg,
      scenarioCount: points.length,
    });
  }
  rows.sort((a, b) => b.avgOpsPerSec - a.avgOpsPerSec);
  return rows;
}

async function runCompareHarness() {
  const args = [
    "run",
    "--allow-read",
    "--allow-write",
    "--allow-run",
    "--allow-env",
    "--allow-net",
    "benchmarks/compare/run.ts",
    "--profile=smoke",
    "--engine-set=all",
    `--engines=${ENGINES.join(",")}`,
    `--scenarios=${SCENARIOS.join(",")}`,
    `--users=${USERS}`,
    `--accounts-per-user=${ACCOUNTS_PER_USER}`,
    `--setup-concurrency=${SETUP_CONCURRENCY}`,
    `--concurrency=${CONCURRENCY}`,
    `--duration-sec=${DURATION_SEC}`,
    `--repeats=${REPEATS}`,
    `--warmup-sec=${WARMUP_SEC}`,
    `--setup-retries=${SETUP_RETRIES}`,
    `--read-share=${READ_SHARE}`,
    `--shuffle-engines=${SHUFFLE_ENGINES ? "1" : "0"}`,
    `--strict-setup=${STRICT_SETUP ? "1" : "0"}`,
  ];
  if (SEED != null) args.push(`--seed=${SEED}`);

  const result = await new Deno.Command("deno", {
    cwd: ROOT,
    args,
    stdout: JSON_ONLY ? "piped" : "inherit",
    stderr: JSON_ONLY ? "piped" : "inherit",
  }).output();

  const stdout = JSON_ONLY ? new TextDecoder().decode(result.stdout) : "";
  const stderr = JSON_ONLY ? new TextDecoder().decode(result.stderr) : "";
  if (!result.success) {
    throw new Error(
      `compare harness failed\n${stdout.trim()}\n${stderr.trim()}`.trim(),
    );
  }
}

async function readLatestRun(): Promise<RunRecord> {
  const raw = await Deno.readTextFile(LATEST_RUN_PATH);
  const parsed = JSON.parse(raw) as RunRecord;
  if (!parsed.runId || !Array.isArray(parsed.scenarios)) {
    throw new Error(`invalid run file at ${LATEST_RUN_PATH}`);
  }
  return parsed;
}

async function main() {
  log("All-engine micro benchmark (flop vs others)");
  log(`Engines: ${ENGINES.join(", ")}`);
  log(
    `Scenarios: ${
      SCENARIOS.join(", ")
    }, users=${USERS}, concurrency=${CONCURRENCY}, duration=${DURATION_SEC}s, repeats=${REPEATS}`,
  );

  await runCompareHarness();
  const run = await readLatestRun();

  const scenarios = run.scenarios.map((s) =>
    summarizeScenario(s.name, s.results)
  );
  const leaderboard = buildLeaderboard(scenarios);

  if (!JSON_ONLY) {
    log("\nMicro summary");
    for (const scenario of scenarios) {
      const bestLabel =
        scenario.bestEngine == null || scenario.bestOpsPerSec == null
          ? "no successful runs"
          : `${scenario.bestEngine} (${
            Math.round(scenario.bestOpsPerSec)
          } ops/s)`;
      log(`  ${scenario.name}: ${bestLabel}`);
    }
    if (leaderboard.length > 0) {
      log("\nLeaderboard (avg ops/s across selected scenarios)");
      for (const row of leaderboard) {
        log(
          `  ${row.engine}: ${
            Math.round(row.avgOpsPerSec)
          } ops/s (${row.scenarioCount} scenario${
            row.scenarioCount === 1 ? "" : "s"
          })`,
        );
      }
    }
  }

  const out: Output = {
    kind: "engine-micro",
    createdAt: new Date().toISOString(),
    config: {
      engines: ENGINES,
      scenarios: SCENARIOS,
      users: USERS,
      accountsPerUser: ACCOUNTS_PER_USER,
      setupConcurrency: SETUP_CONCURRENCY,
      concurrency: CONCURRENCY,
      durationSec: DURATION_SEC,
      repeats: REPEATS,
      warmupSec: WARMUP_SEC,
      setupRetries: SETUP_RETRIES,
      readShare: READ_SHARE,
      shuffleEngines: SHUFFLE_ENGINES,
      strictSetup: STRICT_SETUP,
      seed: SEED,
    },
    run: {
      runId: run.runId,
      createdAt: run.createdAt,
    },
    scenarios,
    leaderboard,
  };

  console.log(`BENCH_JSON:${JSON.stringify(out)}`);
}

if (import.meta.main) {
  main().catch((err) => {
    console.error(err);
    Deno.exit(1);
  });
}
