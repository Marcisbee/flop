/**
 * Workload benchmark for finance API.
 *
 * Modes:
 * - reads: read-only API calls
 * - writes: write-heavy reducers (transfer/deposit)
 * - edits: hot-row transfer edits (balance updates under contention)
 * - mixed: concurrent reads + writes
 *
 * Emits a single machine-readable line:
 *   BENCH_JSON:{...}
 */

type Json = Record<string, unknown>;
type Mode = "reads" | "writes" | "edits" | "mixed";

type SetupStep = {
  durationMs: number;
  attempted: number;
  success: number;
  ratePerSec: number;
};

type WorkloadBenchResult = {
  kind: "workload";
  mode: Mode;
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
    register: SetupStep;
    createAccounts: SetupStep;
    deposit: SetupStep;
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
  stats?: Json;
};

type UserInfo = {
  email: string;
  password: string;
  name: string;
  token: string;
};
type AccountRef = { id: string; ownerToken: string };

const HOST = getArg("host", "http://localhost:1985");
const MODE = getArg("mode", "reads") as Mode;
const USER_COUNT = Number(getArg("users", "300"));
const ACCOUNTS_PER_USER = Number(getArg("accounts-per-user", "3"));
const SETUP_CONCURRENCY = Number(getArg("setup-concurrency", "40"));
const CONCURRENCY = Number(getArg("concurrency", "100"));
const DURATION_SEC = Number(getArg("duration-sec", "10"));
const READ_SHARE = Number(getArg("read-share", "0.6"));
const JSON_ONLY = getArg("json-only", "0") === "1";

function getArg(name: string, fallback: string): string {
  const prefix = `--${name}=`;
  const arg = Deno.args.find((a) => a.startsWith(prefix));
  return arg ? arg.slice(prefix.length) : fallback;
}

function log(msg: string) {
  if (!JSON_ONLY) console.log(msg);
}

async function register(email: string, password: string, name: string) {
  const res = await fetch(`${HOST}/api/auth/register`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password, name }),
  });
  if (!res.ok) throw new Error(`register ${res.status}: ${await res.text()}`);
  return res.json();
}

async function login(email: string, password: string): Promise<string> {
  const res = await fetch(`${HOST}/api/auth/password`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
  if (!res.ok) throw new Error(`login ${res.status}: ${await res.text()}`);
  const data = await res.json();
  return data.token;
}

async function reduce(name: string, params: Json, authToken: string) {
  const res = await fetch(`${HOST}/api/reduce/${name}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${authToken}`,
    },
    body: JSON.stringify(params),
  });
  if (!res.ok) {
    throw new Error(`reduce ${name} ${res.status}: ${await res.text()}`);
  }
  const json = await res.json();
  return json.data;
}

async function view(name: string, params: Json = {}, authToken?: string) {
  const search = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined) search.set(k, String(v));
  }
  const qs = search.toString();
  const url = `${HOST}/api/view/${name}${qs ? `?${qs}` : ""}`;
  const res = await fetch(url, {
    headers: authToken ? { Authorization: `Bearer ${authToken}` } : {},
  });
  if (!res.ok) {
    throw new Error(`view ${name} ${res.status}: ${await res.text()}`);
  }
  const json = await res.json();
  return json.data;
}

function randomItem<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randomAmount(min: number, max: number): number {
  return Math.round((min + Math.random() * (max - min)) * 100) / 100;
}

const FIRST_NAMES = [
  "Alice",
  "Bob",
  "Charlie",
  "Diana",
  "Eve",
  "Frank",
  "Grace",
  "Hank",
  "Ivy",
  "Jack",
  "Karen",
  "Leo",
  "Mia",
  "Noah",
  "Olivia",
  "Paul",
  "Quinn",
  "Ruby",
  "Sam",
  "Tina",
  "Uma",
  "Victor",
  "Wendy",
  "Xander",
  "Yara",
  "Zane",
  "Aria",
  "Blake",
  "Cora",
  "Dante",
];

const LAST_NAMES = [
  "Smith",
  "Johnson",
  "Williams",
  "Brown",
  "Jones",
  "Garcia",
  "Miller",
  "Davis",
  "Rodriguez",
  "Martinez",
  "Hernandez",
  "Lopez",
  "Gonzalez",
  "Wilson",
  "Anderson",
  "Thomas",
  "Taylor",
  "Moore",
  "Jackson",
  "Martin",
];

async function runBatched<T>(
  items: T[],
  concurrency: number,
  fn: (item: T, index: number) => Promise<void>,
) {
  let idx = 0;
  const total = items.length;

  async function worker() {
    while (idx < total) {
      const i = idx++;
      await fn(items[i], i);
    }
  }

  const workers = Array.from(
    { length: Math.min(concurrency, total) },
    () => worker(),
  );
  await Promise.all(workers);
}

async function setupData() {
  const users: UserInfo[] = [];
  const accountRefs: AccountRef[] = [];

  const registerStart = performance.now();
  const templates = Array.from({ length: USER_COUNT }, (_, i) => {
    const first = FIRST_NAMES[i % FIRST_NAMES.length];
    const last =
      LAST_NAMES[Math.floor(i / FIRST_NAMES.length) % LAST_NAMES.length];
    return {
      email: `${first.toLowerCase()}.${last.toLowerCase()}.${i}@bench.test`,
      password: "password123",
      name: `${first} ${last}`,
    };
  });

  await runBatched(templates, SETUP_CONCURRENCY, async (u) => {
    try {
      const r = await register(u.email, u.password, u.name);
      users.push({ ...u, token: r.token });
    } catch {
      try {
        users.push({ ...u, token: await login(u.email, u.password) });
      } catch {
        // ignore
      }
    }
  });

  const registerMs = performance.now() - registerStart;
  const registerStep: SetupStep = {
    durationMs: registerMs,
    attempted: USER_COUNT,
    success: users.length,
    ratePerSec: users.length / (registerMs / 1000),
  };

  if (users.length === 0) throw new Error("setup failed: no users");

  const createStart = performance.now();
  const accountTypes = ["checking", "savings", "credit"] as const;
  const accountTasks = users.flatMap((user) => {
    const types = accountTypes.slice(
      0,
      Math.max(1, Math.min(3, ACCOUNTS_PER_USER)),
    );
    return types.map((type) => ({
      user,
      type,
      name: `${user.name}'s ${type}`,
    }));
  });

  await runBatched(accountTasks, SETUP_CONCURRENCY, async (task) => {
    try {
      const result = await reduce("create_account", {
        name: task.name,
        type: task.type,
        currency: "USD",
      }, task.user.token);
      if (result?.id) {
        accountRefs.push({ id: result.id, ownerToken: task.user.token });
      }
    } catch {
      // ignore
    }
  });

  const createMs = performance.now() - createStart;
  const createStep: SetupStep = {
    durationMs: createMs,
    attempted: accountTasks.length,
    success: accountRefs.length,
    ratePerSec: accountRefs.length / (createMs / 1000),
  };

  if (accountRefs.length < 2) {
    throw new Error("setup failed: not enough accounts");
  }

  const depositStart = performance.now();
  let depositOk = 0;
  await runBatched(accountRefs, SETUP_CONCURRENCY, async (acc) => {
    try {
      await reduce("deposit", {
        accountId: acc.id,
        amount: randomAmount(20000, 120000),
      }, acc.ownerToken);
      depositOk++;
    } catch {
      // ignore
    }
  });

  const depositMs = performance.now() - depositStart;
  const depositStep: SetupStep = {
    durationMs: depositMs,
    attempted: accountRefs.length,
    success: depositOk,
    ratePerSec: depositOk / (depositMs / 1000),
  };

  return {
    users,
    accountRefs,
    setup: {
      register: registerStep,
      createAccounts: createStep,
      deposit: depositStep,
    },
  };
}

async function runForDuration(
  durationMs: number,
  concurrency: number,
  fn: () => Promise<{ op: string; ok: boolean; class: "read" | "write" }>,
) {
  const started = performance.now();
  const deadline = started + durationMs;

  let attempted = 0;
  let success = 0;
  let failed = 0;
  let readOps = 0;
  let writeOps = 0;
  const byOp: Record<string, number> = {};

  async function worker() {
    while (performance.now() < deadline) {
      attempted++;
      try {
        const result = await fn();
        if (result.ok) {
          success++;
          byOp[result.op] = (byOp[result.op] ?? 0) + 1;
          if (result.class === "read") readOps++;
          else writeOps++;
        } else {
          failed++;
        }
      } catch {
        failed++;
      }
    }
  }

  await Promise.all(
    Array.from({ length: Math.max(1, concurrency) }, () => worker()),
  );

  const actualMs = performance.now() - started;
  const sec = actualMs / 1000;
  const transferOps = (byOp.transfer ?? 0) + (byOp.edit_transfer ?? 0);

  return {
    durationMs: actualMs,
    attempted,
    success,
    failed,
    opsPerSec: success / sec,
    readOps,
    writeOps,
    readOpsPerSec: readOps / sec,
    writeOpsPerSec: writeOps / sec,
    transferOpsPerSec: transferOps / sec,
    byOp,
  };
}

function pickTwoDifferent<T>(arr: T[]): [T, T] {
  const a = randomItem(arr);
  let b = randomItem(arr);
  while (b === a) b = randomItem(arr);
  return [a, b];
}

async function runWorkload(mode: Mode, accountRefs: AccountRef[]) {
  const hotAccounts = accountRefs.slice(
    0,
    Math.max(8, Math.min(120, accountRefs.length)),
  );

  const readOp = async () => {
    const roll = Math.random();
    if (roll < 0.35) {
      await view("get_stats");
      return { op: "get_stats", ok: true, class: "read" as const };
    }
    if (roll < 0.7) {
      await view("get_recent_transactions", { limit: 50 });
      return {
        op: "get_recent_transactions",
        ok: true,
        class: "read" as const,
      };
    }
    if (roll < 0.9) {
      await view("get_all_accounts");
      return { op: "get_all_accounts", ok: true, class: "read" as const };
    }
    const acc = randomItem(accountRefs);
    await view("get_transactions", { accountId: acc.id }, acc.ownerToken);
    return { op: "get_transactions", ok: true, class: "read" as const };
  };

  const writeOp = async () => {
    if (Math.random() < 0.25) {
      const acc = randomItem(accountRefs);
      await reduce("deposit", {
        accountId: acc.id,
        amount: randomAmount(1, 120),
      }, acc.ownerToken);
      return { op: "deposit", ok: true, class: "write" as const };
    }
    const [from, to] = pickTwoDifferent(accountRefs);
    const result = await reduce("transfer", {
      fromAccountId: from.id,
      toAccountId: to.id,
      amount: randomAmount(1, 80),
      description: "write-bench",
    }, from.ownerToken);
    return {
      op: "transfer",
      ok: result?.status === "completed",
      class: "write" as const,
    };
  };

  const editOp = async () => {
    const [from, to] = pickTwoDifferent(hotAccounts);
    const result = await reduce("transfer", {
      fromAccountId: from.id,
      toAccountId: to.id,
      amount: randomAmount(1, 20),
      description: "edit-bench",
    }, from.ownerToken);
    if (result?.status === "completed") {
      return { op: "edit_transfer", ok: true, class: "write" as const };
    }

    // Keep hot rows active when one side runs out of funds.
    await reduce("deposit", {
      accountId: from.id,
      amount: randomAmount(50, 200),
    }, from.ownerToken);
    return { op: "edit_topup", ok: true, class: "write" as const };
  };

  const durationMs = Math.max(1, DURATION_SEC) * 1000;

  if (mode === "reads") {
    return runForDuration(durationMs, CONCURRENCY, readOp);
  }
  if (mode === "writes") {
    return runForDuration(durationMs, CONCURRENCY, writeOp);
  }
  if (mode === "edits") {
    return runForDuration(durationMs, CONCURRENCY, editOp);
  }

  // mixed
  const readShare = Math.max(0, Math.min(1, READ_SHARE));
  return runForDuration(durationMs, CONCURRENCY, async () => {
    if (Math.random() < readShare) return readOp();
    return writeOp();
  });
}

async function main() {
  const startedAt = new Date();
  const totalStart = performance.now();

  if (!["reads", "writes", "edits", "mixed"].includes(MODE)) {
    throw new Error(`invalid --mode=${MODE}`);
  }

  log(`\nWorkload Benchmark`);
  log(`Host: ${HOST}`);
  log(`Mode: ${MODE}`);
  log(`Users: ${USER_COUNT}, accounts/user: ${ACCOUNTS_PER_USER}`);
  log(
    `Setup concurrency: ${SETUP_CONCURRENCY}, workload concurrency: ${CONCURRENCY}`,
  );
  log(`Duration: ${DURATION_SEC}s\n`);

  const setup = await setupData();
  log(
    `Setup done: users=${setup.users.length}, accounts=${setup.accountRefs.length}`,
  );

  const workload = await runWorkload(MODE, setup.accountRefs);
  log(
    `Workload done: success=${workload.success}, failed=${workload.failed}, ops/s=${
      Math.round(workload.opsPerSec)
    }`,
  );

  let stats: Json | undefined;
  try {
    stats = await view("get_stats");
  } catch {
    // optional
  }

  const finishedAt = new Date();
  const result: WorkloadBenchResult = {
    kind: "workload",
    mode: MODE,
    host: HOST,
    usersRequested: USER_COUNT,
    accountsPerUser: ACCOUNTS_PER_USER,
    setupConcurrency: SETUP_CONCURRENCY,
    concurrency: CONCURRENCY,
    durationSecRequested: DURATION_SEC,
    startedAt: startedAt.toISOString(),
    finishedAt: finishedAt.toISOString(),
    totalMs: performance.now() - totalStart,
    setup: setup.setup,
    workload,
    ...(stats ? { stats } : {}),
  };

  console.log(`BENCH_JSON:${JSON.stringify(result)}`);
}

if (import.meta.main) {
  main().catch((err) => {
    console.error(err);
    Deno.exit(1);
  });
}
