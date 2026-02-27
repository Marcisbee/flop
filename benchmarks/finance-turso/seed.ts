/**
 * Unified finance benchmark seeder.
 *
 * Produces structured JSON output for automation while remaining readable.
 *
 * Usage:
 *   deno run --allow-net benchmarks/compare/seed.ts --host=http://localhost:1985
 *   deno run --allow-net benchmarks/compare/seed.ts --host=http://localhost:1985 --users=1000 --transfers=50000
 */

type Json = Record<string, unknown>;

type BenchResult = {
  host: string;
  usersRequested: number;
  transfersRequested: number;
  setupConcurrency: number;
  transferConcurrency: number;
  startedAt: string;
  finishedAt: string;
  totalMs: number;
  steps: {
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
    transfers: {
      durationMs: number;
      attempted: number;
      completed: number;
      failed: number;
      ratePerSec: number;
    };
  };
  stats?: Json;
};

const HOST = getArg("host", "http://localhost:1985");
const USER_COUNT = Number(getArg("users", "1000"));
const TRANSFER_COUNT = Number(getArg("transfers", "50000"));
const TRANSFER_CONCURRENCY = Number(getArg("transfer-concurrency", "50"));
const SETUP_CONCURRENCY = Number(getArg("setup-concurrency", "50"));
const JSON_ONLY = getArg("json-only", "0") === "1";

function getArg(name: string, fallback: string): string {
  const prefix = `--${name}=`;
  const arg = Deno.args.find((a) => a.startsWith(prefix));
  return arg ? arg.slice(prefix.length) : fallback;
}

function log(msg: string) {
  if (!JSON_ONLY) console.log(msg);
}

let defaultToken = "";

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

async function reduce(name: string, params: Json, authToken?: string) {
  const res = await fetch(`${HOST}/api/reduce/${name}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(authToken
        ? { Authorization: `Bearer ${authToken}` }
        : { Authorization: `Bearer ${defaultToken}` }),
    },
    body: JSON.stringify(params),
  });
  if (!res.ok) {
    throw new Error(`reduce ${name} ${res.status}: ${await res.text()}`);
  }
  const json = await res.json();
  return json.data;
}

async function view(name: string, params: Json = {}) {
  const search = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined) search.set(k, String(v));
  }
  const qs = search.toString();
  const url = `${HOST}/api/view/${name}${qs ? `?${qs}` : ""}`;
  const res = await fetch(url, {
    headers: defaultToken ? { Authorization: `Bearer ${defaultToken}` } : {},
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

const TRANSFER_DESCRIPTIONS = [
  "Rent payment",
  "Groceries",
  "Salary",
  "Utilities",
  "Subscription",
  "Dinner",
  "Coffee",
  "Gas",
  "Insurance",
  "Loan repayment",
  "Gift",
  "Refund",
  "Invoice #1042",
  "Monthly savings",
  "Investment",
  "Freelance payment",
  "Split bill",
  "Reimbursement",
  "Bonus",
  "Commission",
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

async function main() {
  const startedAt = new Date();
  const totalStart = performance.now();

  log(`\nFinance Benchmark Seeder`);
  log(`Host: ${HOST}`);
  log(`Users: ${USER_COUNT.toLocaleString()}`);
  log(`Transfers: ${TRANSFER_COUNT.toLocaleString()}`);
  log(
    `Setup concurrency: ${SETUP_CONCURRENCY}, transfer concurrency: ${TRANSFER_CONCURRENCY}\n`,
  );

  type UserInfo = {
    email: string;
    password: string;
    name: string;
    token: string;
  };
  type AccountRef = { id: string; ownerToken: string };

  const users: UserInfo[] = [];
  const accountRefs: AccountRef[] = [];

  log(`[1/4] Registering users...`);
  const step1 = performance.now();
  const userTemplates = Array.from({ length: USER_COUNT }, (_, i) => {
    const first = FIRST_NAMES[i % FIRST_NAMES.length];
    const last =
      LAST_NAMES[Math.floor(i / FIRST_NAMES.length) % LAST_NAMES.length];
    return {
      email: `${first.toLowerCase()}.${last.toLowerCase()}.${i}@bank.test`,
      password: "password123",
      name: `${first} ${last}`,
    };
  });

  await runBatched(userTemplates, SETUP_CONCURRENCY, async (u) => {
    try {
      const result = await register(u.email, u.password, u.name);
      users.push({ ...u, token: result.token });
    } catch {
      try {
        users.push({ ...u, token: await login(u.email, u.password) });
      } catch {
        // ignore; counted by success length
      }
    }
  });
  const step1Ms = performance.now() - step1;
  log(
    `Registered/logged users: ${users.length}/${USER_COUNT} in ${
      (step1Ms / 1000).toFixed(2)
    }s (${Math.round(users.length / (step1Ms / 1000))}/s)`,
  );

  if (users.length === 0) {
    throw new Error("No users could be created or logged in");
  }

  log(`[2/4] Creating accounts...`);
  const step2 = performance.now();
  const accountTypes = ["checking", "savings", "credit"] as const;
  const accountTasks = users.flatMap((user) =>
    accountTypes.map((type) => ({ user, type, name: `${user.name}'s ${type}` }))
  );

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
  const step2Ms = performance.now() - step2;
  log(
    `Accounts created: ${accountRefs.length}/${accountTasks.length} in ${
      (step2Ms / 1000).toFixed(2)
    }s (${Math.round(accountRefs.length / (step2Ms / 1000))}/s)`,
  );

  log(`[3/4] Depositing...`);
  const step3 = performance.now();
  let depositsOk = 0;
  await runBatched(accountRefs, SETUP_CONCURRENCY, async (acc) => {
    const amount = randomAmount(1000, 100000);
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        await reduce("deposit", { accountId: acc.id, amount }, acc.ownerToken);
        depositsOk++;
        return;
      } catch {
        if (attempt < 2) {
          await new Promise((r) => setTimeout(r, 50 * (attempt + 1)));
        }
      }
    }
  });
  const step3Ms = performance.now() - step3;
  log(
    `Deposits completed: ${depositsOk}/${accountRefs.length} in ${
      (step3Ms / 1000).toFixed(2)
    }s (${Math.round(depositsOk / (step3Ms / 1000))}/s)`,
  );

  log(`[4/4] Transfers...`);
  const step4 = performance.now();
  if (accountRefs.length < 2) {
    throw new Error("Need at least 2 accounts for transfers");
  }

  const transferTasks = Array.from({ length: TRANSFER_COUNT }, () => {
    const from = randomItem(accountRefs);
    let to = randomItem(accountRefs);
    while (to.id === from.id) to = randomItem(accountRefs);
    return { from, to };
  });

  let completed = 0;
  let failed = 0;

  await runBatched(transferTasks, TRANSFER_CONCURRENCY, async (task, i) => {
    const amount = randomAmount(1, 5000);
    const description = randomItem(TRANSFER_DESCRIPTIONS);
    try {
      const result = await reduce("transfer", {
        fromAccountId: task.from.id,
        toAccountId: task.to.id,
        amount,
        description,
      }, task.from.ownerToken);
      if (result?.status === "completed") completed++;
      else failed++;
    } catch {
      failed++;
    }

    if (!JSON_ONLY && (i + 1) % 5000 === 0) {
      const elapsed = (performance.now() - step4) / 1000;
      log(
        `  progress ${
          (i + 1).toLocaleString()
        }/${TRANSFER_COUNT.toLocaleString()} (${
          Math.round((i + 1) / elapsed)
        } tx/s)`,
      );
    }
  });
  const step4Ms = performance.now() - step4;
  log(
    `Transfers: completed=${completed}, failed=${failed}, ${
      (step4Ms / 1000).toFixed(2)
    }s (${Math.round(TRANSFER_COUNT / (step4Ms / 1000))}/s)`,
  );

  let stats: Json | undefined;
  try {
    stats = await view("get_stats");
  } catch {
    // optional
  }

  const totalMs = performance.now() - totalStart;
  const finishedAt = new Date();

  const result: BenchResult = {
    host: HOST,
    usersRequested: USER_COUNT,
    transfersRequested: TRANSFER_COUNT,
    setupConcurrency: SETUP_CONCURRENCY,
    transferConcurrency: TRANSFER_CONCURRENCY,
    startedAt: startedAt.toISOString(),
    finishedAt: finishedAt.toISOString(),
    totalMs,
    steps: {
      register: {
        durationMs: step1Ms,
        attempted: USER_COUNT,
        success: users.length,
        ratePerSec: users.length / (step1Ms / 1000),
      },
      createAccounts: {
        durationMs: step2Ms,
        attempted: accountTasks.length,
        success: accountRefs.length,
        ratePerSec: accountRefs.length / (step2Ms / 1000),
      },
      deposit: {
        durationMs: step3Ms,
        attempted: accountRefs.length,
        success: depositsOk,
        ratePerSec: depositsOk / (step3Ms / 1000),
      },
      transfers: {
        durationMs: step4Ms,
        attempted: TRANSFER_COUNT,
        completed,
        failed,
        ratePerSec: TRANSFER_COUNT / (step4Ms / 1000),
      },
    },
    ...(stats ? { stats } : {}),
  };

  log(`Total time: ${(totalMs / 1000).toFixed(2)}s`);
  console.log(`BENCH_JSON:${JSON.stringify(result)}`);
}

if (import.meta.main) {
  main().catch((err) => {
    console.error(err);
    Deno.exit(1);
  });
}
