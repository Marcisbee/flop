/**
 * SQLite Finance benchmark seed script
 *
 * Identical to the flop finance seed — both servers expose the same API.
 *
 * Usage:
 *   1. Start the server:  deno run --allow-all benchmarks/sqlite-finance/app.ts
 *   2. Run seed:  deno run --allow-net benchmarks/sqlite-finance/seed.ts
 *
 * Options:
 *   --host=http://localhost:1985   Server host
 *   --users=1000                   Number of users to create
 *   --transfers=50000              Number of random transfers
 */

const HOST = getArg("host", "http://localhost:1985");
const USER_COUNT = Number(getArg("users", "1000"));
const TRANSFER_COUNT = Number(getArg("transfers", "50000"));
const BATCH_CONCURRENCY = 50;
const SETUP_CONCURRENCY = 10;

function getArg(name: string, fallback: string): string {
  const prefix = `--${name}=`;
  const arg = Deno.args.find((a) => a.startsWith(prefix));
  return arg ? arg.slice(prefix.length) : fallback;
}

// ── Helpers ─────────────────────────────────────────────────────────────────

let token = "";

async function register(email: string, password: string, name: string) {
  const res = await fetch(`${HOST}/api/auth/register`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password, name }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Register failed (${res.status}): ${text}`);
  }
  return res.json();
}

async function login(email: string, password: string): Promise<string> {
  const res = await fetch(`${HOST}/api/auth/password`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Login failed (${res.status}): ${text}`);
  }
  const data = await res.json();
  return data.token;
}

async function reduce(name: string, params: Record<string, unknown>, authToken?: string) {
  const res = await fetch(`${HOST}/api/reduce/${name}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(authToken ? { Authorization: `Bearer ${authToken}` } : { Authorization: `Bearer ${token}` }),
    },
    body: JSON.stringify(params),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Reduce ${name} failed (${res.status}): ${text}`);
  }
  const json = await res.json();
  return json.data;
}

async function view(name: string, params: Record<string, unknown> = {}) {
  const search = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined) search.set(k, String(v));
  }
  const qs = search.toString();
  const url = `${HOST}/api/view/${name}${qs ? "?" + qs : ""}`;
  const res = await fetch(url, {
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`View ${name} failed (${res.status}): ${text}`);
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
  "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank",
  "Ivy", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia", "Paul",
  "Quinn", "Ruby", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
  "Yara", "Zane", "Aria", "Blake", "Cora", "Dante",
];

const LAST_NAMES = [
  "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
  "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
  "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
];

const TRANSFER_DESCRIPTIONS = [
  "Rent payment", "Groceries", "Salary", "Utilities", "Subscription",
  "Dinner", "Coffee", "Gas", "Insurance", "Loan repayment",
  "Gift", "Refund", "Invoice #1042", "Monthly savings", "Investment",
  "Freelance payment", "Split bill", "Reimbursement", "Bonus", "Commission",
];

// ── Batch execution helper ──────────────────────────────────────────────────

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

  const workers = Array.from({ length: Math.min(concurrency, total) }, () => worker());
  await Promise.all(workers);
}

// ── Main ────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`\n  SQLite Finance Benchmark Seeder`);
  console.log(`  ─────────────────────────────────`);
  console.log(`  Host:       ${HOST}`);
  console.log(`  Users:      ${USER_COUNT.toLocaleString()}`);
  console.log(`  Transfers:  ${TRANSFER_COUNT.toLocaleString()}`);
  console.log(`  Concurrency: ${BATCH_CONCURRENCY}\n`);

  const totalStart = performance.now();

  // ── Step 1: Register users ────────────────────────────────────────────
  console.log(`  [1/4] Registering ${USER_COUNT.toLocaleString()} users...`);
  const step1 = performance.now();

  interface UserInfo {
    email: string;
    password: string;
    name: string;
    token: string;
  }

  const users: UserInfo[] = [];
  const userTemplates = Array.from({ length: USER_COUNT }, (_, i) => {
    const first = FIRST_NAMES[i % FIRST_NAMES.length];
    const last = LAST_NAMES[Math.floor(i / FIRST_NAMES.length) % LAST_NAMES.length];
    const email = `${first.toLowerCase()}.${last.toLowerCase()}.${i}@bank.test`;
    const name = `${first} ${last}`;
    return { email, password: "password123", name };
  });

  await runBatched(userTemplates, SETUP_CONCURRENCY, async (u) => {
    try {
      const result = await register(u.email, u.password, u.name);
      users.push({ ...u, token: result.token });
    } catch {
      try {
        const t = await login(u.email, u.password);
        users.push({ ...u, token: t });
      } catch (err) {
        console.error(`    Failed to login ${u.email}: ${err}`);
      }
    }
  });

  const step1Time = performance.now() - step1;
  console.log(`        Done in ${(step1Time / 1000).toFixed(1)}s (${Math.round(users.length / (step1Time / 1000))} users/s)`);

  if (users.length === 0) {
    console.error("\n  ERROR: No users could be registered/logged in. Is the server running?");
    Deno.exit(1);
  }

  // ── Step 2: Create accounts ───────────────────────────────────────────
  console.log(`  [2/4] Creating ${(USER_COUNT * 3).toLocaleString()} accounts (3 per user)...`);
  const step2 = performance.now();

  interface AccountRef {
    id: string;
    ownerToken: string;
  }

  const accountRefs: AccountRef[] = [];
  const accountTypes = ["checking", "savings", "credit"] as const;
  const accountTasks = users.flatMap((user) =>
    accountTypes.map((type) => ({ user, type, name: `${user.name}'s ${type}` }))
  );

  await runBatched(accountTasks, SETUP_CONCURRENCY, async (task) => {
    try {
      const result = await reduce(
        "create_account",
        { name: task.name, type: task.type, currency: "USD" },
        task.user.token,
      );
      if (result?.id) {
        accountRefs.push({ id: result.id, ownerToken: task.user.token });
      }
    } catch (err) {
      console.error(`    Failed to create account: ${err}`);
    }
  });

  const step2Time = performance.now() - step2;
  console.log(`        Done in ${(step2Time / 1000).toFixed(1)}s (${Math.round(accountRefs.length / (step2Time / 1000))} accounts/s)`);

  // ── Step 3: Initial deposits ──────────────────────────────────────────
  console.log(`  [3/4] Depositing into ${accountRefs.length.toLocaleString()} accounts...`);
  const step3 = performance.now();

  await runBatched(accountRefs, SETUP_CONCURRENCY, async (acc) => {
    const amount = randomAmount(1000, 100000);
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        await reduce("deposit", { accountId: acc.id, amount }, acc.ownerToken);
        break;
      } catch (err) {
        if (attempt === 2) console.error(`    Failed deposit: ${err}`);
        else await new Promise((r) => setTimeout(r, 100 * (attempt + 1)));
      }
    }
  });

  const step3Time = performance.now() - step3;
  console.log(`        Done in ${(step3Time / 1000).toFixed(1)}s (${Math.round(accountRefs.length / (step3Time / 1000))} deposits/s)`);

  // ── Step 4: Random transfers ──────────────────────────────────────────
  console.log(`  [4/4] Executing ${TRANSFER_COUNT.toLocaleString()} random transfers...`);
  const step4 = performance.now();

  if (accountRefs.length < 2) {
    console.error("\n  ERROR: Need at least 2 accounts for transfers. Only got " + accountRefs.length);
    Deno.exit(1);
  }

  const transferTasks = Array.from({ length: TRANSFER_COUNT }, () => {
    const from = randomItem(accountRefs);
    let to = randomItem(accountRefs);
    while (to.id === from.id) to = randomItem(accountRefs);
    return { from, to };
  });

  let completed = 0;
  let failed = 0;

  await runBatched(transferTasks, BATCH_CONCURRENCY, async (task, i) => {
    const amount = randomAmount(1, 5000);
    const description = randomItem(TRANSFER_DESCRIPTIONS);
    try {
      const result = await reduce(
        "transfer",
        {
          fromAccountId: task.from.id,
          toAccountId: task.to.id,
          amount,
          description,
        },
        task.from.ownerToken,
      );
      if (result?.status === "completed") completed++;
      else failed++;
    } catch {
      failed++;
    }

    if ((i + 1) % 5000 === 0) {
      const elapsed = (performance.now() - step4) / 1000;
      const rate = Math.round((i + 1) / elapsed);
      console.log(`        Progress: ${(i + 1).toLocaleString()}/${TRANSFER_COUNT.toLocaleString()} (${rate} tx/s)`);
    }
  });

  const step4Time = performance.now() - step4;
  console.log(`        Done in ${(step4Time / 1000).toFixed(1)}s (${Math.round(TRANSFER_COUNT / (step4Time / 1000))} tx/s)`);
  console.log(`        Completed: ${completed.toLocaleString()}, Failed: ${failed.toLocaleString()}`);

  // ── Summary ───────────────────────────────────────────────────────────
  const totalTime = performance.now() - totalStart;

  try {
    const stats = await view("get_stats");
    console.log(`\n  Database Stats`);
    console.log(`  ─────────────────────────`);
    console.log(`  Users:          ${stats.userCount?.toLocaleString()}`);
    console.log(`  Accounts:       ${stats.accountCount?.toLocaleString()}`);
    console.log(`  Transactions:   ${stats.transactionCount?.toLocaleString()}`);
    console.log(`  Total Volume:   $${stats.totalVolume?.toLocaleString(undefined, { minimumFractionDigits: 2 })}`);
    console.log(`  Total Balance:  $${stats.totalBalance?.toLocaleString(undefined, { minimumFractionDigits: 2 })}`);
  } catch {
    // stats view may not be available
  }

  console.log(`\n  Total time: ${(totalTime / 1000).toFixed(1)}s\n`);
}

main();
