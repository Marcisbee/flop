/**
 * Turso/libSQL Finance Benchmark server.
 *
 * API-compatible with the workload harness in benchmarks/compare/workload.ts.
 */

import { createHash } from "node:crypto";
import { createClient } from "npm:@libsql/client";

type SQLArg = string | number | null;
type JsonRecord = Record<string, unknown>;

type UserRow = {
  id: string;
  email: string;
  password: string;
  name: string;
  roles: string;
};

type AccountRow = {
  id: string;
  owner_id: string;
  name: string;
  type: string;
  balance: number;
  currency: string;
};

const PORT = Number(
  Deno.args.find((a) => a.startsWith("--port="))?.slice(7) ?? "1985",
);
const DB_PATH = `${import.meta.dirname}/data/finance.db`;
const DB_URL = `file:${DB_PATH}`;
const JWT_SECRET = "turso-bench-secret";

await Deno.mkdir(`${import.meta.dirname}/data`, { recursive: true });

const db = createClient({ url: DB_URL });

async function run(sql: string, args: SQLArg[] = []): Promise<void> {
  await db.execute({ sql, args });
}

async function one<T>(sql: string, args: SQLArg[] = []): Promise<T | null> {
  const result = await db.execute({ sql, args });
  return (result.rows[0] ?? null) as T | null;
}

async function all<T>(sql: string, args: SQLArg[] = []): Promise<T[]> {
  const result = await db.execute({ sql, args });
  return (result.rows ?? []) as T[];
}

async function withTx<T>(fn: () => Promise<T>): Promise<T> {
  await run("BEGIN IMMEDIATE");
  try {
    const out = await fn();
    await run("COMMIT");
    return out;
  } catch (err) {
    try {
      await run("ROLLBACK");
    } catch {
      // ignore rollback failure
    }
    throw err;
  }
}

function asNumber(value: unknown): number {
  if (typeof value === "number") return value;
  if (typeof value === "bigint") return Number(value);
  const n = Number(value ?? 0);
  return Number.isFinite(n) ? n : 0;
}

function genId(): string {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let id = "";
  for (let i = 0; i < 15; i++) {
    id += chars[Math.floor(Math.random() * chars.length)];
  }
  return id;
}

function hashPassword(password: string): string {
  return createHash("sha256").update(password + JWT_SECRET).digest("hex");
}

function verifyPassword(password: string, hash: string): boolean {
  return hashPassword(password) === hash;
}

function createJWT(payload: JsonRecord): string {
  const header = btoa(JSON.stringify({ alg: "HS256", typ: "JWT" })).replace(
    /=/g,
    "",
  );
  const body = btoa(JSON.stringify({ ...payload, exp: Date.now() + 3600_000 }))
    .replace(/=/g, "");
  const sig = createHash("sha256").update(`${header}.${body}.${JWT_SECRET}`)
    .digest("base64url");
  return `${header}.${body}.${sig}`;
}

function verifyJWT(token: string): JsonRecord | null {
  try {
    const [header, body, sig] = token.split(".");
    const expected = createHash("sha256").update(
      `${header}.${body}.${JWT_SECRET}`,
    ).digest("base64url");
    if (sig !== expected) return null;
    const payload = JSON.parse(
      atob(body + "=".repeat((4 - (body.length % 4)) % 4)),
    ) as JsonRecord;
    if (typeof payload.exp === "number" && payload.exp < Date.now()) {
      return null;
    }
    return payload;
  } catch {
    return null;
  }
}

function extractBearerToken(req: Request): string | null {
  const auth = req.headers.get("Authorization");
  if (!auth?.startsWith("Bearer ")) return null;
  return auth.slice(7);
}

function snakeToCamel(row: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(row)) {
    out[k.replace(/_([a-z])/g, (_, c) => c.toUpperCase())] = v;
  }
  return out;
}

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function addCors(response: Response): Response {
  response.headers.set("Access-Control-Allow-Origin", "*");
  response.headers.set(
    "Access-Control-Allow-Methods",
    "GET, POST, PUT, DELETE, OPTIONS",
  );
  response.headers.set(
    "Access-Control-Allow-Headers",
    "Content-Type, Authorization",
  );
  return response;
}

await run("PRAGMA journal_mode = WAL");
await run("PRAGMA synchronous = NORMAL");
await run("PRAGMA cache_size = -64000");
await run("PRAGMA busy_timeout = 5000");

await run(`
  CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password TEXT NOT NULL,
    name TEXT NOT NULL,
    roles TEXT NOT NULL DEFAULT '[]',
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now','subsec') * 1000)
  )
`);
await run(`
  CREATE TABLE IF NOT EXISTS accounts (
    id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    name TEXT NOT NULL,
    type TEXT NOT NULL CHECK(type IN ('checking','savings','credit')),
    balance REAL NOT NULL DEFAULT 0,
    currency TEXT NOT NULL DEFAULT 'USD',
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now','subsec') * 1000)
  )
`);
await run(`
  CREATE TABLE IF NOT EXISTS transactions (
    id TEXT PRIMARY KEY,
    from_account_id TEXT NOT NULL,
    to_account_id TEXT NOT NULL,
    amount REAL NOT NULL,
    currency TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending','completed','failed')),
    description TEXT,
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now','subsec') * 1000)
  )
`);
await run(`
  CREATE TABLE IF NOT EXISTS ledger (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    amount REAL NOT NULL,
    balance_after REAL NOT NULL,
    type TEXT NOT NULL CHECK(type IN ('debit','credit')),
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now','subsec') * 1000)
  )
`);
await run(
  "CREATE INDEX IF NOT EXISTS idx_accounts_owner ON accounts(owner_id)",
);
await run(
  "CREATE INDEX IF NOT EXISTS idx_transactions_from ON transactions(from_account_id)",
);
await run(
  "CREATE INDEX IF NOT EXISTS idx_transactions_to ON transactions(to_account_id)",
);
await run(
  "CREATE INDEX IF NOT EXISTS idx_ledger_account ON ledger(account_id)",
);

async function getStats() {
  const userCount = asNumber(
    (await one<{ count: number }>("SELECT COUNT(*) AS count FROM users"))
      ?.count,
  );
  const accountCount = asNumber(
    (await one<{ count: number }>("SELECT COUNT(*) AS count FROM accounts"))
      ?.count,
  );
  const transactionCount = asNumber(
    (await one<{ count: number }>("SELECT COUNT(*) AS count FROM transactions"))
      ?.count,
  );

  let completedTransactions = 0;
  let failedTransactions = 0;
  let totalVolume = 0;

  const byStatus = await all<{ status: string; count: number; total: number }>(
    "SELECT status, COUNT(*) AS count, SUM(amount) AS total FROM transactions GROUP BY status",
  );
  for (const row of byStatus) {
    if (row.status === "completed") {
      completedTransactions = asNumber(row.count);
      totalVolume = asNumber(row.total);
    } else if (row.status === "failed") {
      failedTransactions = asNumber(row.count);
    }
  }

  const totalBalance = asNumber(
    (await one<{ total: number }>("SELECT SUM(balance) AS total FROM accounts"))
      ?.total,
  );

  return {
    userCount,
    accountCount,
    transactionCount,
    completedTransactions,
    failedTransactions,
    totalVolume,
    totalBalance,
  };
}

async function createAccount(
  userId: string,
  name: string,
  type: string,
  currency: string,
) {
  const id = genId();
  await run(
    "INSERT INTO accounts (id, owner_id, name, type, balance, currency) VALUES (?, ?, ?, ?, ?, ?)",
    [id, userId, name, type, 0, currency || "USD"],
  );
  const row = await one<Record<string, unknown>>(
    "SELECT * FROM accounts WHERE id = ?",
    [id],
  );
  if (!row) throw new Error("Failed to create account");
  return snakeToCamel(row);
}

async function deposit(accountId: string, amount: number) {
  if (amount <= 0) throw new Error("Amount must be positive");

  return await withTx(async () => {
    const account = await one<AccountRow>(
      "SELECT * FROM accounts WHERE id = ?",
      [accountId],
    );
    if (!account) throw new Error("Account not found");

    const newBalance = asNumber(account.balance) + amount;
    await run("UPDATE accounts SET balance = ? WHERE id = ?", [
      newBalance,
      accountId,
    ]);

    const txId = genId();
    await run(
      "INSERT INTO transactions (id, from_account_id, to_account_id, amount, currency, status, description) VALUES (?, ?, ?, ?, ?, ?, ?)",
      [
        txId,
        "EXTERNAL",
        accountId,
        amount,
        account.currency,
        "completed",
        "Deposit",
      ],
    );

    const ledgerId = genId();
    await run(
      "INSERT INTO ledger (id, account_id, transaction_id, amount, balance_after, type) VALUES (?, ?, ?, ?, ?, ?)",
      [ledgerId, accountId, txId, amount, newBalance, "credit"],
    );

    return { balance: newBalance, transactionId: txId };
  });
}

async function transfer(
  fromAccountId: string,
  toAccountId: string,
  amount: number,
  description: string,
) {
  if (amount <= 0) throw new Error("Amount must be positive");
  if (fromAccountId === toAccountId) {
    throw new Error("Cannot transfer to same account");
  }

  return await withTx(async () => {
    const fromAccount = await one<AccountRow>(
      "SELECT id, balance, currency FROM accounts WHERE id = ?",
      [fromAccountId],
    );
    if (!fromAccount) throw new Error("Source account not found");

    const toAccount = await one<AccountRow>(
      "SELECT id, balance, currency FROM accounts WHERE id = ?",
      [toAccountId],
    );
    if (!toAccount) throw new Error("Destination account not found");

    const txId = genId();

    if (asNumber(fromAccount.balance) < amount) {
      await run(
        "INSERT INTO transactions (id, from_account_id, to_account_id, amount, currency, status, description) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
          txId,
          fromAccountId,
          toAccountId,
          amount,
          fromAccount.currency,
          "failed",
          description || "Transfer (insufficient funds)",
        ],
      );
      return {
        status: "failed",
        reason: "insufficient_funds",
        transactionId: txId,
      };
    }

    const newFromBalance = asNumber(fromAccount.balance) - amount;
    const newToBalance = asNumber(toAccount.balance) + amount;

    await run("UPDATE accounts SET balance = ? WHERE id = ?", [
      newFromBalance,
      fromAccountId,
    ]);
    await run("UPDATE accounts SET balance = ? WHERE id = ?", [
      newToBalance,
      toAccountId,
    ]);

    await run(
      "INSERT INTO transactions (id, from_account_id, to_account_id, amount, currency, status, description) VALUES (?, ?, ?, ?, ?, ?, ?)",
      [
        txId,
        fromAccountId,
        toAccountId,
        amount,
        fromAccount.currency,
        "completed",
        description || "Transfer",
      ],
    );

    await run(
      "INSERT INTO ledger (id, account_id, transaction_id, amount, balance_after, type) VALUES (?, ?, ?, ?, ?, ?)",
      [genId(), fromAccountId, txId, -amount, newFromBalance, "debit"],
    );
    await run(
      "INSERT INTO ledger (id, account_id, transaction_id, amount, balance_after, type) VALUES (?, ?, ?, ?, ?, ?)",
      [genId(), toAccountId, txId, amount, newToBalance, "credit"],
    );

    return { status: "completed", transactionId: txId };
  });
}

async function handler(req: Request): Promise<Response> {
  const url = new URL(req.url);
  const pathname = url.pathname;

  if (req.method === "OPTIONS") {
    return addCors(new Response(null, { status: 204 }));
  }

  const getAuth = () => {
    const token = extractBearerToken(req);
    if (!token) return null;
    const payload = verifyJWT(token);
    if (!payload || typeof payload.sub !== "string") return null;
    return {
      id: payload.sub,
      roles: Array.isArray(payload.roles) ? payload.roles : [],
    };
  };

  try {
    if (pathname === "/api/auth/register" && req.method === "POST") {
      const body = await req.json().catch(() => ({}));
      const email = String(body?.email ?? "").trim();
      const password = String(body?.password ?? "");
      const name = String(body?.name ?? "");

      if (!email || !password) {
        return addCors(
          jsonResponse({ error: "Email and password required" }, 400),
        );
      }

      const existing = await one<UserRow>(
        "SELECT * FROM users WHERE email = ?",
        [email],
      );
      if (existing) {
        return addCors(
          jsonResponse({ error: "Email already registered" }, 400),
        );
      }

      const id = genId();
      const hash = hashPassword(password);
      await run(
        "INSERT INTO users (id, email, password, name, roles) VALUES (?, ?, ?, ?, ?)",
        [id, email, hash, name, "[]"],
      );

      const token = createJWT({ sub: id, email, roles: [] });
      return addCors(
        jsonResponse({ token, user: { id, email, name, roles: [] } }),
      );
    }

    if (pathname === "/api/auth/password" && req.method === "POST") {
      const body = await req.json().catch(() => ({}));
      const email = String(body?.email ?? "").trim();
      const password = String(body?.password ?? "");
      if (!email || !password) {
        return addCors(
          jsonResponse({ error: "Email and password required" }, 400),
        );
      }

      const user = await one<UserRow>("SELECT * FROM users WHERE email = ?", [
        email,
      ]);
      if (!user || !verifyPassword(password, user.password)) {
        return addCors(jsonResponse({ error: "Invalid credentials" }, 400));
      }

      const roles = JSON.parse(user.roles || "[]");
      const token = createJWT({ sub: user.id, email: user.email, roles });
      const refreshToken = createJWT({ sub: user.id, type: "refresh" });
      return addCors(
        jsonResponse({
          token,
          refreshToken,
          user: { id: user.id, email: user.email, name: user.name, roles },
        }),
      );
    }

    if (pathname === "/api/auth/refresh" && req.method === "POST") {
      const body = await req.json().catch(() => ({}));
      const refreshToken = String(body?.refreshToken ?? "");
      if (!refreshToken) {
        return addCors(jsonResponse({ error: "Refresh token required" }, 400));
      }
      const payload = verifyJWT(refreshToken);
      if (!payload || typeof payload.sub !== "string") {
        return addCors(jsonResponse({ error: "Invalid refresh token" }, 401));
      }

      const user = await one<UserRow>("SELECT * FROM users WHERE id = ?", [
        payload.sub,
      ]);
      if (!user) return addCors(jsonResponse({ error: "User not found" }, 404));

      const roles = JSON.parse(user.roles || "[]");
      const token = createJWT({ sub: user.id, email: user.email, roles });
      return addCors(jsonResponse({ token }));
    }

    if (pathname.startsWith("/api/view/") && req.method === "GET") {
      const auth = getAuth();
      const viewName = pathname.slice("/api/view/".length);

      if (viewName === "get_stats") {
        return addCors(jsonResponse({ ok: true, data: await getStats() }));
      }

      if (viewName === "get_all_accounts") {
        const rows = await all<Record<string, unknown>>(
          "SELECT * FROM accounts LIMIT 10000",
        );
        return addCors(
          jsonResponse({ ok: true, data: rows.map(snakeToCamel) }),
        );
      }

      if (viewName === "get_accounts") {
        if (!auth) {
          return addCors(
            jsonResponse({ error: "Authentication required" }, 401),
          );
        }
        const rows = await all<Record<string, unknown>>(
          "SELECT * FROM accounts WHERE owner_id = ? ORDER BY created_at DESC LIMIT 10000",
          [auth.id],
        );
        return addCors(
          jsonResponse({ ok: true, data: rows.map(snakeToCamel) }),
        );
      }

      if (viewName === "get_recent_transactions") {
        const limit = Math.max(
          1,
          Number(url.searchParams.get("limit") ?? "100"),
        );
        const rows = await all<Record<string, unknown>>(
          "SELECT * FROM transactions ORDER BY created_at DESC LIMIT ?",
          [limit],
        );
        return addCors(
          jsonResponse({ ok: true, data: rows.map(snakeToCamel) }),
        );
      }

      if (viewName === "get_transactions") {
        if (!auth) {
          return addCors(
            jsonResponse({ error: "Authentication required" }, 401),
          );
        }
        const accountId = String(url.searchParams.get("accountId") ?? "");
        const rows = await all<Record<string, unknown>>(
          "SELECT * FROM transactions WHERE from_account_id = ? OR to_account_id = ? ORDER BY created_at DESC LIMIT 10000",
          [accountId, accountId],
        );
        return addCors(
          jsonResponse({ ok: true, data: rows.map(snakeToCamel) }),
        );
      }

      if (viewName === "get_ledger") {
        if (!auth) {
          return addCors(
            jsonResponse({ error: "Authentication required" }, 401),
          );
        }
        const accountId = String(url.searchParams.get("accountId") ?? "");
        const rows = await all<Record<string, unknown>>(
          "SELECT * FROM ledger WHERE account_id = ? ORDER BY created_at DESC LIMIT 10000",
          [accountId],
        );
        return addCors(
          jsonResponse({ ok: true, data: rows.map(snakeToCamel) }),
        );
      }

      return addCors(jsonResponse({ error: "Unknown view" }, 404));
    }

    if (pathname.startsWith("/api/reduce/") && req.method === "POST") {
      const auth = getAuth();
      if (!auth) {
        return addCors(jsonResponse({ error: "Authentication required" }, 401));
      }

      const reducerName = pathname.slice("/api/reduce/".length);
      const body = await req.json().catch(() => ({}));

      if (reducerName === "create_account") {
        const result = await createAccount(
          auth.id,
          String(body?.name ?? ""),
          String(body?.type ?? "checking"),
          String(body?.currency ?? "USD"),
        );
        return addCors(jsonResponse({ ok: true, data: result }));
      }

      if (reducerName === "deposit") {
        const result = await deposit(
          String(body?.accountId ?? ""),
          Number(body?.amount ?? 0),
        );
        return addCors(jsonResponse({ ok: true, data: result }));
      }

      if (reducerName === "transfer") {
        const result = await transfer(
          String(body?.fromAccountId ?? ""),
          String(body?.toAccountId ?? ""),
          Number(body?.amount ?? 0),
          String(body?.description ?? ""),
        );
        return addCors(jsonResponse({ ok: true, data: result }));
      }

      return addCors(jsonResponse({ error: "Unknown reducer" }, 404));
    }

    if (pathname === "/_schema" && req.method === "GET") {
      return addCors(jsonResponse({}));
    }

    return addCors(jsonResponse({ error: "Not found" }, 404));
  } catch (err) {
    const message = err instanceof Error
      ? err.message
      : "Internal server error";
    return addCors(jsonResponse({ error: message }, 400));
  }
}

console.log(`\n[Turso Benchmark] http://localhost:${PORT} (${DB_URL})`);

const shutdown = () => {
  try {
    (db as { close?: () => void }).close?.();
  } catch {
    // ignore
  }
  Deno.exit(0);
};
if (Deno.build.os !== "windows") {
  Deno.addSignalListener("SIGINT", shutdown);
  Deno.addSignalListener("SIGTERM", shutdown);
}

Deno.serve({ port: PORT }, handler);
