/**
 * SQLite Finance Benchmark — standalone Deno server
 *
 * Implements the same HTTP API surface as the flop finance benchmark
 * so the same seed.ts script and index.html dashboard work against both.
 *
 * Usage:
 *   deno run --allow-all benchmarks/sqlite-finance/app.ts
 */

import { DatabaseSync } from "node:sqlite";
import { createHash } from "node:crypto";

// ── Config ──────────────────────────────────────────────────────────────────

const PORT = Number(Deno.args.find((a) => a.startsWith("--port="))?.slice(7) ?? "1985");
const DB_PATH = `${import.meta.dirname}/data/finance.db`;
const JWT_SECRET = "sqlite-bench-secret";

// Ensure data directory exists
await Deno.mkdir(`${import.meta.dirname}/data`, { recursive: true });

// ── Database ────────────────────────────────────────────────────────────────

const db = new DatabaseSync(DB_PATH);
db.exec("PRAGMA journal_mode = WAL");
db.exec("PRAGMA synchronous = NORMAL");
db.exec("PRAGMA cache_size = -64000"); // 64MB cache
db.exec("PRAGMA busy_timeout = 5000");

db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password TEXT NOT NULL,
    name TEXT NOT NULL,
    roles TEXT NOT NULL DEFAULT '[]',
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now','subsec') * 1000)
  );

  CREATE TABLE IF NOT EXISTS accounts (
    id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    name TEXT NOT NULL,
    type TEXT NOT NULL CHECK(type IN ('checking','savings','credit')),
    balance REAL NOT NULL DEFAULT 0,
    currency TEXT NOT NULL DEFAULT 'USD',
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now','subsec') * 1000)
  );

  CREATE TABLE IF NOT EXISTS transactions (
    id TEXT PRIMARY KEY,
    from_account_id TEXT NOT NULL,
    to_account_id TEXT NOT NULL,
    amount REAL NOT NULL,
    currency TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending','completed','failed')),
    description TEXT,
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now','subsec') * 1000)
  );

  CREATE TABLE IF NOT EXISTS ledger (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    amount REAL NOT NULL,
    balance_after REAL NOT NULL,
    type TEXT NOT NULL CHECK(type IN ('debit','credit')),
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now','subsec') * 1000)
  );

  CREATE INDEX IF NOT EXISTS idx_accounts_owner ON accounts(owner_id);
  CREATE INDEX IF NOT EXISTS idx_transactions_from ON transactions(from_account_id);
  CREATE INDEX IF NOT EXISTS idx_transactions_to ON transactions(to_account_id);
  CREATE INDEX IF NOT EXISTS idx_ledger_account ON ledger(account_id);
`);

// ── Prepared statements ─────────────────────────────────────────────────────

const stmts = {
  insertUser: db.prepare("INSERT INTO users (id, email, password, name, roles) VALUES (?, ?, ?, ?, ?)"),
  getUserByEmail: db.prepare("SELECT * FROM users WHERE email = ?"),
  getUserById: db.prepare("SELECT * FROM users WHERE id = ?"),

  insertAccount: db.prepare("INSERT INTO accounts (id, owner_id, name, type, balance, currency) VALUES (?, ?, ?, ?, ?, ?)"),
  getAccount: db.prepare("SELECT * FROM accounts WHERE id = ?"),
  updateBalance: db.prepare("UPDATE accounts SET balance = ? WHERE id = ?"),
  getAccountsByOwner: db.prepare("SELECT * FROM accounts WHERE owner_id = ?"),
  getAllAccounts: db.prepare("SELECT * FROM accounts LIMIT 10000"),

  insertTransaction: db.prepare("INSERT INTO transactions (id, from_account_id, to_account_id, amount, currency, status, description) VALUES (?, ?, ?, ?, ?, ?, ?)"),
  getRecentTransactions: db.prepare("SELECT * FROM transactions ORDER BY created_at DESC LIMIT ?"),
  getTransactionsByAccount: db.prepare("SELECT * FROM transactions WHERE from_account_id = ? OR to_account_id = ? ORDER BY created_at DESC LIMIT 10000"),

  insertLedger: db.prepare("INSERT INTO ledger (id, account_id, transaction_id, amount, balance_after, type) VALUES (?, ?, ?, ?, ?, ?)"),
  getLedgerByAccount: db.prepare("SELECT * FROM ledger WHERE account_id = ? ORDER BY created_at DESC LIMIT 10000"),

  countUsers: db.prepare("SELECT COUNT(*) as count FROM users"),
  countAccounts: db.prepare("SELECT COUNT(*) as count FROM accounts"),
  countTransactions: db.prepare("SELECT COUNT(*) as count FROM transactions"),
  statsTransactions: db.prepare("SELECT status, COUNT(*) as count, SUM(amount) as total FROM transactions GROUP BY status"),
  statsTotalBalance: db.prepare("SELECT SUM(balance) as total FROM accounts"),
};

// ── Helpers ─────────────────────────────────────────────────────────────────

function genId(): string {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let id = "";
  for (let i = 0; i < 15; i++) id += chars[Math.floor(Math.random() * chars.length)];
  return id;
}

function hashPassword(password: string): string {
  // Simple SHA-256 hash for benchmark (not bcrypt — we're comparing DB perf, not auth perf)
  return createHash("sha256").update(password + JWT_SECRET).digest("hex");
}

function verifyPassword(password: string, hash: string): boolean {
  return hashPassword(password) === hash;
}

// Minimal JWT (same format as flop uses)
function createJWT(payload: Record<string, unknown>): string {
  const header = btoa(JSON.stringify({ alg: "HS256", typ: "JWT" })).replace(/=/g, "");
  const body = btoa(JSON.stringify({ ...payload, exp: Date.now() + 3600_000 })).replace(/=/g, "");
  const sig = createHash("sha256").update(`${header}.${body}.${JWT_SECRET}`).digest("base64url");
  return `${header}.${body}.${sig}`;
}

function verifyJWT(token: string): Record<string, unknown> | null {
  try {
    const [header, body, sig] = token.split(".");
    const expected = createHash("sha256").update(`${header}.${body}.${JWT_SECRET}`).digest("base64url");
    if (sig !== expected) return null;
    const payload = JSON.parse(atob(body + "=".repeat((4 - (body.length % 4)) % 4)));
    if (payload.exp && payload.exp < Date.now()) return null;
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

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function addCors(response: Response): Response {
  response.headers.set("Access-Control-Allow-Origin", "*");
  response.headers.set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  response.headers.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  return response;
}

// Row key mapping: SQLite uses snake_case, API uses camelCase
function snakeToCamel(row: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(row)) {
    const camel = k.replace(/_([a-z])/g, (_, c) => c.toUpperCase());
    out[camel] = v;
  }
  return out;
}

// ── PubSub for SSE ──────────────────────────────────────────────────────────

type ChangeEvent = { table: string; op: string; rowId: string; data: Record<string, unknown> };
type Listener = (event: ChangeEvent) => void;

const listeners = new Set<Listener>();

function publish(event: ChangeEvent) {
  for (const fn of listeners) {
    try { fn(event); } catch { /* ignore */ }
  }
}

function subscribe(fn: Listener): () => void {
  listeners.add(fn);
  return () => listeners.delete(fn);
}

// ── Reducers ────────────────────────────────────────────────────────────────

function createAccount(userId: string, name: string, type: string, currency: string) {
  const id = genId();
  stmts.insertAccount.run(id, userId, name, type, 0, currency || "USD");
  const row = snakeToCamel(stmts.getAccount.get(id) as Record<string, unknown>);
  publish({ table: "accounts", op: "insert", rowId: id, data: row });
  return row;
}

function deposit(accountId: string, amount: number) {
  if (amount <= 0) throw new Error("Amount must be positive");
  const account = stmts.getAccount.get(accountId) as Record<string, unknown> | undefined;
  if (!account) throw new Error("Account not found");

  const newBalance = (account.balance as number) + amount;
  stmts.updateBalance.run(newBalance, accountId);

  const txId = genId();
  stmts.insertTransaction.run(txId, "EXTERNAL", accountId, amount, account.currency as string, "completed", "Deposit");

  const ledgerId = genId();
  stmts.insertLedger.run(ledgerId, accountId, txId, amount, newBalance, "credit");

  publish({ table: "accounts", op: "update", rowId: accountId, data: { ...snakeToCamel(account), balance: newBalance } });
  publish({ table: "transactions", op: "insert", rowId: txId, data: { id: txId, fromAccountId: "EXTERNAL", toAccountId: accountId, amount, currency: account.currency, status: "completed", description: "Deposit", createdAt: Date.now() } });
  publish({ table: "ledger", op: "insert", rowId: ledgerId, data: { id: ledgerId, accountId, transactionId: txId, amount, balanceAfter: newBalance, type: "credit", createdAt: Date.now() } });

  return { balance: newBalance, transactionId: txId };
}

const transferStmt = db.prepare(
  "SELECT id, balance, currency FROM accounts WHERE id = ?",
);

function transfer(_userId: string, fromAccountId: string, toAccountId: string, amount: number, description: string) {
  if (amount <= 0) throw new Error("Amount must be positive");
  if (fromAccountId === toAccountId) throw new Error("Cannot transfer to same account");

  // Manual transaction for atomicity
  db.exec("BEGIN");
  try {
    const fromAccount = transferStmt.get(fromAccountId) as { id: string; balance: number; currency: string } | undefined;
    if (!fromAccount) { db.exec("ROLLBACK"); throw new Error("Source account not found"); }

    const toAccount = transferStmt.get(toAccountId) as { id: string; balance: number; currency: string } | undefined;
    if (!toAccount) { db.exec("ROLLBACK"); throw new Error("Destination account not found"); }

    const txId = genId();

    if (fromAccount.balance < amount) {
      stmts.insertTransaction.run(txId, fromAccountId, toAccountId, amount, fromAccount.currency, "failed", description || "Transfer (insufficient funds)");
      db.exec("COMMIT");
      publish({ table: "transactions", op: "insert", rowId: txId, data: { id: txId, fromAccountId, toAccountId, amount, currency: fromAccount.currency, status: "failed", description: description || "Transfer (insufficient funds)", createdAt: Date.now() } });
      return { status: "failed", reason: "insufficient_funds", transactionId: txId };
    }

    const newFromBalance = fromAccount.balance - amount;
    const newToBalance = toAccount.balance + amount;

    stmts.updateBalance.run(newFromBalance, fromAccountId);
    stmts.updateBalance.run(newToBalance, toAccountId);

    stmts.insertTransaction.run(txId, fromAccountId, toAccountId, amount, fromAccount.currency, "completed", description || "Transfer");

    const debitId = genId();
    const creditId = genId();
    stmts.insertLedger.run(debitId, fromAccountId, txId, -amount, newFromBalance, "debit");
    stmts.insertLedger.run(creditId, toAccountId, txId, amount, newToBalance, "credit");

    db.exec("COMMIT");

    // Publish events after commit
    publish({ table: "accounts", op: "update", rowId: fromAccountId, data: { id: fromAccountId, balance: newFromBalance } });
    publish({ table: "accounts", op: "update", rowId: toAccountId, data: { id: toAccountId, balance: newToBalance } });
    publish({ table: "transactions", op: "insert", rowId: txId, data: { id: txId, fromAccountId, toAccountId, amount, currency: fromAccount.currency, status: "completed", description: description || "Transfer", createdAt: Date.now() } });
    publish({ table: "ledger", op: "insert", rowId: debitId, data: { id: debitId, accountId: fromAccountId, transactionId: txId, amount: -amount, balanceAfter: newFromBalance, type: "debit", createdAt: Date.now() } });
    publish({ table: "ledger", op: "insert", rowId: creditId, data: { id: creditId, accountId: toAccountId, transactionId: txId, amount, balanceAfter: newToBalance, type: "credit", createdAt: Date.now() } });

    return { status: "completed", transactionId: txId };
  } catch (err) {
    try { db.exec("ROLLBACK"); } catch { /* already rolled back */ }
    throw err;
  }
}

// ── Views ───────────────────────────────────────────────────────────────────

function getStats() {
  const userCount = (stmts.countUsers.get() as { count: number }).count;
  const accountCount = (stmts.countAccounts.get() as { count: number }).count;
  const transactionCount = (stmts.countTransactions.get() as { count: number }).count;

  let completedTransactions = 0;
  let failedTransactions = 0;
  let totalVolume = 0;

  const statsRows = stmts.statsTransactions.all() as { status: string; count: number; total: number }[];
  for (const row of statsRows) {
    if (row.status === "completed") {
      completedTransactions = row.count;
      totalVolume = row.total ?? 0;
    } else if (row.status === "failed") {
      failedTransactions = row.count;
    }
  }

  const totalBalance = (stmts.statsTotalBalance.get() as { total: number })?.total ?? 0;

  return { userCount, accountCount, transactionCount, completedTransactions, failedTransactions, totalVolume, totalBalance };
}

// ── SSE Handler ─────────────────────────────────────────────────────────────

function handleSSE(req: Request, tables: string[]): Response {
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    start(controller) {
      let closed = false;

      const enqueue = (eventType: string, payload: unknown) => {
        if (closed) return;
        try {
          controller.enqueue(encoder.encode(`event: ${eventType}\ndata: ${JSON.stringify(payload)}\n\n`));
        } catch { /* closed */ }
      };

      // Send initial snapshot
      const url = new URL(req.url);
      const viewName = url.pathname.split("/").pop();
      if (viewName === "get_stats") {
        enqueue("snapshot", getStats());
      } else if (viewName === "get_recent_transactions") {
        const limit = Number(url.searchParams.get("limit")) || 50;
        const rows = (stmts.getRecentTransactions.all(limit) as Record<string, unknown>[]).map(snakeToCamel);
        enqueue("snapshot", rows);
      } else if (viewName === "get_all_accounts") {
        const rows = (stmts.getAllAccounts.all() as Record<string, unknown>[]).map(snakeToCamel);
        enqueue("snapshot", rows);
      }

      // Subscribe to changes
      const unsub = subscribe((event) => {
        if (tables.length === 0 || tables.includes(event.table)) {
          enqueue("change", event);
        }
      });

      req.signal.addEventListener("abort", () => {
        closed = true;
        unsub();
        try { controller.close(); } catch { /* already closed */ }
      });
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "Connection": "keep-alive",
    },
  });
}

// ── HTTP Server ─────────────────────────────────────────────────────────────

async function handler(req: Request): Promise<Response> {
  const url = new URL(req.url);
  const pathname = url.pathname;

  if (req.method === "OPTIONS") {
    return addCors(new Response(null, { status: 204 }));
  }

  try {
    // Auth endpoints
    if (pathname === "/api/auth/register" && req.method === "POST") {
      const body = await req.json();
      const { email, password, name } = body;
      if (!email || !password) return addCors(jsonResponse({ error: "Email and password required" }, 400));

      const existing = stmts.getUserByEmail.get(email);
      if (existing) return addCors(jsonResponse({ error: "Email already registered" }, 400));

      const id = genId();
      const hash = hashPassword(password);
      stmts.insertUser.run(id, email, hash, name || "", "[]");

      const token = createJWT({ sub: id, email, roles: [] });
      const user = { id, email, name: name || "", roles: [] };
      publish({ table: "users", op: "insert", rowId: id, data: user });
      return addCors(jsonResponse({ token, user }));
    }

    if (pathname === "/api/auth/password" && req.method === "POST") {
      const body = await req.json();
      const { email, password } = body;
      if (!email || !password) return addCors(jsonResponse({ error: "Email and password required" }, 400));

      const user = stmts.getUserByEmail.get(email) as Record<string, unknown> | undefined;
      if (!user || !verifyPassword(password, user.password as string)) {
        return addCors(jsonResponse({ error: "Invalid credentials" }, 400));
      }

      const roles = JSON.parse((user.roles as string) || "[]");
      const token = createJWT({ sub: user.id, email: user.email, roles });
      const refreshToken = createJWT({ sub: user.id, type: "refresh" });
      return addCors(jsonResponse({ token, refreshToken, user: { id: user.id, email: user.email, name: user.name, roles } }));
    }

    if (pathname === "/api/auth/refresh" && req.method === "POST") {
      const body = await req.json();
      const { refreshToken } = body;
      if (!refreshToken) return addCors(jsonResponse({ error: "Refresh token required" }, 400));
      const payload = verifyJWT(refreshToken);
      if (!payload) return addCors(jsonResponse({ error: "Invalid refresh token" }, 401));

      const user = stmts.getUserById.get(payload.sub as string) as Record<string, unknown> | undefined;
      if (!user) return addCors(jsonResponse({ error: "User not found" }, 404));

      const roles = JSON.parse((user.roles as string) || "[]");
      const token = createJWT({ sub: user.id, email: user.email, roles });
      return addCors(jsonResponse({ token }));
    }

    // Auth helper
    const getAuth = () => {
      const bearerToken = extractBearerToken(req);
      if (!bearerToken) return null;
      const payload = verifyJWT(bearerToken);
      if (!payload) return null;
      return { id: payload.sub as string, roles: (payload.roles as string[]) || [] };
    };

    // SSE: views with Accept: text/event-stream
    if (req.headers.get("accept") === "text/event-stream") {
      if (pathname === "/api/view/get_stats") {
        return addCors(handleSSE(req, ["users", "accounts", "transactions", "ledger"]));
      }
      if (pathname === "/api/view/get_recent_transactions") {
        return addCors(handleSSE(req, ["transactions"]));
      }
      if (pathname === "/api/view/get_all_accounts") {
        return addCors(handleSSE(req, ["accounts"]));
      }
    }

    // Views (GET)
    if (pathname.startsWith("/api/view/") && req.method === "GET") {
      const viewName = pathname.slice(10);
      const auth = getAuth();

      if (viewName === "get_stats") {
        return addCors(jsonResponse({ ok: true, data: getStats() }));
      }

      if (viewName === "get_all_accounts") {
        const rows = (stmts.getAllAccounts.all() as Record<string, unknown>[]).map(snakeToCamel);
        return addCors(jsonResponse({ ok: true, data: rows }));
      }

      if (viewName === "get_accounts") {
        if (!auth) return addCors(jsonResponse({ error: "Authentication required" }, 401));
        const rows = (stmts.getAccountsByOwner.all(auth.id) as Record<string, unknown>[]).map(snakeToCamel);
        return addCors(jsonResponse({ ok: true, data: rows }));
      }

      if (viewName === "get_recent_transactions") {
        const limit = Number(url.searchParams.get("limit")) || 100;
        const rows = (stmts.getRecentTransactions.all(limit) as Record<string, unknown>[]).map(snakeToCamel);
        return addCors(jsonResponse({ ok: true, data: rows }));
      }

      if (viewName === "get_transactions") {
        if (!auth) return addCors(jsonResponse({ error: "Authentication required" }, 401));
        const accountId = url.searchParams.get("accountId") || "";
        const rows = (stmts.getTransactionsByAccount.all(accountId, accountId) as Record<string, unknown>[]).map(snakeToCamel);
        return addCors(jsonResponse({ ok: true, data: rows }));
      }

      if (viewName === "get_ledger") {
        if (!auth) return addCors(jsonResponse({ error: "Authentication required" }, 401));
        const accountId = url.searchParams.get("accountId") || "";
        const rows = (stmts.getLedgerByAccount.all(accountId) as Record<string, unknown>[]).map(snakeToCamel);
        return addCors(jsonResponse({ ok: true, data: rows }));
      }

      return addCors(jsonResponse({ error: "Unknown view" }, 404));
    }

    // Reducers (POST)
    if (pathname.startsWith("/api/reduce/") && req.method === "POST") {
      const reducerName = pathname.slice(12);
      const body = await req.json();
      const auth = getAuth();
      if (!auth) return addCors(jsonResponse({ error: "Authentication required" }, 401));

      if (reducerName === "create_account") {
        const result = createAccount(auth.id, body.name, body.type, body.currency);
        return addCors(jsonResponse({ ok: true, data: result }));
      }

      if (reducerName === "deposit") {
        const result = deposit(body.accountId, body.amount);
        return addCors(jsonResponse({ ok: true, data: result }));
      }

      if (reducerName === "transfer") {
        const result = transfer(auth.id, body.fromAccountId, body.toAccountId, body.amount, body.description);
        return addCors(jsonResponse({ ok: true, data: result }));
      }

      return addCors(jsonResponse({ error: "Unknown reducer" }, 404));
    }

    // Schema endpoint (for compatibility)
    if (pathname === "/_schema" && req.method === "GET") {
      return addCors(jsonResponse({}));
    }

    return addCors(jsonResponse({ error: "Not found" }, 404));
  } catch (err) {
    const message = err instanceof Error ? err.message : "Internal server error";
    return addCors(jsonResponse({ error: message }, 400));
  }
}

// ── Start ───────────────────────────────────────────────────────────────────

console.log(`
┌─────────────────────────────────────┐
│   SQLite Finance Benchmark          │
│   Server:  http://localhost:${String(PORT).padEnd(7)} │
│   DB:      ${DB_PATH.split("/").slice(-2).join("/").padEnd(25)}│
│   Engine:  node:sqlite (WAL mode)   │
└─────────────────────────────────────┘
`);

// Graceful shutdown
const shutdown = () => {
  console.log("\nShutting down...");
  db.close();
  Deno.exit(0);
};
Deno.addSignalListener("SIGINT", shutdown);
Deno.addSignalListener("SIGTERM", shutdown);

Deno.serve({ port: PORT }, handler);
