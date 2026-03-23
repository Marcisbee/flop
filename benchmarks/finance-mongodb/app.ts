/**
 * MongoDB Finance Benchmark server.
 *
 * API-compatible with the workload harness in benchmarks/compare/workload.ts.
 * If --mongo-uri is not provided, this process starts a local mongod instance.
 */

import { createHash } from "node:crypto";
import { MongoClient } from "npm:mongodb";

type JsonRecord = Record<string, unknown>;
type AnyDoc = Record<string, unknown>;

type UserDoc = {
  id: string;
  email: string;
  password: string;
  name: string;
  roles: string[];
  createdAt: number;
};

type AccountDoc = {
  id: string;
  ownerId: string;
  name: string;
  type: "checking" | "savings" | "credit";
  balance: number;
  currency: string;
  createdAt: number;
};

type TransactionDoc = {
  id: string;
  fromAccountId: string;
  toAccountId: string;
  amount: number;
  currency: string;
  status: "pending" | "completed" | "failed";
  description?: string;
  createdAt: number;
};

type LedgerDoc = {
  id: string;
  accountId: string;
  transactionId: string;
  amount: number;
  balanceAfter: number;
  type: "debit" | "credit";
  createdAt: number;
};

const PORT = Number(arg("port", "1985"));
const MONGO_PORT = Number(arg("mongo-port", String(PORT + 1000)));
const MONGO_URI = arg("mongo-uri", "");
const MONGOD_BIN = arg("mongod-bin", defaultMongodBin());
const DB_NAME = arg("mongo-db", "finance_bench");
const DATA_DIR = `${import.meta.dirname}/data`;
const MONGO_DATA_DIR = arg("mongo-dir", `${DATA_DIR}/mongod`);
const MONGO_LOG_PATH = arg("mongo-log", `${DATA_DIR}/mongod.log`);
const JWT_SECRET = "mongodb-bench-secret";

await Deno.mkdir(DATA_DIR, { recursive: true });
await Deno.mkdir(MONGO_DATA_DIR, { recursive: true });

let mongodProc: Deno.ChildProcess | null = null;
const mongoUri = MONGO_URI || `mongodb://127.0.0.1:${MONGO_PORT}`;

if (!MONGO_URI) {
  mongodProc = startMongod();
}

const client = new MongoClient(mongoUri, {
  maxPoolSize: 256,
  minPoolSize: 4,
});
await waitForMongo(client);

const db = client.db(DB_NAME);
const users = db.collection<UserDoc>("users");
const accounts = db.collection<AccountDoc>("accounts");
const transactions = db.collection<TransactionDoc>("transactions");
const ledger = db.collection<LedgerDoc>("ledger");

await ensureIndexes();

function arg(name: string, fallback: string): string {
  const prefix = `--${name}=`;
  const found = Deno.args.find((x) => x.startsWith(prefix));
  return found ? found.slice(prefix.length) : fallback;
}

function defaultMongodBin(): string {
  const env = Deno.env.get("MONGOD_BIN");
  if (env && env.trim() !== "") return env.trim();
  const local = `${import.meta.dirname}/../.tools/mongodb/mongod`;
  try {
    Deno.statSync(local);
    return local;
  } catch {
    return "mongod";
  }
}

function genId(): string {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let out = "";
  for (let i = 0; i < 15; i++) {
    out += chars[Math.floor(Math.random() * chars.length)];
  }
  return out;
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

function asNumber(v: unknown): number {
  const n = Number(v ?? 0);
  return Number.isFinite(n) ? n : 0;
}

function stripMongoId<T extends AnyDoc>(doc: T): T {
  const out = { ...doc };
  delete out._id;
  return out as T;
}

function ensureType(v: string): "checking" | "savings" | "credit" {
  if (v === "checking" || v === "savings" || v === "credit") return v;
  return "checking";
}

async function ensureIndexes() {
  await users.createIndex({ id: 1 }, { unique: true });
  await users.createIndex({ email: 1 }, { unique: true });
  await accounts.createIndex({ id: 1 }, { unique: true });
  await accounts.createIndex({ ownerId: 1 });
  await transactions.createIndex({ id: 1 }, { unique: true });
  await transactions.createIndex({ fromAccountId: 1 });
  await transactions.createIndex({ toAccountId: 1 });
  await transactions.createIndex({ createdAt: -1 });
  await ledger.createIndex({ id: 1 }, { unique: true });
  await ledger.createIndex({ accountId: 1 });
  await ledger.createIndex({ createdAt: -1 });
}

function startMongod(): Deno.ChildProcess {
  try {
    return new Deno.Command(MONGOD_BIN, {
      args: [
        "--bind_ip",
        "127.0.0.1",
        "--port",
        String(MONGO_PORT),
        "--dbpath",
        MONGO_DATA_DIR,
        "--logpath",
        MONGO_LOG_PATH,
        "--quiet",
      ],
      stdout: "null",
      stderr: "null",
    }).spawn();
  } catch (err) {
    if (err instanceof Deno.errors.NotFound) {
      throw new Error(
        `mongod not found. Install MongoDB or set --mongod-bin=... (looked for "${MONGOD_BIN}")`,
      );
    }
    throw err;
  }
}

async function waitForMongo(mongo: MongoClient, timeoutMs = 30000) {
  const start = Date.now();
  let lastErr: unknown = null;
  while (Date.now() - start < timeoutMs) {
    try {
      await mongo.connect();
      await mongo.db(DB_NAME).command({ ping: 1 });
      return;
    } catch (err) {
      lastErr = err;
      await sleep(200);
    }
  }
  throw new Error(`mongodb startup timeout: ${String(lastErr)}`);
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function shutdown() {
  try {
    await client.close();
  } catch {
    // ignore
  }
  if (mongodProc) {
    try {
      mongodProc.kill("SIGINT");
    } catch {
      // ignore
    }
    try {
      await Promise.race([
        mongodProc.status,
        sleep(1500),
      ]);
    } catch {
      // ignore
    }
    try {
      mongodProc.kill("SIGKILL");
    } catch {
      // ignore
    }
  }
}

if (Deno.build.os !== "windows") {
  Deno.addSignalListener("SIGINT", () => {
    shutdown().finally(() => Deno.exit(0));
  });
  Deno.addSignalListener("SIGTERM", () => {
    shutdown().finally(() => Deno.exit(0));
  });
}

async function getStats() {
  const userCount = await users.countDocuments();
  const accountCount = await accounts.countDocuments();
  const transactionCount = await transactions.countDocuments();

  let completedTransactions = 0;
  let failedTransactions = 0;
  let totalVolume = 0;

  const byStatus = await transactions.aggregate([
    {
      $group: {
        _id: "$status",
        count: { $sum: 1 },
        total: { $sum: "$amount" },
      },
    },
  ]).toArray();

  for (const row of byStatus) {
    const status = String(row._id ?? "");
    if (status === "completed") {
      completedTransactions = asNumber(row.count);
      totalVolume = asNumber(row.total);
    } else if (status === "failed") {
      failedTransactions = asNumber(row.count);
    }
  }

  const balanceAgg = await accounts.aggregate([
    { $group: { _id: null, total: { $sum: "$balance" } } },
  ]).toArray();
  const totalBalance = asNumber(balanceAgg[0]?.total);

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
  const now = Date.now();
  const id = genId();
  const doc: AccountDoc = {
    id,
    ownerId: userId,
    name,
    type: ensureType(type),
    balance: 0,
    currency: currency || "USD",
    createdAt: now,
  };
  await accounts.insertOne(doc as any);
  return doc;
}

async function deposit(accountId: string, amount: number) {
  if (amount <= 0) throw new Error("Amount must be positive");

  const account = await accounts.findOneAndUpdate(
    { id: accountId },
    { $inc: { balance: amount } },
    { returnDocument: "after" },
  );
  if (!account) throw new Error("Account not found");

  const txId = genId();
  await transactions.insertOne({
    id: txId,
    fromAccountId: "EXTERNAL",
    toAccountId: accountId,
    amount,
    currency: account.currency,
    status: "completed",
    description: "Deposit",
    createdAt: Date.now(),
  } as TransactionDoc as any);

  await ledger.insertOne({
    id: genId(),
    accountId,
    transactionId: txId,
    amount,
    balanceAfter: account.balance,
    type: "credit",
    createdAt: Date.now(),
  } as LedgerDoc as any);

  return { balance: account.balance, transactionId: txId };
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

  const from = await accounts.findOneAndUpdate(
    { id: fromAccountId, balance: { $gte: amount } },
    { $inc: { balance: -amount } },
    { returnDocument: "after" },
  );

  if (!from) {
    const sourceExists = await accounts.findOne({ id: fromAccountId });
    if (!sourceExists) throw new Error("Source account not found");
    const failedTx = genId();
    await transactions.insertOne({
      id: failedTx,
      fromAccountId,
      toAccountId,
      amount,
      currency: sourceExists.currency,
      status: "failed",
      description: description || "Transfer (insufficient funds)",
      createdAt: Date.now(),
    } as TransactionDoc as any);
    return {
      status: "failed",
      reason: "insufficient_funds",
      transactionId: failedTx,
    };
  }

  const to = await accounts.findOneAndUpdate(
    { id: toAccountId },
    { $inc: { balance: amount } },
    { returnDocument: "after" },
  );

  if (!to) {
    await accounts.updateOne({ id: fromAccountId }, {
      $inc: { balance: amount },
    });
    throw new Error("Destination account not found");
  }

  const txId = genId();
  await transactions.insertOne({
    id: txId,
    fromAccountId,
    toAccountId,
    amount,
    currency: from.currency,
    status: "completed",
    description: description || "Transfer",
    createdAt: Date.now(),
  } as TransactionDoc as any);

  await ledger.insertMany([
    {
      id: genId(),
      accountId: fromAccountId,
      transactionId: txId,
      amount: -amount,
      balanceAfter: from.balance,
      type: "debit",
      createdAt: Date.now(),
    },
    {
      id: genId(),
      accountId: toAccountId,
      transactionId: txId,
      amount,
      balanceAfter: to.balance,
      type: "credit",
      createdAt: Date.now(),
    },
  ] as any[]);

  return { status: "completed", transactionId: txId };
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
    if (pathname === "/" && req.method === "GET") {
      return addCors(
        jsonResponse({
          name: "mongodb-finance-benchmark",
          status: "ok",
          mongoUri,
        }),
      );
    }

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

      const existing = await users.findOne({ email });
      if (existing) {
        return addCors(
          jsonResponse({ error: "Email already registered" }, 400),
        );
      }

      const id = genId();
      await users.insertOne({
        id,
        email,
        password: hashPassword(password),
        name,
        roles: [],
        createdAt: Date.now(),
      } as UserDoc as any);

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

      const user = await users.findOne({ email });
      if (!user || !verifyPassword(password, user.password)) {
        return addCors(jsonResponse({ error: "Invalid credentials" }, 400));
      }

      const roles = Array.isArray(user.roles) ? user.roles : [];
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

      const user = await users.findOne({ id: payload.sub });
      if (!user) return addCors(jsonResponse({ error: "User not found" }, 404));

      const roles = Array.isArray(user.roles) ? user.roles : [];
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
        const docs = await accounts.find({}).limit(10000).toArray();
        return addCors(
          jsonResponse({
            ok: true,
            data: docs.map((d) => stripMongoId(d as any)),
          }),
        );
      }

      if (viewName === "get_accounts") {
        if (!auth) {
          return addCors(
            jsonResponse({ error: "Authentication required" }, 401),
          );
        }
        const docs = await accounts.find({ ownerId: auth.id }).sort({
          createdAt: -1,
        })
          .limit(10000).toArray();
        return addCors(
          jsonResponse({
            ok: true,
            data: docs.map((d) => stripMongoId(d as any)),
          }),
        );
      }

      if (viewName === "get_recent_transactions") {
        const limit = Math.max(
          1,
          Number(url.searchParams.get("limit") ?? "100"),
        );
        const docs = await transactions.find({}).sort({ createdAt: -1 }).limit(
          limit,
        )
          .toArray();
        return addCors(
          jsonResponse({
            ok: true,
            data: docs.map((d) => stripMongoId(d as any)),
          }),
        );
      }

      if (viewName === "get_transactions") {
        if (!auth) {
          return addCors(
            jsonResponse({ error: "Authentication required" }, 401),
          );
        }
        const accountId = String(url.searchParams.get("accountId") ?? "");
        const docs = await transactions.find({
          $or: [{ fromAccountId: accountId }, { toAccountId: accountId }],
        }).sort({ createdAt: -1 }).limit(10000).toArray();
        return addCors(
          jsonResponse({
            ok: true,
            data: docs.map((d) => stripMongoId(d as any)),
          }),
        );
      }

      if (viewName === "get_ledger") {
        if (!auth) {
          return addCors(
            jsonResponse({ error: "Authentication required" }, 401),
          );
        }
        const accountId = String(url.searchParams.get("accountId") ?? "");
        const docs = await ledger.find({ accountId }).sort({ createdAt: -1 })
          .limit(
            10000,
          ).toArray();
        return addCors(
          jsonResponse({
            ok: true,
            data: docs.map((d) => stripMongoId(d as any)),
          }),
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

    if (
      pathname === "/_schema" || pathname === "/api/schema" ||
      pathname === "/api/sse"
    ) {
      return addCors(jsonResponse({}));
    }

    return addCors(jsonResponse({ error: "Not found" }, 404));
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return addCors(jsonResponse({ error: msg }, 400));
  }
}

console.log(`\n[MongoDB Benchmark] http://localhost:${PORT} (${mongoUri})`);

Deno.serve({ hostname: "0.0.0.0", port: PORT }, handler);
