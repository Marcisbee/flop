import { flop, table, t } from "flop";

// ── Tables ──────────────────────────────────────────────────────────────────

const users = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{15}/),
    email: t.string().required().unique(),
    password: t.bcrypt(10).required(),
    name: t.string().required(),
    roles: t.roles(),
  },
  auth: true,
});

const accounts = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{15}/),
    ownerId: t.string().required(),
    name: t.string().required(),
    type: t.enum("checking", "savings", "credit").required(),
    balance: t.number().required(),
    currency: t.string().required(),
    createdAt: t.timestamp().default("now"),
  },
});

const transactions = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{15}/),
    fromAccountId: t.string().required(),
    toAccountId: t.string().required(),
    amount: t.number().required(),
    currency: t.string().required(),
    status: t.enum("pending", "completed", "failed").required(),
    description: t.string(),
    createdAt: t.timestamp().default("now"),
  },
});

const ledger = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{15}/),
    accountId: t.string().required(),
    transactionId: t.string().required(),
    amount: t.number().required(),
    balanceAfter: t.number().required(),
    type: t.enum("debit", "credit").required(),
    createdAt: t.timestamp().default("now"),
  },
});

// ── Database ────────────────────────────────────────────────────────────────

export const db = flop({ users, accounts, transactions, ledger }, {
  dataDir: `${import.meta.dirname}/data`,
  syncMode: "normal",
});

// ── Reducers (mutations) ────────────────────────────────────────────────────

export const create_account = db.reduce(
  { name: t.string(), type: t.string(), currency: t.string() },
  async (ctx, params) => {
    if (!ctx.request.auth) throw new Error("Not logged in");
    return ctx.db.accounts.insert({
      ownerId: ctx.request.auth.id,
      name: params.name,
      type: params.type as "checking" | "savings" | "credit",
      balance: 0,
      currency: params.currency || "USD",
    });
  },
);

export const deposit = db.reduce(
  { accountId: t.string(), amount: t.number() },
  async (ctx, { accountId, amount }) => {
    if (!ctx.request.auth) throw new Error("Not logged in");
    if (amount <= 0) throw new Error("Amount must be positive");

    const account = await ctx.db.accounts.get(accountId) as any;
    if (!account) throw new Error("Account not found");

    const newBalance = account.balance + amount;

    return ctx.transaction(async (db) => {
      await db.accounts.update(accountId, { balance: newBalance });

      const tx = await db.transactions.insert({
        fromAccountId: "EXTERNAL",
        toAccountId: accountId,
        amount,
        currency: account.currency,
        status: "completed",
        description: "Deposit",
      });

      await db.ledger.insert({
        accountId,
        transactionId: tx.id,
        amount,
        balanceAfter: newBalance,
        type: "credit",
      });

      return { balance: newBalance, transactionId: tx.id };
    });
  },
);

export const transfer = db.reduce(
  {
    fromAccountId: t.string(),
    toAccountId: t.string(),
    amount: t.number(),
    description: t.string(),
  },
  async (ctx, { fromAccountId, toAccountId, amount, description }) => {
    if (!ctx.request.auth) throw new Error("Not logged in");
    if (amount <= 0) throw new Error("Amount must be positive");
    if (fromAccountId === toAccountId) throw new Error("Cannot transfer to same account");

    const fromAccount = await ctx.db.accounts.get(fromAccountId) as any;
    if (!fromAccount) throw new Error("Source account not found");

    const toAccount = await ctx.db.accounts.get(toAccountId) as any;
    if (!toAccount) throw new Error("Destination account not found");

    if (fromAccount.balance < amount) {
      const tx = await ctx.db.transactions.insert({
        fromAccountId,
        toAccountId,
        amount,
        currency: fromAccount.currency,
        status: "failed",
        description: description || "Transfer (insufficient funds)",
      });
      return { status: "failed", reason: "insufficient_funds", transactionId: tx.id };
    }

    const newFromBalance = fromAccount.balance - amount;
    const newToBalance = toAccount.balance + amount;

    return ctx.transaction(async (db) => {
      await db.accounts.update(fromAccountId, { balance: newFromBalance });
      await db.accounts.update(toAccountId, { balance: newToBalance });

      const tx = await db.transactions.insert({
        fromAccountId,
        toAccountId,
        amount,
        currency: fromAccount.currency,
        status: "completed",
        description: description || "Transfer",
      });

      await db.ledger.insert({
        accountId: fromAccountId,
        transactionId: tx.id,
        amount: -amount,
        balanceAfter: newFromBalance,
        type: "debit",
      });

      await db.ledger.insert({
        accountId: toAccountId,
        transactionId: tx.id,
        amount,
        balanceAfter: newToBalance,
        type: "credit",
      });

      return { status: "completed", transactionId: tx.id };
    });
  },
);

// ── Views (queries) ─────────────────────────────────────────────────────────

export const get_accounts = db.view(
  {},
  async (ctx) => {
    if (!ctx.request.auth) throw new Error("Not logged in");
    const all = await ctx.db.accounts.scan(10000);
    return all.filter((a: any) => a.ownerId === ctx.request.auth!.id);
  },
);

export const get_all_accounts = db.view(
  {},
  async (ctx) => {
    return ctx.db.accounts.scan(10000);
  },
).public();

export const get_transactions = db.view(
  { accountId: t.string() },
  async (ctx, { accountId }) => {
    const all = await ctx.db.transactions.scan(10000);
    return all.filter(
      (tx: any) => tx.fromAccountId === accountId || tx.toAccountId === accountId,
    );
  },
);

export const get_recent_transactions = db.view(
  { limit: t.number() },
  async (ctx, { limit }) => {
    const all = await ctx.db.transactions.scan(limit || 100);
    return all;
  },
).public();

export const get_ledger = db.view(
  { accountId: t.string() },
  async (ctx, { accountId }) => {
    const all = await ctx.db.ledger.scan(10000);
    return all.filter((e: any) => e.accountId === accountId);
  },
);

export const get_stats = db.view(
  {},
  async (ctx) => {
    // Use O(1) count() for row counts instead of scanning
    const userCount = ctx.db.users.count();
    const accountCount = ctx.db.accounts.count();
    const transactionCount = ctx.db.transactions.count();

    // Still need scans for aggregations (balance, volume)
    const allAccounts = await ctx.db.accounts.scan(100000);
    const allTransactions = await ctx.db.transactions.scan(100000);

    let totalBalance = 0;
    for (const a of allAccounts) totalBalance += (a as any).balance;

    let completedCount = 0;
    let failedCount = 0;
    let totalVolume = 0;
    for (const tx of allTransactions) {
      const t = tx as any;
      if (t.status === "completed") {
        completedCount++;
        totalVolume += t.amount;
      } else if (t.status === "failed") {
        failedCount++;
      }
    }

    return {
      userCount,
      accountCount,
      transactionCount,
      completedTransactions: completedCount,
      failedTransactions: failedCount,
      totalVolume,
      totalBalance,
    };
  },
).public();

