# Finance Benchmark

A bank simulation that stress-tests flop with high-volume financial data — users, accounts, transfers, and a double-entry ledger.

## Schema

| Table          | Description                                |
|----------------|--------------------------------------------|
| `users`        | Bank customers (auth-enabled)              |
| `accounts`     | Checking, savings, credit accounts         |
| `transactions` | Transfer records between accounts          |
| `ledger`       | Double-entry bookkeeping (debit/credit)    |

## Quick Start

**1. Start the server**

```bash
deno run --allow-all main.ts benchmarks/finance/app.ts
```

**2. Create admin account** via the setup URL printed by the server.

**3. Seed the database**

```bash
# Default: 1,000 users, 3,000 accounts, 50,000 transfers
deno run --allow-net benchmarks/finance/seed.ts

# Custom scale
deno run --allow-net benchmarks/finance/seed.ts --users=5000 --transfers=200000
```

**4. Open the dashboard**

Open `benchmarks/finance/index.html` in a browser. It connects to `http://localhost:1985` and shows realtime stats, accounts, and transactions.

You can also pass `?host=http://localhost:1985` as a query param.

## Endpoints

### Reducers (POST)
- `create_account` — Create a new account for the logged-in user
- `deposit` — Deposit funds into an account
- `transfer` — Transfer funds between any two accounts

### Views (GET)
- `get_accounts` — Current user's accounts (auth required)
- `get_all_accounts` — All accounts (public)
- `get_transactions` — Transactions for a specific account
- `get_recent_transactions` — Last N transactions (public)
- `get_stats` — Aggregate statistics (public)

## What It Measures

- **Insert throughput**: Bulk user registration, account creation, deposits
- **Update throughput**: Balance updates during transfers (2 updates per transfer)
- **Read throughput**: Scanning large tables for stats and listings
- **Realtime latency**: SSE subscription updates as data changes
- **Concurrent load**: Parallel operations via batched HTTP requests
