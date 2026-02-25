# flop

A code-first, schema-driven, realtime database for Deno. Tables are TypeScript schemas, mutations and queries are plain functions exposed as HTTP, SSE, and WebSocket endpoints. No SQL. Designed for realtime apps and online games.

## Features

- **Code-first schemas** — define tables with TypeScript, get full type safety
- **Custom binary storage** — page-based engine (4KB pages), WAL for durability, per-table files
- **Lazy loading** — only indexes live in memory, row data served via LRU page cache
- **Schema migrations** — built-in lazy per-row migration with rename/transform support
- **Realtime** — SSE subscriptions and WebSocket with automatic change propagation
- **Built-in auth** — password login, JWT tokens, role-based permissions
- **Admin panel** — browse tables, edit rows, backup/restore at `/_`
- **File storage** — upload/serve file assets, MIME validation, automatic cleanup
- **Type-safe client** — browser client shares types with server via generic inference

## Quick Start

```typescript
// app.ts
import { flop, table, t } from "flop";

const users = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{15}/),
    email: t.string().required().unique(),
    password: t.bcrypt(10).required(),
    name: t.string(),
    roles: t.roles(),
  },
  auth: true,
});

const messages = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{15}/),
    text: t.string().required(),
    authorId: t.string().required(),
    createdAt: t.timestamp().default("now"),
  },
});

export const db = flop({ users, messages });

export const send_message = db.reduce(
  { text: t.string(), authorId: t.string() },
  async (ctx, { text, authorId }) => {
    return ctx.db.messages.insert({ text, authorId });
  },
);

export const get_messages = db.view(
  {},
  async (ctx) => {
    return ctx.db.messages.scan(100);
  },
).public();
```

```sh
deno run --allow-all main.ts ./app.ts
```

```
┌─────────────────────────────────────┐
│   Server:  http://localhost:1985    │
│   Admin:   http://localhost:1985/_  │
└─────────────────────────────────────┘

  POST  /reduce/send_message [auth]
  GET   /view/get_messages [public]
```

## Schema Types

```typescript
t.string()                        // UTF-8 string
t.number()                        // float64
t.integer()                       // signed int32
t.boolean()                       // true/false
t.enum("idle", "running", "dead") // constrained string
t.timestamp()                     // epoch ms, supports .default("now")
t.json<{ x: number }>()          // typed JSON blob
t.vector(3)                       // fixed-length number array (e.g. [x, y, z])
t.set()                           // unique string set (auto-deduplicates)
t.roles()                         // string array for auth roles
t.bcrypt(10)                      // hashed on insert
t.refSingle(users, "id")          // foreign key reference
t.refMulti(users, "id")           // array of foreign key references
t.fileSingle("image/png")         // single file asset
t.fileMulti("image/png")          // multiple file assets
```

All types support chaining:

```typescript
t.string().required()               // mandatory on insert
t.string().unique()                 // unique constraint
t.string().default("untitled")      // default value
t.string().autogenerate(/[a-z]{8}/) // auto-generate on insert
```

## Permissions

Endpoints are authenticated by default. Use `.roles()` or `.public()` to change access:

```typescript
// Any authenticated user (default)
export const get_profile = db.view({ userId: t.string() }, async (ctx, { userId }) => {
  return ctx.db.users.get(userId);
});

// Only admins
export const delete_user = db.reduce({ userId: t.string() }, async (ctx, { userId }) => {
  return ctx.db.users.delete(userId);
}).roles("admin");

// Anyone, no auth required
export const get_feed = db.view({}, async (ctx) => {
  return ctx.db.messages.scan(50);
}).public();
```

The `superadmin` role bypasses all role checks.

## Client Library

Type-safe browser client that shares types with the server:

```typescript
// server: app.ts
import type { FlopSchema } from "flop";

export type AppSchema = FlopSchema<typeof db, {
  reducers: { send_message: typeof send_message },
  views: { get_messages: typeof get_messages },
}>;

// client: main.ts
import { Flop } from "flop/client";
import type { AppSchema } from "./app.ts";

const flop = new Flop<AppSchema>({ host: "http://localhost:1985" });

// Auth
await flop.users.authWithPassword("alice@example.com", "password");

// Typed API calls
const messages = await flop.view.get_messages({});
await flop.reduce.send_message({ text: "hello", authorId: "abc" });

// Realtime subscription (SSE with auto-reconnect)
const sub = flop.subscribe.get_messages({});
sub.on("data", (messages) => console.log(messages));

// Or as async iterator
for await (const messages of sub) {
  console.log(messages);
}
```

## Migrations

When you change a schema, flop detects the diff on startup and migrates lazily (per-row on read). For renames or type changes, provide hints:

```typescript
const users = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{15}/),
    displayName: t.string(), // was "name"
    age: t.integer(),        // was t.number()
  },
  migrations: [
    { version: 2, rename: { name: "displayName" } },
    { version: 3, transform: (row) => ({ ...row, age: Math.round(Number(row.age) || 0) }) },
  ],
});
```

## Admin Panel

Navigate to `/_` with a `superadmin` user. Features:

- Table browser with row counts and schemas
- Paginated row list with search
- Edit/delete rows
- Backup download (`.tar.gz`) and restore

## File Storage

```typescript
const posts = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{15}/),
    image: t.fileSingle("image/png", "image/jpeg"),
    attachments: t.fileMulti("application/pdf", "image/png"),
  },
});
```

Files are stored on disk at `data/_files/{table}/{rowId}/{field}/`, served at `GET /_files/*`, validated by MIME type and magic bytes, and automatically cleaned up when rows are deleted.

## Storage Details

- **Per-table files**: each table gets `.flop` (data pages), `.idx` (index), `.wal` (write-ahead log)
- **Page format**: 64B file header + 4KB pages with slot directory
- **Indexes in memory**: ~14 bytes per entry, ~14MB for 1M rows
- **Page cache**: LRU, default 1024 pages (4MB)
- **Crash recovery**: WAL replayed on startup, periodic checkpoints every 30s

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /reduce/{name}` | Execute a reducer (mutation) |
| `GET /view/{name}` | Execute a view (query) |
| `GET /view/{name}` + `Accept: text/event-stream` | SSE subscription |
| WebSocket upgrade on any route | Real-time bidirectional |
| `GET /_schema` | JSON schema of all endpoints |
| `POST /_auth/register` | Register user |
| `POST /_auth/password` | Login with password |
| `POST /_auth/refresh` | Refresh JWT token |
| `GET /_files/*` | Serve uploaded files |
| `/_` | Admin panel (superadmin only) |

## Configuration

```sh
# Environment variables
FLOP_PORT=1985            # Server port
FLOP_JWT_SECRET=changeme  # JWT signing secret
```

## License

MIT
