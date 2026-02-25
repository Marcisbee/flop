# Flop Database — Architecture Specification

> Code-first, schema-driven, embedded database with built-in auth, realtime, and admin panel.
> Reference implementation: TypeScript/Deno. This spec is written for portability to other runtimes (Go, Rust, etc.).

---

## Table of Contents

1. [Overview](#1-overview)
2. [User-Facing API](#2-user-facing-api)
3. [Schema Type System](#3-schema-type-system)
4. [Binary File Formats](#4-binary-file-formats)
5. [Storage Engine](#5-storage-engine)
6. [Indexing](#6-indexing)
7. [Write-Ahead Log (WAL)](#7-write-ahead-log-wal)
8. [Schema Migrations](#8-schema-migrations)
9. [Authentication System](#9-authentication-system)
10. [HTTP Server & Routing](#10-http-server--routing)
11. [Realtime (SSE & WebSocket)](#11-realtime-sse--websocket)
12. [Admin Panel](#12-admin-panel)
13. [File Storage](#13-file-storage)
14. [Client SDK](#14-client-sdk)
15. [CLI Entry Point](#15-cli-entry-point)
16. [Utility Modules](#16-utility-modules)
17. [Directory Layout](#17-directory-layout)

---

## 1. Overview

Flop is a single-binary, code-first database designed for small-to-medium applications. Users define tables and endpoints in a single application file; flop handles storage, auth, API routing, realtime subscriptions, and an admin panel automatically.

**Key design principles:**
- Code-first: schema is defined in application code, not config files
- Embedded: runs in-process, no external database dependencies
- Convention over configuration: sensible defaults, minimal boilerplate
- Binary page storage: custom 4KB page format, not SQLite or LSM

**Architecture layers (bottom-up):**

```
┌─────────────────────────────────────────────────┐
│  User Application (app.ts)                      │
├─────────────────────────────────────────────────┤
│  Client SDK (Flop<T>)                           │
├─────────────────────────────────────────────────┤
│  HTTP Server / Router / SSE / WebSocket         │
├─────────────────────────────────────────────────┤
│  Admin Panel (/_)                               │
├─────────────────────────────────────────────────┤
│  Auth Service (JWT + PBKDF2)                    │
├─────────────────────────────────────────────────┤
│  Database Core (Database, TableInstance)        │
├─────────────────────────────────────────────────┤
│  Schema Migrations (diff, chain, lazy per-row)  │
├─────────────────────────────────────────────────┤
│  Storage Engine (TableFile, Page, Row, WAL)     │
├─────────────────────────────────────────────────┤
│  Index Layer (HashIndex, MultiIndex, .idx)      │
├─────────────────────────────────────────────────┤
│  Binary Utilities (LE read/write, CRC32, LRU)   │
└─────────────────────────────────────────────────┘
```

---

## 2. User-Facing API

### 2.1 Defining Tables

```typescript
import { flop, table, t } from "flop";

const users = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{15}/),
    email: t.string().required().unique(),
    password: t.bcrypt(10).required(),
    name: t.string(),
    roles: t.roles(),
  },
  auth: true,   // marks this as the auth table
});

const messages = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{15}/),
    text: t.string().required(),
    author: t.refSingle(users, "id").required(),
    createdAt: t.timestamp().default("now"),
  },
});

export const db = flop({ users, messages });
```

### 2.2 Defining Endpoints

**Reducers** (write operations, `POST`):
```typescript
export const send_message = db.reduce(
  { text: t.string() },
  (ctx, { text }) => {
    if (!ctx.request.auth) throw new Error("Not logged in");
    return ctx.db.messages.insert({ text, author: ctx.request.auth.id });
  },
);
```

**Views** (read operations, `GET`):
```typescript
export const get_messages = db.view(
  {},
  (ctx) => ctx.db.messages.scan(100),
).public();
```

### 2.3 Access Control Chaining

Every endpoint defaults to `authenticated`. Methods:
- `.public()` — no auth required
- `.roles("admin", "editor")` — requires one of the listed roles (or `superadmin`)

### 2.4 Context Object

Reducers receive `ReduceContext`, views receive `ViewContext`:

```typescript
interface ReduceContext {
  db: Record<string, TableProxy>;  // proxied table access
  request: RequestContext;          // auth, headers, url
}

interface RequestContext {
  auth: AuthContext | null;  // { id, email, roles }
  headers: Headers;
  url: URL;
}
```

### 2.5 TableProxy API

Inside endpoint handlers, each table is accessed through a proxy:

| Method | Signature | Description |
|--------|-----------|-------------|
| `insert` | `(data: Record<string, unknown>) → Promise<Row>` | Insert a new row |
| `get` | `(key: string) → Promise<Row \| null>` | Get by primary key |
| `update` | `(key: string, data: Record<string, unknown>) → Promise<Row \| null>` | Partial update |
| `delete` | `(key: string) → Promise<boolean>` | Delete by primary key |
| `scan` | `(limit?: number, offset?: number) → Promise<Row[]>` | Sequential scan |
| `[field].find` | `(value: unknown) → Promise<Row \| null>` | Find by secondary index |
| `[field].findAll` | `(value: unknown) → Promise<Row[]>` | Find all by secondary index |

---

## 3. Schema Type System

### 3.1 Field Kinds

Each field has a `kind` string and associated metadata:

| Kind | TS Type | TypeTag | Serialized Size | Notes |
|------|---------|---------|-----------------|-------|
| `string` | `string` | `0x01` | 1 + 4 + N bytes | Length-prefixed UTF-8 |
| `number` | `number` | `0x02` | 1 + 8 bytes | IEEE 754 float64, LE |
| `integer` | `number` | `0x05` | 1 + 4 bytes | Signed 32-bit int, LE |
| `boolean` | `boolean` | `0x03` | 1 + 1 byte | 0x00 = false, 0x01 = true |
| `json` | `T \| null` | `0x0D` | 1 + 4 + N bytes | JSON-encoded string |
| `bcrypt` | `string` | `0x01` | 1 + 4 + N bytes | Same as string on disk; hashed on insert |
| `ref` | `string` | `0x01` | 1 + 4 + N bytes | Foreign key (single), stored as string |
| `refMulti` | `string[]` | `0x04` | 1 + 2 + entries | Array of string refs |
| `fileSingle` | `FileRef \| null` | `0x0E` | 1 + 4 + N bytes | JSON-encoded FileRef |
| `fileMulti` | `FileRef[]` | `0x0F` | 1 + 4 + N bytes | JSON-encoded FileRef array |
| `roles` | `string[]` | `0x04` | 1 + 2 + entries | Array of role strings |
| `enum` | `string` | `0x01` | 1 + 4 + N bytes | Validated against allowed values |
| `vector` | `number[]` | `0x06` | 1 + 2 + N×8 bytes | Fixed-dimension float64 array |
| `set` | `string[]` | `0x04` | 1 + 2 + entries | Deduplicated string array |
| `timestamp` | `number` | `0x02` | 1 + 8 bytes | Epoch milliseconds as float64 |

### 3.2 Field Modifiers

All field builders support:
- `.required()` — field must be non-null on insert
- `.unique()` — enforced via secondary index
- `.default(value)` — default value when not provided

Additional modifiers:
- `.autogenerate(pattern: RegExp)` — auto-generate value from regex pattern (string fields only)
  - Pattern format: `/[charset]{length}/` (e.g., `/[a-z0-9]{15}/`)
  - Uses `crypto.getRandomValues()` for secure random generation

### 3.3 TypeTag Constants

```
Null        = 0x00
String      = 0x01
Number      = 0x02
Boolean     = 0x03
Array       = 0x04
Integer     = 0x05
Vector      = 0x06
Json        = 0x0D
FileSingle  = 0x0E
FileMulti   = 0x0F
```

### 3.4 Schema Compilation

`compileSchema(schema)` transforms field builders into `CompiledSchema`:

```typescript
interface CompiledField {
  name: string;
  kind: FieldKind;
  required: boolean;
  unique: boolean;
  defaultValue?: unknown;
  autoGenPattern?: RegExp;
  bcryptRounds?: number;
  refTableName?: string;
  refField?: string;
  mimeTypes?: string[];
  enumValues?: string[];
  vectorDimensions?: number;
}

interface CompiledSchema {
  fields: CompiledField[];     // ordered list
  fieldNames: string[];        // field names in order
  fieldMap: Map<string, CompiledField>;  // name → field lookup
}
```

**Important:** Ref table names are resolved lazily. At compile time, `refTable` is a `TableBuilder` whose `name` is `""`. After `db.open()` calls `_toTableDef(name)` on all tables, `_resolveRefs()` patches the compiled field's `refTableName` with the actual table name.

---

## 4. Binary File Formats

All multi-byte integers are **little-endian**.

### 4.1 Table File (`.flop`)

Per-table data file. Contains a fixed-size header followed by 4KB pages.

```
Offset  Size  Field
──────  ────  ─────
0       4     Magic bytes: "FLPT" (0x46 0x4C 0x50 0x54)
4       2     File format version (currently 1)
6       2     Page size (4096)
8       4     Page count (uint32)
12      4     Total rows (uint32)
16      2     Schema version (uint16)
18      46    Reserved (zero-filled)
──────────────────────────
64      4096  Page 0
4160    4096  Page 1
...
```

**Page offset formula:** `offset = 64 + pageNumber × 4096`

### 4.2 Page Format (4096 bytes)

Slotted page: header at top, slot directory growing forward, row data growing backward.

```
┌──────────────────────────────────────────┐
│ Header (12 bytes)                         │
│   pageNumber:  uint32  (offset 0)         │
│   slotCount:   uint16  (offset 4)         │
│   freeSpaceOff:uint16  (offset 6)         │
│   flags:       uint8   (offset 8)         │
│   reserved:    3 bytes (offset 9)         │
├──────────────────────────────────────────┤
│ Slot directory (grows forward →)          │
│   Slot 0: offset(uint16) + length(uint16) │
│   Slot 1: offset(uint16) + length(uint16) │
│   ...                                     │
├──────────────────────────────────────────┤
│ Free space                                │
├──────────────────────────────────────────┤
│ Row data (grows ← backward from end)      │
│   Row N data                              │
│   Row N-1 data                            │
│   ...                                     │
└──────────────────────────────────────────┘
```

**Slot entry (4 bytes):** `offset:uint16 | length:uint16`
- `length = 0` indicates a deleted (tombstone) slot

**Page flags:**
- `0x00` None
- `0x01` Dirty
- `0x02` Overflow
- `0x04` Deleted

**Constants:**
- `PAGE_SIZE = 4096`
- `FILE_HEADER_SIZE = 64`
- `PAGE_HEADER_SIZE = 12`
- `SLOT_SIZE = 4`

### 4.3 Row Serialization Format

Each row is serialized as a contiguous byte sequence:

```
Offset  Size  Field
──────  ────  ─────
0       2     Schema version (uint16)
2       1     Field count (uint8)
3       ...   Field 0: typeTag(1) + data
              Field 1: typeTag(1) + data
              ...
```

**Per-field encoding by TypeTag:**

| TypeTag | Encoding |
|---------|----------|
| `0x00` Null | 1 byte (just the tag) |
| `0x01` String | tag(1) + length(uint32) + UTF-8 bytes |
| `0x02` Number | tag(1) + float64 LE (8 bytes) |
| `0x03` Boolean | tag(1) + value(1): 0x00 or 0x01 |
| `0x04` Array | tag(1) + count(uint16) + [len(uint16) + UTF-8 bytes]... |
| `0x05` Integer | tag(1) + int32 LE (4 bytes) |
| `0x06` Vector | tag(1) + count(uint16) + [float64 LE]... |
| `0x0D` Json | tag(1) + length(uint32) + JSON string bytes |
| `0x0E` FileSingle | tag(1) + length(uint32) + JSON string bytes |
| `0x0F` FileMulti | tag(1) + length(uint32) + JSON string bytes |

**Insert behavior:**
1. For each field in schema order: apply `autogenerate`, `default`, or `"now"` for timestamps
2. Validate required fields, enum values, integer range, vector dimensions, set dedup
3. Check unique constraints (primary key + secondary unique indexes)
4. Serialize to binary, write WAL entry, write to page, update indexes, commit WAL
5. Publish change event to PubSub

**Update behavior:**
1. Read existing row, merge updates
2. Try in-place update (if new data fits in old slot)
3. If doesn't fit: delete old slot, insert into a new/different page
4. Update index pointers accordingly

### 4.4 Meta File (`_meta.flop`)

Global metadata file, one per database. Tracks all tables and schema versions.

```
Offset  Size  Field
──────  ────  ─────
0       4     Magic bytes: "FLOP" (0x46 0x4C 0x4F 0x50)
4       2     Meta version (currently 1)
6       4     Payload length (uint32)
10      N     JSON payload (UTF-8)
10+N    4     CRC32 checksum of payload
```

**JSON payload structure:**
```json
{
  "version": 1,
  "created": "2025-01-01T00:00:00.000Z",
  "tables": {
    "users": {
      "currentSchemaVersion": 2,
      "schemas": {
        "1": { "columns": [{ "name": "id", "type": "string", "unique": true }, ...] },
        "2": { "columns": [{ "name": "id", "type": "string", "unique": true }, ...] }
      }
    }
  }
}
```

### 4.5 Index File (`.idx`)

Per-table primary key index, persisted to disk.

```
Offset  Size  Field
──────  ────  ─────
0       4     Magic bytes: "FLPI" (0x46 0x4C 0x50 0x49)
4       2     Index version (currently 1)
6       4     Entry count (uint32)
10      ...   Entries:
              keyLen(uint16) + keyBytes + pageNumber(uint32) + slotIndex(uint16)
```

### 4.6 WAL File (`.wal`)

Per-table write-ahead log.

```
WAL Header (16 bytes):
  0       4     Magic bytes: "FLPW" (0x46 0x4C 0x50 0x57)
  4       4     Version (uint32, currently 1)
  8       4     Checkpoint LSN (uint32)
  12      4     Reserved

WAL Entry:
  0       4     Record length (uint32) — excludes this field itself
  4       4     Transaction ID (uint32)
  8       1     Operation code (uint8)
  9       4     Data length (uint32)
  13      N     Data bytes
  13+N    4     CRC32 checksum (over bytes 0..13+N-1 inclusive of recordLen prefix)
```

**Operation codes:**
- `1` INSERT
- `2` UPDATE
- `3` DELETE
- `4` COMMIT

---

## 5. Storage Engine

### 5.1 TableFile

Manages per-table `.flop` file: header I/O, page allocation, page reads/writes.

**Key operations:**
- `create(path, schemaVersion)` — create new file with header
- `open(path)` — open existing, verify magic, read header
- `allocatePage()` — append new 4KB page, increment page count
- `findOrAllocatePage(rowDataSize)` — linear scan from last page backward for free space
- `getPage(n)` → returns from cache or reads from disk
- `flush()` — flush all dirty pages and header
- `scanAllRows()` — async iterator yielding `{pageNumber, slotIndex, data}` for every non-deleted slot

### 5.2 Page

In-memory 4KB page with slotted row storage.

**Key operations:**
- `insertRow(rowData)` → slot index or -1 if no space
- `readRow(slotIndex)` → Uint8Array or null
- `updateRow(slotIndex, newData)` → true if fits in-place, false otherwise
- `deleteRow(slotIndex)` — sets slot length to 0 (tombstone)
- `slots()` — iterator over valid (non-deleted) entries

**Free space calculation:**
```
freeSpace = lowestRowOffset - (PAGE_HEADER_SIZE + (slotCount + 1) × SLOT_SIZE)
```

### 5.3 PageCache (LRU)

LRU cache backed by `Map` insertion order.

- Default capacity: 1024 pages per table
- On eviction: synchronously flush dirty page to disk (`seekSync` + `writeSync`)
- `flushAll()` — async flush all dirty pages + `fsync()`
- Cache key: page number (integer)

### 5.4 RowSerializer

Schema-aware binary encoder/decoder.

- `serialize(row, schemaVersion)` → Uint8Array
- `deserialize(buf, offset)` → `{row, schemaVersion, bytesRead}`
- `estimateSize(row)` → byte count estimate

Also provides `deserializeRawFields(buf, offset)` for migration — positional decoding without field name mapping.

---

## 6. Indexing

### 6.1 Primary Index (`HashIndex`)

In-memory `Map<string, RowPointer>` where `RowPointer = {pageNumber, slotIndex}`.

- Loaded from `.idx` file on startup, or rebuilt by scanning all pages
- Persisted to `.idx` on checkpoint
- Primary key field: first field with `autoGenPattern`, or first field in schema

### 6.2 Secondary Indexes

Defined via `table.index("field1", "field2").unique()`.

Two types:
- `HashIndex` — unique secondary index: `Map<compositeKey, RowPointer>`
- `MultiIndex` — non-unique: `Map<compositeKey, Set<RowPointer>>`

Composite keys join field values with `\0` separator.

Secondary indexes are rebuilt from a full table scan on startup (not persisted to disk).

### 6.3 Unique Constraint Enforcement

On insert:
1. Check primary key uniqueness via `primaryIndex.has(pk)`
2. For each unique secondary index, check `hashIndex.has(compositeKey)`
3. Throw `"Duplicate primary key"` or `"Duplicate unique constraint"` on violation

---

## 7. Write-Ahead Log (WAL)

### 7.1 Transaction Flow

```
1. txId = wal.beginTransaction()        // increment counter
2. wal.append(txId, INSERT, rowData)     // write WAL entry
3. page.insertRow(rowData)               // write to page in memory
4. wal.commit(txId)                      // write COMMIT entry + fsync
```

### 7.2 Crash Recovery

On startup:
1. `wal.replay()` — read all entries, verify CRC32, stop at first corrupted entry
2. `WAL.findCommittedTxIds(entries)` — collect transaction IDs that have COMMIT entries
3. Re-apply committed operations (currently simplified — assumes pages are already written)
4. `wal.truncate()` — reset WAL to header only

### 7.3 Checkpoint

Periodic (every 30 seconds) and on shutdown:
1. Flush all dirty pages to disk
2. Write primary index to `.idx`
3. Truncate WAL
4. Write meta file

---

## 8. Schema Migrations

### 8.1 Schema Change Detection

On `db.open()`, for each table:
1. Load stored schema from `_meta.flop`
2. Compare with current code schema using `schemasEqual(stored, current)`
3. If different, compute `diffSchemas()` → list of `SchemaChange`
4. Validate changes against user-provided migration steps
5. Bump schema version in meta

**Change types:** `added`, `removed`, `typeChanged`, `requireChanged`

### 8.2 Migration Definition

Users provide migrations in table config:
```typescript
const users = table({
  schema: { /* current schema */ },
  migrations: [
    {
      version: 2,
      rename: { "old_field": "new_field" },
      transform: (row) => ({ ...row, new_field: row.old_field.toUpperCase() }),
    },
  ],
});
```

### 8.3 Lazy Per-Row Migration

Rows are migrated on read, not on startup:
1. Each serialized row stores its `schemaVersion` (first 2 bytes)
2. On read, if `row.schemaVersion < currentSchemaVersion`:
   - Look up `MigrationChain` for the source version
   - Deserialize raw field values using the old schema's column order
   - Apply chain steps: renames → transforms → add new fields (null) → remove old fields
3. Return the migrated row (not written back — lazy)

### 8.4 MigrationChain

Precomputed on startup for each old version → current version path:

```typescript
interface MigrationChainStep {
  fromVersion: number;
  toVersion: number;
  rename?: Record<string, string>;
  transform?: (row) => row;
  addedFields: string[];
  removedFields: string[];
  targetSchema: StoredSchema;
}
```

Chain steps are applied sequentially from old version to current.

---

## 9. Authentication System

### 9.1 Overview

Enabled by setting `auth: true` on a table. That table becomes the auth table. The auth table must have at minimum: an ID field (autogenerated), `email` (string, required, unique), `password` (bcrypt, required), and `roles` (roles type).

### 9.2 Password Hashing

Uses PBKDF2 via Web Crypto API (not actual bcrypt despite the field name):
- Algorithm: PBKDF2 with SHA-256
- Iterations: 100,000
- Salt: 16 random bytes
- Output: 256 bits
- Format: `$pbkdf2$<salt_hex>$<hash_hex>`

### 9.3 JWT Tokens

HMAC-SHA256 via Web Crypto API.

**Access token payload:**
```json
{
  "sub": "user_id",
  "email": "user@example.com",
  "name": "User Name",
  "roles": ["user"],
  "iat": 1700000000,
  "exp": 1700000900
}
```

**Token lifetimes:**
- Access token: 900 seconds (15 minutes)
- Refresh token: 604800 seconds (7 days)

**Refresh token:** Same JWT format but with empty email/name/roles fields. Only contains `sub`, `iat`, `exp`.

### 9.4 Auth Endpoints

| Method | Path | Access | Description |
|--------|------|--------|-------------|
| POST | `/api/auth/register` | Public | Register new user (role: `["user"]`) |
| POST | `/api/auth/password` | Public | Login with email/password |
| POST | `/api/auth/refresh` | Public | Exchange refresh token for new access token |
| POST | `/api/auth/verify` | Auth | Request email verification (not yet implemented) |
| POST | `/api/auth/reset-password` | Public | Request password reset (not yet implemented) |
| POST | `/api/auth/change-email` | Auth | Request email change (not yet implemented) |

### 9.5 Token Extraction

Bearer token extracted from:
1. `Authorization: Bearer <token>` header
2. `?_token=<token>` query parameter (fallback for SSE/WebSocket)

### 9.6 Role-Based Access

- `superadmin` role bypasses all role checks
- Access policies: `public`, `authenticated`, `roles` (list of required roles, user needs at least one)

---

## 10. HTTP Server & Routing

### 10.1 Route Discovery

On startup, all named exports from the user module are inspected:
- `Reducer` instances → `POST /api/reduce/<export_name>`
- `View` instances → `GET /api/view/<export_name>`

### 10.2 Request Flow

```
Request
  ├── OPTIONS → CORS preflight (204)
  ├── /_* → Admin handler
  ├── /api/* → API routing
  │    ├── /api/files/* → File serving
  │    ├── /api/schema → JSON schema endpoint
  │    ├── /api/auth/* → Auth endpoints
  │    ├── /api/sse → Multiplexed SSE
  │    └── /api/reduce/* or /api/view/* → Route matching
  │         ├── Access enforcement (public/auth/roles)
  │         ├── SSE check (Accept: text/event-stream) → SSE handler
  │         ├── WebSocket check (Upgrade: websocket) → WS handler
  │         └── Normal HTTP
  │              ├── Reducer: parse JSON body → handler → { ok, data }
  │              └── View: parse query params → handler → { ok, data }
  ├── /assets/* → Static file serving
  └── Page routes → SSR shell (if pages defined)
```

### 10.3 CORS

All responses include:
```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS
Access-Control-Allow-Headers: Content-Type, Authorization
```

### 10.4 Schema Endpoint

`GET /api/schema` returns JSON describing all routes:
```json
{
  "endpoints": [
    {
      "name": "send_message",
      "method": "POST",
      "path": "/api/reduce/send_message",
      "type": "reducer",
      "access": "authenticated",
      "params": { "text": { "type": "string", "required": false } }
    }
  ]
}
```

### 10.5 Error Handling

All errors caught and returned as `{ "error": "<message>" }` with status 400. Auth errors return 401, role violations return 403.

---

## 11. Realtime (SSE & WebSocket)

### 11.1 PubSub

In-process pub/sub system. Each `TableInstance` holds a reference to a shared `PubSub`.

**Change events:**
```typescript
interface ChangeEvent {
  table: string;
  op: "insert" | "update" | "delete";
  rowId: string;
  data?: Record<string, unknown>;
}
```

Published after every successful insert, update, or delete (after WAL commit).

**Subscription types:**
- `subscribe(tables[], callback)` — listen to specific tables
- `subscribeAll(callback)` — listen to all tables

### 11.2 Server-Sent Events (SSE)

Triggered when a View route receives `Accept: text/event-stream`.

1. Execute view handler immediately, send result as `data:` event
2. Subscribe to dependent tables via PubSub
3. On any change to dependent tables, re-execute view and send new data
4. Clean up subscription on client disconnect (`req.signal abort`)

**Response headers:**
```
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
```

### 11.3 WebSocket

Triggered when any route receives `Upgrade: websocket`.

**Client → Server messages:**
```json
{ "type": "reduce", "name": "send_message", "id": "req1", "params": { "text": "hello" } }
{ "type": "view", "name": "get_messages", "id": "req2", "params": {} }
```

**Server → Client messages:**
```json
{ "type": "result", "id": "req1", "data": { ... } }
{ "type": "change", "table": "messages", "op": "insert", "data": { ... } }
{ "type": "error", "id": "req1", "error": "message" }
```

WebSocket subscribes to all table changes globally via `subscribeAll`.

---

## 12. Admin Panel

### 12.1 Overview

Served at `/_` as an inline single-page application (HTML/CSS/JS embedded in a TypeScript template literal). Requires `superadmin` role or JWT-authenticated admin access.

### 12.2 Admin Routes

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/_/` | Superadmin JWT | Admin SPA HTML |
| GET | `/_/login` | Public | Login page |
| POST | `/_/api/login` | Public | Admin login |
| POST | `/_/api/refresh` | Public | Token refresh |
| GET | `/_/api/tables` | Superadmin JWT | List all tables with schemas |
| GET | `/_/api/tables/:name` | Superadmin JWT | List rows for a table |
| POST | `/_/api/tables/:name` | Superadmin JWT | Insert row |
| PUT | `/_/api/tables/:name/:id` | Superadmin JWT | Update row |
| DELETE | `/_/api/tables/:name/:id` | Superadmin JWT | Delete row |
| GET | `/_/api/sse` | Superadmin JWT | SSE stream of all changes |
| GET | `/_/setup` | Token-gated | First-run superadmin creation page |
| POST | `/_/api/setup` | Token-gated | Create superadmin account |

### 12.3 First-Run Setup

When no superadmin exists:
1. Generate a 32-character random setup token
2. Print setup URL in CLI banner
3. `GET /_/setup?token=<token>` renders setup form
4. `POST /_/api/setup` validates token, creates superadmin, clears token
5. After creation, setup routes redirect to login

### 12.4 Admin SPA Features

- Table browser: view rows, inline editing, create new rows
- Reference field dropdowns: ref and enum fields use `<select>` elements
- Ref cell labels: shows referenced row's display field (e.g., email) alongside ID
- Real-time updates via SSE
- Token auto-refresh on 401 responses

---

## 13. File Storage

### 13.1 Directory Structure

```
data/
  _files/
    <tableName>/
      <rowId>/
        <fieldName>/
          <sanitized_filename>
```

### 13.2 FileRef Object

```typescript
interface FileRef {
  path: string;   // relative: "_files/users/abc123/avatar/photo.png"
  name: string;   // sanitized filename
  size: number;   // bytes
  mime: string;    // MIME type
  url: string;     // "/api/files/users/abc123/avatar/photo.png"
}
```

### 13.3 MIME Handling

- Extension-based MIME detection (`.png`, `.jpg`, `.pdf`, etc.)
- Magic byte validation for known types (PNG, JPEG, GIF, WebP, PDF)
- MIME whitelist per field via `t.fileSingle("image/png", "image/jpeg")`

### 13.4 File Lifecycle

- **Store:** sanitize filename, write to disk, return `FileRef`
- **Update:** diff old/new refs, delete orphaned files
- **Delete row:** `deleteRowFiles(dataDir, tableName, rowId)` removes entire row directory
- **Serve:** `GET /api/files/*` streams file with `Cache-Control: public, max-age=31536000, immutable`

### 13.5 Filename Sanitization

Removes: `/\:*?"<>|` and `..` sequences. Falls back to `"unnamed"` if empty.

---

## 14. Client SDK

### 14.1 Flop<T> Client

Generic client class parameterized by `FlopSchema<T>` for type-safe views/reducers:

```typescript
const client = new Flop<typeof schema>({ host: "http://localhost:1985" });

// Typed view call
const messages = await client.view.get_messages({});

// Typed reducer call
await client.reduce.send_message({ text: "hello" });

// Typed SSE subscription
const sub = client.subscribe.get_messages({});
sub.on("data", (data) => console.log(data));
```

### 14.2 Namespaces (Proxy-based)

- `client.view.<name>(params)` → `GET /api/view/<name>?params`
- `client.reduce.<name>(params)` → `POST /api/reduce/<name>` with JSON body
- `client.subscribe.<name>(params)` → SSE `EventSource` to `/api/view/<name>?params&_token=...`

All implemented via ES `Proxy` — no code generation needed.

### 14.3 Auth Client

```typescript
client.users.authWithPassword(email, password)   // login
client.users.register(email, password, name?)     // register
client.users.authWithOAuth2({ provider })         // OAuth2 popup (browser)
client.users.refresh()                            // refresh access token
client.users.logout()                             // clear tokens
client.users.requestVerification(email)           // email verification
client.users.requestPasswordReset(email)          // password reset
client.users.requestEmailChange(newEmail)         // email change
```

### 14.4 Subscription Class

SSE-based with auto-reconnect and dual API:

**Callback API:**
```typescript
const sub = client.subscribe.get_messages({});
sub.on("data", (messages) => render(messages));
sub.on("error", (err) => console.error(err));
sub.close();
```

**Async Iterator API:**
```typescript
for await (const messages of client.subscribe.get_messages({})) {
  render(messages);
}
```

**Reconnection:** exponential backoff starting at 1s, max 30s. Resets on successful message.

### 14.5 Token Store

Pluggable token persistence:
- `LocalStorageTokenStore` — uses `localStorage` (browser default)
- `MemoryTokenStore` — in-memory (server-side default)
- Custom: implement `TokenStore { get(), set(token), clear() }`

Two stores: one for access tokens (`flop_token`), one for refresh tokens (`flop_refresh_token`).

Auto-refresh: on 401 response, client automatically calls `refresh()` and retries the request.

---

## 15. CLI Entry Point

### 15.1 Usage

```
deno run --allow-all main.ts <path-to-app.ts>
```

### 15.2 Startup Sequence

1. Dynamic import of user module
2. Find `Database` instance in exports
3. `db.open()` — create data dir, read meta, open tables, replay WAL, rebuild indexes
4. `discoverRoutes(exports)` — find Reducer/View instances
5. Set up `AuthService` if auth table exists
6. Check for superadmin, generate setup token if needed
7. Create admin handler
8. Start HTTP server on configured port
9. Print banner with server info, routes, setup URL
10. Start checkpoint interval (30s)
11. Register `SIGINT`/`SIGTERM` handlers for graceful shutdown

### 15.3 Configuration

| Source | Priority | Key |
|--------|----------|-----|
| Constructor arg | 1 (highest) | `config.port`, `config.jwtSecret` |
| Environment var | 2 | `FLOP_PORT`, `FLOP_JWT_SECRET` |
| Default | 3 (lowest) | Port: `1985`, Secret: `"flop-dev-secret-change-in-production"` |

### 15.4 Graceful Shutdown

On SIGINT/SIGTERM:
1. Clear checkpoint interval
2. `db.close()` — checkpoint all tables, flush pages, write indexes, close files
3. Exit process

---

## 16. Utility Modules

### 16.1 Binary Helpers (`util/binary.ts`)

All operations are little-endian:

| Function | Description |
|----------|-------------|
| `readUint8(buf, offset)` | Read unsigned 8-bit |
| `writeUint8(buf, offset, value)` | Write unsigned 8-bit |
| `readUint16(buf, offset)` | Read unsigned 16-bit LE |
| `writeUint16(buf, offset, value)` | Write unsigned 16-bit LE |
| `readUint32(buf, offset)` | Read unsigned 32-bit LE |
| `writeUint32(buf, offset, value)` | Write unsigned 32-bit LE |
| `readInt32(buf, offset)` | Read signed 32-bit LE |
| `writeInt32(buf, offset, value)` | Write signed 32-bit LE |
| `readFloat64(buf, offset)` | Read IEEE 754 double LE via DataView |
| `writeFloat64(buf, offset, value)` | Write IEEE 754 double LE via DataView |
| `readString(buf, offset)` | Read length-prefixed (uint32) UTF-8 string |
| `writeString(buf, offset, value)` | Write length-prefixed UTF-8 string |
| `concatBuffers(...bufs)` | Concatenate multiple Uint8Array |
| `allocBuffer(size)` | Allocate zeroed Uint8Array |
| `stringByteLength(value)` | UTF-8 byte length of string |

### 16.2 CRC32 (`util/crc32.ts`)

IEEE polynomial CRC32 (same as zlib):
- Polynomial: `0xEDB88320` (reversed)
- Precomputed 256-entry lookup table
- `crc32(data: Uint8Array) → uint32`
- `crc32Update(prev, data)` — incremental update

### 16.3 LRU Cache (`util/lru.ts`)

Generic LRU cache using `Map` insertion order:
- `get(key)` — read + move to end
- `set(key, value)` — insert, evict oldest if over capacity
- Configurable `onEvict(key, value)` callback
- Used by `PageCache` for page eviction

---

## 17. Directory Layout

### 17.1 Source Code

```
flop/
├── mod.ts                    # Public API barrel (server-side exports)
├── main.ts                   # CLI entry point
├── app.ts                    # Example user application
├── src/
│   ├── types.ts              # Core type definitions, constants, type tags
│   ├── schema.ts             # Field type builders (t.string(), t.number(), etc.)
│   ├── table.ts              # table() function, TableBuilder, compileSchema
│   ├── database.ts           # Database class, TableInstance, flop() factory
│   ├── endpoint.ts           # Reducer, View, Endpoint base class
│   ├── storage/
│   │   ├── page.ts           # 4KB slotted page
│   │   ├── page_cache.ts     # LRU page cache
│   │   ├── table_file.ts     # Per-table .flop file manager
│   │   ├── row.ts            # Row serialization/deserialization
│   │   ├── wal.ts            # Write-ahead log
│   │   ├── index.ts          # HashIndex, MultiIndex, .idx persistence
│   │   ├── meta.ts           # _meta.flop read/write
│   │   └── files.ts          # File asset storage
│   ├── schema/
│   │   ├── diff.ts           # Schema comparison and validation
│   │   └── migration.ts      # Migration chain builder, lazy row migration
│   ├── server/
│   │   ├── handler.ts        # HTTP request handler, SSE, WebSocket
│   │   ├── router.ts         # Route discovery and matching
│   │   └── auth.ts           # JWT, PBKDF2, AuthService
│   ├── realtime/
│   │   └── pubsub.ts         # In-process pub/sub
│   ├── admin/
│   │   ├── mod.ts            # Admin route handler
│   │   └── ui.ts             # Admin SPA HTML/CSS/JS
│   ├── client/
│   │   ├── flop_client.ts    # Flop<T> client class
│   │   ├── auth_client.ts    # AuthClient
│   │   ├── subscription.ts   # SSE Subscription class
│   │   └── token_store.ts    # TokenStore interface + implementations
│   └── util/
│       ├── binary.ts         # LE binary read/write helpers
│       ├── crc32.ts          # CRC32 checksum
│       └── lru.ts            # Generic LRU cache
```

### 17.2 Data Directory (runtime)

```
data/
├── _meta.flop                # Global metadata
├── _files/                   # File uploads
│   └── <table>/<rowId>/<field>/<filename>
├── users.flop                # Table data file
├── users.wal                 # Write-ahead log
├── users.idx                 # Primary key index
├── messages.flop
├── messages.wal
└── messages.idx
```

---

## Porting Notes

When reimplementing in another language:

1. **Binary format is the contract.** All file formats (`.flop`, `.wal`, `.idx`, `_meta.flop`) must match exactly for data compatibility.

2. **Little-endian everywhere.** All multi-byte integers use LE byte order.

3. **CRC32 uses IEEE polynomial** (`0xEDB88320` reflected). Standard zlib CRC32.

4. **Password hashing** currently uses PBKDF2-SHA256 (100k iterations, 16-byte salt, 256-bit output) despite being named "bcrypt". Format: `$pbkdf2$<salt_hex>$<hash_hex>`. A port could switch to actual bcrypt but would break existing password hashes.

5. **JWT** uses standard HMAC-SHA256. Any standard JWT library works.

6. **Schema fields are ordered.** Binary row serialization encodes fields in schema definition order. This order must be preserved.

7. **Lazy migration** means old-version rows coexist with new-version rows on disk. The row's embedded `schemaVersion` (first 2 bytes) determines which deserialization path to take.

8. **Page allocation is append-only.** New pages are always added at the end. Free space search scans backward from the last page. No page compaction or defragmentation exists.

9. **The admin panel** is a self-contained SPA embedded as a string template. For a port, it could be served as static files instead.

10. **Ref table name resolution** is a two-pass process: first assign all table names, then resolve ref fields. This is due to circular table references (table A references table B and vice versa).
