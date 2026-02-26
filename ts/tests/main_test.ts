import { assertAlmostEquals, assertEquals, assertExists } from "@std/assert";

// ---- Utility tests ----

import { crc32 } from "../src/util/crc32.ts";
import { LRUCache } from "../src/util/lru.ts";
import {
  allocBuffer,
  readFloat64,
  readUint16,
  readUint32,
  writeFloat64,
  writeUint16,
  writeUint32,
} from "../src/util/binary.ts";
import { createTar, parseTar } from "../src/util/tar.ts";

Deno.test("crc32 - computes checksum", () => {
  const data = new TextEncoder().encode("hello");
  const checksum = crc32(data);
  assertEquals(checksum, 0x3610a686);
});

Deno.test("binary - uint16 read/write", () => {
  const buf = allocBuffer(4);
  writeUint16(buf, 0, 12345);
  writeUint16(buf, 2, 65535);
  assertEquals(readUint16(buf, 0), 12345);
  assertEquals(readUint16(buf, 2), 65535);
});

Deno.test("binary - uint32 read/write", () => {
  const buf = allocBuffer(4);
  writeUint32(buf, 0, 0xdeadbeef);
  assertEquals(readUint32(buf, 0), 0xdeadbeef);
});

Deno.test("binary - float64 read/write", () => {
  const buf = allocBuffer(8);
  writeFloat64(buf, 0, 3.14159);
  const val = readFloat64(buf, 0);
  assertEquals(Math.round(val * 100000), 314159);
});

Deno.test("LRU - evicts oldest entry", () => {
  const evicted: string[] = [];
  const cache = new LRUCache<string, number>(3, (key) => evicted.push(key));
  cache.set("a", 1);
  cache.set("b", 2);
  cache.set("c", 3);
  cache.set("d", 4); // should evict "a"
  assertEquals(evicted, ["a"]);
  assertEquals(cache.get("a"), undefined);
  assertEquals(cache.get("d"), 4);
});

Deno.test("LRU - access refreshes order", () => {
  const evicted: string[] = [];
  const cache = new LRUCache<string, number>(3, (key) => evicted.push(key));
  cache.set("a", 1);
  cache.set("b", 2);
  cache.set("c", 3);
  cache.get("a"); // refresh "a"
  cache.set("d", 4); // should evict "b" (oldest after "a" refreshed)
  assertEquals(evicted, ["b"]);
  assertEquals(cache.get("a"), 1);
});

Deno.test("tar - create and parse roundtrip", () => {
  const entries = [
    { path: "test.txt", data: new TextEncoder().encode("Hello, World!") },
    {
      path: "dir/nested.json",
      data: new TextEncoder().encode('{"key": "value"}'),
    },
  ];
  const tar = createTar(entries);
  const parsed = parseTar(tar);
  assertEquals(parsed.length, 2);
  assertEquals(parsed[0].path, "test.txt");
  assertEquals(new TextDecoder().decode(parsed[0].data), "Hello, World!");
  assertEquals(parsed[1].path, "dir/nested.json");
});

// ---- Schema tests ----

import { generateFromPattern, t } from "../src/schema.ts";

Deno.test("schema - t.string() builds correctly", () => {
  const field = (t.string() as any)._build();
  assertEquals(field.kind, "string");
  assertEquals(field.required, false);
});

Deno.test("schema - t.string().required() chains", () => {
  const field = (t.string().required() as any)._build();
  assertEquals(field.kind, "string");
  assertEquals(field.required, true);
});

Deno.test("schema - t.bcrypt(10) sets rounds", () => {
  const field = (t.bcrypt(10) as any)._build();
  assertEquals(field.kind, "bcrypt");
  assertEquals(field.bcryptRounds, 10);
  assertEquals(field.required, true);
});

Deno.test("schema - t.json() creates json field", () => {
  const field = (t.json() as any)._build();
  assertEquals(field.kind, "json");
});

Deno.test("schema - t.fileSingle() stores mimeTypes", () => {
  const field = (t.fileSingle("image/png", "image/jpeg") as any)._build();
  assertEquals(field.kind, "fileSingle");
  assertEquals(field.mimeTypes, ["image/png", "image/jpeg"]);
});

Deno.test("schema - t.roles() defaults to empty array", () => {
  const field = (t.roles() as any)._build();
  assertEquals(field.kind, "roles");
  assertEquals(field.defaultValue, []);
});

Deno.test("schema - generateFromPattern creates correct length", () => {
  const result = generateFromPattern(/[a-z0-9]{15}/);
  assertEquals(result.length, 15);
  assertEquals(/^[a-z0-9]{15}$/.test(result), true);
});

// ---- Row serialization tests ----

import { createRowSerializer } from "../src/storage/row.ts";
import { compileSchema } from "../src/table.ts";

Deno.test("row serializer - roundtrip string/number/boolean", () => {
  const schema = compileSchema({
    name: t.string(),
    age: t.number(),
    active: t.boolean(),
  });
  const serializer = createRowSerializer(schema);

  const row = { name: "Alice", age: 30, active: true };
  const buf = serializer.serialize(row, 1);
  const { row: restored, schemaVersion } = serializer.deserialize(buf, 0);

  assertEquals(schemaVersion, 1);
  assertEquals(restored.name, "Alice");
  assertEquals(restored.age, 30);
  assertEquals(restored.active, true);
});

Deno.test("row serializer - roundtrip json field", () => {
  const schema = compileSchema({
    id: t.string(),
    data: t.json(),
  });
  const serializer = createRowSerializer(schema);

  const row = { id: "test", data: { x: 1, y: [2, 3] } };
  const buf = serializer.serialize(row, 1);
  const { row: restored } = serializer.deserialize(buf, 0);

  assertEquals(restored.id, "test");
  assertEquals((restored.data as any).x, 1);
  assertEquals((restored.data as any).y, [2, 3]);
});

Deno.test("row serializer - null values", () => {
  const schema = compileSchema({
    name: t.string(),
    bio: t.string(),
  });
  const serializer = createRowSerializer(schema);

  const row = { name: "Bob", bio: null };
  const buf = serializer.serialize(row, 1);
  const { row: restored } = serializer.deserialize(buf, 0);

  assertEquals(restored.name, "Bob");
  assertEquals(restored.bio, null);
});

Deno.test("row serializer - roles array", () => {
  const schema = compileSchema({
    id: t.string(),
    roles: t.roles(),
  });
  const serializer = createRowSerializer(schema);

  const row = { id: "u1", roles: ["admin", "user"] };
  const buf = serializer.serialize(row, 1);
  const { row: restored } = serializer.deserialize(buf, 0);

  assertEquals(restored.id, "u1");
  assertEquals(restored.roles, ["admin", "user"]);
});

// ---- Page tests ----

import { Page } from "../src/storage/page.ts";

Deno.test("page - insert and read rows", () => {
  const page = Page.create(0);
  const data1 = new TextEncoder().encode("row1data");
  const data2 = new TextEncoder().encode("row2data-longer");

  const slot1 = page.insertRow(data1);
  const slot2 = page.insertRow(data2);

  assertEquals(slot1, 0);
  assertEquals(slot2, 1);

  const read1 = page.readRow(0);
  const read2 = page.readRow(1);

  assertEquals(new TextDecoder().decode(read1!), "row1data");
  assertEquals(new TextDecoder().decode(read2!), "row2data-longer");
});

Deno.test("page - delete row (tombstone)", () => {
  const page = Page.create(0);
  page.insertRow(new TextEncoder().encode("row1"));
  page.insertRow(new TextEncoder().encode("row2"));

  page.deleteRow(0);
  assertEquals(page.readRow(0), null);
  assertEquals(new TextDecoder().decode(page.readRow(1)!), "row2");
});

Deno.test("page - iterate slots", () => {
  const page = Page.create(0);
  page.insertRow(new TextEncoder().encode("a"));
  page.insertRow(new TextEncoder().encode("b"));
  page.insertRow(new TextEncoder().encode("c"));
  page.deleteRow(1);

  const results = [...page.slots()];
  assertEquals(results.length, 2);
  assertEquals(new TextDecoder().decode(results[0].data), "a");
  assertEquals(new TextDecoder().decode(results[1].data), "c");
});

// ---- Index tests ----

import {
  compositeKey,
  deserializeIndex,
  HashIndex,
  serializeIndex,
} from "../src/storage/index.ts";

Deno.test("HashIndex - set/get/delete", () => {
  const idx = new HashIndex();
  idx.set("key1", { pageNumber: 0, slotIndex: 0 });
  idx.set("key2", { pageNumber: 1, slotIndex: 2 });

  assertEquals(idx.get("key1"), { pageNumber: 0, slotIndex: 0 });
  assertEquals(idx.get("key2"), { pageNumber: 1, slotIndex: 2 });
  assertEquals(idx.get("key3"), undefined);

  idx.delete("key1");
  assertEquals(idx.get("key1"), undefined);
  assertEquals(idx.size, 1);
});

Deno.test("HashIndex - serialize/deserialize roundtrip", () => {
  const idx = new HashIndex();
  idx.set("abc", { pageNumber: 5, slotIndex: 3 });
  idx.set("def", { pageNumber: 10, slotIndex: 7 });

  const data = serializeIndex(idx);
  const restored = deserializeIndex(data);

  assertEquals(restored.get("abc"), { pageNumber: 5, slotIndex: 3 });
  assertEquals(restored.get("def"), { pageNumber: 10, slotIndex: 7 });
  assertEquals(restored.size, 2);
});

Deno.test("HashIndex - serialize large index without stack overflow", () => {
  const idx = new HashIndex();
  for (let i = 0; i < 80_000; i++) {
    idx.set(`k${i}`, { pageNumber: (i % 10_000) >>> 0, slotIndex: i % 4096 });
  }

  const data = serializeIndex(idx);
  const restored = deserializeIndex(data);

  assertEquals(restored.size, 80_000);
  assertExists(restored.get("k0"));
  assertExists(restored.get("k79999"));
});

Deno.test("compositeKey - joins values", () => {
  assertEquals(compositeKey(["a", "b"]), "a\0b");
  assertEquals(compositeKey([null, "x"]), "\0\0x");
});

// ---- Meta file tests ----

import {
  addSchemaVersion,
  createEmptyMeta,
  createTableMeta,
  deserializeMeta,
  serializeMeta,
} from "../src/storage/meta.ts";

Deno.test("meta - serialize/deserialize roundtrip", () => {
  const meta = createEmptyMeta();
  meta.tables["users"] = createTableMeta({
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
    ],
  });

  const buf = serializeMeta(meta);
  const restored = deserializeMeta(buf);

  assertEquals(restored.version, 1);
  assertEquals(restored.tables.users.currentSchemaVersion, 1);
  assertEquals(restored.tables.users.schemas[1].columns.length, 2);
});

Deno.test("meta - addSchemaVersion bumps version", () => {
  const tableMeta = createTableMeta({
    columns: [{ name: "id", type: "string" }],
  });

  const newVersion = addSchemaVersion(tableMeta, {
    columns: [
      { name: "id", type: "string" },
      { name: "age", type: "number" },
    ],
  });

  assertEquals(newVersion, 2);
  assertEquals(tableMeta.currentSchemaVersion, 2);
  assertEquals(tableMeta.schemas[2].columns.length, 2);
});

// ---- Schema diff tests ----

import {
  compiledToStored,
  diffSchemas,
  schemasEqual,
} from "../src/schema/diff.ts";

Deno.test("diff - detects added field", () => {
  const stored = { columns: [{ name: "id", type: "string" }] };
  const current = compileSchema({ id: t.string(), name: t.string() });
  const changes = diffSchemas(stored, current);

  assertEquals(changes.length, 1);
  assertEquals(changes[0].type, "added");
  assertEquals(changes[0].field, "name");
});

Deno.test("diff - detects removed field", () => {
  const stored = {
    columns: [{ name: "id", type: "string" }, { name: "old", type: "string" }],
  };
  const current = compileSchema({ id: t.string() });
  const changes = diffSchemas(stored, current);

  assertEquals(changes.length, 1);
  assertEquals(changes[0].type, "removed");
  assertEquals(changes[0].field, "old");
});

Deno.test("diff - schemasEqual returns true for identical", () => {
  const schema = compileSchema({ id: t.string(), name: t.string() });
  const stored = compiledToStored(schema);
  assertEquals(schemasEqual(stored, schema), true);
});

// ---- Migration tests ----

import { buildMigrationChain } from "../src/schema/migration.ts";

Deno.test("migration - rename field", () => {
  const chain = buildMigrationChain(1, 2, [
    { version: 2, rename: { foo: "bar" } },
  ], {
    1: {
      columns: [{ name: "id", type: "string" }, {
        name: "foo",
        type: "string",
      }],
    },
    2: {
      columns: [{ name: "id", type: "string" }, {
        name: "bar",
        type: "string",
      }],
    },
  });

  const migrated = chain.migrate({ id: "1", foo: "value" });
  assertEquals(migrated.bar, "value");
  assertEquals("foo" in migrated, false);
});

Deno.test("migration - add field gets null default", () => {
  const chain = buildMigrationChain(1, 2, [], {
    1: { columns: [{ name: "id", type: "string" }] },
    2: {
      columns: [{ name: "id", type: "string" }, {
        name: "age",
        type: "number",
      }],
    },
  });

  const migrated = chain.migrate({ id: "1" });
  assertEquals(migrated.age, null);
});

Deno.test("migration - custom transform", () => {
  const chain = buildMigrationChain(1, 2, [
    { version: 2, transform: (row) => ({ ...row, age: Number(row.age) || 0 }) },
  ], {
    1: {
      columns: [{ name: "id", type: "string" }, {
        name: "age",
        type: "string",
      }],
    },
    2: {
      columns: [{ name: "id", type: "string" }, {
        name: "age",
        type: "number",
      }],
    },
  });

  const migrated = chain.migrate({ id: "1", age: "25" });
  assertEquals(migrated.age, 25);
});

// ---- PubSub tests ----

import { PubSub } from "../src/realtime/pubsub.ts";

Deno.test("pubsub - subscribe and publish", () => {
  const pubsub = new PubSub();
  const received: string[] = [];

  pubsub.subscribe(["users"], (event) => {
    received.push(`${event.op}:${event.rowId}`);
  });

  pubsub.publish({ table: "users", op: "insert", rowId: "1" });
  pubsub.publish({ table: "messages", op: "insert", rowId: "2" }); // not subscribed
  pubsub.publish({ table: "users", op: "delete", rowId: "1" });

  assertEquals(received, ["insert:1", "delete:1"]);
});

Deno.test("pubsub - unsubscribe", () => {
  const pubsub = new PubSub();
  const received: string[] = [];

  const unsub = pubsub.subscribe(["users"], (event) => {
    received.push(event.rowId);
  });

  pubsub.publish({ table: "users", op: "insert", rowId: "1" });
  unsub();
  pubsub.publish({ table: "users", op: "insert", rowId: "2" });

  assertEquals(received, ["1"]);
});

// ---- Auth tests ----

import {
  createJWT,
  hashPassword,
  verifyJWT,
  verifyPassword,
} from "../src/server/auth.ts";

Deno.test("auth - JWT create and verify", async () => {
  const secret = "test-secret";
  const payload = {
    sub: "user1",
    email: "test@test.com",
    name: "Test",
    roles: ["user"],
    iat: Math.floor(Date.now() / 1000),
    exp: Math.floor(Date.now() / 1000) + 3600,
  };

  const token = await createJWT(payload, secret);
  const verified = await verifyJWT(token, secret);

  assertExists(verified);
  assertEquals(verified!.sub, "user1");
  assertEquals(verified!.email, "test@test.com");
  assertEquals(verified!.roles, ["user"]);
});

Deno.test("auth - JWT rejects wrong secret", async () => {
  const payload = {
    sub: "user1",
    email: "test@test.com",
    name: "Test",
    roles: [],
    iat: Math.floor(Date.now() / 1000),
    exp: Math.floor(Date.now() / 1000) + 3600,
  };

  const token = await createJWT(payload, "secret1");
  const verified = await verifyJWT(token, "secret2");
  assertEquals(verified, null);
});

Deno.test("auth - JWT rejects expired token", async () => {
  const payload = {
    sub: "user1",
    email: "test@test.com",
    name: "Test",
    roles: [],
    iat: Math.floor(Date.now() / 1000) - 7200,
    exp: Math.floor(Date.now() / 1000) - 3600,
  };

  const token = await createJWT(payload, "secret");
  const verified = await verifyJWT(token, "secret");
  assertEquals(verified, null);
});

Deno.test("auth - password hash and verify", async () => {
  const hash = await hashPassword("mypassword");
  assertEquals(hash.startsWith("$pbkdf2$"), true);
  assertEquals(await verifyPassword("mypassword", hash), true);
  assertEquals(await verifyPassword("wrongpassword", hash), false);
});

// ---- Router tests ----

import { Reducer, View } from "../src/endpoint.ts";
import { discoverRoutes } from "../src/server/router.ts";

Deno.test("router - discovers reducers and views", () => {
  const moduleExports = {
    send_message: new Reducer(
      { message: (t.string() as any)._build() },
      () => {},
    ),
    view_messages: new View({ author: (t.string() as any)._build() }, () => []),
    db1: {},
  };

  const routes = discoverRoutes(moduleExports as any);
  assertEquals(routes.length, 2);
  assertEquals(routes[0].name, "send_message");
  assertEquals(routes[0].method, "POST");
  assertEquals(routes[0].path, "/api/reduce/send_message");
  assertEquals(routes[1].name, "view_messages");
  assertEquals(routes[1].method, "GET");
  assertEquals(routes[1].path, "/api/view/view_messages");
});

Deno.test("endpoint - roles() sets access policy", () => {
  const reducer = new Reducer({}, () => {}).roles("admin", "moderator");
  assertEquals(reducer._access, {
    type: "roles",
    roles: ["admin", "moderator"],
  });
});

Deno.test("endpoint - public() sets access policy", () => {
  const view = new View({}, () => {}).public();
  assertEquals(view._access, { type: "public" });
});

// ---- File storage tests ----

import {
  mimeFromExtension,
  sanitizeFilename,
  validateMagicBytes,
  validateMimeType,
} from "../src/storage/files.ts";

Deno.test("files - sanitizeFilename removes dangerous chars", () => {
  assertEquals(sanitizeFilename("test.png"), "test.png");
  assertEquals(sanitizeFilename("../../../etc/passwd"), "______etc_passwd");
  assertEquals(sanitizeFilename("file:name.txt"), "file_name.txt");
});

Deno.test("files - validateMimeType checks allowed types", () => {
  assertEquals(
    validateMimeType("image/png", ["image/png", "image/jpeg"]),
    true,
  );
  assertEquals(
    validateMimeType("image/gif", ["image/png", "image/jpeg"]),
    false,
  );
  assertEquals(validateMimeType("anything", []), true);
});

Deno.test("files - validateMagicBytes for PNG", () => {
  const pngHeader = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a]);
  assertEquals(validateMagicBytes(pngHeader, "image/png"), true);
  assertEquals(validateMagicBytes(pngHeader, "image/jpeg"), false);
});

Deno.test("files - mimeFromExtension", () => {
  assertEquals(mimeFromExtension("photo.png"), "image/png");
  assertEquals(mimeFromExtension("doc.pdf"), "application/pdf");
  assertEquals(mimeFromExtension("file.xyz"), "application/octet-stream");
});

// ---- Integration test: full database lifecycle ----

Deno.test("integration - database open, insert, get, delete", async () => {
  const testDir = await Deno.makeTempDir({ prefix: "flop_test_" });

  try {
    const { table } = await import("../src/table.ts");
    const { flop } = await import("../src/database.ts");

    const users = table({
      schema: {
        id: t.string().autogenerate(/[a-z0-9]{10}/),
        name: t.string().required(),
        age: t.number(),
      },
    });

    const db = flop({ users }, { dataDir: testDir });
    await db.open();

    const userTable = db.getTable("users")!;
    assertExists(userTable);

    // Insert
    const row = await userTable.insert({ name: "Alice", age: 30 });
    assertExists(row.id);
    assertEquals(row.name, "Alice");
    assertEquals(row.age, 30);
    assertEquals((row.id as string).length, 10);

    // Get
    const fetched = await userTable.get(row.id as string);
    assertExists(fetched);
    assertEquals(fetched!.name, "Alice");

    // Update
    const updated = await userTable.update(row.id as string, { age: 31 });
    assertEquals(updated!.age, 31);

    // Scan
    const all = await userTable.scan();
    assertEquals(all.length, 1);

    // Delete
    const deleted = await userTable.delete(row.id as string);
    assertEquals(deleted, true);

    const afterDelete = await userTable.get(row.id as string);
    assertEquals(afterDelete, null);

    await db.close();
  } finally {
    await Deno.remove(testDir, { recursive: true });
  }
});

Deno.test("integration - multiple inserts and scan", async () => {
  const testDir = await Deno.makeTempDir({ prefix: "flop_test_" });

  try {
    const { table } = await import("../src/table.ts");
    const { flop } = await import("../src/database.ts");

    const items = table({
      schema: {
        id: t.string().autogenerate(/[a-z]{8}/),
        value: t.number().required(),
      },
    });

    const db = flop({ items }, { dataDir: testDir });
    await db.open();

    const itemTable = db.getTable("items")!;

    for (let i = 0; i < 10; i++) {
      await itemTable.insert({ value: i * 10 });
    }

    const all = await itemTable.scan();
    assertEquals(all.length, 10);
    assertEquals(itemTable.primaryIndex.size, 10);

    await db.close();
  } finally {
    await Deno.remove(testDir, { recursive: true });
  }
});

// ---- New field type tests ----

Deno.test("schema - t.enum() stores allowed values", () => {
  const field = t.enum("idle", "running", "dead")._build();
  assertEquals(field.kind, "enum");
  assertEquals(field.enumValues, ["idle", "running", "dead"]);
});

Deno.test("schema - t.integer() creates integer field", () => {
  const field = t.integer()._build();
  assertEquals(field.kind, "integer");
});

Deno.test("schema - t.vector(3) stores dimensions", () => {
  const field = t.vector(3)._build();
  assertEquals(field.kind, "vector");
  assertEquals(field.vectorDimensions, 3);
});

Deno.test("schema - t.set() creates set field with empty default", () => {
  const field = t.set()._build();
  assertEquals(field.kind, "set");
  assertEquals(field.defaultValue, []);
});

Deno.test("schema - t.timestamp() creates timestamp field", () => {
  const field = t.timestamp()._build();
  assertEquals(field.kind, "timestamp");
});

Deno.test("schema - t.refMulti() stores ref info", () => {
  const fakeTable = { name: "users" } as any;
  const field = t.refMulti(fakeTable, "id")._build();
  assertEquals(field.kind, "refMulti");
  assertEquals(field.refField, "id");
  assertEquals(field.defaultValue, []);
});

Deno.test("row serializer - roundtrip integer field", () => {
  const schema = compileSchema({
    id: t.string(),
    score: t.integer(),
  });
  const serializer = createRowSerializer(schema);

  const row = { id: "p1", score: -42 };
  const buf = serializer.serialize(row, 1);
  const { row: restored } = serializer.deserialize(buf, 0);

  assertEquals(restored.id, "p1");
  assertEquals(restored.score, -42);
});

Deno.test("row serializer - roundtrip vector field", () => {
  const schema = compileSchema({
    id: t.string(),
    pos: t.vector(3),
  });
  const serializer = createRowSerializer(schema);

  const row = { id: "obj1", pos: [1.5, -2.3, 0.0] };
  const buf = serializer.serialize(row, 1);
  const { row: restored } = serializer.deserialize(buf, 0);

  assertEquals(restored.id, "obj1");
  const pos = restored.pos as number[];
  assertEquals(pos.length, 3);
  assertAlmostEquals(pos[0], 1.5);
  assertAlmostEquals(pos[1], -2.3);
  assertAlmostEquals(pos[2], 0.0);
});

Deno.test("row serializer - roundtrip enum field", () => {
  const schema = compileSchema({
    id: t.string(),
    status: t.enum("idle", "running"),
  });
  const serializer = createRowSerializer(schema);

  const row = { id: "e1", status: "running" };
  const buf = serializer.serialize(row, 1);
  const { row: restored } = serializer.deserialize(buf, 0);

  assertEquals(restored.status, "running");
});

Deno.test("row serializer - roundtrip set field", () => {
  const schema = compileSchema({
    id: t.string(),
    tags: t.set(),
  });
  const serializer = createRowSerializer(schema);

  const row = { id: "s1", tags: ["sword", "shield", "potion"] };
  const buf = serializer.serialize(row, 1);
  const { row: restored } = serializer.deserialize(buf, 0);

  assertEquals(restored.tags, ["sword", "shield", "potion"]);
});

Deno.test("row serializer - roundtrip timestamp field", () => {
  const schema = compileSchema({
    id: t.string(),
    createdAt: t.timestamp(),
  });
  const serializer = createRowSerializer(schema);

  const now = Date.now();
  const row = { id: "t1", createdAt: now };
  const buf = serializer.serialize(row, 1);
  const { row: restored } = serializer.deserialize(buf, 0);

  assertEquals(restored.createdAt, now);
});

Deno.test("row serializer - roundtrip refMulti field", () => {
  const schema = compileSchema({
    id: t.string(),
    members: t.refMulti({ name: "users" } as any, "id"),
  });
  const serializer = createRowSerializer(schema);

  const row = { id: "team1", members: ["u1", "u2", "u3"] };
  const buf = serializer.serialize(row, 1);
  const { row: restored } = serializer.deserialize(buf, 0);

  assertEquals(restored.members, ["u1", "u2", "u3"]);
});
