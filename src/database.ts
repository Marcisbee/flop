// Database core — flop(), Database class, TableInstance, Reducer/View factories, transactions

import type {
  SchemaField, CompiledSchema, CompiledField, RowPointer, FileRef,
  AuthContext, RequestContext,
  StoredMeta, StoredTableMeta,
  MigrationStep,
  TableDef,
  InferParams,
} from "./types.ts";
import { TableBuilder, compileSchema, type SchemaFieldInput } from "./table.ts";
import { Reducer, View } from "./endpoint.ts";
import { createRowSerializer, deserializeRawFields, type RowSerializer } from "./storage/row.ts";
import { TableFile } from "./storage/table_file.ts";
import { WAL, WALOp } from "./storage/wal.ts";
import { HashIndex, MultiIndex, readIndexFile, writeIndexFile, compositeKey } from "./storage/index.ts";
import { readMetaFile, writeMetaFile, createEmptyMeta, createTableMeta, addSchemaVersion, setTableMeta } from "./storage/meta.ts";
import { compiledToStored, schemasEqual, diffSchemas, validateMigration } from "./schema/diff.ts";
import { buildMigrationChain, deserializeWithSchema, type MigrationChain } from "./schema/migration.ts";
import { generateFromPattern } from "./schema.ts";
import { deleteRowFiles, deleteFileRef, cleanupOrphanedFiles } from "./storage/files.ts";
import { PubSub, type ChangeEvent } from "./realtime/pubsub.ts";

export interface DatabaseConfig {
  dataDir?: string;
  maxCachePages?: number;
  /** WAL sync mode:
   *  "full" (default) — fsync after every commit batch (safest, slower)
   *  "normal" — write to OS buffer only, fsync on checkpoint (faster, loses last few seconds on power loss)
   */
  syncMode?: "full" | "normal";
}

// Per-transaction WAL buffer: tableName -> { records, txIds }
export type TxWalBuffer = Map<string, { records: Uint8Array[]; txIds: number[] }>;

function validateFieldValue(field: CompiledField, value: unknown): void {
  switch (field.kind) {
    case "enum": {
      if (field.enumValues && !field.enumValues.includes(String(value))) {
        throw new Error(
          `Invalid value "${value}" for enum field "${field.name}". Allowed: ${field.enumValues.join(", ")}`,
        );
      }
      break;
    }
    case "integer": {
      const n = Number(value);
      if (!Number.isInteger(n)) {
        throw new Error(`Field "${field.name}" must be an integer, got: ${value}`);
      }
      if (n < -2147483648 || n > 2147483647) {
        throw new Error(`Field "${field.name}" must be a 32-bit integer (-2147483648 to 2147483647)`);
      }
      break;
    }
    case "vector": {
      if (!Array.isArray(value)) {
        throw new Error(`Field "${field.name}" must be an array of numbers`);
      }
      if (field.vectorDimensions && value.length !== field.vectorDimensions) {
        throw new Error(
          `Field "${field.name}" requires exactly ${field.vectorDimensions} dimensions, got ${value.length}`,
        );
      }
      for (let i = 0; i < value.length; i++) {
        if (typeof value[i] !== "number") {
          throw new Error(`Field "${field.name}[${i}]" must be a number`);
        }
      }
      break;
    }
    case "set": {
      if (!Array.isArray(value)) {
        throw new Error(`Field "${field.name}" must be an array of strings`);
      }
      // Deduplicate on write
      break;
    }
    case "timestamp": {
      if (typeof value !== "number") {
        throw new Error(`Field "${field.name}" must be a number (epoch ms)`);
      }
      break;
    }
  }
}

export class TableInstance {
  readonly name: string;
  readonly def: TableDef;
  readonly serializer: RowSerializer;
  private tableFile!: TableFile;
  wal!: WAL;
  readonly primaryIndex = new HashIndex();
  readonly secondaryIndexes: Map<string, HashIndex | MultiIndex> = new Map();
  private migrationChains: Map<number, MigrationChain> = new Map();
  private currentSchemaVersion = 1;
  private meta!: StoredTableMeta;
  private dataDir = "";
  private pubsub?: PubSub;
  // Back-reference to Database for group commit
  _db?: Database;
  // Async write lock to prevent concurrent page mutations
  private _writeLock: Promise<void> = Promise.resolve();
  // WAL entry counter for auto-checkpoint
  _walEntryCount = 0;
  static readonly WAL_CHECKPOINT_THRESHOLD = 10000;

  _withWriteLock<T>(fn: () => Promise<T>): Promise<T> {
    const prev = this._writeLock;
    let resolve: () => void;
    this._writeLock = new Promise<void>((r) => { resolve = r; });
    return prev.then(fn).finally(() => resolve!());
  }

  constructor(name: string, def: TableDef) {
    this.name = name;
    this.def = def;
    this.serializer = createRowSerializer(def.compiledSchema);
  }

  async open(dataDir: string, meta: StoredMeta, pubsub?: PubSub): Promise<void> {
    this.dataDir = dataDir;
    this.pubsub = pubsub;

    const flopPath = `${dataDir}/${this.name}.flop`;
    const walPath = `${dataDir}/${this.name}.wal`;
    const idxPath = `${dataDir}/${this.name}.idx`;

    // Check if table exists in meta
    let tableMeta = meta.tables[this.name];
    const currentStored = compiledToStored(this.def.compiledSchema);

    if (!tableMeta) {
      // New table
      tableMeta = createTableMeta(currentStored);
      setTableMeta(meta, this.name, tableMeta);
      this.tableFile = await TableFile.create(flopPath, 1);
    } else {
      // Existing table — check for schema changes
      const latestVersion = tableMeta.currentSchemaVersion;
      const latestSchema = tableMeta.schemas[latestVersion];

      if (latestSchema && !schemasEqual(latestSchema, this.def.compiledSchema)) {
        // Schema changed — bump version
        const changes = diffSchemas(latestSchema, this.def.compiledSchema);
        const newVersion = latestVersion + 1;
        const errors = validateMigration(changes, this.def.migrations, newVersion);
        if (errors.length > 0) {
          throw new Error(
            `Schema migration errors for table "${this.name}":\n${errors.join("\n")}`,
          );
        }
        addSchemaVersion(tableMeta, currentStored);
      }

      this.tableFile = await TableFile.open(flopPath);
    }

    this.meta = tableMeta;
    this.currentSchemaVersion = tableMeta.currentSchemaVersion;

    // Build migration chains for all older versions
    for (let v = 1; v < this.currentSchemaVersion; v++) {
      this.migrationChains.set(
        v,
        buildMigrationChain(v, this.currentSchemaVersion, this.def.migrations, tableMeta.schemas),
      );
    }

    // Open WAL and replay
    this.wal = await WAL.open(walPath);
    await this.replayWAL();

    // Load index from .idx file or rebuild from scan
    try {
      const loadedIndex = await readIndexFile(idxPath);
      if (loadedIndex.size > 0) {
        for (const [key, pointer] of loadedIndex.entries()) {
          this.primaryIndex.set(key, pointer);
        }
      } else {
        await this.rebuildIndex();
      }
    } catch {
      await this.rebuildIndex();
    }

    // Auto-create unique indexes for fields with .unique() flag
    for (const field of this.def.compiledSchema.fields) {
      if (field.unique && !field.autoGenPattern) {
        const indexKey = field.name;
        const alreadyDefined = this.def.indexes.some(
          (idx) => idx.fields.length === 1 && idx.fields[0] === field.name,
        );
        if (!alreadyDefined) {
          this.def.indexes.push({ fields: [field.name], unique: true });
        }
      }
    }

    // Set up secondary indexes
    for (const indexDef of this.def.indexes) {
      const indexKey = indexDef.fields.join(",");
      if (indexDef.unique) {
        this.secondaryIndexes.set(indexKey, new HashIndex());
      } else {
        this.secondaryIndexes.set(indexKey, new MultiIndex());
      }
    }
    // Populate secondary indexes
    if (this.def.indexes.length > 0) {
      await this.rebuildSecondaryIndexes();
    }
  }

  private async replayWAL(): Promise<void> {
    const entries = await this.wal.replay();
    if (entries.length === 0) return;

    const committed = WAL.findCommittedTxIds(entries);

    for (const entry of entries) {
      if (!committed.has(entry.txId)) continue;
      if (entry.op === WALOp.Commit) continue;

      // Re-apply committed operations
      // WAL data format: [opType specific data]
      // For now, entries are already applied to pages via normal flow
      // This is a simplified replay — full implementation would re-apply to pages
    }

    // Truncate WAL after replay
    await this.wal.truncate();
  }

  private async rebuildIndex(): Promise<void> {
    this.primaryIndex.clear();

    // Find the primary key field (first field with autogenerate, or first field)
    const pkField = this.def.compiledSchema.fields.find((f) => f.autoGenPattern) ??
      this.def.compiledSchema.fields[0];

    if (!pkField) return;

    for await (const { pageNumber, slotIndex, data } of this.tableFile.scanAllRows()) {
      const { row, schemaVersion } = this.serializer.deserialize(data, 0);
      let currentRow = row;

      // Migrate if needed
      if (schemaVersion < this.currentSchemaVersion) {
        const chain = this.migrationChains.get(schemaVersion);
        if (chain) {
          const rawFields = deserializeRawFields(data, 0);
          const oldSchema = this.meta.schemas[schemaVersion];
          const oldRow = deserializeWithSchema(rawFields.values, oldSchema);
          currentRow = chain.migrate(oldRow);
        }
      }

      const key = String(currentRow[pkField.name] ?? "");
      if (key) {
        this.primaryIndex.set(key, { pageNumber, slotIndex });
      }
    }
  }

  private async rebuildSecondaryIndexes(): Promise<void> {
    const pkField = this.def.compiledSchema.fields.find((f) => f.autoGenPattern) ??
      this.def.compiledSchema.fields[0];

    for await (const { pageNumber, slotIndex, data } of this.tableFile.scanAllRows()) {
      const { row } = this.serializer.deserialize(data, 0);

      for (const indexDef of this.def.indexes) {
        const indexKey = indexDef.fields.join(",");
        const idx = this.secondaryIndexes.get(indexKey);
        if (!idx) continue;

        const keyValues = indexDef.fields.map((f) => row[f]);
        const key = compositeKey(keyValues);

        if (idx instanceof HashIndex) {
          idx.set(key, { pageNumber, slotIndex });
        } else if (idx instanceof MultiIndex) {
          idx.add(key, { pageNumber, slotIndex });
        }
      }
    }
  }

  // Get primary key field name
  get primaryKeyField(): string {
    const pkField = this.def.compiledSchema.fields.find((f) => f.autoGenPattern) ??
      this.def.compiledSchema.fields[0];
    return pkField?.name ?? "id";
  }

  async insert(data: Record<string, unknown>): Promise<Record<string, unknown>> {
    // Use group commit: buffer WAL in memory, write lock for page safety, then enqueue
    if (this._db) {
      const txBuf: TxWalBuffer = new Map();
      const result = await this._withWriteLock(() => this._insert(data, txBuf));
      if (txBuf.size > 0) await this._db._enqueueCommit(txBuf);
      return result;
    }
    return this._withWriteLock(() => this._insert(data));
  }

  // txBuf: when provided, buffer WAL record there instead of writing to disk
  async _insert(data: Record<string, unknown>, txBuf?: TxWalBuffer): Promise<Record<string, unknown>> {
    const row = { ...data };

    // Apply autogenerate and defaults
    for (const field of this.def.compiledSchema.fields) {
      if (row[field.name] === undefined || row[field.name] === null) {
        if (field.autoGenPattern) {
          row[field.name] = generateFromPattern(field.autoGenPattern);
        } else if (field.kind === "timestamp" && field.defaultValue === "now") {
          row[field.name] = Date.now();
        } else if (field.defaultValue !== undefined) {
          row[field.name] = field.defaultValue;
        }
      }

      // Validate required
      if (field.required && (row[field.name] === undefined || row[field.name] === null)) {
        throw new Error(`Field "${field.name}" is required`);
      }

      // Type-specific validation
      const val = row[field.name];
      if (val !== undefined && val !== null) {
        validateFieldValue(field, val);
        // Deduplicate sets
        if (field.kind === "set" && Array.isArray(val)) {
          row[field.name] = [...new Set(val as string[])];
        }
      }
    }

    // Check unique constraints
    const pk = String(row[this.primaryKeyField] ?? "");
    if (pk && this.primaryIndex.has(pk)) {
      throw new Error(`Duplicate primary key: ${pk}`);
    }

    for (const indexDef of this.def.indexes) {
      if (!indexDef.unique) continue;
      const indexKey = indexDef.fields.join(",");
      const idx = this.secondaryIndexes.get(indexKey);
      if (idx instanceof HashIndex) {
        const key = compositeKey(indexDef.fields.map((f) => row[f]));
        if (idx.has(key)) {
          throw new Error(`Duplicate unique constraint on (${indexDef.fields.join(", ")})`);
        }
      }
    }

    // Serialize and write
    const serialized = this.serializer.serialize(row, this.currentSchemaVersion);

    // WAL — buffer in memory during transactions, write immediately otherwise
    const txId = this.wal.beginTransaction();
    if (txBuf) {
      let entry = txBuf.get(this.name);
      if (!entry) { entry = { records: [], txIds: [] }; txBuf.set(this.name, entry); }
      entry.records.push(this.wal.buildRecord(txId, WALOp.Insert, serialized));
      entry.txIds.push(txId);
    } else {
      await this.wal.append(txId, WALOp.Insert, serialized);
    }

    // Write to page
    const { pageNumber, page } = await this.tableFile.findOrAllocatePage(serialized.byteLength);
    const slotIndex = page.insertRow(serialized);
    if (slotIndex === -1) {
      throw new Error("Failed to insert row into page");
    }
    this.tableFile.markPageDirty(pageNumber);
    this.tableFile.totalRows++;

    // Update indexes
    const pointer: RowPointer = { pageNumber, slotIndex };
    if (pk) {
      this.primaryIndex.set(pk, pointer);
    }

    for (const indexDef of this.def.indexes) {
      const indexKey = indexDef.fields.join(",");
      const idx = this.secondaryIndexes.get(indexKey);
      const key = compositeKey(indexDef.fields.map((f) => row[f]));
      if (idx instanceof HashIndex) {
        idx.set(key, pointer);
      } else if (idx instanceof MultiIndex) {
        idx.add(key, pointer);
      }
    }

    if (!txBuf) {
      await this.wal.commit(txId);
    }

    // Publish change event
    this.pubsub?.publish({
      table: this.name,
      op: "insert",
      rowId: pk,
      data: row,
    });

    return row;
  }

  async get(key: string): Promise<Record<string, unknown> | null> {
    const pointer = this.primaryIndex.get(key);
    if (!pointer) return null;

    const page = await this.tableFile.getPage(pointer.pageNumber);
    const rawData = page.readRow(pointer.slotIndex);
    if (!rawData) return null;

    const { row, schemaVersion } = this.serializer.deserialize(rawData, 0);

    // Migrate if needed
    if (schemaVersion < this.currentSchemaVersion) {
      const chain = this.migrationChains.get(schemaVersion);
      if (chain) {
        const rawFields = deserializeRawFields(rawData, 0);
        const oldSchema = this.meta.schemas[schemaVersion];
        const oldRow = deserializeWithSchema(rawFields.values, oldSchema);
        return chain.migrate(oldRow);
      }
    }

    return row;
  }

  async update(key: string, updates: Record<string, unknown>): Promise<Record<string, unknown> | null> {
    if (this._db) {
      const txBuf: TxWalBuffer = new Map();
      const result = await this._withWriteLock(() => this._update(key, updates, txBuf));
      if (txBuf.size > 0) await this._db._enqueueCommit(txBuf);
      return result;
    }
    return this._withWriteLock(() => this._update(key, updates));
  }

  async _update(key: string, updates: Record<string, unknown>, txBuf?: TxWalBuffer): Promise<Record<string, unknown> | null> {
    const existing = await this.get(key);
    if (!existing) return null;

    const pointer = this.primaryIndex.get(key)!;
    const newRow = { ...existing, ...updates };

    // Validate updated fields
    for (const field of this.def.compiledSchema.fields) {
      const val = newRow[field.name];
      if (val !== undefined && val !== null) {
        validateFieldValue(field, val);
      }
      // Deduplicate sets
      if (field.kind === "set" && Array.isArray(val)) {
        newRow[field.name] = [...new Set(val as string[])];
      }
    }

    // Handle file cleanup for file fields
    for (const field of this.def.compiledSchema.fields) {
      if ((field.kind === "fileSingle" || field.kind === "fileMulti") && field.name in updates) {
        const oldRefs: FileRef[] = field.kind === "fileSingle"
          ? (existing[field.name] ? [existing[field.name] as FileRef] : [])
          : (existing[field.name] as FileRef[] ?? []);

        const newRefs: FileRef[] = field.kind === "fileSingle"
          ? (updates[field.name] ? [updates[field.name] as FileRef] : [])
          : (updates[field.name] as FileRef[] ?? []);

        const newPaths = new Set(newRefs.map((r) => r.path));
        for (const old of oldRefs) {
          if (!newPaths.has(old.path)) {
            await deleteFileRef(this.dataDir, old);
          }
        }
      }
    }

    // Serialize and update
    const serialized = this.serializer.serialize(newRow, this.currentSchemaVersion);

    const txId = this.wal.beginTransaction();
    if (txBuf) {
      let entry = txBuf.get(this.name);
      if (!entry) { entry = { records: [], txIds: [] }; txBuf.set(this.name, entry); }
      entry.records.push(this.wal.buildRecord(txId, WALOp.Update, serialized));
      entry.txIds.push(txId);
    } else {
      await this.wal.append(txId, WALOp.Update, serialized);
    }

    const page = await this.tableFile.getPage(pointer.pageNumber);
    const updated = page.updateRow(pointer.slotIndex, serialized);

    if (!updated) {
      // Doesn't fit — delete old, insert new
      page.deleteRow(pointer.slotIndex);
      this.tableFile.markPageDirty(pointer.pageNumber);

      const { pageNumber: newPage, page: newPageObj } = await this.tableFile.findOrAllocatePage(
        serialized.byteLength,
      );
      const newSlot = newPageObj.insertRow(serialized);
      if (newSlot === -1) throw new Error("Failed to re-insert row during update");
      this.tableFile.markPageDirty(newPage);

      // Update index pointers
      const newPointer: RowPointer = { pageNumber: newPage, slotIndex: newSlot };
      this.primaryIndex.set(key, newPointer);

      for (const indexDef of this.def.indexes) {
        const indexKey = indexDef.fields.join(",");
        const idx = this.secondaryIndexes.get(indexKey);
        const idxKey = compositeKey(indexDef.fields.map((f) => newRow[f]));
        if (idx instanceof HashIndex) {
          idx.set(idxKey, newPointer);
        }
      }
    } else {
      this.tableFile.markPageDirty(pointer.pageNumber);
    }

    if (!txBuf) {
      await this.wal.commit(txId);
    }

    this.pubsub?.publish({
      table: this.name,
      op: "update",
      rowId: key,
      data: newRow,
    });

    return newRow;
  }

  async delete(key: string): Promise<boolean> {
    if (this._db) {
      const txBuf: TxWalBuffer = new Map();
      const result = await this._withWriteLock(() => this._delete(key, txBuf));
      if (txBuf.size > 0) await this._db._enqueueCommit(txBuf);
      return result;
    }
    return this._withWriteLock(() => this._delete(key));
  }

  async _delete(key: string, txBuf?: TxWalBuffer): Promise<boolean> {
    const existing = await this.get(key);
    if (!existing) return false;

    const pointer = this.primaryIndex.get(key);
    if (!pointer) return false;

    // File cleanup
    await deleteRowFiles(this.dataDir, this.name, key);

    const txId = this.wal.beginTransaction();
    const deleteData = new TextEncoder().encode(key);
    if (txBuf) {
      let entry = txBuf.get(this.name);
      if (!entry) { entry = { records: [], txIds: [] }; txBuf.set(this.name, entry); }
      entry.records.push(this.wal.buildRecord(txId, WALOp.Delete, deleteData));
      entry.txIds.push(txId);
    } else {
      await this.wal.append(txId, WALOp.Delete, deleteData);
    }

    // Delete from page
    const page = await this.tableFile.getPage(pointer.pageNumber);
    page.deleteRow(pointer.slotIndex);
    this.tableFile.markPageDirty(pointer.pageNumber);
    this.tableFile.totalRows--;

    // Remove from indexes
    this.primaryIndex.delete(key);
    for (const indexDef of this.def.indexes) {
      const indexKey = indexDef.fields.join(",");
      const idx = this.secondaryIndexes.get(indexKey);
      const idxKey = compositeKey(indexDef.fields.map((f) => existing[f]));
      if (idx instanceof HashIndex) {
        idx.delete(idxKey);
      } else if (idx instanceof MultiIndex) {
        idx.delete(idxKey, pointer);
      }
    }

    if (!txBuf) {
      await this.wal.commit(txId);
    }

    this.pubsub?.publish({
      table: this.name,
      op: "delete",
      rowId: key,
      data: existing,
    });

    return true;
  }

  count(): number {
    return this.primaryIndex.size;
  }

  async scan(limit = 100, offset = 0): Promise<Record<string, unknown>[]> {
    const results: Record<string, unknown>[] = [];
    let count = 0;
    let skipped = 0;

    for await (const { data } of this.tableFile.scanAllRows()) {
      if (skipped < offset) {
        skipped++;
        continue;
      }
      if (count >= limit) break;

      const { row, schemaVersion } = this.serializer.deserialize(data, 0);

      if (schemaVersion < this.currentSchemaVersion) {
        const chain = this.migrationChains.get(schemaVersion);
        if (chain) {
          const rawFields = deserializeRawFields(data, 0);
          const oldSchema = this.meta.schemas[schemaVersion];
          const oldRow = deserializeWithSchema(rawFields.values, oldSchema);
          results.push(chain.migrate(oldRow));
        } else {
          results.push(row);
        }
      } else {
        results.push(row);
      }
      count++;
    }

    return results;
  }

  // Find by secondary index
  findByIndex(fields: string[], value: unknown): RowPointer | undefined {
    const indexKey = fields.join(",");
    const idx = this.secondaryIndexes.get(indexKey);
    if (idx instanceof HashIndex) {
      return idx.get(String(value));
    }
    return undefined;
  }

  // Read a row directly from a page pointer (for secondary index lookups)
  async getByPointer(pointer: RowPointer): Promise<Record<string, unknown> | null> {
    const page = await this.tableFile.getPage(pointer.pageNumber);
    const rawData = page.readRow(pointer.slotIndex);
    if (!rawData) return null;

    const { row, schemaVersion } = this.serializer.deserialize(rawData, 0);

    if (schemaVersion < this.currentSchemaVersion) {
      const chain = this.migrationChains.get(schemaVersion);
      if (chain) {
        const rawFields = deserializeRawFields(rawData, 0);
        const oldSchema = this.meta.schemas[schemaVersion];
        const oldRow = deserializeWithSchema(rawFields.values, oldSchema);
        return chain.migrate(oldRow);
      }
    }

    return row;
  }

  findAllByIndex(fields: string[], value: unknown): Set<RowPointer> {
    const indexKey = fields.join(",");
    const idx = this.secondaryIndexes.get(indexKey);
    if (idx instanceof MultiIndex) {
      return idx.getAll(String(value));
    }
    if (idx instanceof HashIndex) {
      const p = idx.get(String(value));
      return p ? new Set([p]) : new Set();
    }
    return new Set();
  }

  async checkpoint(): Promise<void> {
    return this._withWriteLock(async () => {
      await this.tableFile.flush();
      await writeIndexFile(`${this.dataDir}/${this.name}.idx`, this.primaryIndex);
      // Ensure WAL is durable before truncating (critical for syncMode: "normal")
      await this.wal.fsync();
      await this.wal.truncate();
    });
  }

  async close(): Promise<void> {
    await this.checkpoint();
    await this.tableFile.close();
    await this.wal.close();
  }
}

// ---- Group commit types ----

interface CommitSlot {
  walBuffers: TxWalBuffer;
  resolve: () => void;
  reject: (err: Error) => void;
}

// ---- Database class ----

export class Database {
  readonly tables = new Map<string, TableInstance>();
  private dataDir: string;
  private meta!: StoredMeta;
  private pubsub = new PubSub();
  private _opened = false;
  private _tableProxies: Record<string, TableProxy> | null = null;
  // Group commit queue: transactions waiting to be fsynced together
  private _commitQueue: CommitSlot[] = [];
  private _commitDraining = false;
  private _syncMode: "full" | "normal";

  constructor(
    private tableDefs: Record<string, TableBuilder<any>>,
    config?: DatabaseConfig,
  ) {
    this.dataDir = config?.dataDir ?? "./data";
    this._syncMode = config?.syncMode ?? "full";
  }

  async open(): Promise<void> {
    if (this._opened) return;
    this._opened = true;

    await Deno.mkdir(this.dataDir, { recursive: true });
    await Deno.mkdir(`${this.dataDir}/_files`, { recursive: true });

    const metaPath = `${this.dataDir}/_meta.flop`;
    this.meta = await readMetaFile(metaPath);

    for (const [name, builder] of Object.entries(this.tableDefs)) {
      const def = builder._toTableDef(name);
      const instance = new TableInstance(name, def);
      instance._db = this;
      await instance.open(this.dataDir, this.meta, this.pubsub);
      this.tables.set(name, instance);
    }

    // Re-resolve ref table names now that all table names are assigned
    for (const builder of Object.values(this.tableDefs)) {
      builder._resolveRefs();
    }

    // Save meta (may have new schema versions)
    await writeMetaFile(metaPath, this.meta);

    // Build table proxies once (reused across all requests)
    this._tableProxies = createTableProxies(this);
  }

  getTable(name: string): TableInstance | undefined {
    return this.tables.get(name);
  }

  getPubSub(): PubSub {
    return this.pubsub;
  }

  getDataDir(): string {
    return this.dataDir;
  }

  getMeta(): StoredMeta {
    return this.meta;
  }

  // Create a Reducer bound to this database
  reduce<P extends Record<string, SchemaFieldInput>, R = unknown>(
    params: P,
    handler: (ctx: ReduceContext, params: InferParams<P>) => R,
  ): Reducer<InferParams<P>, R> {
    const db = this;
    return new Reducer(params, (ctx: any, parsedParams: any) => {
      const reduceCtx: ReduceContext = {
        db: db._tableProxies!,
        request: ctx.request,
        transaction: <T>(fn: (tables: Record<string, TableProxy>) => Promise<T>) => db.transaction(fn),
      };
      return handler(reduceCtx, parsedParams);
    });
  }

  // Create a View bound to this database
  view<P extends Record<string, SchemaFieldInput>, R = unknown>(
    params: P,
    handler: (ctx: ViewContext, params: InferParams<P>) => R,
  ): View<InferParams<P>, R> {
    const db = this;
    const v = new View(params, (ctx: any, parsedParams: any) => {
      const viewCtx: ViewContext = {
        db: db._tableProxies!,
        request: ctx.request,
      };
      return handler(viewCtx, parsedParams);
    });

    // Track dependent tables for realtime
    v._dependentTables = Object.keys(this.tableDefs);
    return v;
  }

  // Run multiple operations in a single transaction with group commit
  async transaction<T>(fn: (db: Record<string, TableProxy>) => Promise<T>): Promise<T> {
    // Phase 1: Execute fn() with local WAL buffers (no I/O, no locks)
    const txBuf: TxWalBuffer = new Map();
    const txProxies = createTransactionProxies(this, txBuf);
    const result = await fn(txProxies);

    // Phase 2: If there are WAL records, enqueue for group commit
    if (txBuf.size > 0) {
      await this._enqueueCommit(txBuf);
    }

    return result;
  }

  // Enqueue WAL buffers for group commit — returns when fsync is done
  _enqueueCommit(walBuffers: TxWalBuffer): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this._commitQueue.push({ walBuffers, resolve, reject });

      // If no drain is in progress, become the leader
      if (!this._commitDraining) {
        this._commitDraining = true;
        // Yield one microtick to let other concurrent transactions join the queue
        queueMicrotask(() => this._drainCommitQueue());
      }
    });
  }

  // Leader: flush + fsync all queued transactions in one batch
  private async _drainCommitQueue(): Promise<void> {
    while (this._commitQueue.length > 0) {
      // Snapshot the current queue
      const batch = this._commitQueue;
      this._commitQueue = [];

      try {
        // Merge all WAL buffers by table name
        const merged = new Map<string, { records: Uint8Array[]; txIds: number[] }>();
        for (const slot of batch) {
          for (const [tableName, entry] of slot.walBuffers) {
            let m = merged.get(tableName);
            if (!m) { m = { records: [], txIds: [] }; merged.set(tableName, m); }
            for (const r of entry.records) m.records.push(r);
            for (const id of entry.txIds) m.txIds.push(id);
          }
        }

        // Flush all dirty WALs (one write per table), fsync only in "full" mode
        const flushPromises: Promise<void>[] = [];
        const checkpointTables: TableInstance[] = [];
        const doFsync = this._syncMode === "full";
        for (const [tableName, entry] of merged) {
          const table = this.tables.get(tableName);
          if (!table) continue;
          const p = table.wal.flushBatch(entry.records, entry.txIds);
          flushPromises.push(doFsync ? p.then(() => table.wal.fsync()) : p);
          // Track WAL entry count for auto-checkpoint
          table._walEntryCount += entry.records.length + entry.txIds.length;
          if (table._walEntryCount >= TableInstance.WAL_CHECKPOINT_THRESHOLD) {
            checkpointTables.push(table);
          }
        }
        await Promise.all(flushPromises);

        // Resolve all waiters
        for (const slot of batch) slot.resolve();

        // Auto-checkpoint tables with large WALs (after resolving waiters)
        if (checkpointTables.length > 0) {
          const cpPromises: Promise<void>[] = [];
          for (const table of checkpointTables) {
            table._walEntryCount = 0;
            cpPromises.push(table.checkpoint().catch(() => {}));
          }
          await Promise.all(cpPromises);
        }
      } catch (err) {
        // Reject all waiters
        for (const slot of batch) slot.reject(err as Error);
      }
    }
    this._commitDraining = false;
  }

  async checkpoint(): Promise<void> {
    for (const table of this.tables.values()) {
      await table.checkpoint();
    }
    await writeMetaFile(`${this.dataDir}/_meta.flop`, this.meta);
  }

  async close(): Promise<void> {
    // Wait for any in-flight group commit drain to finish
    while (this._commitDraining || this._commitQueue.length > 0) {
      await new Promise((r) => setTimeout(r, 5));
    }
    for (const table of this.tables.values()) {
      await table.close();
    }
  }

  // Get the auth table (the one with auth: true)
  getAuthTable(): TableInstance | undefined {
    for (const table of this.tables.values()) {
      if (table.def.auth) return table;
    }
    return undefined;
  }
}

// ---- Context types ----

export interface ReduceContext {
  db: Record<string, TableProxy>;
  request: RequestContext;
  transaction<T>(fn: (db: Record<string, TableProxy>) => Promise<T>): Promise<T>;
}

export interface ViewContext {
  db: Record<string, TableProxy>;
  request: RequestContext;
}

export interface TableProxy {
  insert(data: Record<string, unknown>): Promise<Record<string, unknown>>;
  get(key: string): Promise<Record<string, unknown> | null>;
  update(key: string, data: Record<string, unknown>): Promise<Record<string, unknown> | null>;
  delete(key: string): Promise<boolean>;
  scan(limit?: number, offset?: number): Promise<Record<string, unknown>[]>;
  count(): number;
  // Index access via proxy: table.fieldName.find(value)
  [field: string]: any;
}

function createIndexAccessor(table: TableInstance, fieldName: string) {
  return {
    find: (value: unknown) => {
      const pointer = table.findByIndex([fieldName], value);
      if (!pointer) return Promise.resolve(null);
      return table.getByPointer(pointer);
    },
    findAll: async (value: unknown) => {
      const pointers = table.findAllByIndex([fieldName], value);
      const results: Record<string, unknown>[] = [];
      for (const p of pointers) {
        const page = await table["tableFile"].getPage(p.pageNumber);
        const rawData = page.readRow(p.slotIndex);
        if (rawData) {
          const { row } = table.serializer.deserialize(rawData, 0);
          results.push(row);
        }
      }
      return results;
    },
  };
}

function createTableProxies(db: Database): Record<string, TableProxy> {
  const proxies: Record<string, TableProxy> = {};

  for (const [name, table] of db.tables) {
    // Pre-bind core methods once
    const proxy: any = {
      insert: (data: Record<string, unknown>) => table.insert(data),
      get: (key: string) => table.get(key),
      update: (key: string, data: Record<string, unknown>) => table.update(key, data),
      delete: (key: string) => table.delete(key),
      scan: (limit?: number, offset?: number) => table.scan(limit, offset),
      count: () => table.count(),
    };

    // Pre-build index accessors for all schema fields
    for (const field of table.def.compiledSchema.fields) {
      if (!(field.name in proxy)) {
        proxy[field.name] = createIndexAccessor(table, field.name);
      }
    }

    proxies[name] = proxy as TableProxy;
  }

  return proxies;
}

// Transaction proxies — use write locks for page safety, buffer WAL into local txBuf
function createTransactionProxies(db: Database, txBuf: TxWalBuffer): Record<string, TableProxy> {
  const proxies: Record<string, TableProxy> = {};

  for (const [name, table] of db.tables) {
    const proxy: any = {
      insert: (data: Record<string, unknown>) => table._withWriteLock(() => table._insert(data, txBuf)),
      get: (key: string) => table.get(key),
      update: (key: string, data: Record<string, unknown>) => table._withWriteLock(() => table._update(key, data, txBuf)),
      delete: (key: string) => table._withWriteLock(() => table._delete(key, txBuf)),
      scan: (limit?: number, offset?: number) => table.scan(limit, offset),
      count: () => table.count(),
    };

    for (const field of table.def.compiledSchema.fields) {
      if (!(field.name in proxy)) {
        proxy[field.name] = createIndexAccessor(table, field.name);
      }
    }

    proxies[name] = proxy as TableProxy;
  }

  return proxies;
}

// ---- flop() function ----

export function flop(
  tables: Record<string, TableBuilder<any>>,
  config?: DatabaseConfig,
): Database {
  return new Database(tables, config);
}
