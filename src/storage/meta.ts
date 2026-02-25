// _meta.flop management — table registry, schema version history
// Format: "FLOP"(4B) | version(2B) | payload_len(4B) | JSON payload | CRC32(4B)

import { readUint16, writeUint16, readUint32, writeUint32, allocBuffer } from "../util/binary.ts";
import { crc32 } from "../util/crc32.ts";
import { META_FILE_MAGIC, type StoredMeta, type StoredTableMeta, type StoredSchema } from "../types.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const META_VERSION = 1;

export function createEmptyMeta(): StoredMeta {
  return {
    version: META_VERSION,
    created: new Date().toISOString(),
    tables: {},
  };
}

export function serializeMeta(meta: StoredMeta): Uint8Array {
  const jsonStr = JSON.stringify(meta, null, 2);
  const payload = encoder.encode(jsonStr);

  // FLOP(4) + version(2) + payload_len(4) + payload + CRC32(4)
  const total = 4 + 2 + 4 + payload.byteLength + 4;
  const buf = allocBuffer(total);

  // Magic
  buf.set(META_FILE_MAGIC, 0);
  // Version
  writeUint16(buf, 4, META_VERSION);
  // Payload length
  writeUint32(buf, 6, payload.byteLength);
  // Payload
  buf.set(payload, 10);
  // CRC32 of payload
  const checksum = crc32(payload);
  writeUint32(buf, 10 + payload.byteLength, checksum);

  return buf;
}

export function deserializeMeta(buf: Uint8Array): StoredMeta {
  // Verify magic
  for (let i = 0; i < 4; i++) {
    if (buf[i] !== META_FILE_MAGIC[i]) {
      throw new Error("Invalid meta file: bad magic bytes");
    }
  }

  const version = readUint16(buf, 4);
  if (version !== META_VERSION) {
    throw new Error(`Unsupported meta version: ${version}`);
  }

  const payloadLen = readUint32(buf, 6);
  const payload = buf.subarray(10, 10 + payloadLen);

  // Verify CRC32
  const storedChecksum = readUint32(buf, 10 + payloadLen);
  const computedChecksum = crc32(payload);
  if (storedChecksum !== computedChecksum) {
    throw new Error("Meta file corrupted: CRC32 mismatch");
  }

  const jsonStr = decoder.decode(payload);
  return JSON.parse(jsonStr) as StoredMeta;
}

export async function readMetaFile(path: string): Promise<StoredMeta> {
  try {
    const data = await Deno.readFile(path);
    return deserializeMeta(data);
  } catch (e) {
    if (e instanceof Deno.errors.NotFound) {
      return createEmptyMeta();
    }
    throw e;
  }
}

export async function writeMetaFile(path: string, meta: StoredMeta): Promise<void> {
  const data = serializeMeta(meta);
  await Deno.writeFile(path, data);
}

export function getTableMeta(meta: StoredMeta, tableName: string): StoredTableMeta | undefined {
  return meta.tables[tableName];
}

export function setTableMeta(
  meta: StoredMeta,
  tableName: string,
  tableMeta: StoredTableMeta,
): void {
  meta.tables[tableName] = tableMeta;
}

export function createTableMeta(schema: StoredSchema): StoredTableMeta {
  return {
    currentSchemaVersion: 1,
    schemas: { 1: schema },
  };
}

export function addSchemaVersion(
  tableMeta: StoredTableMeta,
  schema: StoredSchema,
): number {
  const newVersion = tableMeta.currentSchemaVersion + 1;
  tableMeta.schemas[newVersion] = schema;
  tableMeta.currentSchemaVersion = newVersion;
  return newVersion;
}
