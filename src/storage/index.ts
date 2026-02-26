// In-memory index — Map<key, RowPointer> + .idx file persistence
//
// .idx format: "FLPI"(4) | version(2) | entryCount(4) | entries...
// Entry: keyLen(2) + keyBytes + pageNumber(4) + slotIndex(2)

import {
  allocBuffer,
  readUint16,
  readUint32,
  writeUint16,
  writeUint32,
} from "../util/binary.ts";
import type { RowPointer } from "../types.ts";

const IDX_MAGIC = new Uint8Array([0x46, 0x4c, 0x50, 0x49]); // "FLPI"
const IDX_VERSION = 1;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export class HashIndex {
  private map = new Map<string, RowPointer>();

  get(key: string): RowPointer | undefined {
    return this.map.get(key);
  }

  set(key: string, pointer: RowPointer): void {
    this.map.set(key, pointer);
  }

  has(key: string): boolean {
    return this.map.has(key);
  }

  delete(key: string): boolean {
    return this.map.delete(key);
  }

  get size(): number {
    return this.map.size;
  }

  clear(): void {
    this.map.clear();
  }

  *entries(): IterableIterator<[string, RowPointer]> {
    yield* this.map.entries();
  }

  *keys(): IterableIterator<string> {
    yield* this.map.keys();
  }

  *values(): IterableIterator<RowPointer> {
    yield* this.map.values();
  }
}

export class MultiIndex {
  private map = new Map<string, Set<RowPointer>>();

  add(key: string, pointer: RowPointer): void {
    let set = this.map.get(key);
    if (!set) {
      set = new Set();
      this.map.set(key, set);
    }
    set.add(pointer);
  }

  getAll(key: string): Set<RowPointer> {
    return this.map.get(key) ?? new Set();
  }

  delete(key: string, pointer: RowPointer): void {
    const set = this.map.get(key);
    if (set) {
      // Remove the specific pointer by matching page+slot
      for (const p of set) {
        if (
          p.pageNumber === pointer.pageNumber &&
          p.slotIndex === pointer.slotIndex
        ) {
          set.delete(p);
          break;
        }
      }
      if (set.size === 0) this.map.delete(key);
    }
  }

  clear(): void {
    this.map.clear();
  }
}

// Serialize a HashIndex to .idx file format
export function serializeIndex(index: HashIndex): Uint8Array {
  // Pre-encode keys and compute exact output size so we can write in one pass.
  const entries: Array<{ keyBytes: Uint8Array; pointer: RowPointer }> = [];
  let totalSize = 10; // magic(4) + version(2) + entryCount(4)
  for (const [key, pointer] of index.entries()) {
    const keyBytes = encoder.encode(key);
    if (keyBytes.byteLength > 0xffff) {
      throw new Error("Index key too long (max 65535 bytes)");
    }
    entries.push({ keyBytes, pointer });
    totalSize += 2 + keyBytes.byteLength + 4 + 2;
  }

  const out = allocBuffer(totalSize);
  out.set(IDX_MAGIC, 0);
  writeUint16(out, 4, IDX_VERSION);
  writeUint32(out, 6, index.size);

  let offset = 10;
  for (const { keyBytes, pointer } of entries) {
    writeUint16(out, offset, keyBytes.byteLength);
    offset += 2;
    out.set(keyBytes, offset);
    offset += keyBytes.byteLength;
    writeUint32(out, offset, pointer.pageNumber);
    offset += 4;
    writeUint16(out, offset, pointer.slotIndex);
    offset += 2;
  }

  return out;
}

// Deserialize a .idx file into a HashIndex
export function deserializeIndex(buf: Uint8Array): HashIndex {
  // Verify magic
  for (let i = 0; i < 4; i++) {
    if (buf[i] !== IDX_MAGIC[i]) {
      throw new Error("Invalid index file: bad magic");
    }
  }

  const version = readUint16(buf, 4);
  if (version !== IDX_VERSION) {
    throw new Error(`Unsupported index version: ${version}`);
  }

  const entryCount = readUint32(buf, 6);
  const index = new HashIndex();

  let offset = 10;
  for (let i = 0; i < entryCount && offset < buf.byteLength; i++) {
    const keyLen = readUint16(buf, offset);
    offset += 2;
    const key = decoder.decode(buf.subarray(offset, offset + keyLen));
    offset += keyLen;
    const pageNumber = readUint32(buf, offset);
    offset += 4;
    const slotIndex = readUint16(buf, offset);
    offset += 2;
    index.set(key, { pageNumber, slotIndex });
  }

  return index;
}

export async function readIndexFile(path: string): Promise<HashIndex> {
  try {
    const data = await Deno.readFile(path);
    return deserializeIndex(data);
  } catch (e) {
    if (e instanceof Deno.errors.NotFound) {
      return new HashIndex();
    }
    throw e;
  }
}

export async function writeIndexFile(
  path: string,
  index: HashIndex,
): Promise<void> {
  const data = serializeIndex(index);
  await Deno.writeFile(path, data);
}

// Build a composite key from multiple field values
export function compositeKey(values: unknown[]): string {
  return values.map((v) => (v === null || v === undefined ? "\0" : String(v)))
    .join("\0");
}
