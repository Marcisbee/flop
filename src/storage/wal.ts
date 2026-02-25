// Write-Ahead Log — per-table append-only durability log
//
// WAL Entry format:
//   [recordLen:uint32] [txId:uint32] [op:uint8] [dataLen:uint32] [data:bytes] [crc32:uint32]
//
// Op codes: INSERT=1, UPDATE=2, DELETE=3, COMMIT=4

import {
  readUint8, writeUint8, readUint32, writeUint32,
  allocBuffer, concatBuffers,
} from "../util/binary.ts";
import { crc32 } from "../util/crc32.ts";

export const WALOp = {
  Insert: 1,
  Update: 2,
  Delete: 3,
  Commit: 4,
} as const;

export type WALOp = (typeof WALOp)[keyof typeof WALOp];

export interface WALEntry {
  txId: number;
  op: WALOp;
  data: Uint8Array;
}

const WAL_HEADER_SIZE = 16;
const WAL_MAGIC = new Uint8Array([0x46, 0x4c, 0x50, 0x57]); // "FLPW"

export class WAL {
  private file!: Deno.FsFile;
  private txCounter = 0;
  private _closed = false;
  readonly path: string;

  constructor(path: string) {
    this.path = path;
  }

  static async open(path: string): Promise<WAL> {
    const wal = new WAL(path);
    await wal._open();
    return wal;
  }

  private async _open(): Promise<void> {
    try {
      this.file = await Deno.open(this.path, { read: true, write: true, create: true });
      const stat = await this.file.stat();

      if (stat.size === 0) {
        // New WAL — write header
        await this.writeHeader();
      } else {
        // Existing WAL — read header
        await this.readHeader();
      }
    } catch {
      // Create new file
      this.file = await Deno.open(this.path, {
        read: true,
        write: true,
        create: true,
        truncate: true,
      });
      await this.writeHeader();
    }
  }

  private async writeHeader(): Promise<void> {
    const buf = allocBuffer(WAL_HEADER_SIZE);
    buf.set(WAL_MAGIC, 0);
    writeUint32(buf, 4, 1); // version
    writeUint32(buf, 8, 0); // checkpoint LSN
    writeUint32(buf, 12, 0); // reserved
    await this.file.seek(0, Deno.SeekMode.Start);
    await this.file.write(buf);
  }

  private async readHeader(): Promise<void> {
    const buf = allocBuffer(WAL_HEADER_SIZE);
    await this.file.seek(0, Deno.SeekMode.Start);
    await this.file.read(buf);
    // Verify magic
    for (let i = 0; i < 4; i++) {
      if (buf[i] !== WAL_MAGIC[i]) {
        throw new Error("Invalid WAL file");
      }
    }
  }

  beginTransaction(): number {
    return ++this.txCounter;
  }

  async append(txId: number, op: WALOp, data: Uint8Array): Promise<void> {
    // recordLen(4) + txId(4) + op(1) + dataLen(4) + data + crc32(4)
    const recordLen = 4 + 1 + 4 + data.byteLength + 4;
    const buf = allocBuffer(4 + recordLen);

    let offset = 0;
    writeUint32(buf, offset, recordLen);
    offset += 4;
    writeUint32(buf, offset, txId);
    offset += 4;
    writeUint8(buf, offset, op);
    offset += 1;
    writeUint32(buf, offset, data.byteLength);
    offset += 4;
    buf.set(data, offset);
    offset += data.byteLength;

    // CRC32 over everything except the crc32 field itself
    const checksum = crc32(buf.subarray(0, offset));
    writeUint32(buf, offset, checksum);

    // Append to end of file
    await this.file.seek(0, Deno.SeekMode.End);
    await this.file.write(buf);
  }

  async commit(txId: number): Promise<void> {
    await this.append(txId, WALOp.Commit, new Uint8Array(0));
    // Ensure durability
    await this.file.sync();
  }

  // Replay all entries (for crash recovery)
  async replay(): Promise<WALEntry[]> {
    const entries: WALEntry[] = [];
    const stat = await this.file.stat();
    if (stat.size <= WAL_HEADER_SIZE) return entries;

    await this.file.seek(WAL_HEADER_SIZE, Deno.SeekMode.Start);
    const fullBuf = new Uint8Array(stat.size - WAL_HEADER_SIZE);
    let readOffset = 0;
    while (readOffset < fullBuf.byteLength) {
      const n = await this.file.read(fullBuf.subarray(readOffset));
      if (n === null) break;
      readOffset += n;
    }

    let offset = 0;
    while (offset + 4 <= fullBuf.byteLength) {
      const recordLen = readUint32(fullBuf, offset);
      if (recordLen === 0 || offset + 4 + recordLen > fullBuf.byteLength) break;

      const recordStart = offset + 4;
      const txId = readUint32(fullBuf, recordStart);
      const op = readUint8(fullBuf, recordStart + 4) as WALOp;
      const dataLen = readUint32(fullBuf, recordStart + 5);
      const data = fullBuf.subarray(recordStart + 9, recordStart + 9 + dataLen);

      // Verify CRC32
      const expectedCrc = readUint32(fullBuf, recordStart + 9 + dataLen);
      const actualCrc = crc32(fullBuf.subarray(offset, recordStart + 9 + dataLen));

      if (expectedCrc === actualCrc) {
        entries.push({ txId, op, data: new Uint8Array(data) });
      } else {
        // Corrupted entry — stop replay
        break;
      }

      offset += 4 + recordLen;
    }

    return entries;
  }

  // Find committed transaction IDs
  static findCommittedTxIds(entries: WALEntry[]): Set<number> {
    const committed = new Set<number>();
    for (const entry of entries) {
      if (entry.op === WALOp.Commit) {
        committed.add(entry.txId);
      }
    }
    return committed;
  }

  // Truncate WAL after checkpoint
  async truncate(): Promise<void> {
    await this.file.truncate(WAL_HEADER_SIZE);
    await this.writeHeader();
  }

  async close(): Promise<void> {
    if (this._closed) return;
    this._closed = true;
    this.file.close();
  }
}
