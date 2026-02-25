// Per-table .flop file — manages file header, page allocation, read/write
//
// File format:
//   Header (64B): "FLPT"(4) | version(2) | pageSize(2) | pageCount(4) | totalRows(4) | schemaVersion(2) | reserved(46)
//   Page 0 (4096B)
//   Page 1 (4096B)
//   ...

import {
  readUint16, writeUint16, readUint32, writeUint32,
  allocBuffer,
} from "../util/binary.ts";
import {
  PAGE_SIZE, FILE_HEADER_SIZE, TABLE_FILE_MAGIC,
  type TableFileHeader,
} from "../types.ts";
import { Page } from "./page.ts";
import { PageCache } from "./page_cache.ts";

const FILE_FORMAT_VERSION = 1;

export class TableFile {
  readonly path: string;
  private file!: Deno.FsFile;
  private _header!: TableFileHeader;
  private _pageCache!: PageCache;
  private _closed = false;
  // Hint: last page that had free space — try it first to avoid linear scan
  private _lastFreePage = -1;

  constructor(path: string) {
    this.path = path;
  }

  get header(): TableFileHeader {
    return this._header;
  }

  get pageCache(): PageCache {
    return this._pageCache;
  }

  static async open(path: string, maxCachePages = 1024): Promise<TableFile> {
    const tf = new TableFile(path);
    await tf._open(maxCachePages);
    return tf;
  }

  static async create(path: string, schemaVersion: number, maxCachePages = 1024): Promise<TableFile> {
    const tf = new TableFile(path);
    await tf._create(schemaVersion, maxCachePages);
    return tf;
  }

  private async _open(maxCachePages: number): Promise<void> {
    this.file = await Deno.open(this.path, { read: true, write: true });
    this._header = await this.readFileHeader();
    this._pageCache = new PageCache(this.file, maxCachePages);
  }

  private async _create(schemaVersion: number, maxCachePages: number): Promise<void> {
    this.file = await Deno.open(this.path, {
      read: true,
      write: true,
      create: true,
      truncate: true,
    });

    this._header = {
      pageCount: 0,
      totalRows: 0,
      schemaVersion,
    };

    await this.writeFileHeader();
    this._pageCache = new PageCache(this.file, maxCachePages);
  }

  private async readFileHeader(): Promise<TableFileHeader> {
    const buf = allocBuffer(FILE_HEADER_SIZE);
    await this.file.seek(0, Deno.SeekMode.Start);
    let offset = 0;
    while (offset < buf.byteLength) {
      const n = await this.file.read(buf.subarray(offset));
      if (n === null) break;
      offset += n;
    }

    // Verify magic
    for (let i = 0; i < 4; i++) {
      if (buf[i] !== TABLE_FILE_MAGIC[i]) {
        throw new Error(`Invalid table file: bad magic at ${this.path}`);
      }
    }

    return {
      pageCount: readUint32(buf, 8),
      totalRows: readUint32(buf, 12),
      schemaVersion: readUint16(buf, 16),
    };
  }

  async writeFileHeader(): Promise<void> {
    const buf = allocBuffer(FILE_HEADER_SIZE);

    // Magic "FLPT"
    buf.set(TABLE_FILE_MAGIC, 0);
    // Version
    writeUint16(buf, 4, FILE_FORMAT_VERSION);
    // Page size
    writeUint16(buf, 6, PAGE_SIZE);
    // Page count
    writeUint32(buf, 8, this._header.pageCount);
    // Total rows
    writeUint32(buf, 12, this._header.totalRows);
    // Schema version
    writeUint16(buf, 16, this._header.schemaVersion);

    await this.file.seek(0, Deno.SeekMode.Start);
    await this.file.write(buf);
  }

  // Allocate a new page at the end of the file
  async allocatePage(): Promise<{ pageNumber: number; page: Page }> {
    const pageNumber = this._header.pageCount;
    const page = Page.create(pageNumber);

    // Write the empty page to disk
    const offset = FILE_HEADER_SIZE + pageNumber * PAGE_SIZE;
    await this.file.seek(offset, Deno.SeekMode.Start);
    await this.file.write(page.data);

    this._header.pageCount++;
    await this.writeFileHeader();

    // Put in cache
    this._pageCache.putPage(pageNumber, page);

    return { pageNumber, page };
  }

  async getPage(pageNumber: number): Promise<Page> {
    return await this._pageCache.getPage(pageNumber);
  }

  markPageDirty(pageNumber: number): void {
    this._pageCache.markDirty(pageNumber);
  }

  async flush(): Promise<void> {
    await this._pageCache.flushAll();
    await this.writeFileHeader();
    await this.file.sync();
  }

  async close(): Promise<void> {
    if (this._closed) return;
    this._closed = true;
    await this._pageCache.flushAll();
    await this.writeFileHeader();
    this.file.close();
  }

  // Scan all pages and slots (for index rebuild)
  async *scanAllRows(): AsyncIterableIterator<{
    pageNumber: number;
    slotIndex: number;
    data: Uint8Array;
  }> {
    for (let p = 0; p < this._header.pageCount; p++) {
      const page = await this.getPage(p);
      for (const { slotIndex, data } of page.slots()) {
        yield { pageNumber: p, slotIndex, data };
      }
    }
  }

  // Find a page with enough free space for rowData, or allocate new
  async findOrAllocatePage(rowDataSize: number): Promise<{
    pageNumber: number;
    page: Page;
  }> {
    const needed = rowDataSize + 4; // +4 for slot entry

    // Fast path: try the last known free page first
    if (this._lastFreePage >= 0 && this._lastFreePage < this._header.pageCount) {
      const page = await this.getPage(this._lastFreePage);
      if (page.freeSpace >= needed) {
        return { pageNumber: this._lastFreePage, page };
      }
    }

    // Try the last page (most likely to have space for append-heavy workloads)
    const lastPage = this._header.pageCount - 1;
    if (lastPage >= 0 && lastPage !== this._lastFreePage) {
      const page = await this.getPage(lastPage);
      if (page.freeSpace >= needed) {
        this._lastFreePage = lastPage;
        return { pageNumber: lastPage, page };
      }
    }

    // No space found, allocate new page
    const result = await this.allocatePage();
    this._lastFreePage = result.pageNumber;
    return result;
  }

  get pageCount(): number {
    return this._header.pageCount;
  }

  get totalRows(): number {
    return this._header.totalRows;
  }

  set totalRows(count: number) {
    this._header.totalRows = count;
  }
}
