// LRU page cache — keeps hot pages in memory, evicts cold ones

import { LRUCache } from "../util/lru.ts";
import { Page } from "./page.ts";
import { PAGE_SIZE, FILE_HEADER_SIZE } from "../types.ts";

export class PageCache {
  private cache: LRUCache<number, Page>;
  private dirtyPages = new Set<number>();
  private file: Deno.FsFile;

  constructor(file: Deno.FsFile, maxPages: number = 1024) {
    this.file = file;
    this.cache = new LRUCache<number, Page>(maxPages, (pageNumber, page) => {
      // On eviction, flush if dirty
      if (this.dirtyPages.has(pageNumber)) {
        this.flushPageSync(pageNumber, page);
      }
    });
  }

  async getPage(pageNumber: number): Promise<Page> {
    const cached = this.cache.get(pageNumber);
    if (cached) return cached;

    // Read from disk
    const page = await this.readPageFromDisk(pageNumber);
    this.cache.set(pageNumber, page);
    return page;
  }

  putPage(pageNumber: number, page: Page): void {
    this.cache.set(pageNumber, page);
  }

  markDirty(pageNumber: number): void {
    this.dirtyPages.add(pageNumber);
  }

  async flushAll(): Promise<void> {
    for (const pageNumber of this.dirtyPages) {
      const page = this.cache.get(pageNumber);
      if (page) {
        await this.writePageToDisk(pageNumber, page);
      }
    }
    this.dirtyPages.clear();
    await this.file.sync();
  }

  async flushPage(pageNumber: number): Promise<void> {
    if (!this.dirtyPages.has(pageNumber)) return;
    const page = this.cache.get(pageNumber);
    if (page) {
      await this.writePageToDisk(pageNumber, page);
      this.dirtyPages.delete(pageNumber);
    }
  }

  private async readPageFromDisk(pageNumber: number): Promise<Page> {
    const offset = FILE_HEADER_SIZE + pageNumber * PAGE_SIZE;
    const buf = new Uint8Array(PAGE_SIZE);
    await this.file.seek(offset, Deno.SeekMode.Start);
    await readFull(this.file, buf);
    return new Page(buf);
  }

  private async writePageToDisk(pageNumber: number, page: Page): Promise<void> {
    const offset = FILE_HEADER_SIZE + pageNumber * PAGE_SIZE;
    await this.file.seek(offset, Deno.SeekMode.Start);
    await writeFull(this.file, page.data);
  }

  private flushPageSync(pageNumber: number, page: Page): void {
    // Synchronous flush during eviction — use blocking write
    const offset = FILE_HEADER_SIZE + pageNumber * PAGE_SIZE;
    this.file.seekSync(offset, Deno.SeekMode.Start);
    this.file.writeSync(page.data);
    this.dirtyPages.delete(pageNumber);
  }

  isDirty(pageNumber: number): boolean {
    return this.dirtyPages.has(pageNumber);
  }

  get dirtyCount(): number {
    return this.dirtyPages.size;
  }

  clear(): void {
    this.dirtyPages.clear();
    this.cache.clear();
  }
}

async function readFull(file: Deno.FsFile, buf: Uint8Array): Promise<void> {
  let offset = 0;
  while (offset < buf.byteLength) {
    const n = await file.read(buf.subarray(offset));
    if (n === null) break;
    offset += n;
  }
}

async function writeFull(file: Deno.FsFile, buf: Uint8Array): Promise<void> {
  let offset = 0;
  while (offset < buf.byteLength) {
    const n = await file.write(buf.subarray(offset));
    offset += n;
  }
}
