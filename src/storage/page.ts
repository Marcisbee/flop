// 4KB page format — header, slot directory, row data
//
// Layout (4096 bytes):
//   Header (12B): pageNumber(4) | slotCount(2) | freeSpaceOffset(2) | flags(1) | reserved(3)
//   Slot directory (grows forward from byte 12): [offset:uint16, length:uint16] per slot
//   Free space (middle)
//   Row data (grows backward from end of page)

import {
  readUint16, writeUint16, readUint32, writeUint32, readUint8, writeUint8,
  allocBuffer,
} from "../util/binary.ts";
import {
  PAGE_SIZE, PAGE_HEADER_SIZE, SLOT_SIZE,
  type PageHeader, type SlotEntry,
} from "../types.ts";

export class Page {
  readonly data: Uint8Array;
  header: PageHeader;

  constructor(data?: Uint8Array) {
    this.data = data ?? allocBuffer(PAGE_SIZE);
    this.header = this.readHeader();
  }

  static create(pageNumber: number): Page {
    const page = new Page();
    page.header = {
      pageNumber,
      slotCount: 0,
      freeSpaceOffset: PAGE_HEADER_SIZE,
      flags: 0,
    };
    page.writeHeader();
    return page;
  }

  private readHeader(): PageHeader {
    return {
      pageNumber: readUint32(this.data, 0),
      slotCount: readUint16(this.data, 4),
      freeSpaceOffset: readUint16(this.data, 6),
      flags: readUint8(this.data, 8),
    };
  }

  writeHeader(): void {
    writeUint32(this.data, 0, this.header.pageNumber);
    writeUint16(this.data, 4, this.header.slotCount);
    writeUint16(this.data, 6, this.header.freeSpaceOffset);
    writeUint8(this.data, 8, this.header.flags);
  }

  getSlot(index: number): SlotEntry {
    const offset = PAGE_HEADER_SIZE + index * SLOT_SIZE;
    return {
      offset: readUint16(this.data, offset),
      length: readUint16(this.data, offset + 2),
    };
  }

  setSlot(index: number, entry: SlotEntry): void {
    const offset = PAGE_HEADER_SIZE + index * SLOT_SIZE;
    writeUint16(this.data, offset, entry.offset);
    writeUint16(this.data, offset + 2, entry.length);
  }

  // How much free space is available for new row data
  get freeSpace(): number {
    const slotDirectoryEnd = PAGE_HEADER_SIZE + this.header.slotCount * SLOT_SIZE + SLOT_SIZE; // +SLOT_SIZE for the new slot
    const rowDataStart = this.rowDataStart();
    return rowDataStart - slotDirectoryEnd;
  }

  // The lowest byte used by row data (rows grow backward from page end)
  private rowDataStart(): number {
    if (this.header.slotCount === 0) return PAGE_SIZE;
    let min = PAGE_SIZE;
    for (let i = 0; i < this.header.slotCount; i++) {
      const slot = this.getSlot(i);
      if (slot.length > 0 && slot.offset < min) {
        min = slot.offset;
      }
    }
    return min;
  }

  // Insert row data, returns slot index or -1 if no space
  insertRow(rowData: Uint8Array): number {
    const neededSpace = rowData.byteLength + SLOT_SIZE;
    if (this.freeSpace < neededSpace) return -1;

    // Place row data at the end, growing backward
    const rowStart = this.rowDataStart() - rowData.byteLength;
    this.data.set(rowData, rowStart);

    // Add slot entry
    const slotIndex = this.header.slotCount;
    this.setSlot(slotIndex, { offset: rowStart, length: rowData.byteLength });

    this.header.slotCount++;
    this.header.freeSpaceOffset = PAGE_HEADER_SIZE + this.header.slotCount * SLOT_SIZE;
    this.writeHeader();

    return slotIndex;
  }

  // Read row data at a slot index
  readRow(slotIndex: number): Uint8Array | null {
    if (slotIndex >= this.header.slotCount) return null;
    const slot = this.getSlot(slotIndex);
    if (slot.length === 0) return null; // deleted slot
    return this.data.subarray(slot.offset, slot.offset + slot.length);
  }

  // Mark a slot as deleted (tombstone — set length to 0)
  deleteRow(slotIndex: number): void {
    if (slotIndex >= this.header.slotCount) return;
    this.setSlot(slotIndex, { offset: 0, length: 0 });
    this.writeHeader();
  }

  // Update row in place if it fits, returns false if it doesn't fit
  updateRow(slotIndex: number, newData: Uint8Array): boolean {
    if (slotIndex >= this.header.slotCount) return false;
    const slot = this.getSlot(slotIndex);

    if (newData.byteLength <= slot.length) {
      // Fits in existing space
      this.data.set(newData, slot.offset);
      if (newData.byteLength < slot.length) {
        // Zero out remaining bytes
        this.data.fill(0, slot.offset + newData.byteLength, slot.offset + slot.length);
      }
      this.setSlot(slotIndex, { offset: slot.offset, length: newData.byteLength });
      return true;
    }

    // Doesn't fit — caller must delete + re-insert
    return false;
  }

  // Iterate all valid (non-deleted) slots
  *slots(): IterableIterator<{ slotIndex: number; data: Uint8Array }> {
    for (let i = 0; i < this.header.slotCount; i++) {
      const slot = this.getSlot(i);
      if (slot.length > 0) {
        yield { slotIndex: i, data: this.data.subarray(slot.offset, slot.offset + slot.length) };
      }
    }
  }

  get slotCount(): number {
    return this.header.slotCount;
  }
}
