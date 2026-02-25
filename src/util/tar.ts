// Minimal tar archive creation and extraction
// Implements POSIX ustar format (512-byte headers, 512-byte aligned data)

import { concatBuffers, encodeString } from "./binary.ts";

const BLOCK_SIZE = 512;
const HEADER_SIZE = 512;

function padBlock(data: Uint8Array): Uint8Array {
  const remainder = data.byteLength % BLOCK_SIZE;
  if (remainder === 0) return data;
  const padded = new Uint8Array(data.byteLength + (BLOCK_SIZE - remainder));
  padded.set(data);
  return padded;
}

function encodeOctal(value: number, length: number): Uint8Array {
  const str = value.toString(8).padStart(length - 1, "0");
  const buf = new Uint8Array(length);
  for (let i = 0; i < str.length && i < length - 1; i++) {
    buf[i] = str.charCodeAt(i);
  }
  buf[length - 1] = 0; // null terminator
  return buf;
}

function decodeOctal(buf: Uint8Array, offset: number, length: number): number {
  let str = "";
  for (let i = offset; i < offset + length; i++) {
    if (buf[i] === 0 || buf[i] === 0x20) break;
    str += String.fromCharCode(buf[i]);
  }
  return str.length > 0 ? parseInt(str, 8) : 0;
}

function computeChecksum(header: Uint8Array): number {
  let sum = 0;
  for (let i = 0; i < HEADER_SIZE; i++) {
    // Checksum field (offset 148, 8 bytes) treated as spaces
    if (i >= 148 && i < 156) {
      sum += 0x20;
    } else {
      sum += header[i];
    }
  }
  return sum;
}

export interface TarEntry {
  path: string;
  data: Uint8Array;
  mode?: number;
  mtime?: number;
}

export function createTarEntry(entry: TarEntry): Uint8Array {
  const header = new Uint8Array(HEADER_SIZE);
  const pathBytes = encodeString(entry.path);
  const mode = entry.mode ?? 0o644;
  const mtime = entry.mtime ?? Math.floor(Date.now() / 1000);

  // Name (0, 100)
  header.set(pathBytes.subarray(0, Math.min(pathBytes.byteLength, 100)), 0);
  // Mode (100, 8)
  header.set(encodeOctal(mode, 8), 100);
  // UID (108, 8)
  header.set(encodeOctal(0, 8), 108);
  // GID (116, 8)
  header.set(encodeOctal(0, 8), 116);
  // Size (124, 12)
  header.set(encodeOctal(entry.data.byteLength, 12), 124);
  // Mtime (136, 12)
  header.set(encodeOctal(mtime, 12), 136);
  // Type flag (156, 1) - '0' = regular file
  header[156] = 0x30;
  // Magic (257, 6) - "ustar\0"
  header.set(encodeString("ustar\0"), 257);
  // Version (263, 2) - "00"
  header[263] = 0x30;
  header[264] = 0x30;

  // Compute and write checksum (148, 8)
  const checksum = computeChecksum(header);
  header.set(encodeOctal(checksum, 7), 148);
  header[155] = 0x20; // trailing space

  return concatBuffers(header, padBlock(entry.data));
}

export function createTar(entries: TarEntry[]): Uint8Array {
  const parts: Uint8Array[] = [];
  for (const entry of entries) {
    parts.push(createTarEntry(entry));
  }
  // End-of-archive: two 512-byte blocks of zeros
  parts.push(new Uint8Array(BLOCK_SIZE * 2));
  return concatBuffers(...parts);
}

export interface ParsedTarEntry {
  path: string;
  size: number;
  data: Uint8Array;
}

export function parseTar(data: Uint8Array): ParsedTarEntry[] {
  const entries: ParsedTarEntry[] = [];
  let offset = 0;

  while (offset + HEADER_SIZE <= data.byteLength) {
    const header = data.subarray(offset, offset + HEADER_SIZE);

    // Check for end-of-archive (all zeros)
    let allZero = true;
    for (let i = 0; i < HEADER_SIZE; i++) {
      if (header[i] !== 0) { allZero = false; break; }
    }
    if (allZero) break;

    // Read path
    let pathEnd = 0;
    while (pathEnd < 100 && header[pathEnd] !== 0) pathEnd++;
    const path = new TextDecoder().decode(header.subarray(0, pathEnd));

    // Read size
    const size = decodeOctal(header, 124, 12);

    // Read type flag
    const typeFlag = header[156];

    offset += HEADER_SIZE;

    if (typeFlag === 0x30 || typeFlag === 0) { // Regular file
      const fileData = data.subarray(offset, offset + size);
      entries.push({ path, size, data: new Uint8Array(fileData) });
    }

    // Advance past data blocks (padded to 512)
    const dataBlocks = Math.ceil(size / BLOCK_SIZE) * BLOCK_SIZE;
    offset += dataBlocks;
  }

  return entries;
}
