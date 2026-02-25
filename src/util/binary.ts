// Binary read/write helpers for little-endian buffer operations

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function readUint8(buf: Uint8Array, offset: number): number {
  return buf[offset];
}

export function writeUint8(buf: Uint8Array, offset: number, value: number): void {
  buf[offset] = value & 0xff;
}

export function readUint16(buf: Uint8Array, offset: number): number {
  return buf[offset] | (buf[offset + 1] << 8);
}

export function writeUint16(buf: Uint8Array, offset: number, value: number): void {
  buf[offset] = value & 0xff;
  buf[offset + 1] = (value >> 8) & 0xff;
}

export function readUint32(buf: Uint8Array, offset: number): number {
  return (
    buf[offset] |
    (buf[offset + 1] << 8) |
    (buf[offset + 2] << 16) |
    ((buf[offset + 3] << 24) >>> 0)
  ) >>> 0;
}

export function writeUint32(buf: Uint8Array, offset: number, value: number): void {
  buf[offset] = value & 0xff;
  buf[offset + 1] = (value >> 8) & 0xff;
  buf[offset + 2] = (value >> 16) & 0xff;
  buf[offset + 3] = (value >> 24) & 0xff;
}

export function readInt32(buf: Uint8Array, offset: number): number {
  return (
    buf[offset] |
    (buf[offset + 1] << 8) |
    (buf[offset + 2] << 16) |
    (buf[offset + 3] << 24)
  );
}

export function writeInt32(buf: Uint8Array, offset: number, value: number): void {
  const v = value | 0;
  buf[offset] = v & 0xff;
  buf[offset + 1] = (v >> 8) & 0xff;
  buf[offset + 2] = (v >> 16) & 0xff;
  buf[offset + 3] = (v >> 24) & 0xff;
}

export function readFloat64(buf: Uint8Array, offset: number): number {
  const view = new DataView(buf.buffer, buf.byteOffset + offset, 8);
  return view.getFloat64(0, true);
}

export function writeFloat64(buf: Uint8Array, offset: number, value: number): void {
  const view = new DataView(buf.buffer, buf.byteOffset + offset, 8);
  view.setFloat64(0, value, true);
}

export function readString(buf: Uint8Array, offset: number): { value: string; bytesRead: number } {
  const len = readUint32(buf, offset);
  if (len === 0) return { value: "", bytesRead: 4 };
  const bytes = buf.subarray(offset + 4, offset + 4 + len);
  return { value: decoder.decode(bytes), bytesRead: 4 + len };
}

export function writeString(buf: Uint8Array, offset: number, value: string): number {
  const encoded = encoder.encode(value);
  writeUint32(buf, offset, encoded.byteLength);
  buf.set(encoded, offset + 4);
  return 4 + encoded.byteLength;
}

export function encodeString(value: string): Uint8Array {
  return encoder.encode(value);
}

export function decodeString(buf: Uint8Array): string {
  return decoder.decode(buf);
}

export function stringByteLength(value: string): number {
  return encoder.encode(value).byteLength;
}

export function concatBuffers(...buffers: Uint8Array[]): Uint8Array {
  let totalLen = 0;
  for (const b of buffers) totalLen += b.byteLength;
  const result = new Uint8Array(totalLen);
  let offset = 0;
  for (const b of buffers) {
    result.set(b, offset);
    offset += b.byteLength;
  }
  return result;
}

export function allocBuffer(size: number): Uint8Array {
  return new Uint8Array(size);
}
