// Row serialization — schema-compiled binary encoder/decoder
// Format: [schemaVersion:uint16] [fieldCount:uint8] [typeTag:uint8 + data]...

import {
  readUint8, writeUint8, readUint16, writeUint16, readUint32, writeUint32,
  readInt32, writeInt32,
  readFloat64, writeFloat64, readString, writeString, stringByteLength,
  allocBuffer, concatBuffers,
} from "../util/binary.ts";
import { TypeTag, type CompiledSchema, type CompiledField } from "../types.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export interface RowSerializer {
  serialize(row: Record<string, unknown>, schemaVersion: number): Uint8Array;
  deserialize(buf: Uint8Array, offset: number): { row: Record<string, unknown>; schemaVersion: number; bytesRead: number };
  estimateSize(row: Record<string, unknown>): number;
}

export function createRowSerializer(schema: CompiledSchema): RowSerializer {
  return {
    serialize(row: Record<string, unknown>, schemaVersion: number): Uint8Array {
      return serializeRow(row, schema, schemaVersion);
    },
    deserialize(buf: Uint8Array, offset: number) {
      return deserializeRow(buf, offset, schema);
    },
    estimateSize(row: Record<string, unknown>): number {
      return estimateRowSize(row, schema);
    },
  };
}

function fieldTypeTag(kind: string): number {
  switch (kind) {
    case "string": case "bcrypt": case "ref": case "enum": return TypeTag.String;
    case "number": case "timestamp": return TypeTag.Number;
    case "integer": return TypeTag.Integer;
    case "boolean": return TypeTag.Boolean;
    case "json": return TypeTag.Json;
    case "fileSingle": return TypeTag.FileSingle;
    case "fileMulti": return TypeTag.FileMulti;
    case "roles": case "refMulti": case "set": return TypeTag.Array;
    case "vector": return TypeTag.Vector;
    default: return TypeTag.Null;
  }
}

function serializeRow(
  row: Record<string, unknown>,
  schema: CompiledSchema,
  schemaVersion: number,
): Uint8Array {
  // First pass: calculate total size
  const parts: Uint8Array[] = [];

  // Header: schemaVersion(2) + fieldCount(1)
  const header = allocBuffer(3);
  writeUint16(header, 0, schemaVersion);
  writeUint8(header, 2, schema.fields.length);
  parts.push(header);

  // Fields
  for (const field of schema.fields) {
    const value = row[field.name];
    parts.push(serializeField(value, field));
  }

  return concatBuffers(...parts);
}

function serializeField(value: unknown, field: CompiledField): Uint8Array {
  if (value === null || value === undefined) {
    return new Uint8Array([TypeTag.Null]);
  }

  const tag = fieldTypeTag(field.kind);

  switch (field.kind) {
    case "string":
    case "bcrypt":
    case "ref":
    case "enum": {
      const str = String(value);
      const encoded = encoder.encode(str);
      const buf = allocBuffer(1 + 4 + encoded.byteLength);
      buf[0] = tag;
      writeUint32(buf, 1, encoded.byteLength);
      buf.set(encoded, 5);
      return buf;
    }

    case "number":
    case "timestamp": {
      const buf = allocBuffer(1 + 8);
      buf[0] = tag;
      writeFloat64(buf, 1, Number(value));
      return buf;
    }

    case "integer": {
      const buf = allocBuffer(1 + 4);
      buf[0] = tag;
      writeInt32(buf, 1, Number(value));
      return buf;
    }

    case "boolean": {
      return new Uint8Array([tag, value ? 1 : 0]);
    }

    case "json":
    case "fileSingle":
    case "fileMulti": {
      const jsonStr = JSON.stringify(value);
      const encoded = encoder.encode(jsonStr);
      const buf = allocBuffer(1 + 4 + encoded.byteLength);
      buf[0] = tag;
      writeUint32(buf, 1, encoded.byteLength);
      buf.set(encoded, 5);
      return buf;
    }

    case "roles":
    case "refMulti":
    case "set": {
      const arr = Array.isArray(value) ? value : [];
      const strParts: Uint8Array[] = [];
      const headerBuf = allocBuffer(1 + 2);
      headerBuf[0] = tag;
      writeUint16(headerBuf, 1, arr.length);
      strParts.push(headerBuf);

      for (const item of arr) {
        const encoded = encoder.encode(String(item));
        const lenBuf = allocBuffer(2);
        writeUint16(lenBuf, 0, encoded.byteLength);
        strParts.push(lenBuf);
        strParts.push(encoded);
      }

      return concatBuffers(...strParts);
    }

    case "vector": {
      const arr = Array.isArray(value) ? value : [];
      const buf = allocBuffer(1 + 2 + arr.length * 8);
      buf[0] = tag;
      writeUint16(buf, 1, arr.length);
      for (let i = 0; i < arr.length; i++) {
        writeFloat64(buf, 3 + i * 8, Number(arr[i]));
      }
      return buf;
    }

    default:
      return new Uint8Array([TypeTag.Null]);
  }
}

function deserializeRow(
  buf: Uint8Array,
  offset: number,
  schema: CompiledSchema,
): { row: Record<string, unknown>; schemaVersion: number; bytesRead: number } {
  const start = offset;
  const schemaVersion = readUint16(buf, offset);
  offset += 2;
  const fieldCount = readUint8(buf, offset);
  offset += 1;

  const row: Record<string, unknown> = {};

  for (let i = 0; i < fieldCount && i < schema.fields.length; i++) {
    const field = schema.fields[i];
    const tag = readUint8(buf, offset);
    offset += 1;

    if (tag === TypeTag.Null) {
      row[field.name] = null;
      continue;
    }

    switch (tag) {
      case TypeTag.String: {
        const len = readUint32(buf, offset);
        offset += 4;
        row[field.name] = decoder.decode(buf.subarray(offset, offset + len));
        offset += len;
        break;
      }

      case TypeTag.Number: {
        row[field.name] = readFloat64(buf, offset);
        offset += 8;
        break;
      }

      case TypeTag.Integer: {
        row[field.name] = readInt32(buf, offset);
        offset += 4;
        break;
      }

      case TypeTag.Boolean: {
        row[field.name] = buf[offset] === 1;
        offset += 1;
        break;
      }

      case TypeTag.Json:
      case TypeTag.FileSingle:
      case TypeTag.FileMulti: {
        const len = readUint32(buf, offset);
        offset += 4;
        if (len === 0) {
          row[field.name] = null;
        } else {
          const jsonStr = decoder.decode(buf.subarray(offset, offset + len));
          row[field.name] = JSON.parse(jsonStr);
        }
        offset += len;
        break;
      }

      case TypeTag.Array: {
        const count = readUint16(buf, offset);
        offset += 2;
        const arr: string[] = [];
        for (let j = 0; j < count; j++) {
          const len = readUint16(buf, offset);
          offset += 2;
          arr.push(decoder.decode(buf.subarray(offset, offset + len)));
          offset += len;
        }
        row[field.name] = arr;
        break;
      }

      case TypeTag.Vector: {
        const count = readUint16(buf, offset);
        offset += 2;
        const vec: number[] = [];
        for (let j = 0; j < count; j++) {
          vec.push(readFloat64(buf, offset));
          offset += 8;
        }
        row[field.name] = vec;
        break;
      }

      default: {
        row[field.name] = null;
        break;
      }
    }
  }

  return { row, schemaVersion, bytesRead: offset - start };
}

function estimateRowSize(row: Record<string, unknown>, schema: CompiledSchema): number {
  let size = 3; // header

  for (const field of schema.fields) {
    const value = row[field.name];
    size += 1; // type tag

    if (value === null || value === undefined) continue;

    switch (field.kind) {
      case "string":
      case "bcrypt":
      case "ref":
      case "enum":
        size += 4 + stringByteLength(String(value));
        break;
      case "number":
      case "timestamp":
        size += 8;
        break;
      case "integer":
        size += 4;
        break;
      case "boolean":
        size += 1;
        break;
      case "json":
      case "fileSingle":
      case "fileMulti":
        size += 4 + stringByteLength(JSON.stringify(value));
        break;
      case "roles":
      case "refMulti":
      case "set": {
        const arr = Array.isArray(value) ? value : [];
        size += 2;
        for (const item of arr) {
          size += 2 + stringByteLength(String(item));
        }
        break;
      }
      case "vector": {
        const arr = Array.isArray(value) ? value : [];
        size += 2 + arr.length * 8;
        break;
      }
    }
  }

  return size;
}

// Deserialize raw field values (for migration — uses positional decoding without schema field names)
export function deserializeRawFields(
  buf: Uint8Array,
  offset: number,
): { values: unknown[]; schemaVersion: number; bytesRead: number } {
  const start = offset;
  const schemaVersion = readUint16(buf, offset);
  offset += 2;
  const fieldCount = readUint8(buf, offset);
  offset += 1;

  const values: unknown[] = [];

  for (let i = 0; i < fieldCount; i++) {
    const tag = readUint8(buf, offset);
    offset += 1;

    if (tag === TypeTag.Null) {
      values.push(null);
      continue;
    }

    switch (tag) {
      case TypeTag.String: {
        const len = readUint32(buf, offset);
        offset += 4;
        values.push(decoder.decode(buf.subarray(offset, offset + len)));
        offset += len;
        break;
      }
      case TypeTag.Number: {
        values.push(readFloat64(buf, offset));
        offset += 8;
        break;
      }
      case TypeTag.Integer: {
        values.push(readInt32(buf, offset));
        offset += 4;
        break;
      }
      case TypeTag.Boolean: {
        values.push(buf[offset] === 1);
        offset += 1;
        break;
      }
      case TypeTag.Json:
      case TypeTag.FileSingle:
      case TypeTag.FileMulti: {
        const len = readUint32(buf, offset);
        offset += 4;
        if (len === 0) {
          values.push(null);
        } else {
          values.push(JSON.parse(decoder.decode(buf.subarray(offset, offset + len))));
        }
        offset += len;
        break;
      }
      case TypeTag.Array: {
        const count = readUint16(buf, offset);
        offset += 2;
        const arr: string[] = [];
        for (let j = 0; j < count; j++) {
          const len = readUint16(buf, offset);
          offset += 2;
          arr.push(decoder.decode(buf.subarray(offset, offset + len)));
          offset += len;
        }
        values.push(arr);
        break;
      }
      case TypeTag.Vector: {
        const count = readUint16(buf, offset);
        offset += 2;
        const vec: number[] = [];
        for (let j = 0; j < count; j++) {
          vec.push(readFloat64(buf, offset));
          offset += 8;
        }
        values.push(vec);
        break;
      }
      default:
        values.push(null);
    }
  }

  return { values, schemaVersion, bytesRead: offset - start };
}
