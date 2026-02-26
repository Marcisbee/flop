// File asset storage — store, serve, cleanup, MIME validation

import type { FileRef } from "../types.ts";

// Magic bytes for common MIME types
const MAGIC_BYTES: Record<string, number[]> = {
  "image/png": [0x89, 0x50, 0x4e, 0x47],
  "image/jpeg": [0xff, 0xd8, 0xff],
  "image/gif": [0x47, 0x49, 0x46],
  "image/webp": [0x52, 0x49, 0x46, 0x46], // RIFF (first 4 bytes)
  "application/pdf": [0x25, 0x50, 0x44, 0x46], // %PDF
};

// File extension to MIME type mapping
const EXT_TO_MIME: Record<string, string> = {
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".pdf": "application/pdf",
  ".json": "application/json",
  ".txt": "text/plain",
  ".html": "text/html",
  ".css": "text/css",
  ".js": "application/javascript",
  ".svg": "image/svg+xml",
  ".mp4": "video/mp4",
  ".mp3": "audio/mpeg",
  ".wav": "audio/wav",
  ".zip": "application/zip",
};

export function mimeFromExtension(filename: string): string {
  const ext = filename.substring(filename.lastIndexOf(".")).toLowerCase();
  return EXT_TO_MIME[ext] ?? "application/octet-stream";
}

export function validateMimeType(
  declaredMime: string,
  allowedMimeTypes: string[],
): boolean {
  if (allowedMimeTypes.length === 0) return true;
  return allowedMimeTypes.some((allowed) => {
    if (allowed === declaredMime) return true;
    if (allowed.endsWith("/*")) {
      return declaredMime.startsWith(allowed.slice(0, -1));
    }
    return false;
  });
}

export function validateMagicBytes(data: Uint8Array, declaredMime: string): boolean {
  const expected = MAGIC_BYTES[declaredMime];
  if (!expected) return true; // No magic bytes check for unknown types
  if (data.byteLength < expected.length) return false;
  return expected.every((b, i) => data[i] === b);
}

export function sanitizeFilename(name: string): string {
  // Remove path separators and dangerous chars
  return name
    .replace(/[/\\:*?"<>|]/g, "_")
    .replace(/\.\./g, "_")
    .trim() || "unnamed";
}

function hashFilename(data: Uint8Array, originalName: string): string {
  // Simple FNV-1a hash of file contents for unique naming
  let h = 0x811c9dc5;
  for (let i = 0; i < data.byteLength; i++) {
    h ^= data[i];
    h = Math.imul(h, 0x01000193);
  }
  const hash = (h >>> 0).toString(36);
  const ext = originalName.includes(".") ? originalName.substring(originalName.lastIndexOf(".")).toLowerCase() : "";
  return hash + ext;
}

export async function storeFile(
  dataDir: string,
  tableName: string,
  rowId: string,
  fieldName: string,
  filename: string,
  data: Uint8Array,
  mime: string,
): Promise<FileRef> {
  const hashedName = hashFilename(data, filename);
  const dirPath = `${dataDir}/_files/${tableName}/${rowId}/${fieldName}`;
  await Deno.mkdir(dirPath, { recursive: true });

  const filePath = `${dirPath}/${hashedName}`;
  await Deno.writeFile(filePath, data);

  const relativePath = `_files/${tableName}/${rowId}/${fieldName}/${hashedName}`;
  return {
    path: relativePath,
    name: filename,
    size: data.byteLength,
    mime,
    url: `/api/files/${tableName}/${rowId}/${fieldName}/${hashedName}`,
  };
}

export async function deleteFileRef(dataDir: string, ref: FileRef): Promise<void> {
  const filePath = `${dataDir}/${ref.path}`;
  try {
    await Deno.remove(filePath);
  } catch {
    // File already gone
  }
}

export async function deleteRowFiles(
  dataDir: string,
  tableName: string,
  rowId: string,
): Promise<void> {
  const dirPath = `${dataDir}/_files/${tableName}/${rowId}`;
  try {
    await Deno.remove(dirPath, { recursive: true });
  } catch {
    // Directory doesn't exist
  }
}

export async function cleanupOrphanedFiles(
  dataDir: string,
  tableName: string,
  rowId: string,
  fieldName: string,
  currentRefs: FileRef[],
): Promise<void> {
  const dirPath = `${dataDir}/_files/${tableName}/${rowId}/${fieldName}`;
  const currentPaths = new Set(currentRefs.map((r) => r.path));

  try {
    for await (const entry of Deno.readDir(dirPath)) {
      const refPath = `_files/${tableName}/${rowId}/${fieldName}/${entry.name}`;
      if (!currentPaths.has(refPath)) {
        await Deno.remove(`${dirPath}/${entry.name}`);
      }
    }
  } catch {
    // Directory doesn't exist
  }
}

// Serve a file from disk — returns Response or null
export async function serveFile(
  dataDir: string,
  urlPath: string,
): Promise<Response | null> {
  // urlPath: "/_files/users/abc123/avatar/photo.png"
  const relativePath = urlPath.replace(/^\/_files\//, "_files/");
  const filePath = `${dataDir}/${relativePath}`;

  try {
    const file = await Deno.open(filePath, { read: true });
    const stat = await file.stat();
    const mime = mimeFromExtension(filePath);

    return new Response(file.readable, {
      headers: {
        "Content-Type": mime,
        "Content-Length": String(stat.size),
        "Cache-Control": "public, max-age=31536000, immutable",
      },
    });
  } catch {
    return null;
  }
}
