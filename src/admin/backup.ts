// Backup and restore — tar.gz creation and extraction

import { createTar, parseTar, type TarEntry } from "../util/tar.ts";
import type { Database } from "../database.ts";

interface BackupManifest {
  version: number;
  created: string;
  flopVersion: string;
  tables: string[];
  totalFiles: number;
}

export async function createBackup(db: Database): Promise<Uint8Array> {
  const dataDir = db.getDataDir();

  // Flush all WALs first
  await db.checkpoint();

  const entries: TarEntry[] = [];
  const tableNames: string[] = [];
  let fileCount = 0;

  // Add table data files
  for (const [name] of db.tables) {
    tableNames.push(name);

    // .flop file
    try {
      const data = await Deno.readFile(`${dataDir}/${name}.flop`);
      entries.push({ path: `tables/${name}.flop`, data });
    } catch { /* skip if missing */ }

    // .idx file
    try {
      const data = await Deno.readFile(`${dataDir}/${name}.idx`);
      entries.push({ path: `tables/${name}.idx`, data });
    } catch { /* skip if missing */ }
  }

  // Add _files directory recursively
  try {
    await collectFiles(`${dataDir}/_files`, "files", entries);
    fileCount = entries.filter((e) => e.path.startsWith("files/")).length;
  } catch { /* no files directory */ }

  // Create manifest
  const manifest: BackupManifest = {
    version: 1,
    created: new Date().toISOString(),
    flopVersion: "0.1.0",
    tables: tableNames,
    totalFiles: fileCount,
  };

  entries.unshift({
    path: "manifest.json",
    data: new TextEncoder().encode(JSON.stringify(manifest, null, 2)),
  });

  // Create tar
  const tar = createTar(entries);

  // Compress with gzip
  const compressed = await compress(tar);
  return compressed;
}

export async function restoreBackup(db: Database, data: Uint8Array): Promise<void> {
  const dataDir = db.getDataDir();

  // Decompress
  const decompressed = await decompress(data);

  // Parse tar
  const entries = parseTar(decompressed);

  // Validate manifest
  const manifestEntry = entries.find((e) => e.path === "manifest.json");
  if (!manifestEntry) {
    throw new Error("Invalid backup: missing manifest.json");
  }

  const manifest = JSON.parse(
    new TextDecoder().decode(manifestEntry.data),
  ) as BackupManifest;

  if (manifest.version !== 1) {
    throw new Error(`Unsupported backup version: ${manifest.version}`);
  }

  // Close all tables
  await db.close();

  // Clear table data files and _files (but keep _meta.flop)
  for (const name of manifest.tables) {
    try { await Deno.remove(`${dataDir}/${name}.flop`); } catch { /* */ }
    try { await Deno.remove(`${dataDir}/${name}.idx`); } catch { /* */ }
    try { await Deno.remove(`${dataDir}/${name}.wal`); } catch { /* */ }
  }

  try {
    await Deno.remove(`${dataDir}/_files`, { recursive: true });
  } catch { /* */ }

  await Deno.mkdir(`${dataDir}/_files`, { recursive: true });

  // Extract files
  for (const entry of entries) {
    if (entry.path === "manifest.json") continue;

    let destPath: string;
    if (entry.path.startsWith("tables/")) {
      destPath = `${dataDir}/${entry.path.slice("tables/".length)}`;
    } else if (entry.path.startsWith("files/")) {
      destPath = `${dataDir}/_files/${entry.path.slice("files/".length)}`;
    } else {
      continue;
    }

    // Ensure directory exists
    const dir = destPath.substring(0, destPath.lastIndexOf("/"));
    await Deno.mkdir(dir, { recursive: true });
    await Deno.writeFile(destPath, entry.data);
  }

  // Reopen database
  await db.open();
}

async function collectFiles(
  dirPath: string,
  prefix: string,
  entries: TarEntry[],
): Promise<void> {
  for await (const entry of Deno.readDir(dirPath)) {
    const fullPath = `${dirPath}/${entry.name}`;
    const tarPath = `${prefix}/${entry.name}`;

    if (entry.isFile) {
      const data = await Deno.readFile(fullPath);
      entries.push({ path: tarPath, data });
    } else if (entry.isDirectory) {
      await collectFiles(fullPath, tarPath, entries);
    }
  }
}

async function compress(data: Uint8Array): Promise<Uint8Array> {
  const buf = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer;
  const stream = new Blob([buf]).stream().pipeThrough(new CompressionStream("gzip"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

async function decompress(data: Uint8Array): Promise<Uint8Array> {
  const buf = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer;
  const stream = new Blob([buf]).stream().pipeThrough(new DecompressionStream("gzip"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}
