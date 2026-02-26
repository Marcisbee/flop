// Admin panel — route registration + API handlers

import type { Database } from "../database.ts";
import type { AuthService } from "../server/auth.ts";
import { extractBearerToken, verifyJWT, jwtToAuthContext } from "../server/auth.ts";
// Read shared admin HTML files (relative to repo root)
const SHARED_ADMIN = new URL("../../../shared/admin/", import.meta.url);
const readSharedHTML = (name: string) => Deno.readTextFile(new URL(name, SHARED_ADMIN));
const loginPageHTML = await readSharedHTML("login.html");
const setupPageHTML = await readSharedHTML("setup.html");
const adminPageHTML = await readSharedHTML("admin.html");
import { createBackup, restoreBackup } from "./backup.ts";
import { storeFile, mimeFromExtension, validateMimeType, validateMagicBytes } from "../storage/files.ts";
import type { FileRef } from "../types.ts";

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function htmlResponse(html: string): Response {
  return new Response(html, {
    headers: { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" },
  });
}

export function createAdminHandler(
  db: Database,
  authService: AuthService | null,
  jwtSecret: string,
  initialSetupToken: string | null = null,
): (req: Request) => Promise<Response | null> {
  let setupToken = initialSetupToken;

  return async (req: Request): Promise<Response | null> => {
    const url = new URL(req.url);
    const pathname = url.pathname;

    // Setup page (token-gated, only when no superadmin exists)
    if ((pathname === "/_/setup" || pathname === "/_/setup/") && req.method === "GET") {
      if (!authService || !setupToken) {
        return new Response(null, { status: 302, headers: { Location: "/_/login" } });
      }
      const hasSuperadmin = await authService.hasSuperadmin();
      if (hasSuperadmin) {
        setupToken = null;
        return new Response(null, { status: 302, headers: { Location: "/_/login" } });
      }
      const token = url.searchParams.get("token");
      if (token !== setupToken) {
        return jsonResponse({ error: "Invalid setup token" }, 403);
      }
      return htmlResponse(setupPageHTML);
    }

    // Setup API (token-gated)
    if (pathname === "/_/api/setup" && req.method === "POST") {
      if (!authService || !setupToken) {
        return jsonResponse({ error: "Setup not available" }, 400);
      }
      try {
        const { token, email, password, name } = await req.json();
        if (token !== setupToken) {
          return jsonResponse({ error: "Invalid setup token" }, 403);
        }
        const hasSuperadmin = await authService.hasSuperadmin();
        if (hasSuperadmin) {
          setupToken = null;
          return jsonResponse({ error: "Superadmin already exists" }, 400);
        }
        if (!email || !password) {
          return jsonResponse({ error: "Email and password required" }, 400);
        }
        await authService.registerSuperadmin(email, password, name);
        setupToken = null;
        return jsonResponse({ ok: true });
      } catch (err) {
        const message = err instanceof Error ? err.message : "Setup failed";
        return jsonResponse({ error: message }, 400);
      }
    }

    // Login page (public)
    if (pathname === "/_/login" || pathname === "/_/login/") {
      return htmlResponse(loginPageHTML);
    }

    // Login API (public)
    if (pathname === "/_/api/login" && req.method === "POST") {
      if (!authService) {
        return jsonResponse({ error: "Auth not configured" }, 400);
      }
      try {
        const { email, password } = await req.json();
        const result = await authService.login(email, password);

        // Check for superadmin role
        if (!result.user.roles.includes("superadmin")) {
          return jsonResponse({ error: "Insufficient privileges. Requires superadmin role." }, 403);
        }

        return jsonResponse({ token: result.token, refreshToken: result.refreshToken });
      } catch (err) {
        const message = err instanceof Error ? err.message : "Login failed";
        return jsonResponse({ error: message }, 401);
      }
    }

    // Refresh token (public — uses refresh token instead of access token)
    if (pathname === "/_/api/refresh" && req.method === "POST") {
      if (!authService) {
        return jsonResponse({ error: "Auth not configured" }, 400);
      }
      try {
        const { refreshToken } = await req.json();
        if (!refreshToken) {
          return jsonResponse({ error: "Refresh token required" }, 400);
        }
        const result = await authService.refresh(refreshToken);
        return jsonResponse({ token: result.token });
      } catch (err) {
        const message = err instanceof Error ? err.message : "Token refresh failed";
        return jsonResponse({ error: message }, 401);
      }
    }

    // Admin SPA — served without auth check; the SPA handles auth via
    // localStorage token and fetch calls with Authorization headers.
    if (pathname === "/_" || pathname === "/_/") {
      return htmlResponse(adminPageHTML);
    }

    // All other admin API routes require superadmin
    const token = extractBearerToken(req);
    if (!token) {
      return jsonResponse({ error: "Authentication required" }, 401);
    }

    const payload = await verifyJWT(token, jwtSecret);
    if (!payload || !payload.roles.includes("superadmin")) {
      return jsonResponse({ error: "Requires superadmin role" }, 403);
    }

    // API endpoints
    if (pathname === "/_/api/tables" && req.method === "GET") {
      return handleListTables(db);
    }

    // SSE event stream for real-time updates
    if (pathname === "/_/api/events" && req.method === "GET") {
      return handleSSEEvents(db, req);
    }

    const rowsMatch = pathname.match(/^\/_\/api\/tables\/([^/]+)\/rows$/);
    if (rowsMatch && req.method === "GET") {
      return handleListRows(db, rowsMatch[1], url);
    }
    if (rowsMatch && req.method === "POST") {
      const body = await req.json();
      return handleCreateRow(db, rowsMatch[1], body);
    }

    // File upload: POST /_/api/tables/{table}/rows/{id}/files/{field}
    const fileMatch = pathname.match(/^\/_\/api\/tables\/([^/]+)\/rows\/([^/]+)\/files\/([^/]+)$/);
    if (fileMatch && req.method === "POST") {
      return handleFileUpload(db, fileMatch[1], fileMatch[2], fileMatch[3], req);
    }
    // File delete: DELETE /_/api/tables/{table}/rows/{id}/files/{field}
    if (fileMatch && req.method === "DELETE") {
      return handleFileDelete(db, fileMatch[1], fileMatch[2], fileMatch[3]);
    }

    const rowMatch = pathname.match(/^\/_\/api\/tables\/([^/]+)\/rows\/([^/]+)$/);
    if (rowMatch) {
      if (req.method === "GET") {
        return handleGetRow(db, rowMatch[1], rowMatch[2]);
      }
      if (req.method === "PUT") {
        const body = await req.json();
        return handleUpdateRow(db, rowMatch[1], rowMatch[2], body);
      }
      if (req.method === "DELETE") {
        return handleDeleteRow(db, rowMatch[1], rowMatch[2]);
      }
    }

    if (pathname === "/_/api/backup") {
      if (req.method === "GET") {
        return handleBackupDownload(db);
      }
      if (req.method === "POST") {
        return handleBackupUpload(db, req);
      }
    }

    return null;
  };
}

function handleListTables(db: Database): Response {
  const tables = [];
  for (const [name, table] of db.tables) {
    tables.push({
      name,
      schema: Object.fromEntries(
        table.def.compiledSchema.fields.map((f) => {
          const entry: Record<string, unknown> = { type: f.kind, required: f.required, unique: f.unique };
          if (f.refTableName) entry.refTable = f.refTableName;
          if (f.refField) entry.refField = f.refField;
          if (f.enumValues) entry.enumValues = f.enumValues;
          if (f.mimeTypes && f.mimeTypes.length > 0) entry.mimeTypes = f.mimeTypes;
          return [f.name, entry];
        }),
      ),
      rowCount: table.primaryIndex.size,
    });
  }
  tables.sort((a, b) => a.name.localeCompare(b.name));
  return jsonResponse({ tables });
}

async function handleListRows(db: Database, tableName: string, url: URL): Promise<Response> {
  const table = db.getTable(tableName);
  if (!table) return jsonResponse({ error: "Table not found" }, 404);

  const page = parseInt(url.searchParams.get("page") ?? "1", 10);
  const limit = parseInt(url.searchParams.get("limit") ?? "50", 10);
  const search = url.searchParams.get("search") ?? "";
  const offset = (page - 1) * limit;

  let rows: Record<string, unknown>[];
  try {
    rows = await table.scan(10000);
  } catch {
    // Scan can fail during concurrent heavy writes; return empty with a hint
    return jsonResponse({ rows: [], total: 0, page, pages: 0, limit, busy: true });
  }

  // Sort by primary key for stable ordering across edits
  if (rows.length > 0) {
    const pk = table.def.compiledSchema.fields[0].name;
    rows.sort((a, b) => {
      const va = a[pk], vb = b[pk];
      if (typeof va === "number" && typeof vb === "number") return va - vb;
      return String(va ?? "").localeCompare(String(vb ?? ""));
    });
  }

  // Search filter
  if (search) {
    const lower = search.toLowerCase();
    rows = rows.filter((row) =>
      Object.values(row).some(
        (v) => typeof v === "string" && v.toLowerCase().includes(lower),
      ),
    );
  }

  const total = rows.length;
  const pages = Math.ceil(total / limit);
  const pageRows = rows.slice(offset, offset + limit);

  // Redact password fields
  const redacted = pageRows.map((row) => {
    const r = { ...row };
    for (const field of table.def.compiledSchema.fields) {
      if (field.kind === "bcrypt" && r[field.name]) {
        r[field.name] = "[REDACTED]";
      }
    }
    return r;
  });

  return jsonResponse({ rows: redacted, total, page, pages, limit });
}

async function handleGetRow(db: Database, tableName: string, id: string): Promise<Response> {
  const table = db.getTable(tableName);
  if (!table) return jsonResponse({ error: "Table not found" }, 404);

  const row = await table.get(id);
  if (!row) return jsonResponse({ error: "Row not found" }, 404);

  return jsonResponse({ row });
}

async function handleUpdateRow(
  db: Database,
  tableName: string,
  id: string,
  updates: Record<string, unknown>,
): Promise<Response> {
  const table = db.getTable(tableName);
  if (!table) return jsonResponse({ error: "Table not found" }, 404);

  const row = await table.update(id, updates);
  if (!row) return jsonResponse({ error: "Row not found" }, 404);

  return jsonResponse({ ok: true, row });
}

async function handleDeleteRow(db: Database, tableName: string, id: string): Promise<Response> {
  const table = db.getTable(tableName);
  if (!table) return jsonResponse({ error: "Table not found" }, 404);

  const deleted = await table.delete(id);
  if (!deleted) return jsonResponse({ error: "Row not found" }, 404);

  return jsonResponse({ ok: true, deleted: id });
}

async function handleCreateRow(
  db: Database,
  tableName: string,
  data: Record<string, unknown>,
): Promise<Response> {
  const table = db.getTable(tableName);
  if (!table) return jsonResponse({ error: "Table not found" }, 404);

  try {
    const row = await table.insert(data);
    return jsonResponse({ ok: true, row }, 201);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Insert failed";
    return jsonResponse({ error: message }, 400);
  }
}

function handleSSEEvents(db: Database, req: Request): Response {
  const encoder = new TextEncoder();
  const pubsub = db.getPubSub();

  const stream = new ReadableStream({
    start(controller) {
      let closed = false;

      const enqueue = (eventType: string, payload: unknown) => {
        if (closed) return;
        try {
          controller.enqueue(
            encoder.encode(`event: ${eventType}\ndata: ${JSON.stringify(payload)}\n\n`),
          );
        } catch {
          // Controller closed
        }
      };

      // Send initial table counts snapshot
      const tableCounts: Record<string, number> = {};
      for (const [name, table] of db.tables) {
        tableCounts[name] = table.primaryIndex.size;
      }
      enqueue("snapshot", { tableCounts });

      // Push every change event directly — zero cost per subscriber
      const unsubscribe = pubsub.subscribeAll((event) => {
        enqueue("change", {
          table: event.table,
          op: event.op,
          rowId: event.rowId,
          data: event.data,
        });
      });

      // Heartbeat every 15s to keep connection alive
      const heartbeat = setInterval(() => {
        if (closed) return;
        try {
          controller.enqueue(encoder.encode(": heartbeat\n\n"));
        } catch {
          clearInterval(heartbeat);
        }
      }, 15000);

      req.signal.addEventListener("abort", () => {
        closed = true;
        unsubscribe();
        clearInterval(heartbeat);
        try {
          controller.close();
        } catch {
          // Already closed
        }
      });
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "Connection": "keep-alive",
    },
  });
}

async function handleBackupDownload(db: Database): Promise<Response> {
  const data = await createBackup(db);
  const buf = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer;
  return new Response(buf, {
    headers: {
      "Content-Type": "application/gzip",
      "Content-Disposition": `attachment; filename="flop-backup-${new Date().toISOString().replace(/[:.]/g, "-")}.tar.gz"`,
    },
  });
}

async function handleBackupUpload(db: Database, req: Request): Promise<Response> {
  try {
    const data = new Uint8Array(await req.arrayBuffer());
    await restoreBackup(db, data);
    return jsonResponse({ ok: true, message: "Backup restored successfully" });
  } catch (err) {
    const message = err instanceof Error ? err.message : "Restore failed";
    return jsonResponse({ error: message }, 400);
  }
}

async function handleFileUpload(
  db: Database,
  tableName: string,
  rowId: string,
  fieldName: string,
  req: Request,
): Promise<Response> {
  const table = db.getTable(tableName);
  if (!table) return jsonResponse({ error: "Table not found" }, 404);

  const field = table.def.compiledSchema.fieldMap.get(fieldName);
  if (!field || (field.kind !== "fileSingle" && field.kind !== "fileMulti")) {
    return jsonResponse({ error: "Field is not a file field" }, 400);
  }

  const row = await table.get(rowId);
  if (!row) return jsonResponse({ error: "Row not found" }, 404);

  // Parse multipart form data
  const contentType = req.headers.get("content-type") ?? "";
  if (!contentType.includes("multipart/form-data")) {
    return jsonResponse({ error: "Expected multipart/form-data" }, 400);
  }

  try {
    const formData = await req.formData();
    const file = formData.get("file") as File | null;
    if (!file) return jsonResponse({ error: "No file provided" }, 400);

    const mime = file.type || mimeFromExtension(file.name);

    // Validate MIME type
    if (field.mimeTypes && field.mimeTypes.length > 0) {
      if (!validateMimeType(mime, field.mimeTypes)) {
        return jsonResponse({ error: `File type ${mime} not allowed. Accepted: ${field.mimeTypes.join(", ")}` }, 400);
      }
    }

    const data = new Uint8Array(await file.arrayBuffer());

    // Validate magic bytes
    if (!validateMagicBytes(data, mime)) {
      return jsonResponse({ error: "File content does not match declared type" }, 400);
    }

    const fileRef = await storeFile(db.getDataDir(), tableName, rowId, fieldName, file.name, data, mime);

    // Update row
    if (field.kind === "fileSingle") {
      await table.update(rowId, { [fieldName]: fileRef });
    } else {
      const existing = ((row as Record<string, unknown>)[fieldName] as FileRef[]) ?? [];
      await table.update(rowId, { [fieldName]: [...existing, fileRef] });
    }

    return jsonResponse({ ok: true, file: fileRef });
  } catch (err) {
    const message = err instanceof Error ? err.message : "Upload failed";
    return jsonResponse({ error: message }, 400);
  }
}

async function handleFileDelete(
  db: Database,
  tableName: string,
  rowId: string,
  fieldName: string,
): Promise<Response> {
  const table = db.getTable(tableName);
  if (!table) return jsonResponse({ error: "Table not found" }, 404);

  const field = table.def.compiledSchema.fieldMap.get(fieldName);
  if (!field || (field.kind !== "fileSingle" && field.kind !== "fileMulti")) {
    return jsonResponse({ error: "Field is not a file field" }, 400);
  }

  const row = await table.get(rowId);
  if (!row) return jsonResponse({ error: "Row not found" }, 404);

  if (field.kind === "fileSingle") {
    await table.update(rowId, { [fieldName]: null });
  } else {
    await table.update(rowId, { [fieldName]: [] });
  }

  return jsonResponse({ ok: true });
}
