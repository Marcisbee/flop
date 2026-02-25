// Admin panel — route registration + API handlers

import type { Database } from "../database.ts";
import type { AuthService } from "../server/auth.ts";
import { extractBearerToken, verifyJWT, jwtToAuthContext } from "../server/auth.ts";
import { renderLoginPage, renderAdminPage, renderSetupPage } from "./ui.ts";
import { createBackup, restoreBackup } from "./backup.ts";

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
      return htmlResponse(renderSetupPage());
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
      return htmlResponse(renderLoginPage());
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

        return jsonResponse({ token: result.token });
      } catch (err) {
        const message = err instanceof Error ? err.message : "Login failed";
        return jsonResponse({ error: message }, 401);
      }
    }

    // Admin SPA — served without auth check; the SPA handles auth via
    // localStorage token and fetch calls with Authorization headers.
    if (pathname === "/_" || pathname === "/_/") {
      return htmlResponse(renderAdminPage());
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
          return [f.name, entry];
        }),
      ),
      rowCount: table.primaryIndex.size,
    });
  }
  return jsonResponse({ tables });
}

async function handleListRows(db: Database, tableName: string, url: URL): Promise<Response> {
  const table = db.getTable(tableName);
  if (!table) return jsonResponse({ error: "Table not found" }, 404);

  const page = parseInt(url.searchParams.get("page") ?? "1", 10);
  const limit = parseInt(url.searchParams.get("limit") ?? "50", 10);
  const search = url.searchParams.get("search") ?? "";
  const offset = (page - 1) * limit;

  let rows = await table.scan(10000); // Scan all for total count

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
      // Send initial heartbeat
      controller.enqueue(encoder.encode(": heartbeat\n\n"));

      const unsubscribe = pubsub.subscribeAll((event) => {
        try {
          const data = JSON.stringify({
            table: event.table,
            op: event.op,
            rowId: event.rowId,
          });
          controller.enqueue(encoder.encode(`data: ${data}\n\n`));
        } catch {
          // Controller closed
        }
      });

      // Heartbeat every 15s to keep connection alive
      const heartbeat = setInterval(() => {
        try {
          controller.enqueue(encoder.encode(": heartbeat\n\n"));
        } catch {
          clearInterval(heartbeat);
        }
      }, 15000);

      req.signal.addEventListener("abort", () => {
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
