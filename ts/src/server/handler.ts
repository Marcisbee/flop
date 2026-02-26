// HTTP request handler — Deno.serve() with HTTP, SSE, WebSocket support

import type { Route } from "./router.ts";
import { matchRoute, generateSchema } from "./router.ts";
import { Reducer, View } from "../endpoint.ts";
import { extractBearerToken, verifyJWT, jwtToAuthContext, type AuthService } from "./auth.ts";
import type { Database } from "../database.ts";
import { serveFile } from "../storage/files.ts";
import type { AuthContext, RequestContext } from "../types.ts";
import type { FlatPageRoute } from "../pages/route.ts";
import { matchPageRoute, renderHeadConfig, mergeHeadConfigs, type HeadConfig } from "../pages/route.ts";

export interface ServerConfig {
  port?: number;
  jwtSecret: string;
  staticDir?: string;
}

export function createHandler(
  db: Database,
  routes: Route[],
  authService: AuthService | null,
  config: ServerConfig,
  adminHandler?: (req: Request) => Promise<Response | null>,
  pageRoutes?: FlatPageRoute[],
  clientBundle?: { js: Uint8Array; css: Uint8Array } | null,
): (req: Request) => Promise<Response> {
  const { jwtSecret } = config;

  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const pathname = url.pathname;

    // CORS headers
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (req.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    try {
      // Admin panel routes
      if (pathname.startsWith("/_")) {
        if (adminHandler) {
          const resp = await adminHandler(req);
          if (resp) return addCors(resp, corsHeaders);
        }
        return addCors(jsonResponse({ error: "Not found" }, 404), corsHeaders);
      }

      // All API routes under /api/
      if (pathname.startsWith("/api/")) {
        return addCors(await handleApi(req, url, pathname, routes, db, authService, jwtSecret, config), corsHeaders);
      }

      // Serve bundled client.js / client.css from memory
      if (pathname === "/assets/client.js" && clientBundle?.js.byteLength && req.method === "GET") {
        return addCors(new Response(clientBundle.js.buffer as ArrayBuffer, {
          headers: { "Content-Type": "application/javascript", "Cache-Control": "public, max-age=31536000, immutable" },
        }), corsHeaders);
      }
      if (pathname === "/assets/client.css" && clientBundle?.css.byteLength && req.method === "GET") {
        return addCors(new Response(clientBundle.css.buffer as ArrayBuffer, {
          headers: { "Content-Type": "text/css", "Cache-Control": "public, max-age=31536000, immutable" },
        }), corsHeaders);
      }

      // Serve static assets from /assets/ prefix
      if (pathname.startsWith("/assets/") && config.staticDir && req.method === "GET") {
        const resp = await serveStaticFile(config.staticDir, pathname);
        if (resp) return addCors(resp, corsHeaders);
        return addCors(jsonResponse({ error: "Not found" }, 404), corsHeaders);
      }

      // Page route matching (SSR shell)
      if (pageRoutes && pageRoutes.length > 0 && req.method === "GET") {
        const match = matchPageRoute(pathname, pageRoutes);
        if (match) {
          const resp = await handlePageRoute(match.route, match.params, routes, db);
          return addCors(resp, corsHeaders);
        }
      }

      return addCors(jsonResponse({ error: "Not found" }, 404), corsHeaders);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Internal server error";
      return addCors(jsonResponse({ error: message }, 400), corsHeaders);
    }
  };
}

async function handleApi(
  req: Request,
  url: URL,
  pathname: string,
  routes: Route[],
  db: Database,
  authService: AuthService | null,
  jwtSecret: string,
  config: ServerConfig,
): Promise<Response> {
  // File serving
  if (pathname.startsWith("/api/files/")) {
    const resp = await serveFile(db.getDataDir(), pathname.replace("/api/files/", "/_files/"));
    if (resp) return resp;
    return jsonResponse({ error: "File not found" }, 404);
  }

  // Schema endpoint
  if (pathname === "/api/schema" && req.method === "GET") {
    return jsonResponse(generateSchema(routes));
  }

  // Auth endpoints
  if (pathname.startsWith("/api/auth/") && authService) {
    return handleAuth(pathname, req, authService);
  }

  // Multiplexed SSE — single connection for multiple views
  if (pathname === "/api/sse" && req.headers.get("accept")?.includes("text/event-stream")) {
    return handleMultiplexedSSE(req, url, routes, db, jwtSecret);
  }

  // Route matching (views + reducers)
  const route = matchRoute(pathname, routes);
  if (!route) {
    return jsonResponse({ error: "Not found" }, 404);
  }

  // Method check
  if (route.endpoint instanceof Reducer && req.method !== "POST") {
    return jsonResponse({ error: "Method not allowed. Use POST." }, 405);
  }

  // Permission enforcement
  const authResult = enforceAccess(req, route, jwtSecret);
  if (authResult.denied) {
    return authResult.response!;
  }

  const requestCtx: RequestContext = {
    auth: authResult.auth,
    headers: req.headers,
    url,
  };

  // Check for SSE
  if (req.headers.get("accept")?.includes("text/event-stream") && route.endpoint instanceof View) {
    return handleSSE(req, route, requestCtx, db);
  }

  // Check for WebSocket
  if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
    return handleWebSocket(req, route, requestCtx, db, routes, jwtSecret);
  }

  // Normal HTTP
  if (route.endpoint instanceof Reducer) {
    const body = await req.json();
    const result = await route.endpoint._handler({ request: requestCtx }, body);
    return jsonResponse({ ok: true, data: result });
  }

  if (route.endpoint instanceof View) {
    const params = Object.fromEntries(url.searchParams);
    const result = await route.endpoint._handler({ request: requestCtx }, params);
    return jsonResponse({ ok: true, data: result });
  }

  return jsonResponse({ error: "Not found" }, 404);
}

async function handlePageRoute(
  pageRoute: FlatPageRoute,
  params: Record<string, string>,
  routes: Route[],
  db: Database,
): Promise<Response> {
  // Build internal API for head() functions — calls view handlers directly, no HTTP
  const api = new Proxy({} as Record<string, (params: any) => Promise<any>>, {
    get: (_: any, name: string) => async (viewParams: any) => {
      const route = routes.find((r) => r.name === name && r.endpoint instanceof View);
      if (!route) throw new Error(`View not found: ${name}`);
      const requestCtx: RequestContext = { auth: null, headers: new Headers(), url: new URL("http://localhost") };
      return route.endpoint._handler({ request: requestCtx }, viewParams ?? {});
    },
  });

  const ctx = { params, api };

  // Run head() from each layer (layout chain), merge results
  const headConfigs: HeadConfig[] = [];
  for (const headFn of pageRoute.headChain) {
    try {
      const config = await headFn(ctx);
      headConfigs.push(config);
    } catch {
      // Skip failed head functions
    }
  }

  const merged = mergeHeadConfigs(headConfigs);
  const headHtml = renderHeadConfig(merged);

  const routeData = JSON.stringify({ pattern: pageRoute.pattern, params });
  const html = `<!DOCTYPE html>
<html>
<head>
${headHtml}
</head>
<body>
<div id="root"></div>
<script>window.__FLOP_ROUTE__=${routeData}</script>
<script type="module" src="/assets/client.js"></script>
</body>
</html>`;

  return new Response(html, {
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}

async function handleAuth(
  pathname: string,
  req: Request,
  authService: AuthService,
): Promise<Response> {
  const body = req.method === "POST" ? await req.json() : {};

  switch (pathname) {
    case "/api/auth/register": {
      const { email, password, name } = body;
      if (!email || !password) {
        return jsonResponse({ error: "Email and password required" }, 400);
      }
      const result = await authService.register(email, password, name);
      return jsonResponse(result);
    }

    case "/api/auth/password": {
      const { email, password } = body;
      if (!email || !password) {
        return jsonResponse({ error: "Email and password required" }, 400);
      }
      const result = await authService.login(email, password);
      return jsonResponse(result);
    }

    case "/api/auth/refresh": {
      const { refreshToken } = body;
      if (!refreshToken) {
        return jsonResponse({ error: "Refresh token required" }, 400);
      }
      const result = await authService.refresh(refreshToken);
      return jsonResponse(result);
    }

    case "/api/auth/verify":
    case "/api/auth/reset-password":
    case "/api/auth/change-email":
      // Placeholder — needs email sending infrastructure
      return jsonResponse({ ok: true, message: "Not yet implemented" });

    default:
      return jsonResponse({ error: "Unknown auth endpoint" }, 404);
  }
}

interface AccessResult {
  denied: boolean;
  auth: AuthContext | null;
  response?: Response;
}

function enforceAccess(
  req: Request,
  route: Route,
  jwtSecret: string,
): AccessResult {
  const policy = route.access;

  if (policy.type === "public") {
    // Still try to parse auth if present
    const token = extractBearerToken(req);
    if (token) {
      const payload = verifyJWT(token, jwtSecret);
      return { denied: false, auth: payload ? jwtToAuthContext(payload) : null };
    }
    return { denied: false, auth: null };
  }

  const token = extractBearerToken(req);
  if (!token) {
    return {
      denied: true,
      auth: null,
      response: jsonResponse({ error: "Authentication required" }, 401),
    };
  }

  const payload = verifyJWT(token, jwtSecret);
  if (!payload) {
    return {
      denied: true,
      auth: null,
      response: jsonResponse({ error: "Invalid or expired token" }, 401),
    };
  }

  const auth = jwtToAuthContext(payload);

  if (policy.type === "roles") {
    const hasAccess =
      auth.roles.includes("superadmin") ||
      policy.roles.some((r) => auth.roles.includes(r));

    if (!hasAccess) {
      return {
        denied: true,
        auth,
        response: jsonResponse(
          {
            error: "Forbidden",
            message: `Requires one of: ${policy.roles.join(", ")}`,
            requiredRoles: policy.roles,
            yourRoles: auth.roles,
          },
          403,
        ),
      };
    }
  }

  return { denied: false, auth };
}

function handleSSE(
  req: Request,
  route: Route,
  requestCtx: RequestContext,
  db: Database,
): Response {
  const url = new URL(req.url);
  const params = Object.fromEntries(url.searchParams);
  const view = route.endpoint as View;
  const encoder = new TextEncoder();

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

      // Send initial full snapshot from the view handler
      (async () => {
        try {
          const result = await view._handler({ request: requestCtx }, params);
          enqueue("snapshot", result);
        } catch (err) {
          const msg = err instanceof Error ? err.message : "Error";
          enqueue("error", { error: msg });
        }
      })();

      // Push change events directly — zero cost per subscriber
      const unsubscribe = db.getPubSub().subscribe(
        view._dependentTables,
        (event) => {
          enqueue("change", {
            table: event.table,
            op: event.op,
            rowId: event.rowId,
            data: event.data,
          });
        },
      );

      // Clean up on disconnect
      req.signal.addEventListener("abort", () => {
        closed = true;
        unsubscribe();
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

function handleMultiplexedSSE(
  req: Request,
  url: URL,
  routes: Route[],
  db: Database,
  jwtSecret: string,
): Response {
  const viewNames = (url.searchParams.get("views") ?? "").split(",").filter(Boolean);
  if (viewNames.length === 0) {
    return jsonResponse({ error: "No views specified. Use ?views=name1,name2" }, 400);
  }

  // Resolve views and parse per-view params
  const views: { name: string; view: View; params: Record<string, string> }[] = [];
  const allTables = new Set<string>();

  for (const name of viewNames) {
    const route = routes.find((r) => r.name === name && r.endpoint instanceof View);
    if (!route) {
      return jsonResponse({ error: `View not found: ${name}` }, 404);
    }

    // Check access
    const authResult = enforceAccess(req, route, jwtSecret);
    if (authResult.denied) {
      return authResult.response!;
    }

    // Extract per-view params: "<viewName>.<param>=value"
    const params: Record<string, string> = {};
    const prefix = name + ".";
    for (const [key, value] of url.searchParams) {
      if (key.startsWith(prefix)) {
        params[key.slice(prefix.length)] = value;
      }
    }

    const view = route.endpoint as View;
    for (const t of view._dependentTables) allTables.add(t);
    views.push({ name, view, params });
  }

  const encoder = new TextEncoder();
  const tableList = [...allTables];

  // Build auth context once (use first view's check — they're all from same request)
  const token = extractBearerToken(req);
  const payload = token ? verifyJWT(token, jwtSecret) : null;
  const requestCtx: RequestContext = {
    auth: payload ? jwtToAuthContext(payload) : null,
    headers: req.headers,
    url,
  };

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

      // Send snapshots for all views in parallel
      (async () => {
        const promises = views.map(async ({ name, view, params }) => {
          try {
            const result = await view._handler({ request: requestCtx }, params);
            enqueue(`snapshot:${name}`, result);
          } catch (err) {
            const msg = err instanceof Error ? err.message : "Error";
            enqueue(`error:${name}`, { error: msg });
          }
        });
        await Promise.all(promises);
      })();

      // Single PubSub subscription for all dependent tables
      const unsubscribe = db.getPubSub().subscribe(
        tableList,
        (event) => {
          enqueue("change", {
            table: event.table,
            op: event.op,
            rowId: event.rowId,
            data: event.data,
          });
        },
      );

      req.signal.addEventListener("abort", () => {
        closed = true;
        unsubscribe();
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

function handleWebSocket(
  req: Request,
  _route: Route,
  requestCtx: RequestContext,
  db: Database,
  routes: Route[],
  _jwtSecret: string,
): Response {
  const { socket, response } = Deno.upgradeWebSocket(req);

  socket.addEventListener("open", () => {
    // Subscribe to all table changes
    const unsubscribe = db.getPubSub().subscribeAll((event) => {
      try {
        socket.send(
          JSON.stringify({ type: "change", table: event.table, op: event.op, data: event.data }),
        );
      } catch {
        // Socket closed
      }
    });

    socket.addEventListener("close", () => unsubscribe());
  });

  socket.addEventListener("message", async (event) => {
    try {
      const msg = JSON.parse(event.data as string);

      if (msg.type === "reduce" && msg.name) {
        const route = routes.find(
          (r) => r.name === msg.name && r.endpoint instanceof Reducer,
        );
        if (route) {
          const result = await route.endpoint._handler(
            { request: requestCtx },
            msg.params ?? {},
          );
          socket.send(
            JSON.stringify({ type: "result", id: msg.id, data: result }),
          );
        } else {
          socket.send(
            JSON.stringify({ type: "error", id: msg.id, error: "Unknown reducer" }),
          );
        }
      } else if (msg.type === "view" && msg.name) {
        const route = routes.find(
          (r) => r.name === msg.name && r.endpoint instanceof View,
        );
        if (route) {
          const result = await route.endpoint._handler(
            { request: requestCtx },
            msg.params ?? {},
          );
          socket.send(
            JSON.stringify({ type: "result", id: msg.id, data: result }),
          );
        }
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : "Error";
      socket.send(JSON.stringify({ type: "error", error: message }));
    }
  });

  return response;
}

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function addCors(response: Response, corsHeaders: Record<string, string>): Response {
  for (const [key, value] of Object.entries(corsHeaders)) {
    response.headers.set(key, value);
  }
  return response;
}

const MIME_TYPES: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css",
  ".js": "application/javascript",
  ".mjs": "application/javascript",
  ".json": "application/json",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".woff": "font/woff",
  ".woff2": "font/woff2",
  ".map": "application/json",
};

async function serveStaticFile(dir: string, pathname: string): Promise<Response | null> {
  // Prevent path traversal
  const normalized = new URL(pathname, "http://x").pathname;
  if (normalized.includes("..")) return null;

  // Strip /assets/ prefix — serve from dir root
  const relative = normalized.startsWith("/assets/") ? normalized.slice(7) : normalized;
  const filePath = `${dir}${relative}`;

  try {
    const stat = await Deno.stat(filePath);
    if (!stat.isFile) return null;
    const body = await Deno.readFile(filePath);
    const ext = filePath.substring(filePath.lastIndexOf("."));
    const contentType = MIME_TYPES[ext] || "application/octet-stream";
    return new Response(body, {
      headers: { "Content-Type": contentType },
    });
  } catch {
    return null;
  }
}
