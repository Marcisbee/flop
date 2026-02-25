// HTTP request handler — Deno.serve() with HTTP, SSE, WebSocket support

import type { Route } from "./router.ts";
import { matchRoute, generateSchema } from "./router.ts";
import { Reducer, View } from "../endpoint.ts";
import { extractBearerToken, verifyJWT, jwtToAuthContext, type AuthService } from "./auth.ts";
import type { Database } from "../database.ts";
import { serveFile } from "../storage/files.ts";
import type { AuthContext, RequestContext } from "../types.ts";

export interface ServerConfig {
  port?: number;
  jwtSecret: string;
}

export function createHandler(
  db: Database,
  routes: Route[],
  authService: AuthService | null,
  config: ServerConfig,
  adminHandler?: (req: Request) => Promise<Response | null>,
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
      if (pathname.startsWith("/_") && !pathname.startsWith("/_auth") && !pathname.startsWith("/_files") && !pathname.startsWith("/_schema")) {
        if (adminHandler) {
          const resp = await adminHandler(req);
          if (resp) return addCors(resp, corsHeaders);
        }
        return addCors(jsonResponse({ error: "Not found" }, 404), corsHeaders);
      }

      // File serving
      if (pathname.startsWith("/_files/")) {
        const resp = await serveFile(db.getDataDir(), pathname);
        if (resp) return addCors(resp, corsHeaders);
        return addCors(jsonResponse({ error: "File not found" }, 404), corsHeaders);
      }

      // Schema endpoint
      if (pathname === "/_schema" && req.method === "GET") {
        return addCors(jsonResponse(generateSchema(routes)), corsHeaders);
      }

      // Auth endpoints
      if (pathname.startsWith("/_auth/") && authService) {
        const resp = await handleAuth(pathname, req, authService);
        return addCors(resp, corsHeaders);
      }

      // Route matching
      const route = matchRoute(pathname, routes);
      if (!route) {
        return addCors(jsonResponse({ error: "Not found" }, 404), corsHeaders);
      }

      // Method check
      if (route.endpoint instanceof Reducer && req.method !== "POST") {
        return addCors(jsonResponse({ error: "Method not allowed. Use POST." }, 405), corsHeaders);
      }

      // Permission enforcement
      const authResult = await enforceAccess(req, route, jwtSecret);
      if (authResult.denied) {
        return addCors(authResult.response!, corsHeaders);
      }

      const requestCtx: RequestContext = {
        auth: authResult.auth,
        headers: req.headers,
        url,
      };

      // Check for SSE
      if (req.headers.get("accept") === "text/event-stream" && route.endpoint instanceof View) {
        return addCors(handleSSE(req, route, requestCtx, db), corsHeaders);
      }

      // Check for WebSocket
      if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
        return handleWebSocket(req, route, requestCtx, db, routes, jwtSecret);
      }

      // Normal HTTP
      if (route.endpoint instanceof Reducer) {
        const body = await req.json();
        const result = await route.endpoint._handler({ request: requestCtx }, body);
        return addCors(jsonResponse({ ok: true, data: result }), corsHeaders);
      }

      if (route.endpoint instanceof View) {
        const params = Object.fromEntries(url.searchParams);
        const result = await route.endpoint._handler({ request: requestCtx }, params);
        return addCors(jsonResponse({ ok: true, data: result }), corsHeaders);
      }

      return addCors(jsonResponse({ error: "Not found" }, 404), corsHeaders);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Internal server error";
      return addCors(jsonResponse({ error: message }, 400), corsHeaders);
    }
  };
}

async function handleAuth(
  pathname: string,
  req: Request,
  authService: AuthService,
): Promise<Response> {
  const body = req.method === "POST" ? await req.json() : {};

  switch (pathname) {
    case "/_auth/register": {
      const { email, password, name } = body;
      if (!email || !password) {
        return jsonResponse({ error: "Email and password required" }, 400);
      }
      const result = await authService.register(email, password, name);
      return jsonResponse(result);
    }

    case "/_auth/password": {
      const { email, password } = body;
      if (!email || !password) {
        return jsonResponse({ error: "Email and password required" }, 400);
      }
      const result = await authService.login(email, password);
      return jsonResponse(result);
    }

    case "/_auth/refresh": {
      const { refreshToken } = body;
      if (!refreshToken) {
        return jsonResponse({ error: "Refresh token required" }, 400);
      }
      const result = await authService.refresh(refreshToken);
      return jsonResponse(result);
    }

    case "/_auth/verify":
    case "/_auth/reset-password":
    case "/_auth/change-email":
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

async function enforceAccess(
  req: Request,
  route: Route,
  jwtSecret: string,
): Promise<AccessResult> {
  const policy = route.access;

  if (policy.type === "public") {
    // Still try to parse auth if present
    const token = extractBearerToken(req);
    if (token) {
      const payload = await verifyJWT(token, jwtSecret);
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

  const payload = await verifyJWT(token, jwtSecret);
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

function handleWebSocket(
  req: Request,
  _route: Route,
  requestCtx: RequestContext,
  db: Database,
  routes: Route[],
  _jwtSecret: string,
): Response {
  const { socket, response } = Deno.upgradeWebSocket(req);
  const encoder = new TextEncoder();

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
