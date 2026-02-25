// Flop CLI entry point — imports user module, discovers exports, starts HTTP server

import { Database } from "./src/database.ts";
import { Reducer, View } from "./src/endpoint.ts";
import { discoverRoutes } from "./src/server/router.ts";
import { createHandler, type ServerConfig } from "./src/server/handler.ts";
import { AuthService } from "./src/server/auth.ts";
import { createAdminHandler } from "./src/admin/mod.ts";
import { generateFromPattern } from "./src/schema.ts";
import { flattenRoutes, type RouteNode, type FlatPageRoute } from "./src/pages/route.ts";

const DEFAULT_PORT = 1985;
const DEFAULT_SECRET = "flop-dev-secret-change-in-production";

export async function startServer(
  userModulePath: string,
  config?: Partial<ServerConfig> & { dataDir?: string },
): Promise<void> {
  // Import user module
  const userModule = await import(userModulePath);

  // Find Database instances
  let db: Database | null = null;
  for (const value of Object.values(userModule)) {
    if (value instanceof Database) {
      db = value;
      break;
    }
  }

  if (!db) {
    console.error("No flop() database instance found in module exports.");
    Deno.exit(1);
  }

  // Open the database
  await db.open();

  // Discover routes from exports
  const routes = discoverRoutes(userModule as Record<string, unknown>);

  // Discover page routes
  let pageRoutes: FlatPageRoute[] = [];
  let routeTree: RouteNode | null = null;
  for (const value of Object.values(userModule)) {
    if (value && typeof value === "object" && (value as any)._type === "route") {
      routeTree = value as RouteNode;
      pageRoutes = flattenRoutes(routeTree);
      break;
    }
  }

  // Set up auth service if there's an auth table
  let authService: AuthService | null = null;
  const authTable = db.getAuthTable();
  const jwtSecret = config?.jwtSecret ?? Deno.env.get("FLOP_JWT_SECRET") ?? DEFAULT_SECRET;

  let setupToken: string | null = null;

  if (authTable) {
    authService = new AuthService(authTable, jwtSecret);

    // Check if a superadmin exists — if not, generate a one-time setup token
    const hasSuperadmin = await authService.hasSuperadmin();
    if (!hasSuperadmin) {
      setupToken = generateFromPattern(/[a-zA-Z0-9]{32}/);
    }
  }

  // Set up admin handler
  const adminHandler = createAdminHandler(db, authService, jwtSecret, setupToken);

  // Resolve app directory for static file serving
  const resolvedPath = new URL(userModulePath, `file://${Deno.cwd()}/`).pathname;
  const staticDir = resolvedPath.substring(0, resolvedPath.lastIndexOf("/"));

  const port = config?.port ?? (Number(Deno.env.get("FLOP_PORT")) || DEFAULT_PORT);
  const serverConfig: ServerConfig = { port, jwtSecret, staticDir };

  // Bundle client app if pages are defined
  let clientBundle: { js: Uint8Array; css: Uint8Array } | null = null;
  if (routeTree && pageRoutes.length > 0) {
    console.log("  Bundling client app...");
    const { bundlePages } = await import("./src/pages/bundler.ts");
    const result = await bundlePages(routeTree, pageRoutes, staticDir);
    if (result.errors.length > 0) {
      console.error("  Bundle errors:", result.errors);
    } else {
      clientBundle = { js: result.js, css: result.css };
      console.log(`  Bundle: ${(clientBundle.js.byteLength / 1024).toFixed(1)}KB JS` +
        (clientBundle.css.byteLength > 0 ? ` + ${(clientBundle.css.byteLength / 1024).toFixed(1)}KB CSS` : ""));
    }
  }

  const handler = createHandler(db, routes, authService, serverConfig, adminHandler, pageRoutes, clientBundle);

  const setupLine = setupToken
    ? `│                                     │\n│   Setup:   /_/setup?token=${setupToken.slice(0, 6)}... │`
    : "";

  console.log(`
┌─────────────────────────────────────┐
│   Server:  http://localhost:${String(port).padEnd(7)} │
│   Admin:   http://localhost:${(String(port) + "/_").padEnd(7)} │
│   Tables:  ${String(db.tables.size).padEnd(25)}│
│   Routes:  ${String(routes.length).padEnd(25)}│
│   Pages:   ${String(pageRoutes.length).padEnd(25)}│
${setupLine}└─────────────────────────────────────┘
`);

  if (setupToken) {
    console.log(`  Create your admin account:`);
    console.log(`  http://localhost:${port}/_/setup?token=${setupToken}\n`);
  }

  for (const route of routes) {
    const access = route.access.type === "public"
      ? "[public]"
      : route.access.type === "roles"
        ? `[roles: ${(route.access as any).roles.join(",")}]`
        : "[auth]";
    console.log(`  ${route.method.padEnd(5)} ${route.path} ${access}`);
  }

  for (const page of pageRoutes) {
    console.log(`  GET  ${page.pattern} [page]`);
  }
  console.log("");

  // Set up periodic checkpoint
  const checkpointInterval = setInterval(async () => {
    try {
      await db!.checkpoint();
    } catch (err) {
      console.error("Checkpoint error:", err);
    }
  }, 30_000); // Every 30 seconds

  // Graceful shutdown
  const shutdown = async () => {
    console.log("\nShutting down...");
    clearInterval(checkpointInterval);
    await db!.close();
    Deno.exit(0);
  };

  Deno.addSignalListener("SIGINT", shutdown);
  Deno.addSignalListener("SIGTERM", shutdown);

  Deno.serve({ port }, handler);
}

if (import.meta.main) {
  const modulePath = Deno.args[0];
  if (!modulePath) {
    console.error("Usage: deno run --allow-all main.ts <path-to-app.ts>");
    console.error("  e.g. deno run --allow-all main.ts ./app.ts");
    Deno.exit(1);
  }

  const absPath = modulePath.startsWith("./") || modulePath.startsWith("/")
    ? modulePath
    : `./${modulePath}`;

  startServer(absPath);
}
