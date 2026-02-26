// Route discovery — inspects user module exports for Reducer/View instances

import { Reducer, View } from "../endpoint.ts";
import type { AccessPolicy } from "../types.ts";

export interface Route {
  name: string;
  method: "GET" | "POST";
  path: string;
  endpoint: Reducer | View;
  access: AccessPolicy;
}

export function discoverRoutes(moduleExports: Record<string, unknown>): Route[] {
  const routes: Route[] = [];

  for (const [name, value] of Object.entries(moduleExports)) {
    if (value instanceof Reducer) {
      routes.push({
        name,
        method: "POST",
        path: `/api/reduce/${name}`,
        endpoint: value,
        access: value._access,
      });
    } else if (value instanceof View) {
      routes.push({
        name,
        method: "GET",
        path: `/api/view/${name}`,
        endpoint: value,
        access: value._access,
      });
    }
  }

  return routes;
}

export function matchRoute(pathname: string, routes: Route[]): Route | null {
  for (const route of routes) {
    if (pathname === route.path) return route;
  }
  return null;
}

export function generateSchema(routes: Route[]): Record<string, unknown> {
  const schema: Record<string, unknown> = {
    endpoints: routes.map((r) => ({
      name: r.name,
      method: r.method,
      path: r.path,
      type: r.endpoint instanceof Reducer ? "reducer" : "view",
      access: r.access.type,
      params: Object.fromEntries(
        Object.entries(r.endpoint._paramSchema).map(([key, field]) => {
          const f = typeof (field as any)._build === "function" ? (field as any)._build() : field;
          return [key, { type: f.kind, required: f.required }];
        }),
      ),
    })),
  };
  return schema;
}
