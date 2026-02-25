// Public API barrel — server-side exports

export { t } from "./src/schema.ts";
export { table } from "./src/table.ts";
export { flop } from "./src/database.ts";
export { Reducer, View } from "./src/endpoint.ts";
export { route } from "./src/pages/route.ts";

// Re-export types
export type {
  SchemaField,
  FileRef,
  AuthContext,
  RequestContext,
  FlopSchema,
  InferSchema,
  InferInsertSchema,
  InferParams,
  MigrationStep,
} from "./src/types.ts";

export type { Database, ReduceContext, ViewContext } from "./src/database.ts";
export type { HeadConfig, PageContext, RouteNode } from "./src/pages/route.ts";
