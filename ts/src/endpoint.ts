// Endpoint class — base for Reducer and View with .roles() and .public() chaining

import type { AccessPolicy, SchemaField } from "./types.ts";
import type { SchemaFieldInput } from "./table.ts";

export class Endpoint<TParams = unknown, TResult = unknown> {
  readonly _type: "reducer" | "view";
  readonly _paramSchema: Record<string, SchemaFieldInput>;
  readonly _handler: (ctx: any, params: any) => TResult;
  _access: AccessPolicy = { type: "authenticated" };

  constructor(
    type: "reducer" | "view",
    paramSchema: Record<string, SchemaFieldInput>,
    handler: (ctx: any, params: any) => TResult,
  ) {
    this._type = type;
    this._paramSchema = paramSchema;
    this._handler = handler;
  }

  roles(...roles: string[]): this {
    this._access = { type: "roles", roles };
    return this;
  }

  public(): this {
    this._access = { type: "public" };
    return this;
  }
}

export class Reducer<TParams = unknown, TResult = unknown> extends Endpoint<TParams, TResult> {
  constructor(
    paramSchema: Record<string, SchemaFieldInput>,
    handler: (ctx: any, params: any) => TResult,
  ) {
    super("reducer", paramSchema, handler);
  }
}

export class View<TParams = unknown, TResult = unknown> extends Endpoint<TParams, TResult> {
  // Track which tables this view reads from (for realtime subscriptions)
  _dependentTables: string[] = [];

  constructor(
    paramSchema: Record<string, SchemaFieldInput>,
    handler: (ctx: any, params: any) => TResult,
  ) {
    super("view", paramSchema, handler);
  }
}
