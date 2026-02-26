// Core type definitions for the flop database

// ---- Field Types ----

export const FieldKind = {
  String: "string",
  Number: "number",
  Boolean: "boolean",
  Json: "json",
  Bcrypt: "bcrypt",
  Ref: "ref",
  RefMulti: "refMulti",
  FileSingle: "fileSingle",
  FileMulti: "fileMulti",
  Roles: "roles",
  Enum: "enum",
  Integer: "integer",
  Vector: "vector",
  Set: "set",
  Timestamp: "timestamp",
} as const;

export type FieldKind = (typeof FieldKind)[keyof typeof FieldKind];

export interface SchemaField<T = unknown> {
  kind: FieldKind;
  required: boolean;
  unique: boolean;
  defaultValue?: T;
  autoGenPattern?: RegExp;
  bcryptRounds?: number;
  refTable?: TableDef<any>;
  refField?: string;
  mimeTypes?: string[];
  enumValues?: string[];
  vectorDimensions?: number;
  // Phantom type for json<T>
  _phantom?: T;
}

// ---- Binary Type Tags ----

export const TypeTag = {
  Null: 0x00,
  String: 0x01,
  Number: 0x02,
  Boolean: 0x03,
  Array: 0x04,
  Integer: 0x05,
  Vector: 0x06,
  Json: 0x0d,
  FileSingle: 0x0e,
  FileMulti: 0x0f,
} as const;

export type TypeTag = (typeof TypeTag)[keyof typeof TypeTag];

// ---- Row Pointer (index entry) ----

export interface RowPointer {
  pageNumber: number;
  slotIndex: number;
}

// ---- File Reference ----

export interface FileRef {
  path: string;
  name: string;
  size: number;
  mime: string;
  url: string;
}

// ---- Schema Compilation ----

export interface CompiledField {
  name: string;
  kind: FieldKind;
  required: boolean;
  unique: boolean;
  defaultValue?: unknown;
  autoGenPattern?: RegExp;
  bcryptRounds?: number;
  refTableName?: string;
  refField?: string;
  mimeTypes?: string[];
  enumValues?: string[];
  vectorDimensions?: number;
}

export interface CompiledSchema {
  fields: CompiledField[];
  fieldNames: string[];
  fieldMap: Map<string, CompiledField>;
}

// ---- Index ----

export interface IndexDef {
  fields: string[];
  unique: boolean;
}

// ---- Table Definition ----

export interface TableDef<S extends Record<string, SchemaField> = Record<string, SchemaField>> {
  name: string;
  schema: S;
  compiledSchema: CompiledSchema;
  indexes: IndexDef[];
  auth: boolean;
  migrations: MigrationStep[];
}

// ---- Migration ----

export interface MigrationStep {
  version: number;
  rename?: Record<string, string>;
  transform?: (row: Record<string, unknown>) => Record<string, unknown>;
}

// ---- Access Control ----

export type AccessPolicy =
  | { type: "authenticated" }
  | { type: "roles"; roles: string[] }
  | { type: "public" };

// ---- Context ----

export interface AuthContext {
  id: string;
  email: string;
  roles: string[];
}

export interface RequestContext {
  auth: AuthContext | null;
  headers: Headers;
  url: URL;
}

// ---- Schema Version Info (stored in _meta.flop) ----

export interface StoredColumnDef {
  name: string;
  type: string;
  required?: boolean;
  unique?: boolean;
  nullable?: boolean;
  default?: unknown;
}

export interface StoredSchema {
  columns: StoredColumnDef[];
}

export interface StoredTableMeta {
  currentSchemaVersion: number;
  schemas: Record<number, StoredSchema>;
}

export interface StoredMeta {
  version: number;
  created: string;
  tables: Record<string, StoredTableMeta>;
}

// ---- Page ----

export const PAGE_SIZE = 4096;
export const FILE_HEADER_SIZE = 64;
export const PAGE_HEADER_SIZE = 12;
export const SLOT_SIZE = 4; // offset(2) + length(2)

export interface PageHeader {
  pageNumber: number;
  slotCount: number;
  freeSpaceOffset: number;
  flags: number;
}

export interface SlotEntry {
  offset: number;
  length: number;
}

// Page flags
export const PageFlags = {
  None: 0,
  Dirty: 1,
  Overflow: 2,
  Deleted: 4,
} as const;

// ---- File Header (.flop table file) ----

export const TABLE_FILE_MAGIC = new Uint8Array([0x46, 0x4c, 0x50, 0x54]); // "FLPT"
export const META_FILE_MAGIC = new Uint8Array([0x46, 0x4c, 0x4f, 0x50]); // "FLOP"

export interface TableFileHeader {
  pageCount: number;
  totalRows: number;
  schemaVersion: number;
}

// ---- Type Inference Helpers ----

// Maps SchemaField builders to their TypeScript types at compile time
// These are used by the public API in schema.ts

export type InferFieldType<F> = F extends { kind: "string" }
  ? string
  : F extends { kind: "number" }
    ? number
    : F extends { kind: "integer" }
      ? number
      : F extends { kind: "boolean" }
        ? boolean
        : F extends { kind: "bcrypt" }
          ? string
          : F extends { kind: "ref" }
            ? string
            : F extends { kind: "refMulti" }
              ? string[]
              : F extends { kind: "json"; _phantom?: infer T }
                ? T | null
                : F extends { kind: "fileSingle" }
                  ? FileRef | null
                  : F extends { kind: "fileMulti" }
                    ? FileRef[]
                    : F extends { kind: "roles" }
                      ? string[]
                      : F extends { kind: "enum" }
                        ? string
                        : F extends { kind: "vector" }
                          ? number[]
                          : F extends { kind: "set" }
                            ? string[]
                            : F extends { kind: "timestamp" }
                              ? number
                              : unknown;

export type InferSchema<S extends Record<string, SchemaField>> = {
  [K in keyof S]: InferFieldType<S[K]>;
};

// Infer value type from either a FieldBuilder<T> (via _build) or a raw SchemaField
export type InferInputFieldType<F> =
  F extends { _build(): SchemaField<infer T> } ? T
  : F extends SchemaField ? InferFieldType<F>
  : unknown;

// Infer params object from a schema of field builders / SchemaFields
export type InferParams<S extends Record<string, any>> = {
  [K in keyof S]: InferInputFieldType<S[K]>;
};

// For insert: required fields are required, others optional
// Fields with autogenerate are always optional on insert
type RequiredInsertKeys<S extends Record<string, SchemaField>> = {
  [K in keyof S]: S[K] extends { required: true; autoGenPattern: undefined }
    ? S[K]["autoGenPattern"] extends RegExp
      ? never
      : K
    : never;
}[keyof S];

type OptionalInsertKeys<S extends Record<string, SchemaField>> = Exclude<
  keyof S,
  RequiredInsertKeys<S>
>;

export type InferInsertSchema<S extends Record<string, SchemaField>> = {
  [K in RequiredInsertKeys<S>]: InferFieldType<S[K]>;
} & {
  [K in OptionalInsertKeys<S>]?: InferFieldType<S[K]>;
};

// ---- Endpoint Types ----

export interface ReducerDef<TParams = unknown, TResult = unknown> {
  _type: "reducer";
  _params: TParams;
  _result: TResult;
  _access: AccessPolicy;
}

export interface ViewDef<TParams = unknown, TResult = unknown> {
  _type: "view";
  _params: TParams;
  _result: TResult;
  _access: AccessPolicy;
}

// ---- FlopSchema (for client type bridge) ----

export type FlopSchema<
  _DB = unknown,
  Endpoints extends {
    reducers: Record<string, ReducerDef>;
    views: Record<string, ViewDef>;
  } = {
    reducers: Record<string, ReducerDef>;
    views: Record<string, ViewDef>;
  },
> = {
  reducers: {
    [K in keyof Endpoints["reducers"]]: {
      params: Endpoints["reducers"][K]["_params"];
      result: Endpoints["reducers"][K]["_result"];
    };
  };
  views: {
    [K in keyof Endpoints["views"]]: {
      params: Endpoints["views"][K]["_params"];
      result: Endpoints["views"][K]["_result"];
    };
  };
};
