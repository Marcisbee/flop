// table() function and TableDef — defines a table's schema, indexes, and migrations

import type {
  SchemaField,
  CompiledSchema,
  CompiledField,
  IndexDef,
  TableDef,
  MigrationStep,
} from "./types.ts";

/** Accepts both raw SchemaField objects and field builders with _build() */
export type SchemaFieldInput = SchemaField | { _build(): SchemaField };

interface TableConfig<S extends Record<string, SchemaFieldInput>> {
  schema: S;
  auth?: boolean;
  migrations?: MigrationStep[];
}

class TableBuilder<S extends Record<string, SchemaFieldInput>> {
  readonly schema: S;
  readonly compiledSchema: CompiledSchema;
  readonly indexes: IndexDef[] = [];
  readonly auth: boolean;
  readonly migrations: MigrationStep[];
  name = "";

  constructor(config: TableConfig<S>) {
    this.schema = config.schema;
    this.auth = config.auth ?? false;
    this.migrations = config.migrations ?? [];
    this.compiledSchema = compileSchema(config.schema);
  }

  index(...fields: Extract<keyof S, string>[]): { unique: () => TableBuilder<S> } {
    const def: IndexDef = { fields: fields as string[], unique: false };
    this.indexes.push(def);
    return {
      unique: () => {
        def.unique = true;
        return this;
      },
    };
  }

  /** @internal Convert to TableDef for storage */
  _toTableDef(name: string): TableDef {
    this.name = name;
    return {
      name,
      schema: this.schema as unknown as Record<string, SchemaField>,
      compiledSchema: this.compiledSchema,
      indexes: this.indexes,
      auth: this.auth,
      migrations: this.migrations,
    };
  }
}

function compileSchema(schema: Record<string, SchemaFieldInput>): CompiledSchema {
  const fields: CompiledField[] = [];
  const fieldNames: string[] = [];
  const fieldMap = new Map<string, CompiledField>();

  for (const [name, fieldBuilder] of Object.entries(schema)) {
    // Handle both raw SchemaField objects and builders with _build()
    const field: SchemaField =
      typeof (fieldBuilder as any)._build === "function"
        ? (fieldBuilder as any)._build()
        : fieldBuilder;

    const compiled: CompiledField = {
      name,
      kind: field.kind,
      required: field.required,
      unique: field.unique,
      defaultValue: field.defaultValue,
      autoGenPattern: field.autoGenPattern,
      bcryptRounds: field.bcryptRounds,
      refTableName: field.refTable?.name,
      refField: field.refField,
      mimeTypes: field.mimeTypes,
      enumValues: field.enumValues,
      vectorDimensions: field.vectorDimensions,
    };

    fields.push(compiled);
    fieldNames.push(name);
    fieldMap.set(name, compiled);
  }

  return { fields, fieldNames, fieldMap };
}

export function table<S extends Record<string, SchemaFieldInput>>(
  config: TableConfig<S>,
): TableBuilder<S> {
  return new TableBuilder(config);
}

export { TableBuilder };
export { compileSchema };
