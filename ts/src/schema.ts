// The `t` module — schema field type builders
// Usage: t.string(), t.number(), t.boolean(), t.json<T>(), t.bcrypt(rounds), etc.

import type { SchemaField, FieldKind, TableDef } from "./types.ts";

// ---- Field Builder Base ----

class FieldBuilder<T = unknown> {
  protected field: SchemaField<T>;

  constructor(kind: FieldKind) {
    this.field = { kind, required: false, unique: false };
  }

  required(): this {
    this.field.required = true;
    return this;
  }

  unique(): this {
    this.field.unique = true;
    return this;
  }

  default(value: T): this {
    this.field.defaultValue = value;
    return this;
  }

  /** @internal */
  _build(): SchemaField<T> {
    return { ...this.field };
  }
}

// ---- String Field ----

class StringFieldBuilder extends FieldBuilder<string> {
  constructor() {
    super("string");
  }

  autogenerate(pattern: RegExp): this {
    this.field.autoGenPattern = pattern;
    return this;
  }
}

// ---- Number Field ----

class NumberFieldBuilder extends FieldBuilder<number> {
  constructor() {
    super("number");
  }
}

// ---- Boolean Field ----

class BooleanFieldBuilder extends FieldBuilder<boolean> {
  constructor() {
    super("boolean");
  }
}

// ---- JSON Field ----

class JsonFieldBuilder<T = unknown> extends FieldBuilder<T> {
  constructor() {
    super("json");
  }
}

// ---- Bcrypt Field ----

class BcryptFieldBuilder extends FieldBuilder<string> {
  constructor(rounds: number) {
    super("bcrypt");
    this.field.bcryptRounds = rounds;
    this.field.required = true; // bcrypt fields are implicitly required
  }
}

// ---- Ref Field ----

class RefFieldBuilder extends FieldBuilder<string> {
  constructor(refTable: TableDef<any>, refField: string) {
    super("ref");
    this.field.refTable = refTable;
    this.field.refField = refField;
  }
}

// ---- File Single Field ----

class FileSingleFieldBuilder extends FieldBuilder<null> {
  constructor(...mimeTypes: string[]) {
    super("fileSingle");
    this.field.mimeTypes = mimeTypes;
  }
}

// ---- File Multi Field ----

class FileMultiFieldBuilder extends FieldBuilder<null> {
  constructor(...mimeTypes: string[]) {
    super("fileMulti");
    this.field.mimeTypes = mimeTypes;
  }
}

// ---- Roles Field ----

class RolesFieldBuilder extends FieldBuilder<string[]> {
  constructor() {
    super("roles");
    this.field.defaultValue = [];
  }
}

// ---- Enum Field ----

class EnumFieldBuilder extends FieldBuilder<string> {
  constructor(...values: string[]) {
    super("enum");
    this.field.enumValues = values;
  }
}

// ---- Integer Field ----

class IntegerFieldBuilder extends FieldBuilder<number> {
  constructor() {
    super("integer");
  }
}

// ---- RefMulti Field ----

class RefMultiFieldBuilder extends FieldBuilder<string[]> {
  constructor(refTable: TableDef<any>, refField: string) {
    super("refMulti");
    this.field.refTable = refTable;
    this.field.refField = refField;
    this.field.defaultValue = [];
  }
}

// ---- Vector Field ----

class VectorFieldBuilder extends FieldBuilder<number[]> {
  constructor(dimensions: number) {
    super("vector");
    this.field.vectorDimensions = dimensions;
  }
}

// ---- Set Field ----

class SetFieldBuilder extends FieldBuilder<string[]> {
  constructor() {
    super("set");
    this.field.defaultValue = [];
  }
}

// ---- Timestamp Field ----

class TimestampFieldBuilder extends FieldBuilder<number> {
  constructor() {
    super("timestamp");
  }

  override default(value: number | "now"): this {
    this.field.defaultValue = value as any;
    return this;
  }
}

// ---- The `t` namespace ----

export const t = {
  string(): StringFieldBuilder {
    return new StringFieldBuilder();
  },

  number(): NumberFieldBuilder {
    return new NumberFieldBuilder();
  },

  boolean(): BooleanFieldBuilder {
    return new BooleanFieldBuilder();
  },

  json<T = unknown>(): JsonFieldBuilder<T> {
    return new JsonFieldBuilder<T>();
  },

  bcrypt(rounds: number): BcryptFieldBuilder {
    return new BcryptFieldBuilder(rounds);
  },

  refSingle(refTable: TableDef<any>, field: string): RefFieldBuilder {
    return new RefFieldBuilder(refTable, field);
  },

  fileSingle(...mimeTypes: string[]): FileSingleFieldBuilder {
    return new FileSingleFieldBuilder(...mimeTypes);
  },

  fileMulti(...mimeTypes: string[]): FileMultiFieldBuilder {
    return new FileMultiFieldBuilder(...mimeTypes);
  },

  roles(): RolesFieldBuilder {
    return new RolesFieldBuilder();
  },

  enum<T extends string>(...values: T[]): EnumFieldBuilder {
    return new EnumFieldBuilder(...values);
  },

  integer(): IntegerFieldBuilder {
    return new IntegerFieldBuilder();
  },

  refMulti(refTable: TableDef<any>, field: string): RefMultiFieldBuilder {
    return new RefMultiFieldBuilder(refTable, field);
  },

  vector(dimensions: number): VectorFieldBuilder {
    return new VectorFieldBuilder(dimensions);
  },

  set(): SetFieldBuilder {
    return new SetFieldBuilder();
  },

  timestamp(): TimestampFieldBuilder {
    return new TimestampFieldBuilder();
  },
};

// ---- Autogenerate from regex pattern ----
// Supports patterns like /[a-z0-9]{15}/

export function generateFromPattern(pattern: RegExp): string {
  const src = pattern.source;
  const match = src.match(/^\[([^\]]+)\]\{(\d+)\}$/);
  if (!match) {
    throw new Error(
      `Autogenerate pattern must be in format [charset]{length}, got: ${src}`
    );
  }

  const charsetStr = match[1];
  const length = parseInt(match[2], 10);
  const charset = expandCharset(charsetStr);

  const bytes = new Uint8Array(length);
  crypto.getRandomValues(bytes);

  let result = "";
  for (let i = 0; i < length; i++) {
    result += charset[bytes[i] % charset.length];
  }
  return result;
}

function expandCharset(spec: string): string {
  let result = "";
  let i = 0;
  while (i < spec.length) {
    if (i + 2 < spec.length && spec[i + 1] === "-") {
      const start = spec.charCodeAt(i);
      const end = spec.charCodeAt(i + 2);
      for (let c = start; c <= end; c++) {
        result += String.fromCharCode(c);
      }
      i += 3;
    } else {
      result += spec[i];
      i++;
    }
  }
  return result;
}

// Re-export builder types for type inference
export type {
  StringFieldBuilder,
  NumberFieldBuilder,
  BooleanFieldBuilder,
  JsonFieldBuilder,
  BcryptFieldBuilder,
  RefFieldBuilder,
  RefMultiFieldBuilder,
  FileSingleFieldBuilder,
  FileMultiFieldBuilder,
  RolesFieldBuilder,
  EnumFieldBuilder,
  IntegerFieldBuilder,
  VectorFieldBuilder,
  SetFieldBuilder,
  TimestampFieldBuilder,
};
