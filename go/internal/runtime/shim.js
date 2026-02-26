// Flop runtime shim — replaces "flop" module in bundled app.ts

const __tables = {};
const __views = {};
const __reducers = {};
let __routeTree = null;
let __dbInstance = null;

// Schema field type builders (matching t.string(), t.number(), etc.)
class FieldBuilder {
  constructor(kind) {
    this.field = { kind, required: false, unique: false };
  }
  required() { this.field.required = true; return this; }
  unique() { this.field.unique = true; return this; }
  default(v) { this.field.defaultValue = v; return this; }
  autogenerate(pattern) {
    // Pattern comes as regex string from TS
    this.field.autoGenPattern = typeof pattern === 'string' ? pattern : pattern.source;
    return this;
  }
  _build() { return { ...this.field }; }
}

class BcryptFieldBuilder extends FieldBuilder {
  constructor(rounds) { super('bcrypt'); this.field.bcryptRounds = rounds; this.field.required = true; }
}

class RefFieldBuilder extends FieldBuilder {
  constructor(refTable, refField) { super('ref'); this.field.refTable = refTable; this.field.refField = refField; }
}

class RefMultiFieldBuilder extends FieldBuilder {
  constructor(refTable, refField) { super('refMulti'); this.field.refTable = refTable; this.field.refField = refField; this.field.defaultValue = []; }
}

class FileSingleFieldBuilder extends FieldBuilder {
  constructor(...mimeTypes) { super('fileSingle'); this.field.mimeTypes = mimeTypes; }
}

class FileMultiFieldBuilder extends FieldBuilder {
  constructor(...mimeTypes) { super('fileMulti'); this.field.mimeTypes = mimeTypes; }
}

class RolesFieldBuilder extends FieldBuilder {
  constructor() { super('roles'); this.field.defaultValue = []; }
}

class EnumFieldBuilder extends FieldBuilder {
  constructor(...values) { super('enum'); this.field.enumValues = values; }
}

class VectorFieldBuilder extends FieldBuilder {
  constructor(dimensions) { super('vector'); this.field.vectorDimensions = dimensions; }
}

class SetFieldBuilder extends FieldBuilder {
  constructor() { super('set'); this.field.defaultValue = []; }
}

class TimestampFieldBuilder extends FieldBuilder {
  constructor() { super('timestamp'); }
  default(v) { this.field.defaultValue = v; return this; }
}

class IntegerFieldBuilder extends FieldBuilder {
  constructor() { super('integer'); }
}

export const t = {
  string: () => new FieldBuilder('string'),
  number: () => new FieldBuilder('number'),
  boolean: () => new FieldBuilder('boolean'),
  json: () => new FieldBuilder('json'),
  bcrypt: (rounds) => new BcryptFieldBuilder(rounds),
  refSingle: (refTable, field) => new RefFieldBuilder(refTable, field),
  refMulti: (refTable, field) => new RefMultiFieldBuilder(refTable, field),
  fileSingle: (...mimeTypes) => new FileSingleFieldBuilder(...mimeTypes),
  fileMulti: (...mimeTypes) => new FileMultiFieldBuilder(...mimeTypes),
  roles: () => new RolesFieldBuilder(),
  enum: (...values) => new EnumFieldBuilder(...values),
  integer: () => new IntegerFieldBuilder(),
  vector: (dimensions) => new VectorFieldBuilder(dimensions),
  set: () => new SetFieldBuilder(),
  timestamp: () => new TimestampFieldBuilder(),
};

// TableBuilder — records table definition
class TableBuilder {
  constructor(config) {
    this.schema = config.schema;
    this.auth = config.auth || false;
    this.migrations = config.migrations || [];
    this.indexes = [];
    this.name = '';
    // Compile schema
    this.compiledFields = [];
    for (const [name, fieldBuilder] of Object.entries(config.schema)) {
      const field = typeof fieldBuilder._build === 'function' ? fieldBuilder._build() : fieldBuilder;
      this.compiledFields.push({ name, ...field });
    }
  }
  index(...fields) {
    const def = { fields, unique: false };
    this.indexes.push(def);
    return { unique: () => { def.unique = true; return this; } };
  }
}

export function table(config) {
  return new TableBuilder(config);
}

// Endpoint base classes
class Endpoint {
  constructor(type, paramSchema, handler) {
    this._type = type;
    this._paramSchema = paramSchema;
    this._handler = handler;
    this._access = { type: 'authenticated' };
    this._name = '';
    // Compile param schema
    this._compiledParams = {};
    for (const [name, fieldBuilder] of Object.entries(paramSchema)) {
      const field = typeof fieldBuilder._build === 'function' ? fieldBuilder._build() : fieldBuilder;
      this._compiledParams[name] = { name, ...field };
    }
  }
  roles(...roles) { this._access = { type: 'roles', roles }; return this; }
  public() { this._access = { type: 'public' }; return this; }
}

class Reducer extends Endpoint {
  constructor(paramSchema, handler) { super('reducer', paramSchema, handler); }
}

class View extends Endpoint {
  constructor(paramSchema, handler) {
    super('view', paramSchema, handler);
    this._dependentTables = [];
  }
}

// Global handler storage for bridge to call handlers by name
const __handlers = {};

function inferDependentTables(handler, tableDefs) {
  const names = Object.keys(tableDefs || {});
  if (names.length === 0) return [];

  let src = '';
  try {
    src = Function.prototype.toString.call(handler);
  } catch {
    return names;
  }

  const deps = [];
  for (const name of names) {
    if (
      src.includes('.db.' + name) ||
      src.includes(".db['" + name + "']") ||
      src.includes('.db["' + name + '"]')
    ) {
      deps.push(name);
    }
  }
  return deps.length > 0 ? deps : names;
}

// Database — captures table definitions and creates views/reducers
class Database {
  constructor(tableDefs, config) {
    this._tableDefs = tableDefs;
    this._config = config || {};
    this._tables = {};
    this._viewCounter = 0;
    this._reducerCounter = 0;
    // Assign names to tables
    for (const [name, builder] of Object.entries(tableDefs)) {
      builder.name = name;
      __tables[name] = {
        name,
        fields: builder.compiledFields,
        auth: builder.auth,
        migrations: builder.migrations,
        indexes: builder.indexes,
      };
      this._tables[name] = builder;
    }
    __dbInstance = this;
  }

  view(params, handler) {
    const v = new View(params, handler);
    v._dependentTables = inferDependentTables(handler, this._tableDefs);
    // Auto-name will be set by export name detection; store handler by temp ID
    const tempId = '__view_' + (this._viewCounter++);
    v._tempId = tempId;
    __handlers[tempId] = v;
    return v;
  }

  reduce(params, handler) {
    const r = new Reducer(params, handler);
    const tempId = '__reducer_' + (this._reducerCounter++);
    r._tempId = tempId;
    __handlers[tempId] = r;
    return r;
  }
}

export function flop(tables, config) {
  return new Database(tables, config);
}

// Route builder
export function route(pattern, config, children) {
  return {
    _type: 'route',
    pattern,
    head: (config && config.head) || null,
    component: (config && config.component) || null,
    children: children || (config && config.children) || [],
  };
}

// Re-export classes for instanceof checks
export { Reducer, View, Database };

// Collect metadata after module execution
globalThis.__FLOP_COLLECT__ = function(moduleExports) {
  const views = {};
  const reducers = {};
  let routeTree = null;
  let db = null;

  for (const [name, value] of Object.entries(moduleExports)) {
    if (value instanceof Database) {
      db = value;
    } else if (value instanceof Reducer) {
      reducers[name] = {
        name,
        params: value._compiledParams,
        access: value._access,
        handlerRef: name,
      };
    } else if (value instanceof View) {
      views[name] = {
        name,
        params: value._compiledParams,
        access: value._access,
        dependentTables: value._dependentTables,
        handlerRef: name,
      };
    } else if (value && typeof value === 'object' && value._type === 'route') {
      routeTree = value;
    }
  }

  return JSON.stringify({
    tables: __tables,
    views,
    reducers,
    routeTree: routeTree ? flattenRoutes(routeTree) : [],
    config: db ? db._config : {},
  });
};

// Flatten route tree for Go consumption
function flattenRoutes(node, parentPattern, parentComponentPaths) {
  parentPattern = parentPattern || '';
  parentComponentPaths = parentComponentPaths || [];
  const results = [];

  let fullPattern;
  if (parentPattern === '/' && node.pattern === '/') fullPattern = '/';
  else if (node.pattern === '/') fullPattern = parentPattern || '/';
  else fullPattern = (parentPattern === '/' ? '' : parentPattern) + node.pattern;

  // Extract import path from component function source
  const componentPaths = [...parentComponentPaths];
  if (node.component) {
    const src = node.component.toString();
    const match = src.match(/import\s*\(\s*["']([^"']+)["']\s*\)/);
    if (match) componentPaths.push(match[1]);
  }

  const hasHead = !!node.head;

  if (!node.children || node.children.length === 0) {
    results.push({
      pattern: fullPattern,
      componentPaths,
      hasHead,
      headChainLength: parentPattern ? 2 : 1,
    });
  } else {
    for (const child of node.children) {
      results.push(...flattenRoutes(child, fullPattern, componentPaths));
    }
  }

  return results;
}
