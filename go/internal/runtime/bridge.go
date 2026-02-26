package runtime

import (
	"encoding/json"
	"fmt"

	"github.com/marcisbee/flop/internal/engine"
	"modernc.org/quickjs"
)

// Bridge connects the QuickJS VM to the Go database engine.
type Bridge struct {
	vm        *VM
	db        *engine.Database
	callBC    []byte                         // pre-compiled GLOBAL script (fast sync path)
	asyncBC   []byte                         // pre-compiled MODULE script (fallback for Promises)
	txBuf     map[string]*engine.WalBufEntry // active transaction buffer (nil = no tx)
	fieldAtom map[string]map[string]quickjs.Atom
}

// NewBridge creates a new bridge and registers all host functions.
func NewBridge(vm *VM, db *engine.Database) *Bridge {
	b := &Bridge{
		vm:        vm,
		db:        db,
		fieldAtom: make(map[string]map[string]quickjs.Atom),
	}
	b.registerHostFunctions()
	return b
}

func (b *Bridge) registerHostFunctions() {
	// __flop_begin_tx() — starts a transaction buffer
	b.vm.RegisterFunc("__flop_begin_tx", func() {
		b.txBuf = make(map[string]*engine.WalBufEntry)
	}, false)

	// __flop_commit_tx() -> "" on success, error JSON on failure
	b.vm.RegisterFunc("__flop_commit_tx", func() string {
		buf := b.txBuf
		b.txBuf = nil
		if buf == nil || len(buf) == 0 {
			return ""
		}
		if err := b.db.EnqueueCommit(buf); err != nil {
			return b.errJSON(err.Error())
		}
		return ""
	}, false)

	// __flop_insert(tableName, dataObject) -> [rowObject, error]
	b.vm.RegisterFunc("__flop_insert", func(tableName string, data quickjs.Value) (quickjs.Value, error) {
		obj, err := b.extractDataObject(tableName, data)
		if err != nil {
			return quickjs.UndefinedValue, err
		}
		table := b.db.GetTable(tableName)
		if table == nil {
			return quickjs.UndefinedValue, fmt.Errorf("table not found: %s", tableName)
		}

		result, err := table.Insert(obj, b.txBuf)
		if err != nil {
			return quickjs.UndefinedValue, err
		}

		jsRow, err := b.rowToJSValue(tableName, result)
		if err != nil {
			return quickjs.UndefinedValue, err
		}
		return jsRow, nil
	}, false)

	// __flop_get(tableName, key) -> [rowObject|undefined, error]
	b.vm.RegisterFunc("__flop_get", func(tableName, key string) (quickjs.Value, error) {
		table := b.db.GetTable(tableName)
		if table == nil {
			return quickjs.UndefinedValue, nil
		}

		result, err := table.Get(key)
		if err != nil {
			return quickjs.UndefinedValue, err
		}
		if result == nil {
			return quickjs.UndefinedValue, nil
		}

		jsRow, err := b.rowToJSValue(tableName, result)
		if err != nil {
			return quickjs.UndefinedValue, err
		}
		return jsRow, nil
	}, false)

	// __flop_update_sparse(tableName, key, dataObject, keysJSON) -> [rowObject|undefined, error]
	b.vm.RegisterFunc("__flop_update_sparse", func(tableName, key string, data quickjs.Value, keysJSON string) (quickjs.Value, error) {
		var keys []string
		if err := json.Unmarshal([]byte(keysJSON), &keys); err != nil {
			return quickjs.UndefinedValue, fmt.Errorf("invalid update keys: %w", err)
		}
		obj, err := b.extractDataObjectKeys(tableName, data, keys)
		if err != nil {
			return quickjs.UndefinedValue, err
		}
		table := b.db.GetTable(tableName)
		if table == nil {
			return quickjs.UndefinedValue, nil
		}

		result, err := table.Update(key, obj, b.txBuf)
		if err != nil {
			return quickjs.UndefinedValue, err
		}
		if result == nil {
			return quickjs.UndefinedValue, nil
		}

		jsRow, err := b.rowToJSValue(tableName, result)
		if err != nil {
			return quickjs.UndefinedValue, err
		}
		return jsRow, nil
	}, false)

	// __flop_delete(tableName, key) -> jsonResult
	b.vm.RegisterFunc("__flop_delete", func(tableName, key string) string {
		table := b.db.GetTable(tableName)
		if table == nil {
			return `{"deleted":false}`
		}

		deleted, err := table.Delete(key, b.txBuf)
		if err != nil {
			return b.errJSON(err.Error())
		}

		return b.okJSON(map[string]interface{}{"deleted": deleted})
	}, false)

	// __flop_scan(tableName, limit, offset) -> jsonResult
	b.vm.RegisterFunc("__flop_scan", func(tableName string, limit, offset int) string {
		table := b.db.GetTable(tableName)
		if table == nil {
			return "[]"
		}

		results, err := table.Scan(limit, offset)
		if err != nil {
			return "[]"
		}

		if results == nil {
			return "[]"
		}
		return b.okJSON(results)
	}, false)

	// __flop_count(tableName) -> int
	b.vm.RegisterFunc("__flop_count", func(tableName string) int {
		table := b.db.GetTable(tableName)
		if table == nil {
			return 0
		}
		return table.Count()
	}, false)

	// __flop_find_by_index(tableName, fieldName, value) -> jsonResult | "null"
	b.vm.RegisterFunc("__flop_find_by_index", func(tableName, fieldName, value string) string {
		table := b.db.GetTable(tableName)
		if table == nil {
			return "null"
		}

		pointer, ok := table.FindByIndex([]string{fieldName}, value)
		if !ok {
			return "null"
		}

		row, err := table.GetByPointer(pointer)
		if err != nil || row == nil {
			return "null"
		}

		return b.okJSON(row)
	}, false)

	// __flop_find_all_by_index(tableName, fieldName, value) -> jsonResult
	b.vm.RegisterFunc("__flop_find_all_by_index", func(tableName, fieldName, value string) string {
		table := b.db.GetTable(tableName)
		if table == nil {
			return "[]"
		}

		pointers := table.FindAllByIndex([]string{fieldName}, value)
		var results []map[string]interface{}
		for _, p := range pointers {
			row, err := table.GetByPointer(p)
			if err == nil && row != nil {
				results = append(results, row)
			}
		}

		if results == nil {
			return "[]"
		}
		return b.okJSON(results)
	}, false)
}

// RegisterHandlerBridge injects the JS-side handler calling mechanism.
func (b *Bridge) RegisterHandlerBridge() error {
	handlerBridge := `
globalThis.__flop_build_db_proxy = function() {
  var __flop_table_cache = Object.create(null);
  return new Proxy({}, {
    get: function(_, tableName) {
      if (__flop_table_cache[tableName]) return __flop_table_cache[tableName];

      var api = {
        insert: function(data) {
          var result = __flop_insert(tableName, data);
          if (result[1] !== null) throw new Error(String(result[1]));
          return result[0];
        },
        get: function(key) {
          var result = __flop_get(tableName, String(key));
          if (result[1] !== null) throw new Error(String(result[1]));
          if (result[0] === undefined) return null;
          return result[0];
        },
        update: function(key, data) {
          var keys = Object.keys(data || {});
          var result = __flop_update_sparse(tableName, String(key), data, JSON.stringify(keys));
          if (result[1] !== null) throw new Error(String(result[1]));
          if (result[0] === undefined) return null;
          return result[0];
        },
        delete: function(key) {
          var result = __flop_delete(tableName, String(key));
          return JSON.parse(result).deleted;
        },
        scan: function(limit, offset) {
          limit = limit || 100;
          offset = offset || 0;
          var result = __flop_scan(tableName, limit, offset);
          return JSON.parse(result);
        },
        count: function() {
          return __flop_count(tableName);
        },
      };
      __flop_table_cache[tableName] = api;
      return api;
    }
  });
};

// Build and cache the db proxy once — reused for all requests in this VM.
globalThis.__flop_db_proxy = globalThis.__flop_build_db_proxy();

// Reusable ctx objects (one per handler type) — avoids per-call object creation.
globalThis.__flop_ctx_view = {
  db: globalThis.__flop_db_proxy,
  request: { auth: null, headers: {}, url: '' },
};
globalThis.__flop_ctx_reducer = {
  db: globalThis.__flop_db_proxy,
  request: { auth: null, headers: {}, url: '' },
  transaction: function(fn) {
    __flop_begin_tx();
    try {
      var txResult = fn(globalThis.__flop_db_proxy);
      var commitErr = __flop_commit_tx();
      if (commitErr) throw new Error(JSON.parse(commitErr).error);
      return txResult;
    } catch(e) {
      __flop_commit_tx();
      throw e;
    }
  },
};

// Handler caller. Returns raw result (may be a Promise for async handlers).
globalThis.__flop_call_handler = function(type, name, params, auth) {
  var exports = globalThis.__FLOP_EXPORTS__;
  if (!exports) throw new Error('Module exports not available');
  var endpoint = exports[name];
  if (!endpoint || !endpoint._handler) {
    throw new Error(type + ' not found: ' + name);
  }

  // Coerce query-string style params into typed values expected by handlers.
  var defs = endpoint._compiledParams || {};
  for (var k in defs) {
    if (!Object.prototype.hasOwnProperty.call(params, k)) continue;
    var def = defs[k] || {};
    var v = params[k];
    if (typeof v !== 'string') continue;

    if (def.kind === 'number' || def.kind === 'timestamp') {
      var n = Number(v);
      if (!Number.isNaN(n)) params[k] = n;
      continue;
    }

    if (def.kind === 'integer') {
      var i = Number(v);
      if (Number.isInteger(i)) params[k] = i;
      continue;
    }

    if (def.kind === 'boolean') {
      if (v === 'true') params[k] = true;
      else if (v === 'false') params[k] = false;
      continue;
    }

    if (
      def.kind === 'json' ||
      def.kind === 'vector' ||
      def.kind === 'set' ||
      def.kind === 'roles' ||
      def.kind === 'refMulti' ||
      def.kind === 'fileMulti'
    ) {
      if (v.length > 0 && (v[0] === '[' || v[0] === '{')) {
        try { params[k] = JSON.parse(v); } catch (_) {}
      }
    }
  }

  // Reuse pre-built ctx, just update auth
  var ctx = (type === 'reducer') ? globalThis.__flop_ctx_reducer : globalThis.__flop_ctx_view;
  ctx.request.auth = auth;

  return endpoint._handler(ctx, params);
};
`
	if _, err := b.vm.Eval(handlerBridge); err != nil {
		return err
	}

	// 1. SYNC path (global script) — fast, no js_std_await.
	// Since async/await is stripped from user handlers at bundle time, most handlers
	// return synchronous values. If a handler still returns a Promise (e.g. Promise.all),
	// the sync path stores it in __flop_pending and returns false as a sentinel.
	callScript := `(function() {
		var r = globalThis.__flop_call_handler(
			globalThis.__flop_call_type,
			globalThis.__flop_call_name,
			globalThis.__flop_call_params,
			globalThis.__flop_call_auth
		);
		if (r && typeof r === 'object' && typeof r.then === 'function') {
			globalThis.__flop_pending = r;
			return false;
		}
		globalThis.__flop_last_result = (r === undefined) ? null : r;
		return true;
	})()`
	callBC, err := b.vm.CompileGlobal(callScript)
	if err != nil {
		return fmt.Errorf("pre-compile call script: %w", err)
	}
	b.callBC = callBC

	// 2. ASYNC fallback (module) — for handlers that still return Promises.
	asyncScript := `{
		var r = await globalThis.__flop_pending;
		globalThis.__flop_last_result = (r === undefined) ? null : r;
	}`
	asyncBC, err := b.vm.CompileModule(asyncScript)
	if err != nil {
		return fmt.Errorf("pre-compile async module: %w", err)
	}
	b.asyncBC = asyncBC

	return nil
}

// CallHandler invokes a JS view or reducer handler.
// Tries the fast sync path first; falls back to async module for Promises.
func (b *Bridge) CallHandler(handlerType, name, paramsJSON, authJSON string) (string, error) {
	result, err := b.vm.CallSyncFirst(handlerType, name, paramsJSON, authJSON, b.callBC, b.asyncBC)
	if err != nil {
		return "", fmt.Errorf("call %s %q: %w", handlerType, name, err)
	}
	return result, nil
}

func (b *Bridge) extractDataObject(tableName string, data quickjs.Value) (map[string]interface{}, error) {
	table := b.db.GetTable(tableName)
	if table == nil {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	fields := table.GetDef().CompiledSchema.Fields
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		names = append(names, f.Name)
	}
	return b.extractDataObjectKeys(tableName, data, names)
}

func (b *Bridge) extractDataObjectKeys(tableName string, data quickjs.Value, keys []string) (map[string]interface{}, error) {
	table := b.db.GetTable(tableName)
	if table == nil {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	fieldSet := make(map[string]struct{}, len(table.GetDef().CompiledSchema.Fields))
	for _, f := range table.GetDef().CompiledSchema.Fields {
		fieldSet[f.Name] = struct{}{}
	}

	row := make(map[string]interface{}, len(keys))
	for _, fieldName := range keys {
		if _, ok := fieldSet[fieldName]; !ok {
			continue
		}

		atom, err := b.getFieldAtom(tableName, fieldName)
		if err != nil {
			return nil, fmt.Errorf("create atom %q: %w", fieldName, err)
		}

		v, err := data.GetPropertyValue(atom)
		if err != nil {
			return nil, fmt.Errorf("read field %q: %w", fieldName, err)
		}

		if v.IsUndefined() {
			v.Free()
			continue
		}

		anyVal, err := v.Any()
		v.Free()
		if err != nil {
			return nil, fmt.Errorf("decode field %q: %w", fieldName, err)
		}

		normalized, err := normalizeJSValue(anyVal)
		if err != nil {
			return nil, fmt.Errorf("normalize field %q: %w", fieldName, err)
		}
		row[fieldName] = normalized
	}

	return row, nil
}

func (b *Bridge) rowToJSValue(tableName string, row map[string]interface{}) (quickjs.Value, error) {
	obj, err := b.vm.QJS().NewObjectValue()
	if err != nil {
		return quickjs.UndefinedValue, err
	}

	for k, v := range row {
		atom, err := b.getFieldAtom(tableName, k)
		if err != nil {
			obj.Free()
			return quickjs.UndefinedValue, err
		}

		// Fast path for primitives/slices supported by quickjs conversion.
		if err := obj.SetProperty(atom, v); err == nil {
			continue
		}

		// Fallback for nested/complex values: marshal once and parse inside QuickJS.
		jsVal, err := b.anyToJSValue(v)
		if err != nil {
			obj.Free()
			return quickjs.UndefinedValue, err
		}
		if err := obj.SetPropertyValue(atom, jsVal); err != nil {
			jsVal.Free()
			obj.Free()
			return quickjs.UndefinedValue, err
		}
		jsVal.Free()
	}

	return obj, nil
}

func (b *Bridge) anyToJSValue(v interface{}) (quickjs.Value, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return quickjs.UndefinedValue, err
	}
	return b.vm.QJS().CallValue("JSON.parse", string(data))
}

func (b *Bridge) getFieldAtom(tableName, fieldName string) (quickjs.Atom, error) {
	tableAtoms := b.fieldAtom[tableName]
	if tableAtoms == nil {
		tableAtoms = make(map[string]quickjs.Atom)
		b.fieldAtom[tableName] = tableAtoms
	}

	if atom, ok := tableAtoms[fieldName]; ok {
		return atom, nil
	}

	atom, err := b.vm.QJS().NewAtom(fieldName)
	if err != nil {
		return 0, err
	}
	tableAtoms[fieldName] = atom
	return atom, nil
}

func normalizeJSValue(v interface{}) (interface{}, error) {
	obj, ok := v.(*quickjs.Object)
	if !ok {
		return v, nil
	}

	var decoded interface{}
	if err := obj.Into(&decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func (b *Bridge) okJSON(v interface{}) string {
	data, _ := json.Marshal(v)
	return string(data)
}

func (b *Bridge) errJSON(msg string) string {
	data, _ := json.Marshal(map[string]string{"error": msg})
	return string(data)
}
