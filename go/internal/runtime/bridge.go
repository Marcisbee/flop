package runtime

import (
	"encoding/json"
	"fmt"

	"github.com/marcisbee/flop/internal/engine"
)

// Bridge connects the QuickJS VM to the Go database engine.
type Bridge struct {
	vm        *VM
	db        *engine.Database
	callBC    []byte                         // pre-compiled GLOBAL script (fast sync path)
	asyncBC   []byte                         // pre-compiled MODULE script (fallback for Promises)
	txBuf     map[string]*engine.WalBufEntry // active transaction buffer (nil = no tx)
}

// NewBridge creates a new bridge and registers all host functions.
func NewBridge(vm *VM, db *engine.Database) *Bridge {
	b := &Bridge{vm: vm, db: db}
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

	// __flop_insert(tableName, jsonData) -> jsonResult
	b.vm.RegisterFunc("__flop_insert", func(tableName, dataJSON string) string {
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(dataJSON), &data); err != nil {
			return b.errJSON(fmt.Sprintf("invalid JSON: %s", err))
		}

		table := b.db.GetTable(tableName)
		if table == nil {
			return b.errJSON(fmt.Sprintf("table not found: %s", tableName))
		}

		result, err := table.Insert(data, b.txBuf)
		if err != nil {
			return b.errJSON(err.Error())
		}

		return b.okJSON(result)
	}, false)

	// __flop_get(tableName, key) -> jsonResult | "null"
	b.vm.RegisterFunc("__flop_get", func(tableName, key string) string {
		table := b.db.GetTable(tableName)
		if table == nil {
			return "null"
		}

		result, err := table.Get(key)
		if err != nil || result == nil {
			return "null"
		}

		return b.okJSON(result)
	}, false)

	// __flop_update(tableName, key, jsonData) -> jsonResult | "null"
	b.vm.RegisterFunc("__flop_update", func(tableName, key, dataJSON string) string {
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(dataJSON), &data); err != nil {
			return b.errJSON(fmt.Sprintf("invalid JSON: %s", err))
		}

		table := b.db.GetTable(tableName)
		if table == nil {
			return "null"
		}

		result, err := table.Update(key, data, b.txBuf)
		if err != nil {
			return b.errJSON(err.Error())
		}
		if result == nil {
			return "null"
		}

		return b.okJSON(result)
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
  return new Proxy({}, {
    get: function(_, tableName) {
      return {
        insert: function(data) {
          var result = __flop_insert(tableName, JSON.stringify(data));
          return JSON.parse(result);
        },
        get: function(key) {
          var result = __flop_get(tableName, String(key));
          if (result === null || result === 'null') return null;
          return JSON.parse(result);
        },
        update: function(key, data) {
          var result = __flop_update(tableName, String(key), JSON.stringify(data));
          if (result === null || result === 'null') return null;
          return JSON.parse(result);
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
globalThis.__flop_call_handler = function(type, name, paramsJSON, authJSON) {
  var params = JSON.parse(paramsJSON);
  var auth = (authJSON && authJSON !== 'null') ? JSON.parse(authJSON) : null;

  var exports = globalThis.__FLOP_EXPORTS__;
  if (!exports) throw new Error('Module exports not available');
  var endpoint = exports[name];
  if (!endpoint || !endpoint._handler) {
    throw new Error(type + ' not found: ' + name);
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
		var a = globalThis.__flop_call_args;
		var r = globalThis.__flop_call_handler(a[0], a[1], a[2], a[3]);
		if (r && typeof r === 'object' && typeof r.then === 'function') {
			globalThis.__flop_pending = r;
			return false;
		}
		globalThis.__flop_last_result = JSON.stringify(r);
		return true;
	})()`
	callBC, err := b.vm.CompileGlobal(callScript)
	if err != nil {
		return fmt.Errorf("pre-compile call script: %w", err)
	}
	b.callBC = callBC

	// 2. ASYNC fallback (module) — for handlers that still return Promises.
	asyncScript := `globalThis.__flop_last_result = JSON.stringify(await globalThis.__flop_pending);`
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

func (b *Bridge) okJSON(v interface{}) string {
	data, _ := json.Marshal(v)
	return string(data)
}

func (b *Bridge) errJSON(msg string) string {
	data, _ := json.Marshal(map[string]string{"error": msg})
	return string(data)
}
