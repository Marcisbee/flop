package runtime

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"modernc.org/quickjs"
)

// VM wraps a QuickJS runtime for executing user JavaScript.
type VM struct {
	mu     sync.Mutex
	qjs    *quickjs.VM
	closed bool
}

// NewVM creates a new QuickJS VM instance.
func NewVM() *VM {
	qjs, err := quickjs.NewVM()
	if err != nil {
		panic(fmt.Sprintf("failed to create QuickJS VM: %v", err))
	}
	qjs.SetMemoryLimit(256 * 1024 * 1024) // 256MB
	qjs.SetEvalTimeout(30 * time.Second)
	return &VM{qjs: qjs}
}

// Close frees the VM resources.
func (vm *VM) Close() {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	if vm.closed {
		return
	}
	vm.closed = true
	vm.qjs.Close()
}

// Eval evaluates JavaScript code in global scope, returns Go value.
func (vm *VM) Eval(code string) (interface{}, error) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	return vm.qjs.Eval(code, quickjs.EvalGlobal)
}

// EvalModule evaluates JavaScript code as an ES module.
func (vm *VM) EvalModule(code string) (interface{}, error) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	return vm.qjs.Eval(code, quickjs.EvalModule)
}

// EvalValue evaluates JavaScript code and returns a quickjs.Value.
func (vm *VM) EvalValue(code string) (quickjs.Value, error) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	return vm.qjs.EvalValue(code, quickjs.EvalGlobal)
}

// Call calls a global JavaScript function by name.
func (vm *VM) Call(funcName string, args ...interface{}) (interface{}, error) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	return vm.qjs.Call(funcName, args...)
}

// CallJSON calls a JS function with string args and returns the JSON string result.
func (vm *VM) CallJSON(funcName string, args ...string) (string, error) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	// Build the call expression
	argsStr := ""
	for i, arg := range args {
		if i > 0 {
			argsStr += ","
		}
		argJSON, _ := json.Marshal(arg)
		argsStr += string(argJSON)
	}

	expr := fmt.Sprintf("%s(%s)", funcName, argsStr)
	result, err := vm.qjs.Eval(expr, quickjs.EvalGlobal)
	if err != nil {
		return "", fmt.Errorf("call %s: %w", funcName, err)
	}

	if s, ok := result.(string); ok {
		return s, nil
	}
	b, _ := json.Marshal(result)
	return string(b), nil
}

// GetString evaluates an expression and returns its string value.
func (vm *VM) GetString(expr string) (string, error) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	result, err := vm.qjs.Eval(expr, quickjs.EvalGlobal)
	if err != nil {
		return "", err
	}

	if s, ok := result.(string); ok {
		return s, nil
	}
	b, _ := json.Marshal(result)
	return string(b), nil
}

// RegisterFunc registers a Go function as a global JS function.
// The function signature must be typed Go functions — see modernc.org/quickjs docs.
// hasThis: if true, the first parameter receives 'this'.
func (vm *VM) RegisterFunc(name string, fn interface{}, hasThis bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	vm.qjs.RegisterFunc(name, fn, hasThis)
}

// CompileModule compiles JS module code to bytecode for reuse with EvalBytecode.
func (vm *VM) CompileModule(code string) ([]byte, error) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	return vm.qjs.Compile(code, quickjs.EvalModule)
}

// CompileGlobal compiles a global JS script to bytecode for reuse with EvalBytecode.
func (vm *VM) CompileGlobal(code string) ([]byte, error) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	return vm.qjs.Compile(code, quickjs.EvalGlobal)
}

// CallSyncFirst tries the fast sync path (global bytecode, no js_std_await).
// If the handler returns a Promise, falls back to the async path (module bytecode).
func (vm *VM) CallSyncFirst(handlerType, name, paramsJSON, authJSON string, syncBC, asyncBC []byte) (string, error) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	// Set call args via Eval (avoids Call's function-name eval overhead).
	setExpr := `globalThis.__flop_call_args=[` +
		jsonStr(handlerType) + `,` +
		jsonStr(name) + `,` +
		jsonStr(paramsJSON) + `,` +
		jsonStr(authJSON) + `]`
	if _, err := vm.qjs.Eval(setExpr, quickjs.EvalGlobal); err != nil {
		return "", err
	}

	// Execute sync global bytecode. Returns true if sync, false if Promise.
	result, err := vm.qjs.EvalBytecode(syncBC)
	if err != nil {
		return "", err
	}

	if result == false {
		// Handler returned a Promise — fall back to module bytecode with js_std_await.
		if _, err := vm.qjs.EvalBytecode(asyncBC); err != nil {
			return "", err
		}
	}

	// Read result (set by either the sync or async path).
	last, err := vm.qjs.Eval("globalThis.__flop_last_result", quickjs.EvalGlobal)
	if err != nil {
		return "", err
	}

	if s, ok := last.(string); ok {
		return s, nil
	}
	b, _ := json.Marshal(last)
	return string(b), nil
}

// jsonStr wraps a Go string as a JSON string literal for embedding in JS.
func jsonStr(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// QJS returns the underlying quickjs.VM for advanced operations.
func (vm *VM) QJS() *quickjs.VM {
	return vm.qjs
}
