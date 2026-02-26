package runtime

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"

	"github.com/marcisbee/flop/internal/engine"
)

// HandlerPool manages a pool of Bridge instances, each with its own QuickJS VM.
// This allows concurrent request handling without serializing through a single VM mutex.
type HandlerPool struct {
	bridges   chan *Bridge
	size      int
	closeOnce sync.Once
}

// NewHandlerPool creates a pool of N VMs, each initialized with the app bundle and host functions.
func NewHandlerPool(db *engine.Database, bundleCode string) (*HandlerPool, error) {
	workers := runtime.GOMAXPROCS(0)
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	// Default to a moderate pool size to avoid scheduler/mutex thrash.
	size := workers * 2
	if size < 8 {
		size = 8
	}
	if size > 64 {
		size = 64
	}

	// Optional override for benchmarks/tuning.
	if env := os.Getenv("FLOP_VM_POOL_SIZE"); env != "" {
		if n, err := strconv.Atoi(env); err == nil && n > 0 {
			size = n
		}
	}

	pool := &HandlerPool{
		bridges: make(chan *Bridge, size),
		size:    size,
	}

	for i := 0; i < size; i++ {
		vm := NewVM()

		// Initialize the bundle (sets globalThis.__FLOP_EXPORTS__)
		if _, err := vm.Eval(bundleCode); err != nil {
			vm.Close()
			pool.Close()
			return nil, fmt.Errorf("init pool VM %d: eval bundle: %w", i, err)
		}

		// Create bridge and register host functions
		bridge := NewBridge(vm, db)
		if err := bridge.RegisterHandlerBridge(); err != nil {
			vm.Close()
			pool.Close()
			return nil, fmt.Errorf("init pool VM %d: register bridge: %w", i, err)
		}

		pool.bridges <- bridge
	}

	return pool, nil
}

// CallHandler acquires a bridge from the pool, calls the handler, and releases back.
func (p *HandlerPool) CallHandler(handlerType, name, paramsJSON, authJSON string) (string, error) {
	bridge := <-p.bridges
	defer func() { p.bridges <- bridge }()
	return bridge.CallHandler(handlerType, name, paramsJSON, authJSON)
}

// Size returns the pool size.
func (p *HandlerPool) Size() int {
	return p.size
}

// Close shuts down all VMs in the pool. Safe to call multiple times.
func (p *HandlerPool) Close() {
	p.closeOnce.Do(func() {
		close(p.bridges)
		for bridge := range p.bridges {
			bridge.vm.Close()
		}
	})
}
