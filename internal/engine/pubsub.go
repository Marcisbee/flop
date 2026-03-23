package engine

import (
	"sync"
	"sync/atomic"
)

// ChangeEvent represents a table mutation event.
type ChangeEvent struct {
	Table string                 `json:"table"`
	Op    string                 `json:"op"` // "insert", "update", "delete"
	RowID string                 `json:"rowId"`
	Data  map[string]interface{} `json:"data,omitempty"`
}

// ChangeListener is a callback for table change events.
type ChangeListener func(event ChangeEvent)

// PubSub provides in-process pub/sub for table change events.
type PubSub struct {
	mu              sync.RWMutex
	listeners       map[string]map[*ChangeListener]struct{}
	globalListeners map[*ChangeListener]struct{}
	events          chan ChangeEvent
	stop            chan struct{}
	closeOnce       sync.Once
	closed          atomic.Bool
	droppedEvents   atomic.Uint64
}

const pubSubEventQueueSize = 16384

func NewPubSub() *PubSub {
	ps := &PubSub{
		listeners:       make(map[string]map[*ChangeListener]struct{}),
		globalListeners: make(map[*ChangeListener]struct{}),
		events:          make(chan ChangeEvent, pubSubEventQueueSize),
		stop:            make(chan struct{}),
	}
	go ps.dispatchLoop()
	return ps
}

// Subscribe registers a listener for specific tables. Returns an unsubscribe function.
func (ps *PubSub) Subscribe(tables []string, callback ChangeListener) func() {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	cb := &callback
	for _, table := range tables {
		if ps.listeners[table] == nil {
			ps.listeners[table] = make(map[*ChangeListener]struct{})
		}
		ps.listeners[table][cb] = struct{}{}
	}

	return func() {
		ps.mu.Lock()
		defer ps.mu.Unlock()
		for _, table := range tables {
			delete(ps.listeners[table], cb)
		}
	}
}

// SubscribeAll registers a listener for all table events.
func (ps *PubSub) SubscribeAll(callback ChangeListener) func() {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	cb := &callback
	ps.globalListeners[cb] = struct{}{}

	return func() {
		ps.mu.Lock()
		defer ps.mu.Unlock()
		delete(ps.globalListeners, cb)
	}
}

// Publish sends an event to all matching listeners.
func (ps *PubSub) Publish(event ChangeEvent) {
	if ps.closed.Load() {
		return
	}
	select {
	case ps.events <- event:
	default:
		// Drop instead of blocking write paths.
		ps.droppedEvents.Add(1)
	}
}

func (ps *PubSub) dispatchLoop() {
	for {
		select {
		case event := <-ps.events:
			ps.dispatch(event)
		case <-ps.stop:
			return
		}
	}
}

func (ps *PubSub) dispatch(event ChangeEvent) {
	ps.mu.RLock()
	// Snapshot listeners to avoid holding lock during callbacks
	var tableCallbacks []ChangeListener
	if set, ok := ps.listeners[event.Table]; ok {
		tableCallbacks = make([]ChangeListener, 0, len(set))
		for cb := range set {
			tableCallbacks = append(tableCallbacks, *cb)
		}
	}
	globalCallbacks := make([]ChangeListener, 0, len(ps.globalListeners))
	for cb := range ps.globalListeners {
		globalCallbacks = append(globalCallbacks, *cb)
	}
	ps.mu.RUnlock()

	for _, cb := range tableCallbacks {
		func() {
			defer func() { recover() }()
			cb(event)
		}()
	}
	for _, cb := range globalCallbacks {
		func() {
			defer func() { recover() }()
			cb(event)
		}()
	}
}

// Close stops the dispatcher. Publish becomes a no-op afterwards.
func (ps *PubSub) Close() {
	ps.closeOnce.Do(func() {
		ps.closed.Store(true)
		close(ps.stop)
	})
}

// DroppedEvents returns how many events were dropped due to backpressure.
func (ps *PubSub) DroppedEvents() uint64 {
	return ps.droppedEvents.Load()
}

// ListenerCount returns the number of listeners for a specific table (or all if empty).
func (ps *PubSub) ListenerCount(table string) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if table != "" {
		return len(ps.listeners[table])
	}
	total := len(ps.globalListeners)
	for _, set := range ps.listeners {
		total += len(set)
	}
	return total
}
