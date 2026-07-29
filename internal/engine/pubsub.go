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
	listeners       map[string]map[*ChangeListener]uint64
	globalListeners map[*ChangeListener]uint64
	events          chan queuedChangeEvent
	stop            chan struct{}
	closeOnce       sync.Once
	closed          atomic.Bool
	sequence        atomic.Uint64
	droppedEvents   atomic.Uint64
}

type queuedChangeEvent struct {
	sequence uint64
	event    ChangeEvent
}

const pubSubEventQueueSize = 16384

func NewPubSub() *PubSub {
	ps := &PubSub{
		listeners:       make(map[string]map[*ChangeListener]uint64),
		globalListeners: make(map[*ChangeListener]uint64),
		events:          make(chan queuedChangeEvent, pubSubEventQueueSize),
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
	since := ps.sequence.Load()
	for _, table := range tables {
		if ps.listeners[table] == nil {
			ps.listeners[table] = make(map[*ChangeListener]uint64)
		}
		ps.listeners[table][cb] = since
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
	ps.globalListeners[cb] = ps.sequence.Load()

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
	queued := queuedChangeEvent{
		sequence: ps.sequence.Add(1),
		event:    event,
	}
	select {
	case ps.events <- queued:
	default:
		// Drop instead of blocking write paths.
		ps.droppedEvents.Add(1)
	}
}

func (ps *PubSub) dispatchLoop() {
	for {
		select {
		case queued := <-ps.events:
			ps.dispatch(queued)
		case <-ps.stop:
			return
		}
	}
}

func (ps *PubSub) dispatch(queued queuedChangeEvent) {
	ps.mu.RLock()
	// Snapshot listeners to avoid holding lock during callbacks
	var tableCallbacks []ChangeListener
	if set, ok := ps.listeners[queued.event.Table]; ok {
		tableCallbacks = make([]ChangeListener, 0, len(set))
		for cb, since := range set {
			if queued.sequence > since {
				tableCallbacks = append(tableCallbacks, *cb)
			}
		}
	}
	globalCallbacks := make([]ChangeListener, 0, len(ps.globalListeners))
	for cb, since := range ps.globalListeners {
		if queued.sequence > since {
			globalCallbacks = append(globalCallbacks, *cb)
		}
	}
	ps.mu.RUnlock()

	for _, cb := range tableCallbacks {
		func() {
			defer func() { recover() }()
			cb(queued.event)
		}()
	}
	for _, cb := range globalCallbacks {
		func() {
			defer func() { recover() }()
			cb(queued.event)
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
