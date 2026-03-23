package reqtrace

import (
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Span is one operation in a request-local DB trace timeline.
type Span struct {
	Op       string `json:"op"`
	Table    string `json:"table,omitempty"`
	Index    string `json:"index,omitempty"`
	Rows     int    `json:"rows,omitempty"`
	Scanned  int    `json:"scanned,omitempty"`
	MS       int64  `json:"ms"`
	Note     string `json:"note,omitempty"`
	Started  int64  `json:"started,omitempty"`
	Finished int64  `json:"finished,omitempty"`
}

// Collector stores spans for one request.
type Collector struct {
	mu    sync.Mutex
	spans []Span
}

var (
	activeMu sync.Mutex
	active   = make(map[int64]*Collector)
)

// Start binds a new collector to the current goroutine.
func Start() *Collector {
	c := &Collector{spans: make([]Span, 0, 8)}
	gid := curGID()
	if gid <= 0 {
		return c
	}
	activeMu.Lock()
	active[gid] = c
	activeMu.Unlock()
	return c
}

// End unbinds this collector from the current goroutine.
func (c *Collector) End() {
	gid := curGID()
	if gid <= 0 {
		return
	}
	activeMu.Lock()
	if active[gid] == c {
		delete(active, gid)
	}
	activeMu.Unlock()
}

// Current returns the collector bound to the current goroutine, if any.
func Current() *Collector {
	gid := curGID()
	if gid <= 0 {
		return nil
	}
	activeMu.Lock()
	c := active[gid]
	activeMu.Unlock()
	return c
}

// Add appends a span to the active collector for this goroutine.
func Add(span Span) {
	c := Current()
	if c == nil {
		return
	}
	c.mu.Lock()
	c.spans = append(c.spans, span)
	c.mu.Unlock()
}

// AddDuration appends a span with duration metadata from a start timestamp.
func AddDuration(op, table, index string, rows, scanned int, note string, started time.Time) {
	ms := time.Since(started).Milliseconds()
	span := Span{
		Op:       op,
		Table:    strings.TrimSpace(table),
		Index:    strings.TrimSpace(index),
		Rows:     rows,
		Scanned:  scanned,
		MS:       ms,
		Note:     strings.TrimSpace(note),
		Started:  started.UnixMilli(),
		Finished: time.Now().UnixMilli(),
	}
	Add(span)
}

// Spans returns a copy of collected spans.
func (c *Collector) Spans() []map[string]interface{} {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.spans) == 0 {
		return nil
	}
	out := make([]map[string]interface{}, 0, len(c.spans))
	for _, s := range c.spans {
		entry := map[string]interface{}{
			"op": s.Op,
			"ms": s.MS,
		}
		if s.Table != "" {
			entry["table"] = s.Table
		}
		if s.Index != "" {
			entry["index"] = s.Index
		}
		if s.Rows != 0 {
			entry["rows"] = s.Rows
		}
		if s.Scanned != 0 {
			entry["scanned"] = s.Scanned
		}
		if s.Note != "" {
			entry["note"] = s.Note
		}
		if s.Started != 0 {
			entry["started"] = s.Started
		}
		if s.Finished != 0 {
			entry["finished"] = s.Finished
		}
		out = append(out, entry)
	}
	return out
}

func curGID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	if n <= 0 {
		return 0
	}
	line := string(buf[:n])
	const prefix = "goroutine "
	if !strings.HasPrefix(line, prefix) {
		return 0
	}
	rest := line[len(prefix):]
	space := strings.IndexByte(rest, ' ')
	if space <= 0 {
		return 0
	}
	id, err := strconv.ParseInt(rest[:space], 10, 64)
	if err != nil {
		return 0
	}
	return id
}
