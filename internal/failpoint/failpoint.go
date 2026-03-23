package failpoint

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

var cfg = newConfig()

type config struct {
	enabled bool
	names   map[string]struct{}
	hit     int64
	mode    string

	counters sync.Map // map[string]*atomic.Int64
}

func newConfig() *config {
	raw := strings.TrimSpace(os.Getenv("FLOP_FAILPOINT"))
	if raw == "" {
		return &config{}
	}
	names := make(map[string]struct{})
	for _, part := range strings.Split(raw, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		names[name] = struct{}{}
	}
	if len(names) == 0 {
		return &config{}
	}

	hit := int64(1)
	if hv := strings.TrimSpace(os.Getenv("FLOP_FAILPOINT_HIT")); hv != "" {
		var parsed int64
		if _, err := fmt.Sscanf(hv, "%d", &parsed); err == nil && parsed > 0 {
			hit = parsed
		}
	}

	mode := strings.TrimSpace(os.Getenv("FLOP_FAILPOINT_MODE"))
	if mode == "" {
		mode = "exit"
	}
	return &config{
		enabled: true,
		names:   names,
		hit:     hit,
		mode:    mode,
	}
}

// Enabled reports whether failpoints are active for this process.
func Enabled() bool {
	return cfg.enabled
}

// Hit triggers a configured failpoint crash when the requested name matches.
func Hit(name string) {
	if !cfg.enabled {
		return
	}
	if _, ok := cfg.names[name]; !ok {
		if _, any := cfg.names["*"]; !any {
			return
		}
	}

	counter := cfg.counterFor(name)
	if counter.Add(1) != cfg.hit {
		return
	}

	switch cfg.mode {
	case "panic":
		panic("flop failpoint triggered: " + name)
	default:
		// os.Exit intentionally bypasses defers/sync to simulate an abrupt crash.
		os.Exit(197)
	}
}

func (c *config) counterFor(name string) *atomic.Int64 {
	if v, ok := c.counters.Load(name); ok {
		return v.(*atomic.Int64)
	}
	v := &atomic.Int64{}
	actual, _ := c.counters.LoadOrStore(name, v)
	return actual.(*atomic.Int64)
}
