package reqtrace

import (
	"fmt"
	"testing"
	"time"
)

func TestSnapshotBoundsAndCopiesSpans(t *testing.T) {
	c := Start()
	defer c.End()
	for i := 0; i < 200; i++ {
		Add(Span{Op: fmt.Sprintf("op-%d", i), Rows: i})
	}

	spans, dropped := c.Snapshot(128)
	if len(spans) != 128 || dropped != 72 {
		t.Fatalf("snapshot len=%d dropped=%d, want 128 and 72", len(spans), dropped)
	}
	spans[0].Op = "changed"
	again, _ := c.Snapshot(1)
	if len(again) != 1 || again[0].Op != "op-0" {
		t.Fatalf("snapshot exposed collector storage: %+v", again)
	}
}

// Collector replacement and repeated End calls must not detach a newer trace.
func TestCollectorLifecycle(t *testing.T) {
	if Current() != nil {
		t.Fatal("unexpected active collector")
	}
	old := Start()
	current := Start()
	defer current.End()
	old.End()
	old.End()
	if Current() != current {
		t.Fatal("ending replaced collector detached current trace")
	}
	started := time.Now().Add(-time.Millisecond)
	AddDuration("lookup", " users ", " email ", 2, 3, " hit ", started)
	current.End()
	current.End()
	if Current() != nil {
		t.Fatal("collector still bound after End")
	}
	AddDuration("ignored", "", "", 0, 0, "", started)
	spans, _ := current.Snapshot(0)
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want one", len(spans))
	}
	s := spans[0]
	if s.Op != "lookup" || s.Table != "users" || s.Index != "email" || s.Note != "hit" || s.Rows != 2 || s.Scanned != 3 || s.Started != started.UnixMilli() || s.Finished < s.Started || s.MS < 0 {
		t.Fatalf("duration metadata changed: %+v", s)
	}
	if spans, _ := old.Snapshot(0); len(spans) != 0 {
		t.Fatalf("replaced collector received spans: %+v", spans)
	}
}

// An untraced goroutine must not inherit another request's collector, and ending
// one request must not disable collection in a different request still running.
func TestConcurrentCollectorsRemainIsolated(t *testing.T) {
	ready, stop, ended := make(chan *Collector), make(chan struct{}), make(chan struct{})
	go func() {
		c := Start()
		AddDuration("other", "", "", 1, 1, "", time.Now())
		ready <- c
		<-stop
		c.End()
		close(ended)
	}()
	other := <-ready
	if Current() != nil {
		close(stop)
		<-ended
		t.Fatal("inherited another goroutine's collector")
	}
	AddDuration("unbound", "", "", 0, 0, "", time.Now())
	current := Start()
	defer current.End()
	close(stop)
	<-ended
	AddDuration("current", "", "", 1, 1, "", time.Now())
	current.End()
	for _, tc := range []struct {
		c  *Collector
		op string
	}{{other, "other"}, {current, "current"}} {
		spans, _ := tc.c.Snapshot(0)
		if len(spans) != 1 || spans[0].Op != tc.op {
			t.Fatalf("%s trace lost or mixed spans: %+v", tc.op, spans)
		}
	}
}
