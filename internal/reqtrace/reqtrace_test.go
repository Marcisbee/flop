package reqtrace

import (
	"fmt"
	"testing"
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
