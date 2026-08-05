package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/marcisbee/flop/internal/jsonx"
	"github.com/marcisbee/flop/internal/reqtrace"
)

func TestAnalyticsTraceIsBoundedBeforeQueueing(t *testing.T) {
	c := reqtrace.Start()
	defer c.End()
	for i := 0; i < requestTraceSpanLimit+50; i++ {
		reqtrace.Add(reqtrace.Span{Op: fmt.Sprintf("lookup-%d", i)})
	}

	event := AnalyticsEvent{
		Timestamp: time.Now(),
		OK:        true,
		Details:   map[string]interface{}{"source": "test"},
	}
	event.CaptureTrace(c)
	if len(event.Trace) != requestTraceSpanLimit || event.TraceDropped != 50 {
		t.Fatalf("queued trace len=%d dropped=%d", len(event.Trace), event.TraceDropped)
	}
	if _, exists := event.Details["trace"]; exists {
		t.Fatal("queued details retained map-based trace data")
	}

	ra := &RequestAnalytics{
		retention: time.Hour,
		metrics:   make(map[string]*requestMinuteBucket),
		lastPrune: time.Now().UnixMilli(),
	}
	ra.apply(event)
	if len(ra.logs) != 1 {
		t.Fatalf("analytics logs=%d, want 1", len(ra.logs))
	}
	var details map[string]interface{}
	if err := jsonx.Unmarshal([]byte(ra.logs[0].Details), &details); err != nil {
		t.Fatalf("decode stored details: %v", err)
	}
	if got := int(toFloatValue(details["traceDroppedSpans"])); got < 50 {
		t.Fatalf("stored dropped spans=%d, want at least 50", got)
	}
}

func TestRequestLogsAreCapacityBounded(t *testing.T) {
	ra := &RequestAnalytics{
		logs: make([]requestLogRecord, requestLogMaxRetained+requestLogTrimBatch+1),
	}
	if !ra.trimLogCapacityLocked(false) {
		t.Fatal("expected over-capacity logs to be trimmed")
	}
	if len(ra.logs) != requestLogMaxRetained {
		t.Fatalf("retained logs=%d, want %d", len(ra.logs), requestLogMaxRetained)
	}
	if got := ra.DroppedEvents(); got != requestLogTrimBatch+1 {
		t.Fatalf("dropped events=%d, want %d", got, requestLogTrimBatch+1)
	}
	if !ra.compact {
		t.Fatal("capacity trim did not schedule disk compaction")
	}
}
