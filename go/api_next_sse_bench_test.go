package flop

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcisbee/flop/internal/engine"
	internalserver "github.com/marcisbee/flop/internal/server"
)

func BenchmarkAPISSE(b *testing.B) {
	scenarios := []struct {
		name        string
		withPolicy  bool
		subscribers int
		ownerID     string
	}{
		{name: "no_policy/sub_1", withPolicy: false, subscribers: 1, ownerID: "u1"},
		{name: "no_policy/sub_64", withPolicy: false, subscribers: 64, ownerID: "u1"},
		{name: "row_policy_readable/sub_1", withPolicy: true, subscribers: 1, ownerID: "u1"},
		{name: "row_policy_readable/sub_64", withPolicy: true, subscribers: 64, ownerID: "u1"},
		{name: "row_policy_hidden/sub_64", withPolicy: true, subscribers: 64, ownerID: "u2"},
	}

	for _, sc := range scenarios {
		sc := sc
		b.Run(sc.name, func(b *testing.B) {
			benchmarkAPISSEScenario(b, sc.withPolicy, sc.subscribers, sc.ownerID)
		})
	}
}

func benchmarkAPISSEScenario(b *testing.B, withPolicy bool, subscribers int, ownerID string) {
	b.Helper()

	app := New(Config{DataDir: b.TempDir(), SyncMode: "normal"})
	Define(app, "posts", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.String("ownerId").Required().Index()
		s.String("title").Required()
		if withPolicy {
			s.Access(TableAccess{
				Read: func(c *TableReadCtx) bool {
					if c.Auth == nil {
						return false
					}
					return toString(c.Row["ownerId"]) == c.Auth.ID
				},
			})
		}
	})

	db, err := app.Open()
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	secret := "bench-sse-secret"
	db.SetJWTSecret(secret)
	token := internalserver.CreateJWT(&internalserver.JWTPayload{
		Sub:   "u1",
		Roles: []string{"user"},
		Iat:   time.Now().Unix(),
		Exp:   time.Now().Add(time.Hour).Unix(),
	}, secret)

	h := app.APIHandler(db)
	done := make(chan struct{}, subscribers)
	cancels := make([]context.CancelFunc, 0, subscribers)
	writers := make([]*benchSSEWriter, 0, subscribers)

	for i := 0; i < subscribers; i++ {
		rec := newBenchSSEWriter()
		writers = append(writers, rec)

		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		req := httptest.NewRequest(
			http.MethodGet,
			fmt.Sprintf("/api/sse?tables=posts&_token=%s", token),
			nil,
		).WithContext(ctx)

		go func(w *benchSSEWriter, r *http.Request) {
			h.ServeHTTP(w, r)
			done <- struct{}{}
		}(rec, req)
	}

	// Give handlers time to subscribe to pubsub.
	time.Sleep(40 * time.Millisecond)

	change := engine.ChangeEvent{
		Table: "posts",
		Op:    "update",
		Data: map[string]any{
			"ownerId": ownerID,
			"title":   "bench",
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		change.RowID = fmt.Sprintf("r-%d", i)
		db.db.GetPubSub().Publish(change)
	}
	b.StopTimer()

	// Allow subscribers to drain a short tail of events.
	time.Sleep(25 * time.Millisecond)

	for _, cancel := range cancels {
		cancel()
	}
	for i := 0; i < subscribers; i++ {
		<-done
	}

	var delivered int64
	var bytesWritten int64
	for _, w := range writers {
		delivered += w.delivered.Load()
		bytesWritten += w.bytes.Load()
	}

	elapsed := b.Elapsed().Seconds()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "published_ev/s")
		b.ReportMetric(float64(delivered)/elapsed, "delivered_ev/s")
	}
	if subscribers > 0 {
		expected := float64(b.N * subscribers)
		if expected > 0 {
			b.ReportMetric((float64(delivered)/expected)*100, "deliver_%")
		}
		b.ReportMetric(float64(delivered)/float64(subscribers), "delivered/sub")
	}
	b.ReportMetric(float64(bytesWritten), "bytes_out")
}

type benchSSEWriter struct {
	header    http.Header
	status    int
	bytes     atomic.Int64
	delivered atomic.Int64
	mu        sync.Mutex
}

func newBenchSSEWriter() *benchSSEWriter {
	return &benchSSEWriter{
		header: make(http.Header),
		status: http.StatusOK,
	}
}

func (w *benchSSEWriter) Header() http.Header {
	return w.header
}

func (w *benchSSEWriter) WriteHeader(code int) {
	w.status = code
}

func (w *benchSSEWriter) Write(p []byte) (int, error) {
	// Writer can be called from a single handler goroutine, but keep this safe.
	w.mu.Lock()
	defer w.mu.Unlock()
	w.bytes.Add(int64(len(p)))
	if bytes.Contains(p, []byte("event: change\n")) {
		w.delivered.Add(1)
	}
	return len(p), nil
}

func (w *benchSSEWriter) Flush() {}
