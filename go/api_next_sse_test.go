package flop

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	internalserver "github.com/marcisbee/flop/internal/server"
)

func TestAPISSERespectsRowReadAccess(t *testing.T) {
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})
	Define(app, "posts", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.String("ownerId").Required().Index()
		s.String("title").Required()
		s.Access(TableAccess{
			Read: func(c *TableReadCtx) bool {
				if c.Auth == nil {
					return false
				}
				return toString(c.Row["ownerId"]) == c.Auth.ID
			},
		})
	})

	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	secret := "sse-test-secret"
	db.SetJWTSecret(secret)
	raw := db.Table("posts")
	if raw == nil {
		t.Fatal("posts table missing")
	}

	// Seed before subscription.
	_, _ = raw.Insert(map[string]any{"id": "p1", "ownerId": "u1", "title": "own"})
	_, _ = raw.Insert(map[string]any{"id": "p2", "ownerId": "u2", "title": "foreign"})

	token := internalserver.CreateJWT(&internalserver.JWTPayload{
		Sub:   "u1",
		Roles: []string{"user"},
		Iat:   time.Now().Unix(),
		Exp:   time.Now().Add(time.Hour).Unix(),
	}, secret)

	h := app.APIHandler(db)
	rec := httptest.NewRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/api/sse?tables=posts&_token="+token, nil).WithContext(ctx)

	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()

	// Give handler time to subscribe.
	time.Sleep(30 * time.Millisecond)

	// Readable update -> should emit normal update with row id.
	if _, err := raw.Update("p1", map[string]any{"title": "own-updated"}); err != nil {
		t.Fatalf("update p1: %v", err)
	}
	// Unreadable update -> should emit scrubbed touch only (no row id).
	if _, err := raw.Update("p2", map[string]any{"title": "foreign-updated"}); err != nil {
		t.Fatalf("update p2: %v", err)
	}
	// Unreadable insert -> should not emit.
	if _, err := raw.Insert(map[string]any{"id": "p3", "ownerId": "u2", "title": "foreign-insert"}); err != nil {
		t.Fatalf("insert p3: %v", err)
	}
	// Readable insert -> should emit.
	if _, err := raw.Insert(map[string]any{"id": "p4", "ownerId": "u1", "title": "own-insert"}); err != nil {
		t.Fatalf("insert p4: %v", err)
	}
	// Readable delete -> should emit.
	if ok, err := raw.Delete("p1"); err != nil || !ok {
		t.Fatalf("delete p1: ok=%v err=%v", ok, err)
	}

	if !waitForMinChangeEvents(rec, 4, 2*time.Second) {
		cancel()
		<-done
		t.Fatalf("timed out waiting for SSE events; body=%q", rec.Body.String())
	}
	cancel()
	<-done

	events := parseSSEChanges(rec.Body.String())
	if len(events) != 4 {
		t.Fatalf("expected 4 change events, got %d: %#v", len(events), events)
	}

	if got := events[0]["op"]; got != "update" {
		t.Fatalf("event[0] op: got %v, want update", got)
	}
	if got := events[0]["rowId"]; got != "p1" {
		t.Fatalf("event[0] rowId: got %v, want p1", got)
	}

	if got := events[1]["op"]; got != "touch" {
		t.Fatalf("event[1] op: got %v, want touch", got)
	}
	if got := events[1]["rowId"]; got != "" {
		t.Fatalf("event[1] rowId: got %v, want empty", got)
	}

	if got := events[2]["op"]; got != "insert" {
		t.Fatalf("event[2] op: got %v, want insert", got)
	}
	if got := events[2]["rowId"]; got != "p4" {
		t.Fatalf("event[2] rowId: got %v, want p4", got)
	}

	if got := events[3]["op"]; got != "delete" {
		t.Fatalf("event[3] op: got %v, want delete", got)
	}
	if got := events[3]["rowId"]; got != "p1" {
		t.Fatalf("event[3] rowId: got %v, want p1", got)
	}

	// SSE payload must never leak full row data.
	for i, ev := range events {
		if _, ok := ev["data"]; ok {
			t.Fatalf("event[%d] leaked data payload: %#v", i, ev)
		}
	}
}

func TestAPISSEPublicTableEmitsRawChanges(t *testing.T) {
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})
	Define(app, "logs", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.String("msg").Required()
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	raw := db.Table("logs")
	if raw == nil {
		t.Fatal("logs table missing")
	}

	h := app.APIHandler(db)
	rec := httptest.NewRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/api/sse?tables=logs", nil).WithContext(ctx)

	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)

	if _, err := raw.Insert(map[string]any{"id": "l1", "msg": "hello"}); err != nil {
		t.Fatalf("insert l1: %v", err)
	}
	if !waitForMinChangeEvents(rec, 1, time.Second) {
		cancel()
		<-done
		t.Fatalf("timed out waiting for SSE event; body=%q", rec.Body.String())
	}
	cancel()
	<-done

	events := parseSSEChanges(rec.Body.String())
	if len(events) < 1 {
		t.Fatalf("expected at least 1 event, got %d", len(events))
	}
	last := events[len(events)-1]
	if got := last["op"]; got != "insert" {
		t.Fatalf("last op: got %v, want insert", got)
	}
	if got := last["rowId"]; got != "l1" {
		t.Fatalf("last rowId: got %v, want l1", got)
	}
}

func parseSSEChanges(raw string) []map[string]any {
	var out []map[string]any
	sc := bufio.NewScanner(strings.NewReader(raw))
	currentType := ""
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "event: ") {
			currentType = strings.TrimSpace(strings.TrimPrefix(line, "event: "))
			continue
		}
		if strings.HasPrefix(line, "data: ") && currentType == "change" {
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
			var m map[string]any
			if err := json.Unmarshal([]byte(payload), &m); err == nil {
				out = append(out, m)
			}
		}
		if line == "" {
			currentType = ""
		}
	}
	return out
}

func waitForMinChangeEvents(rec *httptest.ResponseRecorder, min int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(parseSSEChanges(rec.Body.String())) >= min {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return len(parseSSEChanges(rec.Body.String())) >= min
}
