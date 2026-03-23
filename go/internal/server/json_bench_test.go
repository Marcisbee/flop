package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/marcisbee/flop/internal/engine"
	"github.com/marcisbee/flop/internal/jsonstd"
	"github.com/marcisbee/flop/internal/jsonx"
)

var benchSinkBytes int

func benchJSONRoundTripHandler(response any) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := jsonx.NewDecoder(r.Body).Decode(&body); err != nil {
			jsonError(w, "invalid json", http.StatusBadRequest)
			return
		}
		jsonResponse(w, response)
	})
}

func smallRequestBody() []byte {
	return []byte(`{"email":"user@example.com","password":"secret","name":"User"}`)
}

func mediumRequestBody() []byte {
	type item struct {
		ID      string   `json:"id"`
		Title   string   `json:"title"`
		Tags    []string `json:"tags"`
		Score   float64  `json:"score"`
		Enabled bool     `json:"enabled"`
	}
	payload := struct {
		UserID string `json:"userId"`
		Items  []item `json:"items"`
	}{
		UserID: "u_123",
		Items:  make([]item, 64),
	}
	for i := 0; i < len(payload.Items); i++ {
		payload.Items[i] = item{
			ID:      "id_" + string(rune('a'+(i%26))) + "_" + string(rune('0'+(i%10))),
			Title:   "Benchmark JSON payload",
			Tags:    []string{"flop", "json", "benchmark", "server"},
			Score:   float64(i) * 1.25,
			Enabled: i%2 == 0,
		}
	}
	b, _ := jsonx.Marshal(payload)
	return b
}

func mediumResponseBody() map[string]any {
	rows := make([]map[string]any, 0, 64)
	for i := 0; i < 64; i++ {
		rows = append(rows, map[string]any{
			"id":      i,
			"name":    "row",
			"active":  i%2 == 0,
			"score":   float64(i) * 2.5,
			"created": 1700000000000 + i,
		})
	}
	return map[string]any{
		"ok":   true,
		"rows": rows,
	}
}

func BenchmarkHTTPJSONRoundTripSmall(b *testing.B) {
	b.ReportAllocs()
	h := benchJSONRoundTripHandler(map[string]any{"ok": true, "token": "abc123"})
	body := smallRequestBody()
	b.SetBytes(int64(len(body)))

	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/_/api/login", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		benchSinkBytes += rec.Body.Len()
	}
}

func BenchmarkHTTPJSONRoundTripMedium(b *testing.B) {
	b.ReportAllocs()
	h := benchJSONRoundTripHandler(mediumResponseBody())
	body := mediumRequestBody()
	b.SetBytes(int64(len(body)))

	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/_/api/query", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		benchSinkBytes += rec.Body.Len()
	}
}

func BenchmarkSSEChangeBatchMarshal(b *testing.B) {
	b.ReportAllocs()
	event := map[string]any{
		"table": "messages",
		"type":  "insert",
		"id":    "m_123",
		"row": map[string]any{
			"id":        "m_123",
			"text":      "hello world",
			"authorId":  "u_1",
			"createdAt": 1700000000000,
		},
	}

	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		for j := 0; j < 128; j++ {
			data, _ := jsonstd.Marshal(event)
			sseEventBytes(rec, "change", data)
		}
		benchSinkBytes += rec.Body.Len()
	}
}

func BenchmarkSSEChangeBatchMarshalOnly(b *testing.B) {
	b.ReportAllocs()
	event := map[string]any{
		"table": "messages",
		"type":  "insert",
		"id":    "m_123",
		"row": map[string]any{
			"id":        "m_123",
			"text":      "hello world",
			"authorId":  "u_1",
			"createdAt": 1700000000000,
		},
	}

	for i := 0; i < b.N; i++ {
		total := 0
		for j := 0; j < 128; j++ {
			data, _ := jsonstd.Marshal(event)
			total += len(data)
		}
		benchSinkBytes += total
	}
}

func BenchmarkSSEChangeBatchWriteOnly(b *testing.B) {
	b.ReportAllocs()
	payload := `{"table":"messages","type":"insert","id":"m_123","row":{"id":"m_123","text":"hello world","authorId":"u_1","createdAt":1700000000000}}`

	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		for j := 0; j < 128; j++ {
			sseEventString(rec, "change", payload)
		}
		benchSinkBytes += rec.Body.Len()
	}
}

func realisticChangeEvent() engine.ChangeEvent {
	return engine.ChangeEvent{
		Table: "messages",
		Op:    "update",
		RowID: "m_123",
		Data: map[string]any{
			"id":        "m_123",
			"text":      "This is a realistic event payload for flop SSE benchmark",
			"authorId":  "u_42",
			"createdAt": 1700000000000,
			"updatedAt": 1700000005000,
			"likes":     128,
			"tags":      []any{"flop", "benchmark", "sse", "json"},
			"meta": map[string]any{
				"edited":     true,
				"editorId":   "u_admin",
				"visibility": "public",
			},
		},
	}
}

func BenchmarkSSEEngineChangeEventMarshalOnly(b *testing.B) {
	b.ReportAllocs()
	event := realisticChangeEvent()

	for i := 0; i < b.N; i++ {
		total := 0
		for j := 0; j < 128; j++ {
			data, _ := jsonstd.Marshal(event)
			total += len(data)
		}
		benchSinkBytes += total
	}
}

func BenchmarkSSEEngineChangeEventMarshal(b *testing.B) {
	b.ReportAllocs()
	event := realisticChangeEvent()

	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		for j := 0; j < 128; j++ {
			data, _ := jsonstd.Marshal(event)
			sseEventBytes(rec, "change", data)
		}
		benchSinkBytes += rec.Body.Len()
	}
}
