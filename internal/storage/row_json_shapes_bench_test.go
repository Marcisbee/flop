package storage

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/marcisbee/flop/internal/schema"
)

func BenchmarkJSONFieldShapes(b *testing.B) {
	type item struct {
		ID      string `json:"id"`
		Enabled bool   `json:"enabled"`
		Count   int    `json:"count"`
	}
	rows := make([]any, 64)
	for i := range rows {
		rows[i] = map[string]any{"id": fmt.Sprint(i), "title": "A JSON row", "enabled": true, "count": i, "tags": []any{"a", "b", "c"}}
	}
	deep := any("leaf")
	for i := 0; i < 80; i++ {
		deep = map[string]any{"next": deep}
	}
	cs := schema.NewCompiledSchema([]schema.CompiledField{{Name: "value", Kind: schema.KindJson}})
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"EmptyObject", map[string]any{}}, {"EmptyArray", []any{}},
		{"Array1", []any{"one"}}, {"Array4", []any{"one", 2, true, nil}}, {"Array8", []any{1, 2, 3, 4, 5, 6, 7, 8}},
		{"Scalars", map[string]any{"id": "row-1", "count": 42, "active": true, "score": 1.5}},
		{"Rows64", map[string]any{"rows": rows}}, {"Array64", rows},
		{"TypedLeaf", map[string]any{"record": item{ID: "one", Enabled: true, Count: 42}}},
		{"RawLeaf", map[string]any{"record": json.RawMessage(`{"id":"one","tags":["a","b"],"count":42}`)}},
		{"Deep", deep}, {"TopStruct", item{ID: "one", Count: 42}},
		{"TopString", "ordinary string"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			row := map[string]any{"value": tc.value}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				data, err := SerializeRow(row, cs, 1)
				if err != nil {
					b.Fatal(err)
				}
				if len(data) == 0 {
					b.Fatal("empty JSON")
				}
			}
		})
	}
}
