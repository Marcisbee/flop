package engine

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/marcisbee/flop/internal/schema"
)

func TestJSONWriteRejectsInvalidValueWithoutChangingStoredRow(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"invalid_utf8", string([]byte{0xff})},
		{"duplicate_raw_member", json.RawMessage(`{"a":1,"a":2}`)},
		{"non_finite_number", math.NaN()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			defs := map[string]*schema.TableDef{"items": {Name: "items", CompiledSchema: schema.NewCompiledSchema([]schema.CompiledField{{Name: "id", Kind: schema.KindString, Required: true}, {Name: "meta", Kind: schema.KindJson}})}}
			db := NewDatabase(DatabaseConfig{DataDir: dir, SyncMode: "full"})
			if err := db.Open(defs); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			table := db.GetTable("items")
			if _, err := table.Insert(map[string]any{"id": "good", "meta": map[string]any{"text": "original"}}, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := table.Insert(map[string]any{"id": "bad", "meta": tc.value}, nil); err == nil {
				t.Fatal("invalid insert accepted")
			}
			if _, err := table.Update("good", map[string]any{"meta": tc.value}, nil); err == nil {
				t.Fatal("invalid update accepted")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = NewDatabase(DatabaseConfig{DataDir: dir, SyncMode: "full"})
			if err := db.Open(defs); err != nil {
				t.Fatal(err)
			}
			table = db.GetTable("items")
			bad, err := table.Get("bad")
			if err != nil || bad != nil {
				t.Fatalf("rejected insert persisted: %v, %v", bad, err)
			}
			good, err := table.Get("good")
			if err != nil {
				t.Fatal(err)
			}
			if good["meta"].(map[string]any)["text"] != "original" {
				t.Fatalf("rejected update changed row: %v", good)
			}
		})
	}
}
