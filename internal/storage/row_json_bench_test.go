package storage

import (
	"github.com/marcisbee/flop/internal/schema"
	"testing"
)

// Exercises real binary row storage with an embedded JSON field.
func BenchmarkJSONRowCodec(b *testing.B) {
	cs := schema.NewCompiledSchema([]schema.CompiledField{{Name: "id", Kind: schema.KindString}, {Name: "meta", Kind: schema.KindJson}})
	row := map[string]any{"id": "row-1", "meta": map[string]any{"editor": "user-42", "count": 12, "public": true, "tags": []string{"flop", "json", "example"}}}
	raw, err := SerializeRow(row, cs, 1)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("Encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			got, err := SerializeRow(row, cs, 1)
			if err != nil {
				b.Fatal(err)
			}
			if len(got) == 0 {
				b.Fatal("empty row")
			}
		}
	})
	b.Run("Decode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			got, _, _, err := DeserializeRow(raw, 0, cs)
			if err != nil {
				b.Fatal(err)
			}
			if got["id"] != "row-1" || got["meta"].(map[string]any)["editor"] != "user-42" {
				b.Fatal("record changed")
			}
		}
	})
}
