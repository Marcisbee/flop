package storage

import (
	"fmt"
	"testing"

	"github.com/marcisbee/flop/internal/schema"
)

func BenchmarkArrayRowCodec(b *testing.B) {
	cs := schema.NewCompiledSchema([]schema.CompiledField{{Name: "id", Kind: schema.KindString}, {Name: "refs", Kind: schema.KindRefMulti}})
	refs := make([]interface{}, 64)
	for i := range refs {
		refs[i] = fmt.Sprintf("reference-%03d", i)
	}
	row := map[string]interface{}{"id": "row-1", "refs": refs}
	raw := SerializeRow(row, cs, 1)
	b.Run("Encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if got := SerializeRow(row, cs, 1); len(got) != len(raw) {
				b.Fatal("row length changed")
			}
		}
	})
	b.Run("Decode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, _, _, err := DeserializeRow(raw, 0, cs); err != nil {
				b.Fatal(err)
			}
		}
	})
}
