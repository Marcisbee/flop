package storage

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/marcisbee/flop/internal/schema"
)

// Fixed wire fixtures guard compatibility with already-persisted arrays and
// the positional decoder used by schema migration, including value ownership.
func TestRowArrayWireCompatibility(t *testing.T) {
	for _, kind := range []schema.FieldKind{schema.KindRoles, schema.KindRefMulti, schema.KindSet} {
		t.Run(string(kind), func(t *testing.T) {
			cs := schema.NewCompiledSchema([]schema.CompiledField{{Name: "items", Kind: kind}})
			for _, tc := range []struct {
				name  string
				input interface{}
				wire  string
				want  interface{}
			}{
				{"null", nil, "07000100", nil},
				{"empty", []interface{}{}, "070001040000", []interface{}{}},
				{"typed-nil", []string(nil), "070001040000", []interface{}{}},
				{"unsupported", 42, "070001040000", []interface{}{}},
				{"strings", []string{"a", "é"}, "0700010402000100610200c3a9", []interface{}{"a", "é"}},
				{"interfaces", []interface{}{"a", "é"}, "0700010402000100610200c3a9", []interface{}{"a", "é"}},
				{"coercions", []interface{}{float64(12), true, nil}, "0700010403000200313204007472756504006e756c6c", []interface{}{"12", "true", "null"}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					wire, err := hex.DecodeString(tc.wire)
					if err != nil {
						t.Fatal(err)
					}
					if got := SerializeRow(map[string]interface{}{"items": tc.input}, cs, 7); !bytes.Equal(got, wire) {
						t.Fatalf("wire: %x, want %x", got, wire)
					}
					raw := append([]byte{99}, wire...)
					row, version, n, err := DeserializeRow(raw, 1, cs)
					if err != nil || version != 7 || n != len(wire) {
						t.Fatalf("row decode: version=%d bytes=%d err=%v", version, n, err)
					}
					values, version, n, err := DeserializeRawFields(raw, 1)
					if err != nil || version != 7 || n != len(wire) {
						t.Fatalf("migration decode: version=%d bytes=%d err=%v", version, n, err)
					}
					clear(raw)
					if !reflect.DeepEqual(row["items"], tc.want) {
						t.Fatalf("row: %#v, want %#v", row["items"], tc.want)
					}
					if !reflect.DeepEqual(values, []interface{}{tc.want}) {
						t.Fatalf("migration: %#v", values)
					}
					for cut := 0; cut < len(wire); cut++ {
						if _, _, _, err := DeserializeRow(wire[:cut], 0, cs); err == nil {
							t.Fatalf("row accepted truncation at %d", cut)
						}
						if _, _, _, err := DeserializeRawFields(wire[:cut], 0); err == nil {
							t.Fatalf("migration accepted truncation at %d", cut)
						}
					}
				})
			}
		})
	}
}
