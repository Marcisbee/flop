package storage

import (
	"testing"

	"github.com/marcisbee/flop/internal/schema"
)

func TestDeserializeRowRejectsInvalidTypeTag(t *testing.T) {
	compiled := schema.NewCompiledSchema([]schema.CompiledField{
		{Name: "id", Kind: schema.KindString},
	})
	raw := []byte{1, 0, 1, byte(schema.TagNumber)}
	if _, _, _, err := DeserializeRow(raw, 0, compiled); err == nil {
		t.Fatal("expected mismatched field type tag to fail")
	}
}

func TestDeserializeRawFieldsRejectsTruncatedValues(t *testing.T) {
	cases := [][]byte{
		{1, 0, 1},
		{1, 0, 1, byte(schema.TagString), 5, 0, 0, 0, 'a'},
		{1, 0, 1, byte(schema.TagNumber), 0},
		{1, 0, 1, byte(schema.TagArray), 1, 0, 3, 0, 'a'},
	}
	for i, raw := range cases {
		if _, _, _, err := DeserializeRawFields(raw, 0); err == nil {
			t.Fatalf("case %d: expected truncated value to fail", i)
		}
	}
}
