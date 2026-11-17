package jsonx

import (
	"bytes"
	"testing"
)

func TestHTTPWriterPreservesAbsentRowsAndWireShape(t *testing.T) {
	payload := struct {
		Row   map[string]any   `json:"row"`
		Rows  []map[string]any `json:"rows"`
		Empty map[string]any   `json:"empty"`
		Count int              `json:"count,omitempty"`
		Flag  bool             `json:"flag,omitempty"`
		Text  string           `json:"text"`
		Keys  map[string]int   `json:"keys"`
	}{Empty: map[string]any{}, Text: "<>&\u2028", Keys: map[string]int{"z": 2, "a": 1}}
	var out bytes.Buffer
	if err := MarshalWrite(&out, payload); err != nil {
		t.Fatal(err)
	}
	want := `{"row":null,"rows":null,"empty":{},"text":"\u003c\u003e\u0026\u2028","keys":{"a":1,"z":2}}` + "\n"
	if out.String() != want {
		t.Fatalf("HTTP response contract changed: got %s; want %s", out.String(), want)
	}
}
