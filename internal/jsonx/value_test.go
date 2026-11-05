package jsonx

import (
	"bytes"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"math"
	"testing"
	"time"
)

type customObject map[string]any

func (customObject) MarshalJSON() ([]byte, error) { return []byte(`{"custom":true}`), nil }

type customStream struct{}

func (customStream) MarshalJSONTo(e *jsontext.Encoder) error {
	return e.WriteValue(jsontext.Value(`{"stream":true}`))
}

type customString string

func (*customString) MarshalJSON() ([]byte, error) { return []byte(`"custom-string"`), nil }

type failedValue struct{}

var errCustom = errors.New("custom marshal failure")

func (failedValue) MarshalJSON() ([]byte, error) { return nil, errCustom }

// The standard v2 implementation is the independent oracle for the optimized
// value codec. Canonicalization permits equivalent JSON representations.
func TestPlainContainersPreserveV2Values(t *testing.T) {
	deep := any("bottom")
	for i := 0; i < 80; i++ {
		deep = map[string]any{"next": deep}
	}
	cases := map[string]any{
		"nil-map": map[string]any(nil), "empty-map": map[string]any{},
		"nil-array": []any(nil), "empty-array": []any{},
		"scalars": map[string]any{"nil": nil, "bool": true, "string": "é雪\x00\n\"\\</script>", "int32": int32(-42), "negative": int64(-9223372036854775808), "unsigned": uint64(18446744073709551615), "float": 1.2345678901234567, "negative-zero": math.Copysign(0, -1), "small-float": float32(0.1)},
		"nested":  map[string]any{"array": []any{nil, true, "x", map[string]any{"a": []string{"é", "\t"}}}, "nil-strings": []string(nil), "bytes": []byte{0, 255}},
		"custom":  map[string]any{"named-map": customObject{"ignored": 42}, "pointer-receiver": customString("original"), "stream": customStream{}, "raw": RawMessage(`{"valid":[1,false]}`), "time": time.Date(2026, 11, 5, 1, 2, 3, 0, time.UTC), "number": Number("12345678901234567890")},
		"deep":    deep, "deep-in-mixed-object": map[string]any{"id": "row", "deep": deep},
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			object, ok := value.(map[string]any)
			if !ok {
				object = map[string]any{"value": value}
			}
			want, err := json.Marshal(object)
			if err != nil {
				t.Fatal(err)
			}
			w := jsontext.Value(want)
			if err := w.Canonicalize(); err != nil {
				t.Fatal(err)
			}
			data, err := MarshalObject(object)
			if err != nil {
				t.Fatal(err)
			}
			canonical := jsontext.Value(data)
			if err := canonical.Canonicalize(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(canonical, w) {
				t.Fatalf("got %s; want %s", canonical, w)
			}
		})
	}
	// Canonicalization uses IEEE-754 numbers; protect exact integer precision,
	// negative zero, and float32 formatting independently.
	for _, tc := range []struct {
		value any
		want  string
	}{
		{uint64(18446744073709551615), `{"n":18446744073709551615}`},
		{int64(-9223372036854775808), `{"n":-9223372036854775808}`},
		{math.Copysign(0, -1), `{"n":-0}`},
		{float32(0.1), `{"n":0.1}`},
	} {
		got, err := MarshalObject(map[string]any{"n": tc.value})
		if err != nil || string(got) != tc.want {
			t.Fatalf("numeric representation: %s, %v; want %s", got, err, tc.want)
		}
	}

}

func TestPlainContainersRetainV2Rejections(t *testing.T) {
	cycle := map[string]any{}
	cycle["self"] = cycle
	cycle["id"] = "cycle"
	arrayCycle := []any{nil}
	arrayCycle[0] = arrayCycle
	for name, value := range map[string]any{
		"invalid-value": map[string]any{"x": string([]byte{255})},
		"invalid-name":  map[string]any{string([]byte{255}): 1},
		"duplicate-raw": map[string]any{"x": RawMessage(`{"a":1,"a":2}`)},
		"nan":           map[string]any{"x": math.NaN()}, "infinity": []any{math.Inf(1)},
		"cycle": cycle, "array-cycle": arrayCycle, "unsupported": map[string]any{"x": make(chan int)},
		"custom-error": map[string]any{"x": failedValue{}},
	} {
		t.Run(name, func(t *testing.T) {
			object, ok := value.(map[string]any)
			if !ok {
				object = map[string]any{"value": value}
			}
			if _, err := json.Marshal(object); err == nil {
				t.Fatal("invalid reference fixture")
			}
			_, err := MarshalObject(object)
			if err == nil {
				t.Fatal("invalid value accepted")
			}
			if name == "custom-error" && !errors.Is(err, errCustom) {
				t.Fatalf("lost custom error: %v", err)
			}
		})
	}
}

// Compare arbitrary decoded JSON objects with the standard codec. This checks
// value preservation across nested containers beyond the hand-picked fixtures.
func FuzzMarshalObjectMatchesV2(f *testing.F) {
	for _, seed := range []string{`{}`, `{"n":-0,"a":[1,true,null,"é"]}`, `{"nested":{"x":[{},[],1.25]},"large":1e100}`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var object map[string]any
		if err := json.Unmarshal(data, &object); err != nil {
			return
		}
		want, wantErr := json.Marshal(object)
		got, gotErr := MarshalObject(object)
		if (wantErr == nil) != (gotErr == nil) {
			t.Fatalf("error mismatch: standard=%v optimized=%v", wantErr, gotErr)
		}
		if wantErr != nil {
			return
		}
		a, b := jsontext.Value(want), jsontext.Value(got)
		if err := a.Canonicalize(); err != nil {
			t.Fatal(err)
		}
		if err := b.Canonicalize(); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(a, b) {
			t.Fatalf("standard=%s optimized=%s", a, b)
		}
	})
}
