//go:build !sonicjson

package jsonx

import (
	stdjson "encoding/json"
	"io"
)

type RawMessage = stdjson.RawMessage
type Number = stdjson.Number

type Encoder = stdjson.Encoder
type Decoder = stdjson.Decoder

func Marshal(v any) ([]byte, error) {
	return stdjson.Marshal(v)
}

func MarshalIndent(v any, prefix, indent string) ([]byte, error) {
	return stdjson.MarshalIndent(v, prefix, indent)
}

func Unmarshal(data []byte, v any) error {
	return stdjson.Unmarshal(data, v)
}

func NewEncoder(w io.Writer) *Encoder {
	return stdjson.NewEncoder(w)
}

func NewDecoder(r io.Reader) *Decoder {
	return stdjson.NewDecoder(r)
}
