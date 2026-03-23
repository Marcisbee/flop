//go:build sonicjson

package jsonx

import (
	stdjson "encoding/json"
	"io"

	"github.com/bytedance/sonic"
)

type RawMessage = stdjson.RawMessage
type Number = stdjson.Number

type Encoder struct {
	enc interface{ Encode(any) error }
}

type Decoder struct {
	dec interface{ Decode(any) error }
}

func Marshal(v any) ([]byte, error) {
	return sonic.Marshal(v)
}

func MarshalIndent(v any, prefix, indent string) ([]byte, error) {
	return sonic.MarshalIndent(v, prefix, indent)
}

func Unmarshal(data []byte, v any) error {
	return sonic.Unmarshal(data, v)
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{enc: sonic.ConfigDefault.NewEncoder(w)}
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{dec: sonic.ConfigDefault.NewDecoder(r)}
}

func (e *Encoder) Encode(v any) error {
	return e.enc.Encode(v)
}

func (d *Decoder) Decode(v any) error {
	return d.dec.Decode(v)
}
