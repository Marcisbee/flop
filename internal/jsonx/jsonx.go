// Package jsonx provides Flop's JSON v2 codec and streaming helpers.
package jsonx

import (
	stdjson "encoding/json"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"io"
)

// Number remains an alias so callers can supply existing json.Number values.
// Serialization and parsing below use v2, not the legacy codec.
type Number = stdjson.Number
type RawMessage = jsontext.Value

func Marshal(v any) ([]byte, error) { return json.Marshal(v) }
func MarshalIndent(v any, prefix, indent string) ([]byte, error) {
	return json.Marshal(v, jsontext.WithIndentPrefix(prefix), jsontext.WithIndent(indent), json.Deterministic(true))
}
func Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// MarshalWrite preserves the HTTP response newline while using v2's pooled
// writer implementation instead of allocating a streaming encoder per request.
func MarshalWrite(w io.Writer, v any) error {
	if err := json.MarshalWrite(w, v); err != nil {
		return err
	}
	_, err := io.WriteString(w, "\n")
	return err
}

// UnmarshalRead consumes exactly one JSON value, rejecting trailing input.
func UnmarshalRead(r io.Reader, v any) error { return json.UnmarshalRead(r, v) }
func UnmarshalStrict(data []byte, v any) error {
	return json.Unmarshal(data, v, json.RejectUnknownMembers(true))
}
