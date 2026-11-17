package jsonx

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"math"
)

// Plain JSON containers have no user-defined methods. Traversing them directly
// avoids the addressable reflection copies made by v2 for interface map values.
// The jsontext encoder still owns syntax, UTF-8, duplicate-name and depth checks.
type objectValue map[string]any

func (v objectValue) MarshalJSONTo(enc *jsontext.Encoder) error {
	return writeValue(enc, map[string]any(v), 0)
}

// MarshalObject avoids reflection copies for populated plain JSON objects.
// Other Go types are handled directly by Marshal at the caller, so their hot
// paths do not pay for object traversal or additional interface conversions.
func MarshalObject(object map[string]any) ([]byte, error) {
	if len(object) == 0 || (len(object) == 1 && objectEnvelope(object)) {
		return json.Marshal(object)
	}
	return json.Marshal(objectValue(object))
}

func objectEnvelope(object map[string]any) bool {
	for _, value := range object {
		_, nested := value.(map[string]any)
		return nested
	}
	return false
}

func writeValue(enc *jsontext.Encoder, v any, depth int) error {
	// Specialize shallow JSON trees; v2 is already efficient for deep trees
	// and retains responsibility for cyclic-reference detection.
	// No per-request visited map or persistent object cache is needed.
	if depth >= 8 {
		return json.MarshalEncode(enc, v)
	}
	switch v := v.(type) {
	case nil:
		return enc.WriteToken(jsontext.Null)
	case string:
		return enc.WriteToken(jsontext.String(v))
	case bool:
		return enc.WriteToken(jsontext.Bool(v))
	case int:
		return enc.WriteToken(jsontext.Int(int64(v)))
	case int32:
		return enc.WriteToken(jsontext.Int(int64(v)))
	case int64:
		return enc.WriteToken(jsontext.Int(v))
	case uint64:
		return enc.WriteToken(jsontext.Uint(v))
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return json.MarshalEncode(enc, v)
		}
		return enc.WriteToken(jsontext.Float(v))
	case map[string]any:
		if err := enc.WriteToken(jsontext.BeginObject); err != nil {
			return err
		}
		for name, value := range v {
			if err := enc.WriteToken(jsontext.String(name)); err != nil {
				return err
			}
			if err := writeValue(enc, value, depth+1); err != nil {
				return err
			}
		}
		return enc.WriteToken(jsontext.EndObject)
	case []any:
		if err := enc.WriteToken(jsontext.BeginArray); err != nil {
			return err
		}
		for _, value := range v {
			if err := writeValue(enc, value, depth+1); err != nil {
				return err
			}
		}
		return enc.WriteToken(jsontext.EndArray)
	case []string:
		if err := enc.WriteToken(jsontext.BeginArray); err != nil {
			return err
		}
		for _, value := range v {
			if err := enc.WriteToken(jsontext.String(value)); err != nil {
				return err
			}
		}
		return enc.WriteToken(jsontext.EndArray)
	default:
		// Named types, custom marshalers, RawMessage, byte slices, float32 and other
		// Go values retain the standard v2 representation and error behavior.
		return json.MarshalEncode(enc, v)
	}
}
