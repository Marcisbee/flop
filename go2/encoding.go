package flop

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"
)

func unixNanoToTime(ns int64) time.Time {
	return time.Unix(0, ns)
}

// Encoding utilities for converting Go values to/from sortable byte keys
// and compact binary values.

// EncodeUint64 encodes a uint64 as big-endian bytes (naturally sortable).
func EncodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

func DecodeUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// EncodeInt64 encodes an int64 as sortable bytes (flip sign bit).
func EncodeInt64(v int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v)^(1<<63))
	return buf
}

func DecodeInt64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b) ^ (1 << 63))
}

// EncodeFloat64 encodes a float64 as sortable bytes.
func EncodeFloat64(v float64) []byte {
	bits := math.Float64bits(v)
	if v >= 0 {
		bits ^= 1 << 63
	} else {
		bits = ^bits
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, bits)
	return buf
}

func DecodeFloat64(b []byte) float64 {
	bits := binary.BigEndian.Uint64(b)
	if bits&(1<<63) != 0 {
		bits ^= 1 << 63
	} else {
		bits = ^bits
	}
	return math.Float64frombits(bits)
}

// EncodeString encodes a string as sortable bytes (null-terminated for prefix sorting).
func EncodeString(s string) []byte {
	b := make([]byte, len(s)+1)
	copy(b, s)
	b[len(s)] = 0
	return b
}

func DecodeString(b []byte) string {
	if len(b) > 0 && b[len(b)-1] == 0 {
		return string(b[:len(b)-1])
	}
	return string(b)
}

// EncodeTime encodes time as sortable bytes (unix nano).
func EncodeTime(t time.Time) []byte {
	return EncodeInt64(t.UnixNano())
}

func DecodeTime(b []byte) time.Time {
	return time.Unix(0, DecodeInt64(b))
}

// EncodeBool encodes a bool.
func EncodeBool(v bool) []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}

func DecodeBool(b []byte) bool {
	return len(b) > 0 && b[0] != 0
}

// EncodeValue encodes an arbitrary value based on field type.
func EncodeValue(v any, ft FieldType) ([]byte, error) {
	if v == nil {
		return []byte{}, nil
	}
	switch ft {
	case FieldString:
		switch s := v.(type) {
		case string:
			return EncodeString(s), nil
		default:
			return EncodeString(fmt.Sprintf("%v", v)), nil
		}
	case FieldInt:
		switch n := v.(type) {
		case int:
			return EncodeInt64(int64(n)), nil
		case int64:
			return EncodeInt64(n), nil
		case uint64:
			return EncodeUint64(n), nil
		case float64:
			return EncodeInt64(int64(n)), nil
		case string:
			parsed, err := strconv.ParseInt(n, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("expected int, got string %q: %w", n, err)
			}
			return EncodeInt64(parsed), nil
		default:
			return nil, fmt.Errorf("expected int, got %T", v)
		}
	case FieldFloat:
		switch n := v.(type) {
		case float64:
			return EncodeFloat64(n), nil
		case float32:
			return EncodeFloat64(float64(n)), nil
		case int:
			return EncodeFloat64(float64(n)), nil
		case int64:
			return EncodeFloat64(float64(n)), nil
		case string:
			parsed, err := strconv.ParseFloat(n, 64)
			if err != nil {
				return nil, fmt.Errorf("expected float, got string %q: %w", n, err)
			}
			return EncodeFloat64(parsed), nil
		default:
			return nil, fmt.Errorf("expected float, got %T", v)
		}
	case FieldBool:
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("expected bool, got %T", v)
		}
		return EncodeBool(b), nil
	case FieldTime:
		t, ok := v.(time.Time)
		if !ok {
			return nil, fmt.Errorf("expected time.Time, got %T", v)
		}
		return EncodeTime(t), nil
	case FieldRef:
		switch n := v.(type) {
		case uint64:
			return EncodeUint64(n), nil
		case int:
			return EncodeUint64(uint64(n)), nil
		case int64:
			return EncodeUint64(uint64(n)), nil
		case float64:
			return EncodeUint64(uint64(n)), nil
		case string:
			if n == "" {
				return []byte{}, nil
			}
			parsed, err := strconv.ParseUint(n, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("expected uint64 ref, got string %q: %w", n, err)
			}
			return EncodeUint64(parsed), nil
		default:
			return nil, fmt.Errorf("expected uint64 ref, got %T", v)
		}
	case FieldJSON:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("json marshal: %w", err)
		}
		return b, nil
	default:
		return fmt.Appendf(nil, "%v", v), nil
	}
}

// DecodeValue decodes bytes back to a Go value based on field type.
func DecodeValue(b []byte, ft FieldType) any {
	if len(b) == 0 {
		return nil
	}
	switch ft {
	case FieldString:
		return DecodeString(b)
	case FieldInt:
		if len(b) == 8 {
			return DecodeInt64(b)
		}
		return nil
	case FieldFloat:
		if len(b) == 8 {
			return DecodeFloat64(b)
		}
		return nil
	case FieldBool:
		return DecodeBool(b)
	case FieldTime:
		if len(b) == 8 {
			return DecodeTime(b)
		}
		return nil
	case FieldRef:
		if len(b) == 8 {
			return DecodeUint64(b)
		}
		return nil
	case FieldJSON:
		var v any
		if err := json.Unmarshal(b, &v); err != nil {
			return string(b) // fallback
		}
		return v
	default:
		return string(b)
	}
}

// RowEncoder encodes/decodes Row objects for storage.
type RowEncoder struct {
	schema     *Schema
	fieldTypes map[string]FieldType // precomputed for O(1) lookup
}

func NewRowEncoder(schema *Schema) *RowEncoder {
	ft := make(map[string]FieldType, len(schema.Fields))
	for _, f := range schema.Fields {
		ft[f.Name] = f.Type
	}
	return &RowEncoder{schema: schema, fieldTypes: ft}
}

// ExtractSortFloat extracts the row ID and a numeric field value from encoded data
// without full decode. Avoids map/string allocations for fast sorting.
func (e *RowEncoder) ExtractSortFloat(data []byte, fieldName string) (id uint64, val float64, ok bool) {
	if len(data) < 34 {
		return 0, 0, false
	}
	id = binary.BigEndian.Uint64(data[0:8])

	if fieldName == "id" {
		return id, float64(id), true
	}

	offset := 32 // skip ID + CreatedAt + UpdatedAt + Version
	numFields := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	fnLen := len(fieldName)
	for i := 0; i < numFields; i++ {
		nameLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2

		// Compare field name bytes directly (no string allocation)
		match := nameLen == fnLen
		if match {
			for j := 0; j < fnLen; j++ {
				if data[offset+j] != fieldName[j] {
					match = false
					break
				}
			}
		}

		offset += nameLen
		dataLen := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4

		if match {
			if dataLen == 8 {
				ft := e.fieldTypes[fieldName]
				switch ft {
				case FieldFloat:
					return id, DecodeFloat64(data[offset : offset+8]), true
				case FieldInt:
					return id, float64(DecodeInt64(data[offset : offset+8])), true
				case FieldRef:
					return id, float64(DecodeUint64(data[offset : offset+8])), true
				}
			}
			v := DecodeValue(data[offset:offset+dataLen], e.fieldTypes[fieldName])
			return id, toFloat(v), true
		}
		offset += dataLen
	}
	return id, 0, false
}

// EncodeRow serializes a Row into bytes for storage as a B+ tree value.
// Format: [8:ID][8:CreatedAt][8:UpdatedAt][8:Version][N fields...]
// Each field: [2:fieldNameLen][fieldName][4:dataLen][data]
func (e *RowEncoder) EncodeRow(row *Row) ([]byte, error) {
	buf := make([]byte, 0, 256)

	// Header
	buf = binary.BigEndian.AppendUint64(buf, row.ID)
	buf = append(buf, EncodeTime(row.CreatedAt)...)
	buf = append(buf, EncodeTime(row.UpdatedAt)...)
	buf = binary.BigEndian.AppendUint64(buf, row.Version)

	// Sort field names for deterministic encoding
	fieldNames := make([]string, 0, len(row.Data))
	for k := range row.Data {
		fieldNames = append(fieldNames, k)
	}
	sort.Strings(fieldNames)

	// Number of fields
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(fieldNames)))

	for _, name := range fieldNames {
		val := row.Data[name]
		ft := e.fieldType(name)

		encoded, err := EncodeValue(val, ft)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", name, err)
		}

		// Field name
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(name)))
		buf = append(buf, name...)

		// Field data
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(encoded)))
		buf = append(buf, encoded...)
	}

	return buf, nil
}

// DecodeRow deserializes bytes back to a Row.
func (e *RowEncoder) DecodeRow(data []byte) (*Row, error) {
	if len(data) < 34 { // minimum: 8+8+8+8+2
		return nil, fmt.Errorf("row data too short: %d bytes", len(data))
	}

	row := &Row{
		Data: make(map[string]any),
	}

	offset := 0
	row.ID = binary.BigEndian.Uint64(data[offset:])
	offset += 8
	row.CreatedAt = DecodeTime(data[offset : offset+8])
	offset += 8
	row.UpdatedAt = DecodeTime(data[offset : offset+8])
	offset += 8
	row.Version = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	numFields := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	for i := 0; i < numFields; i++ {
		nameLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		name := string(data[offset : offset+nameLen])
		offset += nameLen

		dataLen := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		fieldData := data[offset : offset+dataLen]
		offset += dataLen

		ft := e.fieldType(name)
		row.Data[name] = DecodeValue(fieldData, ft)
	}

	return row, nil
}

func (e *RowEncoder) fieldType(name string) FieldType {
	if ft, ok := e.fieldTypes[name]; ok {
		return ft
	}
	return FieldString // default
}
