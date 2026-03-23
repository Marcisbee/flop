package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"

	"github.com/marcisbee/flop/internal/schema"
)

// SerializeRow encodes a row into binary format.
// Format: schemaVersion:u16, fieldCount:u8, fields...
func SerializeRow(row map[string]interface{}, cs *schema.CompiledSchema, schemaVersion uint16) []byte {
	// Header: schemaVersion(2) + fieldCount(1)
	buf := make([]byte, 3, 256)
	binary.LittleEndian.PutUint16(buf[0:2], schemaVersion)
	buf[2] = byte(len(cs.Fields))

	for _, field := range cs.Fields {
		val := row[field.Name]
		buf = appendField(buf, val, field.Kind)
	}
	return buf
}

func appendField(buf []byte, value interface{}, kind schema.FieldKind) []byte {
	if value == nil {
		return append(buf, byte(schema.TagNull))
	}

	tag := schema.FieldTypeTag(kind)

	switch kind {
	case schema.KindString, schema.KindBcrypt, schema.KindRef, schema.KindEnum:
		s := toString(value)
		encoded := []byte(s)
		buf = append(buf, byte(tag))
		buf = appendUint32(buf, uint32(len(encoded)))
		buf = append(buf, encoded...)

	case schema.KindNumber, schema.KindTimestamp:
		buf = append(buf, byte(tag))
		buf = appendFloat64(buf, toFloat64(value))

	case schema.KindInteger:
		buf = append(buf, byte(tag))
		buf = appendInt32(buf, toInt32(value))

	case schema.KindBoolean:
		buf = append(buf, byte(tag))
		if toBool(value) {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}

	case schema.KindJson, schema.KindFileSingle, schema.KindFileMulti:
		jsonBytes, _ := json.Marshal(value)
		buf = append(buf, byte(tag))
		buf = appendUint32(buf, uint32(len(jsonBytes)))
		buf = append(buf, jsonBytes...)

	case schema.KindRoles, schema.KindRefMulti, schema.KindSet:
		arr := toStringSlice(value)
		buf = append(buf, byte(tag))
		buf = appendUint16(buf, uint16(len(arr)))
		for _, item := range arr {
			encoded := []byte(item)
			buf = appendUint16(buf, uint16(len(encoded)))
			buf = append(buf, encoded...)
		}

	case schema.KindVector:
		arr := toFloat64Slice(value)
		buf = append(buf, byte(tag))
		buf = appendUint16(buf, uint16(len(arr)))
		for _, v := range arr {
			buf = appendFloat64(buf, v)
		}

	default:
		buf = append(buf, byte(schema.TagNull))
	}

	return buf
}

// DeserializeRow decodes a binary row into a map.
func DeserializeRow(buf []byte, offset int, cs *schema.CompiledSchema) (row map[string]interface{}, schemaVersion uint16, bytesRead int, err error) {
	start := offset
	if offset+3 > len(buf) {
		return nil, 0, 0, ErrShortBuffer
	}

	schemaVersion = binary.LittleEndian.Uint16(buf[offset : offset+2])
	offset += 2
	fieldCount := int(buf[offset])
	offset += 1

	row = make(map[string]interface{}, fieldCount)

	for i := 0; i < fieldCount && i < len(cs.Fields); i++ {
		field := cs.Fields[i]
		if offset >= len(buf) {
			break
		}
		tag := schema.TypeTag(buf[offset])
		offset++

		if tag == schema.TagNull {
			row[field.Name] = nil
			continue
		}

		switch tag {
		case schema.TagString:
			if offset+4 > len(buf) {
				return nil, 0, 0, ErrShortBuffer
			}
			length := binary.LittleEndian.Uint32(buf[offset : offset+4])
			offset += 4
			if offset+int(length) > len(buf) {
				return nil, 0, 0, ErrShortBuffer
			}
			row[field.Name] = string(buf[offset : offset+int(length)])
			offset += int(length)

		case schema.TagNumber:
			if offset+8 > len(buf) {
				return nil, 0, 0, ErrShortBuffer
			}
			bits := binary.LittleEndian.Uint64(buf[offset : offset+8])
			row[field.Name] = math.Float64frombits(bits)
			offset += 8

		case schema.TagInteger:
			if offset+4 > len(buf) {
				return nil, 0, 0, ErrShortBuffer
			}
			row[field.Name] = int32(binary.LittleEndian.Uint32(buf[offset : offset+4]))
			offset += 4

		case schema.TagBoolean:
			if offset >= len(buf) {
				return nil, 0, 0, ErrShortBuffer
			}
			row[field.Name] = buf[offset] == 1
			offset++

		case schema.TagJson, schema.TagFileSingle, schema.TagFileMulti:
			if offset+4 > len(buf) {
				return nil, 0, 0, ErrShortBuffer
			}
			length := binary.LittleEndian.Uint32(buf[offset : offset+4])
			offset += 4
			if length == 0 {
				row[field.Name] = nil
			} else {
				if offset+int(length) > len(buf) {
					return nil, 0, 0, ErrShortBuffer
				}
				var v interface{}
				json.Unmarshal(buf[offset:offset+int(length)], &v)
				row[field.Name] = v
			}
			offset += int(length)

		case schema.TagArray:
			if offset+2 > len(buf) {
				return nil, 0, 0, ErrShortBuffer
			}
			count := binary.LittleEndian.Uint16(buf[offset : offset+2])
			offset += 2
			arr := make([]string, 0, count)
			for j := 0; j < int(count); j++ {
				if offset+2 > len(buf) {
					return nil, 0, 0, ErrShortBuffer
				}
				length := binary.LittleEndian.Uint16(buf[offset : offset+2])
				offset += 2
				if offset+int(length) > len(buf) {
					return nil, 0, 0, ErrShortBuffer
				}
				arr = append(arr, string(buf[offset:offset+int(length)]))
				offset += int(length)
			}
			// Convert to []interface{} for JSON compat
			iArr := make([]interface{}, len(arr))
			for k, v := range arr {
				iArr[k] = v
			}
			row[field.Name] = iArr

		case schema.TagVector:
			if offset+2 > len(buf) {
				return nil, 0, 0, ErrShortBuffer
			}
			count := binary.LittleEndian.Uint16(buf[offset : offset+2])
			offset += 2
			vec := make([]interface{}, 0, count)
			for j := 0; j < int(count); j++ {
				if offset+8 > len(buf) {
					return nil, 0, 0, ErrShortBuffer
				}
				bits := binary.LittleEndian.Uint64(buf[offset : offset+8])
				vec = append(vec, math.Float64frombits(bits))
				offset += 8
			}
			row[field.Name] = vec

		default:
			row[field.Name] = nil
		}
	}

	return row, schemaVersion, offset - start, nil
}

// DeserializeRawFields decodes field values positionally (for migration).
func DeserializeRawFields(buf []byte, offset int) (values []interface{}, schemaVersion uint16, bytesRead int, err error) {
	start := offset
	if offset+3 > len(buf) {
		return nil, 0, 0, ErrShortBuffer
	}

	schemaVersion = binary.LittleEndian.Uint16(buf[offset : offset+2])
	offset += 2
	fieldCount := int(buf[offset])
	offset += 1

	values = make([]interface{}, 0, fieldCount)

	for i := 0; i < fieldCount; i++ {
		if offset >= len(buf) {
			break
		}
		tag := schema.TypeTag(buf[offset])
		offset++

		if tag == schema.TagNull {
			values = append(values, nil)
			continue
		}

		switch tag {
		case schema.TagString:
			length := binary.LittleEndian.Uint32(buf[offset : offset+4])
			offset += 4
			values = append(values, string(buf[offset:offset+int(length)]))
			offset += int(length)

		case schema.TagNumber:
			bits := binary.LittleEndian.Uint64(buf[offset : offset+8])
			values = append(values, math.Float64frombits(bits))
			offset += 8

		case schema.TagInteger:
			values = append(values, int32(binary.LittleEndian.Uint32(buf[offset:offset+4])))
			offset += 4

		case schema.TagBoolean:
			values = append(values, buf[offset] == 1)
			offset++

		case schema.TagJson, schema.TagFileSingle, schema.TagFileMulti:
			length := binary.LittleEndian.Uint32(buf[offset : offset+4])
			offset += 4
			if length == 0 {
				values = append(values, nil)
			} else {
				var v interface{}
				json.Unmarshal(buf[offset:offset+int(length)], &v)
				values = append(values, v)
			}
			offset += int(length)

		case schema.TagArray:
			count := binary.LittleEndian.Uint16(buf[offset : offset+2])
			offset += 2
			arr := make([]string, 0, count)
			for j := 0; j < int(count); j++ {
				length := binary.LittleEndian.Uint16(buf[offset : offset+2])
				offset += 2
				arr = append(arr, string(buf[offset:offset+int(length)]))
				offset += int(length)
			}
			iArr := make([]interface{}, len(arr))
			for k, v := range arr {
				iArr[k] = v
			}
			values = append(values, iArr)

		case schema.TagVector:
			count := binary.LittleEndian.Uint16(buf[offset : offset+2])
			offset += 2
			vec := make([]interface{}, 0, count)
			for j := 0; j < int(count); j++ {
				bits := binary.LittleEndian.Uint64(buf[offset : offset+8])
				vec = append(vec, math.Float64frombits(bits))
				offset += 8
			}
			values = append(values, vec)

		default:
			values = append(values, nil)
		}
	}

	return values, schemaVersion, offset - start, nil
}

// --- helpers ---

func appendUint16(buf []byte, v uint16) []byte {
	b := [2]byte{}
	binary.LittleEndian.PutUint16(b[:], v)
	return append(buf, b[:]...)
}

func appendUint32(buf []byte, v uint32) []byte {
	b := [4]byte{}
	binary.LittleEndian.PutUint32(b[:], v)
	return append(buf, b[:]...)
}

func appendInt32(buf []byte, v int32) []byte {
	b := [4]byte{}
	binary.LittleEndian.PutUint32(b[:], uint32(v))
	return append(buf, b[:]...)
}

func appendFloat64(buf []byte, v float64) []byte {
	b := [8]byte{}
	binary.LittleEndian.PutUint64(b[:], math.Float64bits(v))
	return append(buf, b[:]...)
}

func toString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case json.Number:
		return string(val)
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case json.Number:
		f, _ := val.Float64()
		return f
	default:
		return 0
	}
}

func toInt32(v interface{}) int32 {
	switch val := v.(type) {
	case float64:
		return int32(val)
	case int:
		return int32(val)
	case int32:
		return val
	case int64:
		return int32(val)
	case json.Number:
		n, _ := val.Int64()
		return int32(n)
	default:
		return 0
	}
}

func toBool(v interface{}) bool {
	switch val := v.(type) {
	case bool:
		return val
	case float64:
		return val != 0
	case string:
		return val != "" && val != "false"
	default:
		return false
	}
}

func toStringSlice(v interface{}) []string {
	switch val := v.(type) {
	case []string:
		return val
	case []interface{}:
		result := make([]string, len(val))
		for i, item := range val {
			result[i] = toString(item)
		}
		return result
	default:
		return nil
	}
}

func toFloat64Slice(v interface{}) []float64 {
	switch val := v.(type) {
	case []float64:
		return val
	case []interface{}:
		result := make([]float64, len(val))
		for i, item := range val {
			result[i] = toFloat64(item)
		}
		return result
	default:
		return nil
	}
}

