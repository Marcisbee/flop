// Package flop is a precompiled, zero-allocation read-path database engine.
// All queries are defined as Views (reads) or Reducers (writes) at startup.
// Storage uses copy-on-write B+ trees over memory-mapped files.
package flop

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"
)

// FieldType describes column types supported by the engine.
type FieldType uint8

const (
	FieldString FieldType = iota
	FieldInt
	FieldFloat
	FieldBool
	FieldTime
	FieldRef       // foreign key
	FieldAsset     // file stored on disk
	FieldJSON      // arbitrary JSON
	FieldBytes     // raw bytes
	FieldStringArr // string slice
)

// Field describes a single column in a table schema.
type Field struct {
	Name       string
	Type       FieldType
	MaxLen     int
	Required   bool
	Unique     bool
	Indexed    bool   // create non-unique secondary index
	Searchable bool   // enable full-text indexing
	RefTable   string
	SelfRef    bool
	EnumValues []string
}

// Schema describes the shape of a table.
type Schema struct {
	Name             string
	Fields           []Field
	UniqueConstraints [][]string // composite unique constraints
	CascadeDeletes   []string   // table names to cascade delete into
	IsAuth           bool       // has built-in auth fields
}

// Row is a generic database row stored as field-indexed values.
type Row struct {
	ID        uint64
	TableID   uint16
	Data      map[string]any
	CreatedAt time.Time
	UpdatedAt time.Time
	Version   uint64
}

// Asset represents a file stored on disk with metadata in the DB.
type Asset struct {
	Hash        [sha256.Size]byte
	ContentType string
	Size        int64
	Width       int
	Height      int
	Path        string // reconstructed serving path
}

// RowID encodes a table + row identifier.
type RowID [10]byte

func MakeRowID(tableID uint16, id uint64) RowID {
	var rid RowID
	binary.BigEndian.PutUint16(rid[:2], tableID)
	binary.BigEndian.PutUint64(rid[2:], id)
	return rid
}

func (r RowID) TableID() uint16 { return binary.BigEndian.Uint16(r[:2]) }
func (r RowID) ID() uint64     { return binary.BigEndian.Uint64(r[2:]) }
func (r RowID) String() string { return fmt.Sprintf("%d:%d", r.TableID(), r.ID()) }
