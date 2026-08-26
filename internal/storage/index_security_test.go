package storage

import (
	"encoding/binary"
	"testing"

	"github.com/marcisbee/flop/internal/schema"
)

// Regression: crafted index files declaring impossible entry counts must be
// rejected instead of pre-sizing maps from the untrusted count (a 10-byte
// file could force a multi-GB allocation and kill the process at startup or
// during backup restore).
func TestDeserializeIndexRejectsImpossibleEntryCounts(t *testing.T) {
	for _, version := range []uint16{1, 2} {
		data := make([]byte, 10)
		copy(data, schema.IndexFileMagic[:])
		binary.LittleEndian.PutUint16(data[4:6], version)
		binary.LittleEndian.PutUint32(data[6:10], 50_000_000)
		if _, err := DeserializeIndex(data); err == nil {
			t.Fatalf("v%d: 50M-entry count in a 10-byte file accepted", version)
		}
	}
}

// A consistent small file must still parse.
func TestDeserializeIndexAcceptsEmptyFile(t *testing.T) {
	data := make([]byte, 10)
	copy(data, schema.IndexFileMagic[:])
	binary.LittleEndian.PutUint16(data[4:6], idxVersion)
	binary.LittleEndian.PutUint32(data[6:10], 0)
	idx, err := DeserializeIndex(data)
	if err != nil || idx == nil {
		t.Fatalf("empty index: idx=%v err=%v", idx != nil, err)
	}
}
