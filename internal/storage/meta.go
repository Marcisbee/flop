package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/util"
)

const metaVersion = 1

// CreateEmptyMeta returns a fresh StoredMeta.
func CreateEmptyMeta() *schema.StoredMeta {
	return &schema.StoredMeta{
		Version: metaVersion,
		Created: time.Now().UTC().Format(time.RFC3339Nano),
		Tables:  make(map[string]*schema.StoredTableMeta),
	}
}

// SerializeMeta encodes StoredMeta to _meta.flop binary format.
func SerializeMeta(meta *schema.StoredMeta) ([]byte, error) {
	payload, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return nil, err
	}

	// FLOP(4) + version(2) + payload_len(4) + payload + CRC32(4)
	total := 4 + 2 + 4 + len(payload) + 4
	buf := make([]byte, total)

	copy(buf[0:4], schema.MetaFileMagic[:])
	binary.LittleEndian.PutUint16(buf[4:6], metaVersion)
	binary.LittleEndian.PutUint32(buf[6:10], uint32(len(payload)))
	copy(buf[10:], payload)
	checksum := util.CRC32(payload)
	binary.LittleEndian.PutUint32(buf[10+len(payload):], checksum)

	return buf, nil
}

// DeserializeMeta decodes a _meta.flop file.
func DeserializeMeta(data []byte) (*schema.StoredMeta, error) {
	if len(data) < 14 {
		return nil, fmt.Errorf("meta file too short")
	}
	// Verify magic
	for i := 0; i < 4; i++ {
		if data[i] != schema.MetaFileMagic[i] {
			return nil, fmt.Errorf("invalid meta file: bad magic bytes")
		}
	}
	version := binary.LittleEndian.Uint16(data[4:6])
	if version != metaVersion {
		return nil, fmt.Errorf("unsupported meta version: %d", version)
	}
	payloadLen := binary.LittleEndian.Uint32(data[6:10])
	if int(10+payloadLen+4) > len(data) {
		return nil, fmt.Errorf("meta file truncated")
	}
	payload := data[10 : 10+payloadLen]
	storedChecksum := binary.LittleEndian.Uint32(data[10+payloadLen:])
	computedChecksum := util.CRC32(payload)
	if storedChecksum != computedChecksum {
		return nil, fmt.Errorf("meta file corrupted: CRC32 mismatch")
	}

	var meta schema.StoredMeta
	if err := json.Unmarshal(payload, &meta); err != nil {
		return nil, fmt.Errorf("meta file JSON parse error: %w", err)
	}
	return &meta, nil
}

// ReadMetaFile reads _meta.flop from disk.
func ReadMetaFile(path string) (*schema.StoredMeta, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return CreateEmptyMeta(), nil
		}
		return nil, err
	}
	return DeserializeMeta(data)
}

// WriteMetaFile writes _meta.flop to disk.
func WriteMetaFile(path string, meta *schema.StoredMeta) error {
	data, err := SerializeMeta(meta)
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data, 0o644)
}

// CreateTableMeta creates a new table meta with initial schema.
func CreateTableMeta(stored *schema.StoredSchema) *schema.StoredTableMeta {
	return &schema.StoredTableMeta{
		CurrentSchemaVersion: 1,
		Schemas:              map[int]*schema.StoredSchema{1: stored},
	}
}

// AddSchemaVersion bumps the schema version and records the new schema.
func AddSchemaVersion(tableMeta *schema.StoredTableMeta, stored *schema.StoredSchema) int {
	newVersion := tableMeta.CurrentSchemaVersion + 1
	tableMeta.Schemas[newVersion] = stored
	tableMeta.CurrentSchemaVersion = newVersion
	return newVersion
}
