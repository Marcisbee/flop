package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/marcisbee/flop/internal/schema"
)

const idxVersion = 1

// HashIndex is an in-memory Map<string, RowPointer> for primary/unique indexes.
type HashIndex struct {
	mu   sync.RWMutex
	data map[string]schema.RowPointer
}

type HashIndexStats struct {
	Keys                  int
	EstimatedPayloadBytes uint64
}

func NewHashIndex() *HashIndex {
	return &HashIndex{data: make(map[string]schema.RowPointer)}
}

func (h *HashIndex) Get(key string) (schema.RowPointer, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	p, ok := h.data[key]
	return p, ok
}

func (h *HashIndex) Set(key string, pointer schema.RowPointer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.data[key] = pointer
}

func (h *HashIndex) Has(key string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.data[key]
	return ok
}

func (h *HashIndex) Delete(key string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.data[key]
	if ok {
		delete(h.data, key)
	}
	return ok
}

func (h *HashIndex) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.data)
}

func (h *HashIndex) Clear() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.data = make(map[string]schema.RowPointer)
}

func (h *HashIndex) Range(fn func(string, schema.RowPointer) bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for k, v := range h.data {
		if !fn(k, v) {
			return
		}
	}
}

// Stats returns key count and a lower-bound payload estimate in bytes.
// The estimate excludes Go runtime map/slice overhead and allocator metadata.
func (h *HashIndex) Stats() HashIndexStats {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var payload uint64
	for k := range h.data {
		payload += uint64(len(k) + 6) // key bytes + row pointer payload
	}
	return HashIndexStats{
		Keys:                  len(h.data),
		EstimatedPayloadBytes: payload,
	}
}

// MultiIndex is Map<string, []RowPointer> for non-unique indexes.
type MultiIndex struct {
	mu   sync.RWMutex
	data map[string][]schema.RowPointer
}

type MultiIndexStats struct {
	Keys                  int
	Entries               int
	EstimatedPayloadBytes uint64
}

func NewMultiIndex() *MultiIndex {
	return &MultiIndex{data: make(map[string][]schema.RowPointer)}
}

func (m *MultiIndex) Add(key string, pointer schema.RowPointer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = append(m.data[key], pointer)
}

func (m *MultiIndex) GetAll(key string) []schema.RowPointer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.data[key]
}

func (m *MultiIndex) Delete(key string, pointer schema.RowPointer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ptrs := m.data[key]
	for i, p := range ptrs {
		if p.PageNumber == pointer.PageNumber && p.SlotIndex == pointer.SlotIndex {
			m.data[key] = append(ptrs[:i], ptrs[i+1:]...)
			break
		}
	}
	if len(m.data[key]) == 0 {
		delete(m.data, key)
	}
}

func (m *MultiIndex) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string][]schema.RowPointer)
}

// Stats returns key count, posting entries, and a lower-bound payload estimate.
// The estimate excludes Go runtime map/slice overhead and allocator metadata.
func (m *MultiIndex) Stats() MultiIndexStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var entries int
	var payload uint64
	for k, ptrs := range m.data {
		entries += len(ptrs)
		payload += uint64(len(k) + 6*len(ptrs))
	}
	return MultiIndexStats{
		Keys:                  len(m.data),
		Entries:               entries,
		EstimatedPayloadBytes: payload,
	}
}

// CompositeKey builds a composite key from multiple field values.
func CompositeKey(values []interface{}) string {
	parts := make([]string, len(values))
	for i, v := range values {
		if v == nil {
			parts[i] = "\x00"
		} else {
			parts[i] = fmt.Sprintf("%v", v)
		}
	}
	return strings.Join(parts, "\x00")
}

// SerializeIndex writes a HashIndex to .idx file format.
func SerializeIndex(index *HashIndex) []byte {
	size := index.Size()
	// Header: magic(4) + version(2) + entryCount(4) = 10 bytes
	buf := make([]byte, 10, 10+size*20)
	copy(buf[0:4], schema.IndexFileMagic[:])
	binary.LittleEndian.PutUint16(buf[4:6], idxVersion)
	binary.LittleEndian.PutUint32(buf[6:10], uint32(size))

	index.Range(func(key string, pointer schema.RowPointer) bool {
		keyBytes := []byte(key)
		entry := make([]byte, 2+len(keyBytes)+4+2)
		binary.LittleEndian.PutUint16(entry[0:2], uint16(len(keyBytes)))
		copy(entry[2:], keyBytes)
		binary.LittleEndian.PutUint32(entry[2+len(keyBytes):], pointer.PageNumber)
		binary.LittleEndian.PutUint16(entry[2+len(keyBytes)+4:], pointer.SlotIndex)
		buf = append(buf, entry...)
		return true
	})

	return buf
}

// DeserializeIndex reads a .idx file into a HashIndex.
func DeserializeIndex(data []byte) (*HashIndex, error) {
	if len(data) < 10 {
		return NewHashIndex(), nil
	}
	// Verify magic
	for i := 0; i < 4; i++ {
		if data[i] != schema.IndexFileMagic[i] {
			return nil, fmt.Errorf("invalid index file: bad magic")
		}
	}
	version := binary.LittleEndian.Uint16(data[4:6])
	if version != idxVersion {
		return nil, fmt.Errorf("unsupported index version: %d", version)
	}

	entryCount := binary.LittleEndian.Uint32(data[6:10])
	index := NewHashIndex()

	offset := 10
	for i := uint32(0); i < entryCount && offset < len(data); i++ {
		if offset+2 > len(data) {
			break
		}
		keyLen := binary.LittleEndian.Uint16(data[offset : offset+2])
		offset += 2
		if offset+int(keyLen)+6 > len(data) {
			break
		}
		key := string(data[offset : offset+int(keyLen)])
		offset += int(keyLen)
		pageNumber := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4
		slotIndex := binary.LittleEndian.Uint16(data[offset : offset+2])
		offset += 2
		index.Set(key, schema.RowPointer{PageNumber: pageNumber, SlotIndex: slotIndex})
	}

	return index, nil
}

// ReadIndexFile reads an index from disk.
func ReadIndexFile(path string) (*HashIndex, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return NewHashIndex(), nil
		}
		return nil, err
	}
	return DeserializeIndex(data)
}

// WriteIndexFile persists an index to disk.
func WriteIndexFile(path string, index *HashIndex) error {
	data := SerializeIndex(index)
	return os.WriteFile(path, data, 0644)
}
