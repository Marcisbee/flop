package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/marcisbee/flop/internal/schema"
)

const idxVersion = 2

// HashIndex is an in-memory Map<string, RowPointer> for primary/unique indexes.
type HashIndex struct {
	mu               sync.RWMutex
	data             map[string]schema.RowPointer
	uuid             map[[16]byte]schema.RowPointer
	mapped           *mappedHashBase
	mappedRelease    func()
	deleted          map[string]struct{}
	mappedExtraCount int
}

type HashIndexStats struct {
	Keys                  int
	EstimatedPayloadBytes uint64
}

func NewHashIndex() *HashIndex {
	return &HashIndex{data: make(map[string]schema.RowPointer)}
}

func NewHashIndexWithCapacity(capacity int) *HashIndex {
	if capacity < 0 {
		capacity = 0
	}
	return &HashIndex{data: make(map[string]schema.RowPointer, capacity)}
}

func (h *HashIndex) Get(key string) (schema.RowPointer, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.mapped != nil {
		if p, ok := h.data[key]; ok {
			return p, true
		}
		if _, deleted := h.deleted[key]; deleted {
			return schema.RowPointer{}, false
		}
		return h.mapped.Get(key)
	}
	if uuidKey, ok := parseUUIDIndexKey(key); ok {
		if p, found := h.uuid[uuidKey]; found {
			return p, true
		}
	}
	p, ok := h.data[key]
	return p, ok
}

func (h *HashIndex) Set(key string, pointer schema.RowPointer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.mapped != nil {
		if _, exists := h.data[key]; exists {
			h.data[key] = pointer
			return
		}
		if _, tomb := h.deleted[key]; tomb {
			delete(h.deleted, key)
			h.data[key] = pointer
			if !h.mapped.Has(key) {
				h.mappedExtraCount++
			}
			return
		}
		if !h.mapped.Has(key) {
			h.mappedExtraCount++
		}
		h.data[key] = pointer
		return
	}
	if uuidKey, ok := parseUUIDIndexKey(key); ok {
		if h.uuid == nil {
			h.uuid = make(map[[16]byte]schema.RowPointer)
		}
		h.uuid[uuidKey] = pointer
		return
	}
	h.data[key] = pointer
}

func (h *HashIndex) Has(key string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.mapped != nil {
		if _, ok := h.data[key]; ok {
			return true
		}
		if _, deleted := h.deleted[key]; deleted {
			return false
		}
		return h.mapped.Has(key)
	}
	if uuidKey, ok := parseUUIDIndexKey(key); ok {
		if _, found := h.uuid[uuidKey]; found {
			return true
		}
	}
	_, ok := h.data[key]
	return ok
}

func (h *HashIndex) Delete(key string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.mapped != nil {
		if _, ok := h.data[key]; ok {
			delete(h.data, key)
			if !h.mapped.Has(key) {
				h.mappedExtraCount--
			}
			return true
		}
		if _, deleted := h.deleted[key]; deleted {
			return false
		}
		if h.mapped.Has(key) {
			h.deleted[key] = struct{}{}
			return true
		}
		return false
	}
	if uuidKey, ok := parseUUIDIndexKey(key); ok {
		if _, found := h.uuid[uuidKey]; found {
			delete(h.uuid, uuidKey)
			return true
		}
	}
	_, ok := h.data[key]
	if ok {
		delete(h.data, key)
	}
	return ok
}

func (h *HashIndex) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.mapped != nil {
		return h.mapped.Size() - len(h.deleted) + h.mappedExtraCount
	}
	return len(h.data) + len(h.uuid)
}

func (h *HashIndex) Clear() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clearMappedLocked()
	h.data = make(map[string]schema.RowPointer)
	h.uuid = nil
}

func (h *HashIndex) ResetWithCapacity(capacity int) {
	if capacity < 0 {
		capacity = 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clearMappedLocked()
	h.data = make(map[string]schema.RowPointer, capacity)
	h.uuid = nil
}

func (h *HashIndex) Range(fn func(string, schema.RowPointer) bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.mapped != nil {
		seen := make(map[string]struct{}, len(h.data))
		for k, v := range h.data {
			seen[k] = struct{}{}
			if !fn(k, v) {
				return
			}
		}
		h.mapped.Range(func(k string, v schema.RowPointer) bool {
			if _, deleted := h.deleted[k]; deleted {
				return true
			}
			if _, shadowed := seen[k]; shadowed {
				return true
			}
			return fn(k, v)
		})
		return
	}
	for k, v := range h.data {
		if !fn(k, v) {
			return
		}
	}
	for k, v := range h.uuid {
		if !fn(formatUUIDCanonical(k), v) {
			return
		}
	}
}

// Stats returns key count and a lower-bound payload estimate in bytes.
// The estimate excludes Go runtime map/slice overhead and allocator metadata.
func (h *HashIndex) Stats() HashIndexStats {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.mapped != nil {
		var payload uint64
		for k := range h.data {
			payload += uint64(len(k) + 6)
		}
		payload += h.mapped.EstimatedPayloadBytes
		return HashIndexStats{
			Keys:                  h.mapped.Size() - len(h.deleted) + h.mappedExtraCount,
			EstimatedPayloadBytes: payload,
		}
	}
	var payload uint64
	for k := range h.data {
		payload += uint64(len(k) + 6) // key bytes + row pointer payload
	}
	payload += uint64(len(h.uuid) * (16 + 6))
	return HashIndexStats{
		Keys:                  len(h.data) + len(h.uuid),
		EstimatedPayloadBytes: payload,
	}
}

func (h *HashIndex) clearMappedLocked() {
	if h.mappedRelease != nil {
		h.mappedRelease()
		h.mappedRelease = nil
	}
	h.mapped = nil
	h.deleted = nil
	h.mappedExtraCount = 0
}

func newHashIndexWithMapped(base *mappedHashBase, release func()) *HashIndex {
	if release == nil {
		release = func() {}
	}
	return &HashIndex{
		data:          make(map[string]schema.RowPointer),
		mapped:        base,
		mappedRelease: release,
		deleted:       make(map[string]struct{}),
	}
}

// MultiIndex is Map<string, []RowPointer> for non-unique indexes.
type MultiIndex struct {
	mu               sync.RWMutex
	data             map[string][]schema.RowPointer
	uuid             map[[16]byte][]schema.RowPointer
	mapped           *mappedMultiBase
	mappedRelease    func()
	deleted          map[string]map[uint64]struct{}
	mappedExtraKeys  int
	mappedExtraPosts int
}

type MultiIndexStats struct {
	Keys                  int
	Entries               int
	EstimatedPayloadBytes uint64
}

func NewMultiIndex() *MultiIndex {
	return &MultiIndex{data: make(map[string][]schema.RowPointer)}
}

func NewMultiIndexWithCapacity(capacity int) *MultiIndex {
	if capacity < 0 {
		capacity = 0
	}
	return &MultiIndex{data: make(map[string][]schema.RowPointer, capacity)}
}

func (m *MultiIndex) Add(key string, pointer schema.RowPointer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mapped != nil {
		if tombs, ok := m.deleted[key]; ok {
			pk := rowPointerKey(pointer)
			if _, exists := tombs[pk]; exists {
				delete(tombs, pk)
				if len(tombs) == 0 {
					delete(m.deleted, key)
				}
				return
			}
		}
		if len(m.data[key]) == 0 {
			m.mappedExtraKeys++
		}
		m.data[key] = append(m.data[key], pointer)
		m.mappedExtraPosts++
		return
	}
	if uuidKey, ok := parseUUIDIndexKey(key); ok {
		if m.uuid == nil {
			m.uuid = make(map[[16]byte][]schema.RowPointer)
		}
		m.uuid[uuidKey] = append(m.uuid[uuidKey], pointer)
		return
	}
	m.data[key] = append(m.data[key], pointer)
}

func (m *MultiIndex) GetAll(key string) []schema.RowPointer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.mapped != nil {
		base := m.mapped.GetAll(key)
		added := m.data[key]
		tombs := m.deleted[key]
		out := make([]schema.RowPointer, 0, len(base)+len(added))
		for _, p := range base {
			if len(tombs) > 0 {
				if _, deleted := tombs[rowPointerKey(p)]; deleted {
					continue
				}
			}
			out = append(out, p)
		}
		out = append(out, added...)
		return out
	}
	if uuidKey, ok := parseUUIDIndexKey(key); ok {
		if ptrs, found := m.uuid[uuidKey]; found {
			return ptrs
		}
	}
	return m.data[key]
}

func (m *MultiIndex) Delete(key string, pointer schema.RowPointer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mapped != nil {
		ptrs := m.data[key]
		for i, p := range ptrs {
			if p.PageNumber == pointer.PageNumber && p.SlotIndex == pointer.SlotIndex {
				m.data[key] = append(ptrs[:i], ptrs[i+1:]...)
				m.mappedExtraPosts--
				if len(m.data[key]) == 0 {
					delete(m.data, key)
					m.mappedExtraKeys--
				}
				return
			}
		}
		tombs := m.deleted[key]
		if tombs == nil {
			tombs = make(map[uint64]struct{})
			m.deleted[key] = tombs
		}
		tombs[rowPointerKey(pointer)] = struct{}{}
		return
	}
	if uuidKey, ok := parseUUIDIndexKey(key); ok {
		if ptrs, found := m.uuid[uuidKey]; found {
			for i, p := range ptrs {
				if p.PageNumber == pointer.PageNumber && p.SlotIndex == pointer.SlotIndex {
					m.uuid[uuidKey] = append(ptrs[:i], ptrs[i+1:]...)
					break
				}
			}
			if len(m.uuid[uuidKey]) == 0 {
				delete(m.uuid, uuidKey)
			}
			return
		}
	}
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
	m.clearMappedLocked()
	m.data = make(map[string][]schema.RowPointer)
	m.uuid = nil
}

func (m *MultiIndex) ResetWithCapacity(capacity int) {
	if capacity < 0 {
		capacity = 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clearMappedLocked()
	m.data = make(map[string][]schema.RowPointer, capacity)
	m.uuid = nil
}

// Stats returns key count, posting entries, and a lower-bound payload estimate.
// The estimate excludes Go runtime map/slice overhead and allocator metadata.
func (m *MultiIndex) Stats() MultiIndexStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.mapped != nil {
		var entries int
		var payload uint64
		for k, ptrs := range m.data {
			entries += len(ptrs)
			payload += uint64(len(k) + 6*len(ptrs))
		}
		deletedEntries := 0
		for _, tombs := range m.deleted {
			deletedEntries += len(tombs)
		}
		payload += m.mapped.EstimatedPayloadBytes
		return MultiIndexStats{
			Keys:                  m.mapped.KeyCount() + m.mappedExtraKeys,
			Entries:               m.mapped.EntryCount() - deletedEntries + entries,
			EstimatedPayloadBytes: payload,
		}
	}
	var entries int
	var payload uint64
	for k, ptrs := range m.data {
		entries += len(ptrs)
		payload += uint64(len(k) + 6*len(ptrs))
	}
	for _, ptrs := range m.uuid {
		entries += len(ptrs)
		payload += uint64(16 + 6*len(ptrs))
	}
	return MultiIndexStats{
		Keys:                  len(m.data) + len(m.uuid),
		Entries:               entries,
		EstimatedPayloadBytes: payload,
	}
}

func (m *MultiIndex) clearMappedLocked() {
	if m.mappedRelease != nil {
		m.mappedRelease()
		m.mappedRelease = nil
	}
	m.mapped = nil
	m.deleted = nil
	m.mappedExtraKeys = 0
	m.mappedExtraPosts = 0
}

func newMultiIndexWithMapped(base *mappedMultiBase, release func()) *MultiIndex {
	if release == nil {
		release = func() {}
	}
	return &MultiIndex{
		data:          make(map[string][]schema.RowPointer),
		mapped:        base,
		mappedRelease: release,
		deleted:       make(map[string]map[uint64]struct{}),
	}
}

func (m *MultiIndex) Range(fn func(string, []schema.RowPointer) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.mapped != nil {
		seen := make(map[string]struct{}, len(m.data))
		for k, extra := range m.data {
			seen[k] = struct{}{}
			base := m.mapped.GetAll(k)
			tombs := m.deleted[k]
			merged := make([]schema.RowPointer, 0, len(base)+len(extra))
			for _, p := range base {
				if len(tombs) > 0 {
					if _, deleted := tombs[rowPointerKey(p)]; deleted {
						continue
					}
				}
				merged = append(merged, p)
			}
			merged = append(merged, extra...)
			if !fn(k, merged) {
				return
			}
		}
		m.mapped.Range(func(k string, base []schema.RowPointer) bool {
			if _, done := seen[k]; done {
				return true
			}
			tombs := m.deleted[k]
			if len(tombs) == 0 {
				return fn(k, base)
			}
			filtered := make([]schema.RowPointer, 0, len(base))
			for _, p := range base {
				if _, deleted := tombs[rowPointerKey(p)]; deleted {
					continue
				}
				filtered = append(filtered, p)
			}
			return fn(k, filtered)
		})
		return
	}
	for k, v := range m.data {
		if !fn(k, v) {
			return
		}
	}
	for k, v := range m.uuid {
		if !fn(formatUUIDCanonical(k), v) {
			return
		}
	}
}

func rowPointerKey(p schema.RowPointer) uint64 {
	return (uint64(p.PageNumber) << 16) | uint64(p.SlotIndex)
}

// CompositeKey builds a composite key from multiple field values.
func CompositeKey(values []interface{}) string {
	if len(values) == 0 {
		return ""
	}
	var b strings.Builder
	for i, v := range values {
		if i > 0 {
			b.WriteByte(0)
		}
		appendCompositePart(&b, v)
	}
	return b.String()
}

// CompositeKeyFromRow builds a composite key directly from row fields, avoiding
// intermediate []interface{} allocations in hot index paths.
func CompositeKeyFromRow(row map[string]interface{}, fields []string) string {
	if len(fields) == 0 {
		return ""
	}
	var b strings.Builder
	for i, f := range fields {
		if i > 0 {
			b.WriteByte(0)
		}
		appendCompositePart(&b, row[f])
	}
	return b.String()
}

func appendCompositePart(b *strings.Builder, v interface{}) {
	if v == nil {
		b.WriteByte(0)
		return
	}
	switch t := v.(type) {
	case string:
		b.WriteString(t)
	case []byte:
		b.Write(t)
	case bool:
		if t {
			b.WriteByte('1')
		} else {
			b.WriteByte('0')
		}
	case int:
		b.WriteString(strconv.FormatInt(int64(t), 10))
	case int8:
		b.WriteString(strconv.FormatInt(int64(t), 10))
	case int16:
		b.WriteString(strconv.FormatInt(int64(t), 10))
	case int32:
		b.WriteString(strconv.FormatInt(int64(t), 10))
	case int64:
		b.WriteString(strconv.FormatInt(t, 10))
	case uint:
		b.WriteString(strconv.FormatUint(uint64(t), 10))
	case uint8:
		b.WriteString(strconv.FormatUint(uint64(t), 10))
	case uint16:
		b.WriteString(strconv.FormatUint(uint64(t), 10))
	case uint32:
		b.WriteString(strconv.FormatUint(uint64(t), 10))
	case uint64:
		b.WriteString(strconv.FormatUint(t, 10))
	case float32:
		b.WriteString(strconv.FormatFloat(float64(t), 'g', -1, 32))
	case float64:
		b.WriteString(strconv.FormatFloat(t, 'g', -1, 64))
	default:
		b.WriteString(fmt.Sprint(v))
	}
}

// parseUUIDIndexKey recognizes canonical UUID text keys (8-4-4-4-12)
// and returns a fixed-size binary key for compact storage.
func parseUUIDIndexKey(key string) ([16]byte, bool) {
	var out [16]byte
	if len(key) != 36 || strings.IndexByte(key, 0) >= 0 {
		return out, false
	}
	if key[8] != '-' || key[13] != '-' || key[18] != '-' || key[23] != '-' {
		return out, false
	}
	nib := 0
	var current byte
	for i := 0; i < len(key); i++ {
		if key[i] == '-' {
			continue
		}
		v := fromHexNibble(key[i])
		if v < 0 {
			return out, false
		}
		if nib%2 == 0 {
			current = byte(v) << 4
		} else {
			out[nib/2] = current | byte(v)
		}
		nib++
	}
	if nib != 32 {
		return out, false
	}
	return out, true
}

func parseUUIDIndexKeyBytes(key []byte) ([16]byte, bool) {
	var out [16]byte
	if len(key) != 36 {
		return out, false
	}
	if key[8] != '-' || key[13] != '-' || key[18] != '-' || key[23] != '-' {
		return out, false
	}
	nib := 0
	var current byte
	for i := 0; i < len(key); i++ {
		if key[i] == '-' {
			continue
		}
		v := fromHexNibble(key[i])
		if v < 0 {
			return out, false
		}
		if nib%2 == 0 {
			current = byte(v) << 4
		} else {
			out[nib/2] = current | byte(v)
		}
		nib++
	}
	if nib != 32 {
		return out, false
	}
	return out, true
}

func fromHexNibble(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	default:
		return -1
	}
}

func formatUUIDCanonical(key [16]byte) string {
	const hextable = "0123456789abcdef"
	var out [36]byte
	pos := 0
	for i, b := range key {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out[pos] = '-'
			pos++
		}
		out[pos] = hextable[b>>4]
		out[pos+1] = hextable[b&0x0f]
		pos += 2
	}
	return string(out[:])
}

// SerializeIndex writes a HashIndex to .idx file format.
func SerializeIndex(index *HashIndex) []byte {
	// Header: magic(4) + version(2) + entryCount(4) = 10 bytes
	index.mu.RLock()
	defer index.mu.RUnlock()

	size := len(index.data) + len(index.uuid)
	buf := make([]byte, 10, 10+size*24)
	copy(buf[0:4], schema.IndexFileMagic[:])
	binary.LittleEndian.PutUint16(buf[4:6], idxVersion)
	binary.LittleEndian.PutUint32(buf[6:10], uint32(size))

	for key, pointer := range index.data {
		keyBytes := []byte(key)
		buf = append(buf, 0) // string key entry
		lenPos := len(buf)
		buf = append(buf, 0, 0)
		binary.LittleEndian.PutUint16(buf[lenPos:lenPos+2], uint16(len(keyBytes)))
		buf = append(buf, keyBytes...)
		var ptr [6]byte
		binary.LittleEndian.PutUint32(ptr[0:4], pointer.PageNumber)
		binary.LittleEndian.PutUint16(ptr[4:6], pointer.SlotIndex)
		buf = append(buf, ptr[:]...)
	}
	for key, pointer := range index.uuid {
		buf = append(buf, 1) // binary UUID key entry
		buf = append(buf, key[:]...)
		var ptr [6]byte
		binary.LittleEndian.PutUint32(ptr[0:4], pointer.PageNumber)
		binary.LittleEndian.PutUint16(ptr[4:6], pointer.SlotIndex)
		buf = append(buf, ptr[:]...)
	}

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
	switch version {
	case 1:
		return deserializeIndexV1(data)
	case idxVersion:
		// continue below
	default:
		return nil, fmt.Errorf("unsupported index version: %d", version)
	}

	entryCount := binary.LittleEndian.Uint32(data[6:10])
	index := NewHashIndex()
	index.uuid = make(map[[16]byte]schema.RowPointer, int(entryCount))

	offset := 10
	for i := uint32(0); i < entryCount && offset < len(data); i++ {
		if offset+1 > len(data) {
			break
		}
		entryType := data[offset]
		offset++
		switch entryType {
		case 0:
			if offset+2 > len(data) {
				return index, nil
			}
			keyLen := binary.LittleEndian.Uint16(data[offset : offset+2])
			offset += 2
			if offset+int(keyLen)+6 > len(data) {
				return index, nil
			}
			keyBytes := data[offset : offset+int(keyLen)]
			offset += int(keyLen)
			pageNumber := binary.LittleEndian.Uint32(data[offset : offset+4])
			offset += 4
			slotIndex := binary.LittleEndian.Uint16(data[offset : offset+2])
			offset += 2
			pointer := schema.RowPointer{PageNumber: pageNumber, SlotIndex: slotIndex}
			if uuidKey, ok := parseUUIDIndexKeyBytes(keyBytes); ok {
				index.uuid[uuidKey] = pointer
				continue
			}
			key := string(keyBytes)
			index.data[key] = pointer
		case 1:
			if offset+16+6 > len(data) {
				return index, nil
			}
			var key [16]byte
			copy(key[:], data[offset:offset+16])
			offset += 16
			pageNumber := binary.LittleEndian.Uint32(data[offset : offset+4])
			offset += 4
			slotIndex := binary.LittleEndian.Uint16(data[offset : offset+2])
			offset += 2
			index.uuid[key] = schema.RowPointer{PageNumber: pageNumber, SlotIndex: slotIndex}
		default:
			return nil, fmt.Errorf("invalid index entry type: %d", entryType)
		}
	}

	return index, nil
}

func deserializeIndexV1(data []byte) (*HashIndex, error) {
	entryCount := binary.LittleEndian.Uint32(data[6:10])
	index := NewHashIndex()
	index.uuid = make(map[[16]byte]schema.RowPointer, int(entryCount))

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
		keyBytes := data[offset : offset+int(keyLen)]
		offset += int(keyLen)
		pageNumber := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4
		slotIndex := binary.LittleEndian.Uint16(data[offset : offset+2])
		offset += 2
		pointer := schema.RowPointer{PageNumber: pageNumber, SlotIndex: slotIndex}
		if uuidKey, ok := parseUUIDIndexKeyBytes(keyBytes); ok {
			index.uuid[uuidKey] = pointer
		} else {
			index.data[string(keyBytes)] = pointer
		}
	}
	return index, nil
}

// ReadIndexFile reads an index from disk.
func ReadIndexFile(path string) (*HashIndex, error) {
	data, release, err := readIndexFileBytes(path)
	if err != nil {
		if os.IsNotExist(err) {
			return NewHashIndex(), nil
		}
		return nil, err
	}
	defer release()
	return DeserializeIndex(data)
}

// WriteIndexFile persists an index to disk.
func WriteIndexFile(path string, index *HashIndex) error {
	data := SerializeIndex(index)
	return os.WriteFile(path, data, 0644)
}
