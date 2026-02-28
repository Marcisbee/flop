package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	"github.com/marcisbee/flop/internal/schema"
)

var mappedIndexMagic = [4]byte{'M', 'I', 'D', 'X'}
var mappedMultiMagic = [4]byte{'M', 'M', 'I', 'X'}

const mappedIndexVersion uint16 = 1

type mappedHashBase struct {
	data                  []byte
	offsets               []uint32
	count                 int
	EstimatedPayloadBytes uint64
}

func (m *mappedHashBase) Size() int {
	if m == nil {
		return 0
	}
	return m.count
}

func (m *mappedHashBase) Has(key string) bool {
	_, ok := m.Get(key)
	return ok
}

func (m *mappedHashBase) Get(key string) (schema.RowPointer, bool) {
	if m == nil || m.count == 0 {
		return schema.RowPointer{}, false
	}
	target := []byte(key)
	lo, hi := 0, m.count-1
	for lo <= hi {
		mid := (lo + hi) / 2
		k, ptr, ok := m.entry(mid)
		if !ok {
			return schema.RowPointer{}, false
		}
		switch bytes.Compare(k, target) {
		case 0:
			return ptr, true
		case -1:
			lo = mid + 1
		default:
			hi = mid - 1
		}
	}
	return schema.RowPointer{}, false
}

func (m *mappedHashBase) Range(fn func(string, schema.RowPointer) bool) {
	if m == nil || m.count == 0 {
		return
	}
	for i := 0; i < m.count; i++ {
		k, ptr, ok := m.entry(i)
		if !ok {
			return
		}
		if !fn(string(k), ptr) {
			return
		}
	}
}

func (m *mappedHashBase) entry(i int) ([]byte, schema.RowPointer, bool) {
	if m == nil || i < 0 || i >= m.count || i >= len(m.offsets) {
		return nil, schema.RowPointer{}, false
	}
	off := int(m.offsets[i])
	if off < 0 || off+2 > len(m.data) {
		return nil, schema.RowPointer{}, false
	}
	keyLen := int(binary.LittleEndian.Uint16(m.data[off : off+2]))
	off += 2
	if keyLen < 0 || off+keyLen+6 > len(m.data) {
		return nil, schema.RowPointer{}, false
	}
	key := m.data[off : off+keyLen]
	off += keyLen
	page := binary.LittleEndian.Uint32(m.data[off : off+4])
	off += 4
	slot := binary.LittleEndian.Uint16(m.data[off : off+2])
	return key, schema.RowPointer{PageNumber: page, SlotIndex: slot}, true
}

func ReadMappedIndexFile(path string) (*HashIndex, error) {
	data, release, err := readIndexFileBytes(path)
	if err != nil {
		if os.IsNotExist(err) {
			return NewHashIndex(), nil
		}
		return nil, err
	}
	base, err := parseMappedIndex(data)
	if err != nil {
		release()
		return nil, err
	}
	if base == nil || base.count == 0 {
		release()
		return NewHashIndex(), nil
	}
	return newHashIndexWithMapped(base, release), nil
}

func WriteMappedIndexFile(path string, index *HashIndex) error {
	data, err := serializeMappedIndex(index)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func parseMappedIndex(raw []byte) (*mappedHashBase, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if len(raw) < 10 {
		return nil, fmt.Errorf("invalid mapped index: truncated header")
	}
	for i := 0; i < 4; i++ {
		if raw[i] != mappedIndexMagic[i] {
			return nil, fmt.Errorf("invalid mapped index: bad magic")
		}
	}
	version := binary.LittleEndian.Uint16(raw[4:6])
	if version != mappedIndexVersion {
		return nil, fmt.Errorf("unsupported mapped index version: %d", version)
	}
	count := int(binary.LittleEndian.Uint32(raw[6:10]))
	offsetStart := 10
	offsetBytes := count * 4
	if count < 0 || offsetStart+offsetBytes > len(raw) {
		return nil, fmt.Errorf("invalid mapped index: bad offsets")
	}
	dataStart := offsetStart + offsetBytes
	offsets := make([]uint32, count)
	for i := 0; i < count; i++ {
		offsets[i] = binary.LittleEndian.Uint32(raw[offsetStart+i*4 : offsetStart+(i+1)*4])
	}

	var payload uint64
	for i := 0; i < count; i++ {
		off := int(offsets[i])
		if off < 0 || dataStart+off+2 > len(raw) {
			return nil, fmt.Errorf("invalid mapped index: bad entry offset")
		}
		keyLen := int(binary.LittleEndian.Uint16(raw[dataStart+off : dataStart+off+2]))
		if dataStart+off+2+keyLen+6 > len(raw) {
			return nil, fmt.Errorf("invalid mapped index: truncated entry")
		}
		payload += uint64(keyLen + 6)
	}

	return &mappedHashBase{
		data:                  raw[dataStart:],
		offsets:               offsets,
		count:                 count,
		EstimatedPayloadBytes: payload,
	}, nil
}

type mappedEntry struct {
	key string
	ptr schema.RowPointer
}

func serializeMappedIndex(index *HashIndex) ([]byte, error) {
	entries := make([]mappedEntry, 0, index.Size())
	index.Range(func(k string, ptr schema.RowPointer) bool {
		entries = append(entries, mappedEntry{key: k, ptr: ptr})
		return true
	})

	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })

	count := len(entries)
	headerSize := 10
	offsetSize := count * 4
	dataSize := 0
	for _, e := range entries {
		if len(e.key) > 0xFFFF {
			return nil, fmt.Errorf("mapped index key too long: %d", len(e.key))
		}
		dataSize += 2 + len(e.key) + 6
	}

	out := make([]byte, headerSize+offsetSize+dataSize)
	copy(out[0:4], mappedIndexMagic[:])
	binary.LittleEndian.PutUint16(out[4:6], mappedIndexVersion)
	binary.LittleEndian.PutUint32(out[6:10], uint32(count))

	offsetPos := headerSize
	dataPos := headerSize + offsetSize
	rel := 0
	for _, e := range entries {
		binary.LittleEndian.PutUint32(out[offsetPos:offsetPos+4], uint32(rel))
		offsetPos += 4
		keyLen := len(e.key)
		binary.LittleEndian.PutUint16(out[dataPos:dataPos+2], uint16(keyLen))
		dataPos += 2
		copy(out[dataPos:dataPos+keyLen], e.key)
		dataPos += keyLen
		binary.LittleEndian.PutUint32(out[dataPos:dataPos+4], e.ptr.PageNumber)
		dataPos += 4
		binary.LittleEndian.PutUint16(out[dataPos:dataPos+2], e.ptr.SlotIndex)
		dataPos += 2
		rel += 2 + keyLen + 6
	}
	return out, nil
}

type mappedMultiBase struct {
	data                  []byte
	offsets               []uint32
	count                 int
	entryCount            int
	EstimatedPayloadBytes uint64
}

func (m *mappedMultiBase) KeyCount() int {
	if m == nil {
		return 0
	}
	return m.count
}

func (m *mappedMultiBase) EntryCount() int {
	if m == nil {
		return 0
	}
	return m.entryCount
}

func (m *mappedMultiBase) GetAll(key string) []schema.RowPointer {
	if m == nil || m.count == 0 {
		return nil
	}
	target := []byte(key)
	lo, hi := 0, m.count-1
	for lo <= hi {
		mid := (lo + hi) / 2
		k, ptrs, ok := m.entry(mid)
		if !ok {
			return nil
		}
		switch bytes.Compare(k, target) {
		case 0:
			return ptrs
		case -1:
			lo = mid + 1
		default:
			hi = mid - 1
		}
	}
	return nil
}

func (m *mappedMultiBase) HasPointer(key string, pointer schema.RowPointer) bool {
	_, off, ptrCount, ok := m.findEntryHeader(key)
	if !ok {
		return false
	}
	for i := 0; i < ptrCount; i++ {
		if off+6 > len(m.data) {
			return false
		}
		page := binary.LittleEndian.Uint32(m.data[off : off+4])
		off += 4
		slot := binary.LittleEndian.Uint16(m.data[off : off+2])
		off += 2
		if page == pointer.PageNumber && slot == pointer.SlotIndex {
			return true
		}
	}
	return false
}

func (m *mappedMultiBase) HasKey(key string) bool {
	_, _, _, ok := m.findEntryHeader(key)
	return ok
}

func (m *mappedMultiBase) Range(fn func(string, []schema.RowPointer) bool) {
	if m == nil || m.count == 0 {
		return
	}
	for i := 0; i < m.count; i++ {
		key, ptrs, ok := m.entry(i)
		if !ok {
			return
		}
		if !fn(string(key), ptrs) {
			return
		}
	}
}

func (m *mappedMultiBase) entry(i int) ([]byte, []schema.RowPointer, bool) {
	if m == nil || i < 0 || i >= m.count || i >= len(m.offsets) {
		return nil, nil, false
	}
	off := int(m.offsets[i])
	if off < 0 || off+2 > len(m.data) {
		return nil, nil, false
	}
	keyLen := int(binary.LittleEndian.Uint16(m.data[off : off+2]))
	off += 2
	if keyLen < 0 || off+keyLen+4 > len(m.data) {
		return nil, nil, false
	}
	key := m.data[off : off+keyLen]
	off += keyLen
	ptrCount := int(binary.LittleEndian.Uint32(m.data[off : off+4]))
	off += 4
	if ptrCount < 0 || off+ptrCount*6 > len(m.data) {
		return nil, nil, false
	}
	out := make([]schema.RowPointer, ptrCount)
	for j := 0; j < ptrCount; j++ {
		page := binary.LittleEndian.Uint32(m.data[off : off+4])
		off += 4
		slot := binary.LittleEndian.Uint16(m.data[off : off+2])
		off += 2
		out[j] = schema.RowPointer{PageNumber: page, SlotIndex: slot}
	}
	return key, out, true
}

// findEntryHeader locates key and returns key bytes, pointer payload offset and count.
func (m *mappedMultiBase) findEntryHeader(key string) ([]byte, int, int, bool) {
	if m == nil || m.count == 0 {
		return nil, 0, 0, false
	}
	target := []byte(key)
	lo, hi := 0, m.count-1
	for lo <= hi {
		mid := (lo + hi) / 2
		off := int(m.offsets[mid])
		if off < 0 || off+2 > len(m.data) {
			return nil, 0, 0, false
		}
		keyLen := int(binary.LittleEndian.Uint16(m.data[off : off+2]))
		off += 2
		if keyLen < 0 || off+keyLen+4 > len(m.data) {
			return nil, 0, 0, false
		}
		k := m.data[off : off+keyLen]
		cmp := bytes.Compare(k, target)
		off += keyLen
		ptrCount := int(binary.LittleEndian.Uint32(m.data[off : off+4]))
		off += 4
		switch cmp {
		case 0:
			return k, off, ptrCount, true
		case -1:
			lo = mid + 1
		default:
			hi = mid - 1
		}
	}
	return nil, 0, 0, false
}

func ReadMappedMultiIndexFile(path string) (*MultiIndex, error) {
	data, release, err := readIndexFileBytes(path)
	if err != nil {
		if os.IsNotExist(err) {
			return NewMultiIndex(), nil
		}
		return nil, err
	}
	base, err := parseMappedMultiIndex(data)
	if err != nil {
		release()
		return nil, err
	}
	if base == nil || base.count == 0 {
		release()
		return NewMultiIndex(), nil
	}
	return newMultiIndexWithMapped(base, release), nil
}

func WriteMappedMultiIndexFile(path string, index *MultiIndex) error {
	data, err := serializeMappedMultiIndex(index)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func parseMappedMultiIndex(raw []byte) (*mappedMultiBase, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if len(raw) < 10 {
		return nil, fmt.Errorf("invalid mapped multi index: truncated header")
	}
	for i := 0; i < 4; i++ {
		if raw[i] != mappedMultiMagic[i] {
			return nil, fmt.Errorf("invalid mapped multi index: bad magic")
		}
	}
	version := binary.LittleEndian.Uint16(raw[4:6])
	if version != mappedIndexVersion {
		return nil, fmt.Errorf("unsupported mapped multi index version: %d", version)
	}
	count := int(binary.LittleEndian.Uint32(raw[6:10]))
	offsetStart := 10
	offsetBytes := count * 4
	if count < 0 || offsetStart+offsetBytes > len(raw) {
		return nil, fmt.Errorf("invalid mapped multi index: bad offsets")
	}
	dataStart := offsetStart + offsetBytes
	offsets := make([]uint32, count)
	for i := 0; i < count; i++ {
		offsets[i] = binary.LittleEndian.Uint32(raw[offsetStart+i*4 : offsetStart+(i+1)*4])
	}

	var payload uint64
	entries := 0
	for i := 0; i < count; i++ {
		off := int(offsets[i])
		if off < 0 || dataStart+off+2 > len(raw) {
			return nil, fmt.Errorf("invalid mapped multi index: bad entry offset")
		}
		keyLen := int(binary.LittleEndian.Uint16(raw[dataStart+off : dataStart+off+2]))
		off += 2
		if keyLen < 0 || dataStart+off+keyLen+4 > len(raw) {
			return nil, fmt.Errorf("invalid mapped multi index: truncated key")
		}
		off += keyLen
		ptrCount := int(binary.LittleEndian.Uint32(raw[dataStart+off : dataStart+off+4]))
		off += 4
		if ptrCount < 0 || dataStart+off+ptrCount*6 > len(raw) {
			return nil, fmt.Errorf("invalid mapped multi index: truncated postings")
		}
		payload += uint64(keyLen + 6*ptrCount)
		entries += ptrCount
	}

	return &mappedMultiBase{
		data:                  raw[dataStart:],
		offsets:               offsets,
		count:                 count,
		entryCount:            entries,
		EstimatedPayloadBytes: payload,
	}, nil
}

type mappedMultiEntry struct {
	key  string
	ptrs []schema.RowPointer
}

func serializeMappedMultiIndex(index *MultiIndex) ([]byte, error) {
	entries := make([]mappedMultiEntry, 0, 1024)
	index.Range(func(k string, ptrs []schema.RowPointer) bool {
		if len(ptrs) == 0 {
			return true
		}
		copied := make([]schema.RowPointer, len(ptrs))
		copy(copied, ptrs)
		entries = append(entries, mappedMultiEntry{key: k, ptrs: copied})
		return true
	})

	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })

	count := len(entries)
	headerSize := 10
	offsetSize := count * 4
	dataSize := 0
	for _, e := range entries {
		if len(e.key) > 0xFFFF {
			return nil, fmt.Errorf("mapped multi key too long: %d", len(e.key))
		}
		dataSize += 2 + len(e.key) + 4 + 6*len(e.ptrs)
	}

	out := make([]byte, headerSize+offsetSize+dataSize)
	copy(out[0:4], mappedMultiMagic[:])
	binary.LittleEndian.PutUint16(out[4:6], mappedIndexVersion)
	binary.LittleEndian.PutUint32(out[6:10], uint32(count))

	offsetPos := headerSize
	dataPos := headerSize + offsetSize
	rel := 0
	for _, e := range entries {
		binary.LittleEndian.PutUint32(out[offsetPos:offsetPos+4], uint32(rel))
		offsetPos += 4
		keyLen := len(e.key)
		binary.LittleEndian.PutUint16(out[dataPos:dataPos+2], uint16(keyLen))
		dataPos += 2
		copy(out[dataPos:dataPos+keyLen], e.key)
		dataPos += keyLen
		binary.LittleEndian.PutUint32(out[dataPos:dataPos+4], uint32(len(e.ptrs)))
		dataPos += 4
		for _, p := range e.ptrs {
			binary.LittleEndian.PutUint32(out[dataPos:dataPos+4], p.PageNumber)
			dataPos += 4
			binary.LittleEndian.PutUint16(out[dataPos:dataPos+2], p.SlotIndex)
			dataPos += 2
		}
		rel += 2 + keyLen + 4 + 6*len(e.ptrs)
	}
	return out, nil
}
