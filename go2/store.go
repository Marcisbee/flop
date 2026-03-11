package flop

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
)

// Store is a fast in-memory sorted key-value store with disk persistence.
type Store struct {
	mu      sync.RWMutex
	entries map[string][]byte
	keys    []string
	sorted  bool
	path    string
}

func NewStore(path string) *Store {
	s := &Store{
		entries: make(map[string][]byte),
		path:    path,
		sorted:  true,
	}
	if path != "" {
		s.loadFromDisk()
	}
	return s
}

// Get retrieves a value (thread-safe).
func (s *Store) Get(key []byte) []byte {
	s.mu.RLock()
	v := s.entries[string(key)]
	s.mu.RUnlock()
	return v
}

// Put inserts or updates (thread-safe).
func (s *Store) Put(key, val []byte) {
	s.mu.Lock()
	s.putUnlocked(key, val)
	s.mu.Unlock()
}

// Delete removes a key (thread-safe).
func (s *Store) Delete(key []byte) bool {
	s.mu.Lock()
	ok := s.deleteUnlocked(key)
	s.mu.Unlock()
	return ok
}

// Lockless internal methods for use within already-locked contexts (Table, FTS).

func (s *Store) putUnlocked(key, val []byte) {
	k := string(key)
	if _, exists := s.entries[k]; !exists {
		s.keys = append(s.keys, k)
		s.sorted = false
	}
	s.entries[k] = val
}

func (s *Store) getUnlocked(key []byte) []byte {
	return s.entries[string(key)]
}

func (s *Store) deleteUnlocked(key []byte) bool {
	k := string(key)
	if _, exists := s.entries[k]; !exists {
		return false
	}
	delete(s.entries, k)
	for i, sk := range s.keys {
		if sk == k {
			s.keys[i] = s.keys[len(s.keys)-1]
			s.keys = s.keys[:len(s.keys)-1]
			s.sorted = false
			break
		}
	}
	return true
}

// Scan iterates all key-value pairs in sorted order.
func (s *Store) Scan(fn func(key, val []byte) bool) {
	s.mu.RLock()
	if !s.sorted {
		s.mu.RUnlock()
		s.mu.Lock()
		sort.Strings(s.keys)
		s.sorted = true
		// Downgrade to read lock
		keys := make([]string, len(s.keys))
		copy(keys, s.keys)
		s.mu.Unlock()

		for _, k := range keys {
			s.mu.RLock()
			v := s.entries[k]
			s.mu.RUnlock()
			if v != nil {
				if !fn([]byte(k), v) {
					return
				}
			}
		}
		return
	}

	keys := make([]string, len(s.keys))
	copy(keys, s.keys)
	s.mu.RUnlock()

	for _, k := range keys {
		s.mu.RLock()
		v := s.entries[k]
		s.mu.RUnlock()
		if v != nil {
			if !fn([]byte(k), v) {
				return
			}
		}
	}
}

// ScanReverse iterates all key-value pairs in reverse sorted order.
func (s *Store) ScanReverse(fn func(key, val []byte) bool) {
	s.mu.RLock()
	if !s.sorted {
		s.mu.RUnlock()
		s.mu.Lock()
		sort.Strings(s.keys)
		s.sorted = true
		keys := make([]string, len(s.keys))
		copy(keys, s.keys)
		s.mu.Unlock()

		for i := len(keys) - 1; i >= 0; i-- {
			s.mu.RLock()
			v := s.entries[keys[i]]
			s.mu.RUnlock()
			if v != nil {
				if !fn([]byte(keys[i]), v) {
					return
				}
			}
		}
		return
	}

	keys := make([]string, len(s.keys))
	copy(keys, s.keys)
	s.mu.RUnlock()

	for i := len(keys) - 1; i >= 0; i-- {
		s.mu.RLock()
		v := s.entries[keys[i]]
		s.mu.RUnlock()
		if v != nil {
			if !fn([]byte(keys[i]), v) {
				return
			}
		}
	}
}

// ScanPrefix iterates all key-value pairs whose key starts with the given prefix.
func (s *Store) ScanPrefix(prefix []byte, fn func(key, val []byte) bool) {
	s.mu.RLock()
	if !s.sorted {
		s.mu.RUnlock()
		s.mu.Lock()
		sort.Strings(s.keys)
		s.sorted = true
		s.mu.Unlock()
		s.mu.RLock()
	}

	p := string(prefix)
	// Binary search for the first key >= prefix
	start := sort.SearchStrings(s.keys, p)
	keys := make([]string, 0)
	for i := start; i < len(s.keys); i++ {
		k := s.keys[i]
		if len(k) < len(p) || k[:len(p)] != p {
			break
		}
		keys = append(keys, k)
	}
	s.mu.RUnlock()

	for _, k := range keys {
		s.mu.RLock()
		v := s.entries[k]
		s.mu.RUnlock()
		if v != nil {
			if !fn([]byte(k), v) {
				return
			}
		}
	}
}

// ScanRange iterates key-value pairs where key >= start and key < end (sorted order).
// If start is nil, scan from the beginning. If end is nil, scan to the end.
func (s *Store) ScanRange(start, end []byte, fn func(key, val []byte) bool) {
	s.mu.RLock()
	if !s.sorted {
		s.mu.RUnlock()
		s.mu.Lock()
		sort.Strings(s.keys)
		s.sorted = true
		s.mu.Unlock()
		s.mu.RLock()
	}

	var startIdx int
	if start != nil {
		startIdx = sort.SearchStrings(s.keys, string(start))
	}

	endStr := ""
	hasEnd := end != nil
	if hasEnd {
		endStr = string(end)
	}

	keys := make([]string, 0)
	for i := startIdx; i < len(s.keys); i++ {
		k := s.keys[i]
		if hasEnd && k >= endStr {
			break
		}
		keys = append(keys, k)
	}
	s.mu.RUnlock()

	for _, k := range keys {
		s.mu.RLock()
		v := s.entries[k]
		s.mu.RUnlock()
		if v != nil {
			if !fn([]byte(k), v) {
				return
			}
		}
	}
}

// Count returns total entries.
func (s *Store) Count() int {
	s.mu.RLock()
	n := len(s.entries)
	s.mu.RUnlock()
	return n
}

// Flush persists to disk using buffered writes.
func (s *Store) Flush() error {
	if s.path == "" {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Pre-calculate size
	size := 8 // count header
	for k, v := range s.entries {
		size += 8 + len(k) + len(v)
	}

	buf := make([]byte, 0, size)

	// Count
	countBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(countBuf, uint64(len(s.entries)))
	buf = append(buf, countBuf...)

	header := make([]byte, 8)
	for k, v := range s.entries {
		kb := []byte(k)
		binary.BigEndian.PutUint32(header[:4], uint32(len(kb)))
		binary.BigEndian.PutUint32(header[4:], uint32(len(v)))
		buf = append(buf, header...)
		buf = append(buf, kb...)
		buf = append(buf, v...)
	}

	return os.WriteFile(s.path, buf, 0600)
}

func (s *Store) loadFromDisk() {
	data, err := os.ReadFile(s.path)
	if err != nil || len(data) < 8 {
		return
	}

	count := int(binary.BigEndian.Uint64(data[:8]))
	offset := 8

	s.entries = make(map[string][]byte, count)
	s.keys = make([]string, 0, count)

	for i := 0; i < count && offset+8 <= len(data); i++ {
		klen := int(binary.BigEndian.Uint32(data[offset:]))
		vlen := int(binary.BigEndian.Uint32(data[offset+4:]))
		offset += 8

		if offset+klen+vlen > len(data) {
			break
		}

		k := string(data[offset : offset+klen])
		v := make([]byte, vlen)
		copy(v, data[offset+klen:offset+klen+vlen])
		offset += klen + vlen

		s.entries[k] = v
		s.keys = append(s.keys, k)
	}

	s.sorted = false
}

func (s *Store) Close() error {
	return s.Flush()
}

// StoreIndex is a secondary index backed by a Store.
type StoreIndex struct {
	Name   string
	Fields []string
	Unique bool
	store  *Store
}

// Row encoding using JSON for map[string]any (fast, no registration needed).

type rowEnvelope struct {
	ID        uint64         `json:"i"`
	CreatedAt int64          `json:"c"`
	UpdatedAt int64          `json:"u"`
	Version   uint64         `json:"v"`
	Data      map[string]any `json:"d"`
}

func EncodeRowBytes(row *Row) []byte {
	env := rowEnvelope{
		ID:        row.ID,
		CreatedAt: row.CreatedAt.UnixNano(),
		UpdatedAt: row.UpdatedAt.UnixNano(),
		Version:   row.Version,
		Data:      row.Data,
	}
	b, _ := json.Marshal(env)
	return b
}

func DecodeRowBytes(data []byte) (*Row, error) {
	var env rowEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}
	return &Row{
		ID:        env.ID,
		CreatedAt: unixNanoToTime(env.CreatedAt),
		UpdatedAt: unixNanoToTime(env.UpdatedAt),
		Version:   env.Version,
		Data:      env.Data,
	}, nil
}
