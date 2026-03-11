package flop

import (
	"encoding/binary"
	"hash/crc32"
)

// Page sizes and layout constants.
const (
	PageSize    = 4096
	MaxKeySize  = 256
	MaxValSize  = PageSize - pageHeaderSize - 32 // leave room for one entry
	pageHeaderSize = 24
)

// Page types.
const (
	PageFree     uint8 = 0
	PageInternal uint8 = 1
	PageLeaf     uint8 = 2
	PageOverflow uint8 = 3
	PageMeta     uint8 = 4
)

// Page is a fixed-size 4KB block. The layout:
//   [0:4]   checksum (CRC32)
//   [4:5]   page type
//   [5:7]   num entries
//   [7:8]   flags
//   [8:16]  page id
//   [16:24] overflow page id (0 = none)
//   [24:]   entries
type Page [PageSize]byte

func (p *Page) Checksum() uint32   { return binary.BigEndian.Uint32(p[0:4]) }
func (p *Page) Type() uint8        { return p[4] }
func (p *Page) NumEntries() uint16 { return binary.BigEndian.Uint16(p[5:7]) }
func (p *Page) Flags() uint8       { return p[7] }
func (p *Page) PageID() uint64     { return binary.BigEndian.Uint64(p[8:16]) }
func (p *Page) OverflowID() uint64 { return binary.BigEndian.Uint64(p[16:24]) }

func (p *Page) SetType(t uint8)        { p[4] = t }
func (p *Page) SetNumEntries(n uint16) { binary.BigEndian.PutUint16(p[5:7], n) }
func (p *Page) SetFlags(f uint8)       { p[7] = f }
func (p *Page) SetPageID(id uint64)    { binary.BigEndian.PutUint64(p[8:16], id) }
func (p *Page) SetOverflowID(id uint64) { binary.BigEndian.PutUint64(p[16:24], id) }

func (p *Page) ComputeChecksum() uint32 {
	return crc32.ChecksumIEEE(p[4:])
}

func (p *Page) UpdateChecksum() {
	binary.BigEndian.PutUint32(p[0:4], p.ComputeChecksum())
}

func (p *Page) ValidateChecksum() bool {
	return p.Checksum() == p.ComputeChecksum()
}

// Entry layout within a page (after header):
//   [0:2]  key length
//   [2:4]  value length
//   [4:4+klen] key
//   [4+klen:4+klen+vlen] value
// For internal nodes, value is an 8-byte child page ID.

// EntryAt returns key and value at the given index within the page.
func (p *Page) EntryAt(idx int) (key, val []byte) {
	offset := pageHeaderSize
	for i := 0; i < idx; i++ {
		klen := int(binary.BigEndian.Uint16(p[offset : offset+2]))
		vlen := int(binary.BigEndian.Uint16(p[offset+2 : offset+4]))
		offset += 4 + klen + vlen
	}
	klen := int(binary.BigEndian.Uint16(p[offset : offset+2]))
	vlen := int(binary.BigEndian.Uint16(p[offset+2 : offset+4]))
	key = p[offset+4 : offset+4+klen]
	val = p[offset+4+klen : offset+4+klen+vlen]
	return
}

// AppendEntry adds a key-value pair to the page. Returns false if no space.
func (p *Page) AppendEntry(key, val []byte) bool {
	n := p.NumEntries()
	offset := p.usedBytes()
	needed := 4 + len(key) + len(val)
	if offset+needed > PageSize {
		return false
	}
	binary.BigEndian.PutUint16(p[offset:offset+2], uint16(len(key)))
	binary.BigEndian.PutUint16(p[offset+2:offset+4], uint16(len(val)))
	copy(p[offset+4:], key)
	copy(p[offset+4+len(key):], val)
	p.SetNumEntries(n + 1)
	return true
}

// usedBytes returns total bytes used including header and all entries.
func (p *Page) usedBytes() int {
	offset := pageHeaderSize
	n := int(p.NumEntries())
	for i := 0; i < n; i++ {
		klen := int(binary.BigEndian.Uint16(p[offset : offset+2]))
		vlen := int(binary.BigEndian.Uint16(p[offset+2 : offset+4]))
		offset += 4 + klen + vlen
	}
	return offset
}

// FreeSpace returns available bytes in this page.
func (p *Page) FreeSpace() int {
	return PageSize - p.usedBytes()
}

// ClearEntries resets the page to have zero entries.
func (p *Page) ClearEntries() {
	p.SetNumEntries(0)
}

// AllEntries returns all key-value pairs in the page.
func (p *Page) AllEntries() (keys, vals [][]byte) {
	n := int(p.NumEntries())
	keys = make([][]byte, n)
	vals = make([][]byte, n)
	offset := pageHeaderSize
	for i := 0; i < n; i++ {
		klen := int(binary.BigEndian.Uint16(p[offset : offset+2]))
		vlen := int(binary.BigEndian.Uint16(p[offset+2 : offset+4]))
		keys[i] = make([]byte, klen)
		vals[i] = make([]byte, vlen)
		copy(keys[i], p[offset+4:offset+4+klen])
		copy(vals[i], p[offset+4+klen:offset+4+klen+vlen])
		offset += 4 + klen + vlen
	}
	return
}

// InsertEntryAt inserts a key-value pair at the given index, shifting existing entries right.
func (p *Page) InsertEntryAt(idx int, key, val []byte) bool {
	needed := 4 + len(key) + len(val)
	if p.FreeSpace() < needed {
		return false
	}
	// Get all existing entries
	n := int(p.NumEntries())
	keys, vals := p.AllEntries()

	// Rebuild page with new entry inserted
	p.ClearEntries()
	for i := 0; i < n+1; i++ {
		if i == idx {
			p.AppendEntry(key, val)
		}
		if i < n {
			ai := i
			if i >= idx {
				ai = i
			}
			if i < idx {
				p.AppendEntry(keys[ai], vals[ai])
			} else {
				p.AppendEntry(keys[ai], vals[ai])
			}
		}
	}
	return true
}

// SetEntryAt replaces the entry at the given index.
func (p *Page) SetEntryAt(idx int, key, val []byte) bool {
	n := int(p.NumEntries())
	keys, vals := p.AllEntries()
	p.ClearEntries()
	for i := 0; i < n; i++ {
		if i == idx {
			if !p.AppendEntry(key, val) {
				return false
			}
		} else {
			if !p.AppendEntry(keys[i], vals[i]) {
				return false
			}
		}
	}
	return true
}

// RemoveEntryAt removes the entry at the given index.
func (p *Page) RemoveEntryAt(idx int) {
	n := int(p.NumEntries())
	keys, vals := p.AllEntries()
	p.ClearEntries()
	for i := 0; i < n; i++ {
		if i != idx {
			p.AppendEntry(keys[i], vals[i])
		}
	}
}
