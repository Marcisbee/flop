package storage

import (
	"encoding/binary"

	"github.com/marcisbee/flop/internal/schema"
)

// Page is a 4KB slotted page: header at top, slot directory growing forward,
// row data growing backward from end.
type Page struct {
	Data       [schema.PageSize]byte
	PageNumber uint32
	SlotCount  uint16
	FreeOff    uint16 // freeSpaceOffset in header
	Flags      byte
}

// NewPage creates a page from raw bytes (read from disk).
func NewPage(data []byte) *Page {
	p := &Page{}
	copy(p.Data[:], data)
	p.readHeader()
	return p
}

// CreatePage initializes an empty page with the given number.
func CreatePage(pageNumber uint32) *Page {
	p := &Page{
		PageNumber: pageNumber,
		SlotCount:  0,
		FreeOff:    schema.PageHeaderSize,
		Flags:      0,
	}
	p.writeHeader()
	return p
}

func (p *Page) readHeader() {
	p.PageNumber = binary.LittleEndian.Uint32(p.Data[0:4])
	p.SlotCount = binary.LittleEndian.Uint16(p.Data[4:6])
	p.FreeOff = binary.LittleEndian.Uint16(p.Data[6:8])
	p.Flags = p.Data[8]
}

func (p *Page) writeHeader() {
	binary.LittleEndian.PutUint32(p.Data[0:4], p.PageNumber)
	binary.LittleEndian.PutUint16(p.Data[4:6], p.SlotCount)
	binary.LittleEndian.PutUint16(p.Data[6:8], p.FreeOff)
	p.Data[8] = p.Flags
	p.Data[9] = 0
	p.Data[10] = 0
	p.Data[11] = 0
}

// GetSlot reads slot entry at the given index.
func (p *Page) GetSlot(index int) (offset, length uint16) {
	off := schema.PageHeaderSize + index*schema.SlotSize
	offset = binary.LittleEndian.Uint16(p.Data[off : off+2])
	length = binary.LittleEndian.Uint16(p.Data[off+2 : off+4])
	return
}

// SetSlot writes a slot entry.
func (p *Page) SetSlot(index int, offset, length uint16) {
	off := schema.PageHeaderSize + index*schema.SlotSize
	binary.LittleEndian.PutUint16(p.Data[off:off+2], offset)
	binary.LittleEndian.PutUint16(p.Data[off+2:off+4], length)
}

// rowDataStart returns the lowest byte used by row data.
func (p *Page) rowDataStart() uint16 {
	if p.SlotCount == 0 {
		return schema.PageSize
	}
	min := uint16(schema.PageSize)
	for i := 0; i < int(p.SlotCount); i++ {
		off, length := p.GetSlot(i)
		if length > 0 && off < min {
			min = off
		}
	}
	return min
}

// FreeSpace returns available bytes for new row data + slot entry.
func (p *Page) FreeSpace() int {
	slotDirEnd := schema.PageHeaderSize + int(p.SlotCount)*schema.SlotSize + schema.SlotSize
	rowStart := int(p.rowDataStart())
	free := rowStart - slotDirEnd
	if free < 0 {
		return 0
	}
	return free
}

// InsertRow inserts row data into the page. Returns slot index or -1 if no space.
func (p *Page) InsertRow(rowData []byte) int {
	needed := len(rowData) + schema.SlotSize
	if p.FreeSpace() < needed {
		return -1
	}

	rowStart := p.rowDataStart() - uint16(len(rowData))
	copy(p.Data[rowStart:], rowData)

	slotIndex := int(p.SlotCount)
	p.SetSlot(slotIndex, rowStart, uint16(len(rowData)))

	p.SlotCount++
	p.FreeOff = uint16(schema.PageHeaderSize + int(p.SlotCount)*schema.SlotSize)
	p.writeHeader()

	return slotIndex
}

// ReadRow reads row data at the given slot index.
func (p *Page) ReadRow(slotIndex int) []byte {
	if slotIndex >= int(p.SlotCount) {
		return nil
	}
	off, length := p.GetSlot(slotIndex)
	if length == 0 {
		return nil // deleted
	}
	result := make([]byte, length)
	copy(result, p.Data[off:off+length])
	return result
}

// DeleteRow marks a slot as deleted (tombstone).
func (p *Page) DeleteRow(slotIndex int) {
	if slotIndex >= int(p.SlotCount) {
		return
	}
	p.SetSlot(slotIndex, 0, 0)
	p.writeHeader()
}

// UpdateRow tries to update a row in place. Returns false if it doesn't fit.
func (p *Page) UpdateRow(slotIndex int, newData []byte) bool {
	if slotIndex >= int(p.SlotCount) {
		return false
	}
	off, length := p.GetSlot(slotIndex)
	if uint16(len(newData)) <= length {
		copy(p.Data[off:], newData)
		if uint16(len(newData)) < length {
			// Zero remaining bytes
			for i := off + uint16(len(newData)); i < off+length; i++ {
				p.Data[i] = 0
			}
		}
		p.SetSlot(slotIndex, off, uint16(len(newData)))
		return true
	}
	return false
}

// SlotEntry holds info for iterating valid slots.
type SlotEntry struct {
	SlotIndex int
	Data      []byte
}

// ForEachSlot calls fn for each non-deleted slot.
// The provided data slice points into the page buffer and is valid only
// for the duration of fn.
func (p *Page) ForEachSlot(fn func(slotIndex int, data []byte) bool) {
	for i := 0; i < int(p.SlotCount); i++ {
		off, length := p.GetSlot(i)
		if length == 0 {
			continue
		}
		if !fn(i, p.Data[off:off+length]) {
			return
		}
	}
}

// Slots returns all valid (non-deleted) slot entries.
func (p *Page) Slots() []SlotEntry {
	var entries []SlotEntry
	p.ForEachSlot(func(slotIndex int, raw []byte) bool {
		data := make([]byte, len(raw))
		copy(data, raw)
		entries = append(entries, SlotEntry{SlotIndex: slotIndex, Data: data})
		return true
	})
	return entries
}
