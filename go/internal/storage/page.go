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
	PageLSN    uint32
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
		PageLSN:    0,
	}
	p.writeHeader()
	return p
}

func (p *Page) readHeader() {
	p.PageNumber = binary.LittleEndian.Uint32(p.Data[0:4])
	p.SlotCount = binary.LittleEndian.Uint16(p.Data[4:6])
	p.FreeOff = binary.LittleEndian.Uint16(p.Data[6:8])
	p.PageLSN = binary.LittleEndian.Uint32(p.Data[8:12])
}

func (p *Page) writeHeader() {
	binary.LittleEndian.PutUint32(p.Data[0:4], p.PageNumber)
	binary.LittleEndian.PutUint16(p.Data[4:6], p.SlotCount)
	binary.LittleEndian.PutUint16(p.Data[6:8], p.FreeOff)
	binary.LittleEndian.PutUint32(p.Data[8:12], p.PageLSN)
}

// SetPageLSN updates the page-level LSN watermark stored in the page header.
// NOTE: Callers (InsertRow, UpdateRow, DeleteRow) already call writeHeader(),
// so we only update the field here and write the 4-byte LSN directly to avoid
// a redundant full header encode.
func (p *Page) SetPageLSN(lsn uint64) {
	if lsn == 0 {
		return
	}
	if uint64(p.PageLSN) >= lsn {
		return
	}
	p.PageLSN = uint32(lsn)
	binary.LittleEndian.PutUint32(p.Data[8:12], p.PageLSN)
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

// FreeSpace returns contiguous free bytes between the slot directory
// and the lowest live row payload.
func (p *Page) FreeSpace() int {
	slotDirEnd := schema.PageHeaderSize + int(p.SlotCount)*schema.SlotSize
	rowStart := int(p.rowDataStart())
	free := rowStart - slotDirEnd
	if free < 0 {
		return 0
	}
	return free
}

// InsertRow inserts row data into the page. Returns slot index or -1 if no space.
func (p *Page) InsertRow(rowData []byte) int {
	slotIndex := p.firstDeletedSlot()
	needsNewSlot := slotIndex < 0

	needed := len(rowData)
	if needsNewSlot {
		needed += schema.SlotSize
	}
	if p.FreeSpace() < needed {
		p.compactRows()
		if p.FreeSpace() < needed {
			return -1
		}
	}

	rowStart := p.rowDataStart() - uint16(len(rowData))
	copy(p.Data[rowStart:], rowData)

	if needsNewSlot {
		slotIndex = int(p.SlotCount)
		p.SlotCount++
		p.FreeOff = uint16(schema.PageHeaderSize + int(p.SlotCount)*schema.SlotSize)
	}
	p.SetSlot(slotIndex, rowStart, uint16(len(rowData)))

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
	size := uint16(len(p.Data))
	maxSlots := (size - schema.PageHeaderSize) / schema.SlotSize
	count := int(p.SlotCount)
	if count > int(maxSlots) {
		count = int(maxSlots)
	}
	for i := 0; i < count; i++ {
		off, length := p.GetSlot(i)
		if length == 0 {
			continue
		}
		end := off + length
		if end < off || end > size {
			continue // corrupted slot — skip
		}
		if !fn(i, p.Data[off:end]) {
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

// CanFit reports whether the page can fit rowDataSize bytes, accounting for
// reusable tombstoned slots and potential in-page compaction.
func (p *Page) CanFit(rowDataSize int) bool {
	if rowDataSize <= 0 {
		return false
	}
	reusableSlot := p.firstDeletedSlot() >= 0
	needed := rowDataSize
	if !reusableSlot {
		needed += schema.SlotSize
	}
	if p.FreeSpace() >= needed {
		return true
	}

	slotDirEnd := schema.PageHeaderSize + int(p.SlotCount)*schema.SlotSize
	if !reusableSlot {
		slotDirEnd += schema.SlotSize
	}
	freeAfterCompact := schema.PageSize - slotDirEnd - p.liveDataSize()
	return freeAfterCompact >= rowDataSize
}

func (p *Page) firstDeletedSlot() int {
	for i := 0; i < int(p.SlotCount); i++ {
		_, length := p.GetSlot(i)
		if length == 0 {
			return i
		}
	}
	return -1
}

func (p *Page) liveDataSize() int {
	total := 0
	for i := 0; i < int(p.SlotCount); i++ {
		_, length := p.GetSlot(i)
		total += int(length)
	}
	return total
}

func (p *Page) compactRows() {
	var next [schema.PageSize]byte
	writePos := schema.PageSize

	for i := 0; i < int(p.SlotCount); i++ {
		off, length := p.GetSlot(i)
		if length == 0 {
			continue
		}
		writePos -= int(length)
		copy(next[writePos:], p.Data[off:off+length])
		slotOff := schema.PageHeaderSize + i*schema.SlotSize
		binary.LittleEndian.PutUint16(next[slotOff:slotOff+2], uint16(writePos))
		binary.LittleEndian.PutUint16(next[slotOff+2:slotOff+4], length)
	}

	p.Data = next
	p.FreeOff = uint16(schema.PageHeaderSize + int(p.SlotCount)*schema.SlotSize)
	p.writeHeader()
}
