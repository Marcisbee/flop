package storage

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/marcisbee/flop/internal/schema"
)

const fileFormatVersion = 1

// TableFile manages a per-table .flop file.
type TableFile struct {
	Path          string
	file          *os.File
	PageCount     uint32
	TotalRows     uint32
	SchemaVersion uint16
	pageCache     *PageCache
	lastFreePage  int32 // hint for free space search; -1 = none
	closed        bool
}

// OpenTableFile opens an existing .flop file.
func OpenTableFile(path string, maxCachePages int) (*TableFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	tf := &TableFile{Path: path, file: f, lastFreePage: -1}
	if err := tf.readFileHeader(); err != nil {
		f.Close()
		return nil, err
	}
	tf.pageCache = NewPageCache(f, maxCachePages)
	return tf, nil
}

// CreateTableFile creates a new .flop file.
func CreateTableFile(path string, schemaVersion uint16, maxCachePages int) (*TableFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	tf := &TableFile{
		Path:          path,
		file:          f,
		PageCount:     0,
		TotalRows:     0,
		SchemaVersion: schemaVersion,
		lastFreePage:  -1,
	}
	if err := tf.writeFileHeader(); err != nil {
		f.Close()
		return nil, err
	}
	tf.pageCache = NewPageCache(f, maxCachePages)
	return tf, nil
}

func (tf *TableFile) readFileHeader() error {
	buf := make([]byte, schema.FileHeaderSize)
	n, err := tf.file.ReadAt(buf, 0)
	if err != nil && n < schema.FileHeaderSize {
		return fmt.Errorf("read header: %w", err)
	}
	// Verify magic
	for i := 0; i < 4; i++ {
		if buf[i] != schema.TableFileMagic[i] {
			return fmt.Errorf("invalid table file: bad magic at %s", tf.Path)
		}
	}
	tf.PageCount = binary.LittleEndian.Uint32(buf[8:12])
	tf.TotalRows = binary.LittleEndian.Uint32(buf[12:16])
	tf.SchemaVersion = binary.LittleEndian.Uint16(buf[16:18])
	return nil
}

func (tf *TableFile) writeFileHeader() error {
	buf := make([]byte, schema.FileHeaderSize)
	copy(buf[0:4], schema.TableFileMagic[:])
	binary.LittleEndian.PutUint16(buf[4:6], fileFormatVersion)
	binary.LittleEndian.PutUint16(buf[6:8], schema.PageSize)
	binary.LittleEndian.PutUint32(buf[8:12], tf.PageCount)
	binary.LittleEndian.PutUint32(buf[12:16], tf.TotalRows)
	binary.LittleEndian.PutUint16(buf[16:18], tf.SchemaVersion)
	_, err := tf.file.WriteAt(buf, 0)
	return err
}

// AllocatePage appends a new empty page.
func (tf *TableFile) AllocatePage() (uint32, *Page, error) {
	pageNumber := tf.PageCount
	page := CreatePage(pageNumber)

	offset := int64(schema.FileHeaderSize) + int64(pageNumber)*int64(schema.PageSize)
	if _, err := tf.file.WriteAt(page.Data[:], offset); err != nil {
		return 0, nil, err
	}

	tf.PageCount++
	if err := tf.writeFileHeader(); err != nil {
		return 0, nil, err
	}

	tf.pageCache.PutPage(pageNumber, page)
	return pageNumber, page, nil
}

// GetPage returns a page from cache or disk.
func (tf *TableFile) GetPage(pageNumber uint32) (*Page, error) {
	return tf.pageCache.GetPage(pageNumber)
}

// MarkPageDirty marks a page as needing flush.
func (tf *TableFile) MarkPageDirty(pageNumber uint32) {
	tf.pageCache.MarkDirty(pageNumber)
}

// Flush writes all dirty pages and the header to disk.
func (tf *TableFile) Flush() error {
	if err := tf.pageCache.FlushAll(); err != nil {
		return err
	}
	if err := tf.writeFileHeader(); err != nil {
		return err
	}
	return tf.file.Sync()
}

// Close flushes and closes the file.
func (tf *TableFile) Close() error {
	if tf.closed {
		return nil
	}
	tf.closed = true
	if err := tf.pageCache.FlushAll(); err != nil {
		tf.file.Close()
		return err
	}
	if err := tf.writeFileHeader(); err != nil {
		tf.file.Close()
		return err
	}
	return tf.file.Close()
}

// ScanAllRows yields all non-deleted rows from all pages.
type ScannedRow struct {
	PageNumber uint32
	SlotIndex  int
	Data       []byte
}

// ForEachRow iterates all non-deleted rows in table order.
// The row data slice is only valid during the callback.
func (tf *TableFile) ForEachRow(fn func(ScannedRow) bool) error {
	for p := uint32(0); p < tf.PageCount; p++ {
		page, err := tf.GetPage(p)
		if err != nil {
			continue // skip unreadable pages (truncated file, I/O error)
		}
		keepGoing := true
		page.ForEachSlot(func(slotIndex int, data []byte) bool {
			keepGoing = fn(ScannedRow{
				PageNumber: p,
				SlotIndex:  slotIndex,
				Data:       data,
			})
			return keepGoing
		})
		if !keepGoing {
			return nil
		}
	}
	return nil
}

func (tf *TableFile) ScanAllRows() ([]ScannedRow, error) {
	var rows []ScannedRow
	err := tf.ForEachRow(func(scanned ScannedRow) bool {
		data := make([]byte, len(scanned.Data))
		copy(data, scanned.Data)
		rows = append(rows, ScannedRow{
			PageNumber: scanned.PageNumber,
			SlotIndex:  scanned.SlotIndex,
			Data:       data,
		})
		return true
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// FindOrAllocatePage finds a page with enough free space or allocates a new one.
func (tf *TableFile) FindOrAllocatePage(rowDataSize int) (uint32, *Page, error) {
	// Fast path: try the last known free page
	if tf.lastFreePage >= 0 && uint32(tf.lastFreePage) < tf.PageCount {
		page, err := tf.GetPage(uint32(tf.lastFreePage))
		if err == nil && page.CanFit(rowDataSize) {
			return uint32(tf.lastFreePage), page, nil
		}
	}

	// Try the last page
	if tf.PageCount > 0 {
		lastPage := tf.PageCount - 1
		if int32(lastPage) != tf.lastFreePage {
			page, err := tf.GetPage(lastPage)
			if err == nil && page.CanFit(rowDataSize) {
				tf.lastFreePage = int32(lastPage)
				return lastPage, page, nil
			}
		}
	}

	// Allocate new page
	pageNum, page, err := tf.AllocatePage()
	if err != nil {
		return 0, nil, err
	}
	tf.lastFreePage = int32(pageNum)
	return pageNum, page, nil
}
