package storage

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcisbee/flop/internal/schema"
)

func TestScanRejectsCorruptPageSlot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.flop")
	tf, err := CreateTableFile(path, 1, 4)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	pageNumber, page, err := tf.AllocatePage()
	if err != nil {
		t.Fatalf("allocate page: %v", err)
	}
	if slot := page.InsertRow([]byte("row")); slot != 0 {
		t.Fatalf("insert row slot=%d", slot)
	}
	tf.MarkPageDirty(pageNumber)
	if err := tf.Close(); err != nil {
		t.Fatalf("close table: %v", err)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open table for corruption: %v", err)
	}
	var invalidOffset [2]byte
	binary.LittleEndian.PutUint16(invalidOffset[:], 1)
	slotOffset := int64(schema.FileHeaderSize + schema.PageHeaderSize)
	if _, err := f.WriteAt(invalidOffset[:], slotOffset); err != nil {
		_ = f.Close()
		t.Fatalf("corrupt slot: %v", err)
	}
	_ = f.Close()

	reopened, err := OpenTableFile(path, 4)
	if err != nil {
		t.Fatalf("open table header: %v", err)
	}
	defer func() { _ = reopened.file.Close() }()
	if _, err := reopened.ScanAllRows(); err == nil {
		t.Fatal("expected corrupt page slot to fail scan")
	}
}

func TestFindOrAllocatePageRejectsOversizedRow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "movies.flop")
	tf, err := CreateTableFile(path, 1, 8)
	if err != nil {
		t.Fatalf("create table file: %v", err)
	}
	t.Cleanup(func() { _ = tf.Close() })

	_, _, err = tf.FindOrAllocatePage(MaxRowDataSize() + 1)
	if !errors.Is(err, ErrRowTooLarge) {
		t.Fatalf("expected ErrRowTooLarge, got %v", err)
	}
	if tf.PageCount != 0 {
		t.Fatalf("expected no page allocation for oversized row, got PageCount=%d", tf.PageCount)
	}
}

func TestOpenTableFileRejectsMismatchedHeaderPageSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "movies.flop")
	tf, err := CreateTableFile(path, 1, 8)
	if err != nil {
		t.Fatalf("create table file: %v", err)
	}
	if err := tf.Close(); err != nil {
		t.Fatalf("close table file: %v", err)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open file for mutation: %v", err)
	}
	defer f.Close()

	var pageSize [2]byte
	binary.LittleEndian.PutUint16(pageSize[:], 4096)
	if _, err := f.WriteAt(pageSize[:], 6); err != nil {
		t.Fatalf("mutate header page size: %v", err)
	}

	_, err = OpenTableFile(path, 8)
	if !errors.Is(err, ErrPageSizeMismatch) {
		t.Fatalf("expected ErrPageSizeMismatch, got %v", err)
	}
}
