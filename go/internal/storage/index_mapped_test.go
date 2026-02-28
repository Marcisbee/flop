package storage

import (
	"path/filepath"
	"testing"

	"github.com/marcisbee/flop/internal/schema"
)

func TestMappedIndexReadWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "users.midx")

	idx := NewHashIndex()
	idx.Set("a", schema.RowPointer{PageNumber: 1, SlotIndex: 1})
	idx.Set("b", schema.RowPointer{PageNumber: 2, SlotIndex: 2})
	idx.Set("c", schema.RowPointer{PageNumber: 3, SlotIndex: 3})

	if err := WriteMappedIndexFile(path, idx); err != nil {
		t.Fatalf("write mapped: %v", err)
	}

	mapped, err := ReadMappedIndexFile(path)
	if err != nil {
		t.Fatalf("read mapped: %v", err)
	}

	if got := mapped.Size(); got != 3 {
		t.Fatalf("size = %d, want 3", got)
	}
	if p, ok := mapped.Get("b"); !ok || p.PageNumber != 2 || p.SlotIndex != 2 {
		t.Fatalf("get b = %+v,%v", p, ok)
	}
}

func TestMappedIndexOverlayMutations(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tweets.midx")

	base := NewHashIndex()
	base.Set("x", schema.RowPointer{PageNumber: 10, SlotIndex: 1})
	base.Set("y", schema.RowPointer{PageNumber: 20, SlotIndex: 2})
	if err := WriteMappedIndexFile(path, base); err != nil {
		t.Fatalf("write mapped: %v", err)
	}

	idx, err := ReadMappedIndexFile(path)
	if err != nil {
		t.Fatalf("read mapped: %v", err)
	}

	// Override existing key, add new key, delete existing key.
	idx.Set("x", schema.RowPointer{PageNumber: 99, SlotIndex: 9})
	idx.Set("z", schema.RowPointer{PageNumber: 30, SlotIndex: 3})
	if !idx.Delete("y") {
		t.Fatalf("delete existing key y = false")
	}

	if got := idx.Size(); got != 2 {
		t.Fatalf("size = %d, want 2", got)
	}
	if p, ok := idx.Get("x"); !ok || p.PageNumber != 99 {
		t.Fatalf("get x = %+v,%v", p, ok)
	}
	if _, ok := idx.Get("y"); ok {
		t.Fatalf("get y should be deleted")
	}
	if p, ok := idx.Get("z"); !ok || p.PageNumber != 30 {
		t.Fatalf("get z = %+v,%v", p, ok)
	}
}
