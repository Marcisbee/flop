package storage

import (
	"fmt"
	"path/filepath"
	"slices"
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

// The same ordered postings must survive mapped reads, overlay mutations and
// another checkpoint/reopen. Shared prefixes, binary keys and misses exercise
// both directions of the key search without depending on its implementation.
func TestMappedMultiIndexReadWriteAndOverlay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "likes.smidx")
	idx := NewMultiIndex()
	keys := []string{"", "a", "a\x00", "a\x00b", "tenant/shared/01", "tenant/shared/02", "é", "\xff"}
	want := make(map[string][]schema.RowPointer)
	for i, key := range keys {
		for j := 0; j <= i; j++ {
			p := schema.RowPointer{PageNumber: uint32(i + 1), SlotIndex: uint16(j)}
			idx.Add(key, p)
			want[key] = append(want[key], p)
		}
	}
	for cycle := 0; cycle < 3; cycle++ {
		if err := WriteMappedMultiIndexFile(path, idx); err != nil {
			t.Fatal(err)
		}
		idx.Clear()
		var err error
		idx, err = ReadMappedMultiIndexFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range append(append([]string{}, keys...), "a\x00a", "tenant/shared/015", "zz", "\xffz") {
			if got := idx.GetAll(key); !slices.Equal(got, want[key]) {
				t.Fatalf("cycle %d key %q: got %v, want %v", cycle, key, got, want[key])
			}
		}
		for _, key := range keys {
			old := want[key][0]
			idx.Delete(key, old)
			want[key] = want[key][1:]
			p := schema.RowPointer{PageNumber: uint32(100 + cycle), SlotIndex: uint16(len(key))}
			idx.Add(key, p)
			want[key] = append(want[key], p)
			if got := idx.GetAll(key); !slices.Equal(got, want[key]) {
				t.Fatalf("overlay cycle %d key %q: got %v, want %v", cycle, key, got, want[key])
			}
		}
	}
	idx.Clear()
}

func TestMultiIndexGetAllReturnsSnapshot(t *testing.T) {
	for _, mapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("mapped=%t", mapped), func(t *testing.T) {
			idx := NewMultiIndex()
			want := schema.RowPointer{PageNumber: 7, SlotIndex: 3}
			idx.Add("k", want)
			if mapped {
				path := filepath.Join(t.TempDir(), "index.smidx")
				if err := WriteMappedMultiIndexFile(path, idx); err != nil {
					t.Fatal(err)
				}
				var err error
				idx, err = ReadMappedMultiIndexFile(path)
				if err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(idx.Clear)
			got := idx.GetAll("k")
			got[0] = schema.RowPointer{PageNumber: 99, SlotIndex: 9}
			current := idx.GetAll("k")
			if len(current) != 1 || current[0] != want {
				t.Fatalf("GetAll exposed mutable index storage: got %+v, want %+v", current, want)
			}
		})
	}
}
