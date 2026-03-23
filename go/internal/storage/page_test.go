package storage

import (
	"bytes"
	"testing"
)

func TestPageInsertReusesDeletedSlot(t *testing.T) {
	p := CreatePage(0)
	s0 := p.InsertRow([]byte("row-0"))
	s1 := p.InsertRow([]byte("row-1"))
	if s0 != 0 || s1 != 1 {
		t.Fatalf("unexpected initial slots: %d %d", s0, s1)
	}

	p.DeleteRow(0)
	s2 := p.InsertRow([]byte("row-2"))
	if s2 != 0 {
		t.Fatalf("expected deleted slot 0 reuse, got %d", s2)
	}
	if p.SlotCount != 2 {
		t.Fatalf("expected slot count to remain 2, got %d", p.SlotCount)
	}
}

func TestPageInsertCompactsTombstones(t *testing.T) {
	p := CreatePage(0)
	rowA := bytes.Repeat([]byte{0xAA}, 18000)
	rowB := bytes.Repeat([]byte{0xBB}, 18000)
	rowC := bytes.Repeat([]byte{0xCC}, 18000)

	sA := p.InsertRow(rowA)
	sB := p.InsertRow(rowB)
	sC := p.InsertRow(rowC)
	if sA != 0 || sB != 1 || sC != 2 {
		t.Fatalf("unexpected slots: %d %d %d", sA, sB, sC)
	}

	p.DeleteRow(1) // create a large hole not visible in contiguous free space

	large := bytes.Repeat([]byte{0xDD}, 15000)
	if p.FreeSpace() >= len(large) {
		t.Fatalf("expected fragmented page before compaction, free=%d", p.FreeSpace())
	}
	if !p.CanFit(len(large)) {
		t.Fatalf("expected CanFit true due to compaction potential")
	}

	slot := p.InsertRow(large)
	if slot != 1 {
		t.Fatalf("expected reuse of deleted slot 1, got %d", slot)
	}
	got := p.ReadRow(slot)
	if !bytes.Equal(got, large) {
		t.Fatalf("inserted row mismatch after compaction")
	}
}
