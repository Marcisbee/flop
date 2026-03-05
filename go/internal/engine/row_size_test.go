package engine

import (
	"errors"
	"strings"
	"testing"

	"github.com/marcisbee/flop/internal/storage"
)

func TestInsertRejectsOversizedRow(t *testing.T) {
	db := openTestDB(t, t.TempDir(), false, true)
	t.Cleanup(func() { _ = db.Close() })
	ti := mustTable(t, db)

	_, err := ti.Insert(map[string]interface{}{
		"id":    "id-oversized",
		"slug":  "slug-oversized",
		"title": strings.Repeat("x", storage.MaxRowDataSize()+1024),
		"genre": "drama",
	}, nil)
	if !errors.Is(err, storage.ErrRowTooLarge) {
		t.Fatalf("expected ErrRowTooLarge, got %v", err)
	}
	if got := ti.Count(); got != 0 {
		t.Fatalf("expected no inserted rows, got %d", got)
	}
}

func TestUpdateRejectsOversizedRowWithoutMutatingExistingRow(t *testing.T) {
	db := openTestDB(t, t.TempDir(), false, true)
	t.Cleanup(func() { _ = db.Close() })
	ti := mustTable(t, db)

	if _, err := ti.Insert(map[string]interface{}{
		"id":    "id-1",
		"slug":  "slug-1",
		"title": "small",
		"genre": "action",
	}, nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	_, err := ti.Update("id-1", map[string]interface{}{
		"title": strings.Repeat("y", storage.MaxRowDataSize()+1024),
	}, nil)
	if !errors.Is(err, storage.ErrRowTooLarge) {
		t.Fatalf("expected ErrRowTooLarge, got %v", err)
	}

	row, err := ti.Get("id-1")
	if err != nil {
		t.Fatalf("get row after failed update: %v", err)
	}
	if row == nil {
		t.Fatal("expected row to remain after failed oversized update")
	}
	if got := toString(row["title"]); got != "small" {
		t.Fatalf("expected original title to remain, got %q", got)
	}
}
