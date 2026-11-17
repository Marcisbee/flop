package engine

import (
	"errors"
	"fmt"
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
	for _, size := range []int{storage.MaxRowDataSize() + 1024, 1 << 16} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			dir := t.TempDir()
			db := openTestDB(t, dir, false, true)
			t.Cleanup(func() { _ = db.Close() })
			ti := mustTable(t, db)
			for _, id := range []string{"id-1", "neighbor"} {
				if _, err := ti.Insert(map[string]interface{}{"id": id, "slug": id, "title": "small", "genre": "action"}, nil); err != nil {
					t.Fatal(err)
				}
			}
			_, err := ti.Update("id-1", map[string]interface{}{"title": strings.Repeat("y", size)}, nil)
			if !errors.Is(err, storage.ErrRowTooLarge) {
				t.Fatalf("expected ErrRowTooLarge, got %v", err)
			}
			check := func() {
				t.Helper()
				for _, id := range []string{"id-1", "neighbor"} {
					row, err := ti.Get(id)
					if err != nil || row == nil || row["title"] != "small" {
						t.Fatalf("row %q changed after rejected update: %v, %v", id, row, err)
					}
				}
			}
			check()
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = openTestDB(t, dir, false, true)
			ti = mustTable(t, db)
			check()
		})
	}
}
