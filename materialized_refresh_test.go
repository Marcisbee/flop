package flop

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/storage"
)

func TestRefreshMaterializedRepairsGhostPrimaryIndex(t *testing.T) {
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})
	Materialized(app, "leaderboard", func(s *MaterializedBuilder) {
		s.String("key").Primary()
		s.JSON("payload")
		s.RefreshOnStartup(false)
		s.Refresh(func(db *Database) error {
			return db.Table("leaderboard").ReplaceAll([]map[string]any{
				{"key": "weekly_leaderboard:meta", "payload": map[string]any{"ok": true}},
				{"key": "weekly_leaderboard:current:1", "payload": []any{"a"}},
			})
		})
	})

	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.RefreshMaterialized("leaderboard"); err != nil {
		t.Fatalf("initial refresh: %v", err)
	}

	raw := db.db.Tables["leaderboard"]
	idx := primaryIndexForTest(t, raw)
	ptr, ok := idx.Get("weekly_leaderboard:meta")
	if !ok {
		t.Fatal("meta pointer missing")
	}
	if deleted, err := db.Table("leaderboard").Delete("weekly_leaderboard:meta"); err != nil || !deleted {
		t.Fatalf("delete meta: deleted=%v err=%v", deleted, err)
	}
	idx.Set("weekly_leaderboard:meta", ptr)

	if got := db.Table("leaderboard").Count(); got != 1 {
		t.Fatalf("count with repaired read path = %d, want 1", got)
	}
	rows, err := db.Table("leaderboard").Scan(10, 0)
	if err != nil {
		t.Fatalf("scan before repair: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("scan before repair len = %d, want 1", len(rows))
	}
	if row, _ := db.Table("leaderboard").Get("weekly_leaderboard:meta"); row != nil {
		t.Fatalf("ghost meta row should remain unreadable until refresh, got %#v", row)
	}

	if err := db.RefreshMaterialized("leaderboard"); err != nil {
		t.Fatalf("refresh after ghost index: %v", err)
	}

	rows, err = db.Table("leaderboard").Scan(10, 0)
	if err != nil {
		t.Fatalf("scan after repair: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("scan after repair len = %d, want 2", len(rows))
	}
	if row, _ := db.Table("leaderboard").Get("weekly_leaderboard:meta"); row == nil {
		t.Fatal("meta row missing after repair")
	}
	if got := db.Table("leaderboard").Count(); got != 2 {
		t.Fatalf("count after repair = %d, want 2", got)
	}
}

func primaryIndexForTest(t *testing.T, table any) *storage.HashIndex {
	t.Helper()
	v := reflect.ValueOf(table)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		t.Fatal("table must be a non-nil pointer")
	}
	field := v.Elem().FieldByName("primaryIndex")
	if !field.IsValid() {
		t.Fatal("primaryIndex field missing")
	}
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface().(*storage.HashIndex)
}

var _ schema.RowPointer
