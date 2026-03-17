package flop

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestMediaIndexTracksUploadUsageAndDelete(t *testing.T) {
	tmp := t.TempDir()
	app := New(Config{DataDir: tmp, SyncMode: "normal"})
	Define(app, "users", func(s *SchemaBuilder) {
		s.String("id").Primary()
		s.FileSingle("avatar", "image/png").
			ImageMax("180x180").
			ImageFitCover().
			Thumbs("90x90", "180x180").
			DiscardOriginal()
	})

	db, err := app.Open()
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if _, err := db.Table("users").Insert(map[string]any{"id": "u1"}); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	ref, err := db.StoreFileForField("users", "u1", "avatar", "avatar.png", makeSolidPNG(t, 400, 300), "image/png")
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	if ref == nil {
		t.Fatal("expected file ref")
	}

	if _, err := os.Stat(filepath.Join(tmp, "_system", "media_index.json")); err != nil {
		t.Fatalf("expected persisted media index: %v", err)
	}

	provider := &EngineAdminProvider{DB: db}
	rows, total, err := provider.AdminMediaRows(50, 0, "", false)
	if err != nil {
		t.Fatalf("admin media rows before row ref: %v", err)
	}
	if total != 1 || len(rows) != 1 {
		t.Fatalf("expected one media row before row ref, got total=%d len=%d", total, len(rows))
	}
	if !rows[0].Orphaned {
		t.Fatalf("expected uploaded file to be orphaned before row reference")
	}

	if _, err := db.Table("users").Update("u1", map[string]any{"avatar": *ref}); err != nil {
		t.Fatalf("update row avatar: %v", err)
	}

	rows, total, err = provider.AdminMediaRows(50, 0, "", false)
	if err != nil {
		t.Fatalf("admin media rows after row ref: %v", err)
	}
	if total != 1 || len(rows) != 1 {
		t.Fatalf("expected one media row after row ref, got total=%d len=%d", total, len(rows))
	}
	if rows[0].Orphaned {
		t.Fatalf("expected file to be linked after row update")
	}
	if rows[0].TableName != "users" || rows[0].RowID != "u1" || rows[0].FieldName != "avatar" {
		t.Fatalf("unexpected usage row: %#v", rows[0])
	}

	deleted, err := db.Table("users").Delete("u1")
	if err != nil || !deleted {
		t.Fatalf("delete row: deleted=%v err=%v", deleted, err)
	}

	rows, total, err = provider.AdminMediaRows(50, 0, "", false)
	if err != nil {
		t.Fatalf("admin media rows after delete: %v", err)
	}
	if total != 0 || len(rows) != 0 {
		t.Fatalf("expected no media rows after deleting row, got total=%d len=%d", total, len(rows))
	}
}

func TestMediaIndexIgnoresStringReferences(t *testing.T) {
	tmp := t.TempDir()
	app := New(Config{DataDir: tmp, SyncMode: "normal"})
	Define(app, "users", func(s *SchemaBuilder) {
		s.String("id").Primary()
		s.FileSingle("avatar", "image/png")
	})
	Define(app, "posts", func(s *SchemaBuilder) {
		s.String("id").Primary()
		s.String("body")
	})

	db, err := app.Open()
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if _, err := db.Table("users").Insert(map[string]any{"id": "u1"}); err != nil {
		t.Fatalf("insert user: %v", err)
	}
	if _, err := db.Table("posts").Insert(map[string]any{"id": "p1", "body": ""}); err != nil {
		t.Fatalf("insert post: %v", err)
	}
	ref, err := db.StoreFileForField("users", "u1", "avatar", "avatar.png", makeSolidPNG(t, 200, 200), "image/png")
	if err != nil {
		t.Fatalf("store avatar: %v", err)
	}
	if _, err := db.Table("posts").Update("p1", map[string]any{"body": "![avatar](" + ref.URL + ")"}); err != nil {
		t.Fatalf("update post body: %v", err)
	}

	provider := &EngineAdminProvider{DB: db}
	rows, total, err := provider.AdminMediaRows(50, 0, "", false)
	if err != nil {
		t.Fatalf("admin media rows: %v", err)
	}
	if total != 1 || len(rows) != 1 {
		t.Fatalf("expected one media row, got total=%d len=%d", total, len(rows))
	}
	if !rows[0].Orphaned {
		t.Fatalf("expected string-only reference to be ignored, got %#v", rows[0])
	}
}

func TestReplacingFileSingleRemovesOldFileAndBlocksOldURL(t *testing.T) {
	tmp := t.TempDir()
	app := New(Config{DataDir: tmp, SyncMode: "normal"})
	Define(app, "users", func(s *SchemaBuilder) {
		s.String("id").Primary()
		s.FileSingle("avatar", "image/png").
			ImageMax("180x180").
			ImageFitCover().
			Thumbs("90x90", "180x180").
			DiscardOriginal()
	})

	db, err := app.Open()
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if _, err := db.Table("users").Insert(map[string]any{"id": "u1"}); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	firstRef, err := db.StoreFileForField("users", "u1", "avatar", "avatar.png", makeSolidPNG(t, 400, 300), "image/png")
	if err != nil {
		t.Fatalf("store first file: %v", err)
	}
	if _, err := db.Table("users").Update("u1", map[string]any{"avatar": *firstRef}); err != nil {
		t.Fatalf("set first avatar: %v", err)
	}

	secondRef, err := db.StoreFileForField("users", "u1", "avatar", "avatar.png", makeSolidPNGColor(t, 400, 300, 240, 80, 80), "image/png")
	if err != nil {
		t.Fatalf("store second file: %v", err)
	}
	if _, err := db.Table("users").Update("u1", map[string]any{"avatar": *secondRef}); err != nil {
		t.Fatalf("replace avatar: %v", err)
	}

	if _, err := os.Stat(filepath.Join(tmp, firstRef.Path)); !os.IsNotExist(err) {
		t.Fatalf("expected old file to be removed, got err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(tmp, secondRef.Path)); err != nil {
		t.Fatalf("expected new file on disk: %v", err)
	}

	provider := &EngineAdminProvider{DB: db}
	rows, total, err := provider.AdminMediaRows(50, 0, "", false)
	if err != nil {
		t.Fatalf("admin media rows: %v", err)
	}
	if total != 1 || len(rows) != 1 {
		t.Fatalf("expected one media row after replacement, got total=%d len=%d", total, len(rows))
	}
	if rows[0].Path != secondRef.Path {
		t.Fatalf("expected only new file in media rows, got %#v", rows[0])
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, firstRef.URL, nil)
	db.FileHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected old file URL to 404, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, secondRef.URL, nil)
	db.FileHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected new file URL to serve, got %d", rec.Code)
	}
}
