package flop

import (
	"archive/zip"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLocalBackupCreateAndRestore(t *testing.T) {
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})

	Define(app, "items", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.String("title").Required()
	})

	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Table("items").Insert(map[string]any{
		"id":    "item1",
		"title": "before",
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	filePath := filepath.Join(db.GetDataDir(), "_files", "items", "item1", "image", "sample.txt")
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		t.Fatalf("mkdir file dir: %v", err)
	}
	if err := os.WriteFile(filePath, []byte("hello backup"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	key, err := db.backupManager.CreateManual(context.Background())
	if err != nil {
		t.Fatalf("create backup: %v", err)
	}

	if err := db.backupManager.Delete(context.Background(), "missing.zip"); err == nil {
		t.Fatal("expected delete missing backup to fail")
	}

	if _, err := db.Table("items").Update("item1", map[string]any{"title": "after"}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := os.Remove(filePath); err != nil {
		t.Fatalf("remove file: %v", err)
	}

	if err := db.backupManager.Restore(context.Background(), key); err != nil {
		t.Fatalf("restore backup: %v", err)
	}

	row, err := db.Table("items").Get("item1")
	if err != nil {
		t.Fatalf("get restored row: %v", err)
	}
	if got := row["title"]; got != "before" {
		t.Fatalf("expected restored title %q, got %#v", "before", got)
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(data) != "hello backup" {
		t.Fatalf("expected restored file contents %q, got %q", "hello backup", string(data))
	}
}

func TestBackupSnapshotSkipsSystemBackupZipArtifacts(t *testing.T) {
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})

	Define(app, "items", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.String("title").Required()
	})

	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Table("items").Insert(map[string]any{
		"id":    "item1",
		"title": "before",
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	systemDir := filepath.Join(db.GetDataDir(), "_system")
	if err := os.MkdirAll(systemDir, 0o755); err != nil {
		t.Fatalf("mkdir _system: %v", err)
	}
	if err := os.WriteFile(filepath.Join(systemDir, "backup-stray.zip"), []byte("should not be backed up"), 0o644); err != nil {
		t.Fatalf("write stray backup: %v", err)
	}

	zipPath, err := db.backupManager.writeSnapshotZip()
	if err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	defer os.Remove(zipPath)

	if strings.HasPrefix(filepath.Clean(zipPath), filepath.Clean(db.GetDataDir())+string(os.PathSeparator)) {
		t.Fatalf("expected temp backup zip to be created outside data dir, got %s", zipPath)
	}

	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		t.Fatalf("open snapshot zip: %v", err)
	}
	defer reader.Close()

	for _, file := range reader.File {
		if file.Name == "_system/backup-stray.zip" {
			t.Fatalf("snapshot should not include stray backup zips from _system")
		}
	}
}
