package flop

import (
	"archive/zip"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testProtectedSuperadminSentinel = "_superadmin.custom"

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

func TestBackupSnapshotSkipsProtectedAdminFiles(t *testing.T) {
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

	protectedFiles := map[string]string{
		backupSettingsRelPath:      `{"cron":"0 0 * * *"}`,
		emailSettingsBackupRelPath: `{"appName":"Strike"}`,
		testProtectedSuperadminSentinel:      `[{"email":"admin@example.com"}]`,
	}
	for rel, body := range protectedFiles {
		path := filepath.Join(db.GetDataDir(), filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}

	zipPath, err := db.backupManager.writeSnapshotZip()
	if err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	defer os.Remove(zipPath)

	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		t.Fatalf("open snapshot zip: %v", err)
	}
	defer reader.Close()

	for _, file := range reader.File {
		switch file.Name {
		case backupSettingsRelPath, emailSettingsBackupRelPath, testProtectedSuperadminSentinel:
			t.Fatalf("snapshot should not include protected admin data: %s", file.Name)
		}
	}
}

func TestBackupRestorePreservesProtectedAdminFiles(t *testing.T) {
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

	protectedPaths := map[string]string{
		backupSettingsRelPath:      `{"cron":"0 0 * * *","cronMaxKeep":1}`,
		emailSettingsBackupRelPath: `{"appName":"at-backup","templates":{}}`,
		testProtectedSuperadminSentinel:      `[{"email":"before@example.com"}]`,
	}
	for rel, body := range protectedPaths {
		path := filepath.Join(db.GetDataDir(), filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}

	key, err := db.backupManager.CreateManual(context.Background())
	if err != nil {
		t.Fatalf("create backup: %v", err)
	}

	if _, err := db.Table("items").Update("item1", map[string]any{"title": "after"}); err != nil {
		t.Fatalf("update: %v", err)
	}

	currentProtected := map[string]string{
		backupSettingsRelPath:      `{"cron":"0 1 * * *","cronMaxKeep":2}`,
		emailSettingsBackupRelPath: `{"appName":"keep-current","templates":{}}`,
		testProtectedSuperadminSentinel:      `[{"email":"current@example.com"}]`,
	}
	for rel, body := range currentProtected {
		path := filepath.Join(db.GetDataDir(), filepath.FromSlash(rel))
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("rewrite %s: %v", rel, err)
		}
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

	for rel, want := range currentProtected {
		path := filepath.Join(db.GetDataDir(), filepath.FromSlash(rel))
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		if string(got) != want {
			t.Fatalf("expected %s to remain %q, got %q", rel, want, string(got))
		}
	}
}
