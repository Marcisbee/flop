package engine

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcisbee/flop/internal/schema"
)

func TestOpenRejectsSecondProcessForSameDataDir(t *testing.T) {
	dataDir := t.TempDir()
	db1 := openTestDB(t, dataDir, false, true)
	t.Cleanup(func() { _ = db1.Close() })

	db2 := NewDatabase(DatabaseConfig{DataDir: dataDir, SyncMode: "normal"})
	err := db2.Open(map[string]*schema.TableDef{"movies": testMovieTableDef(true)})
	if err == nil {
		_ = db2.Close()
		t.Fatal("expected second open against same data dir to fail")
	}
}

func TestGetHidesPendingBufferedInsert(t *testing.T) {
	db := openTestDB(t, t.TempDir(), false, true)
	t.Cleanup(func() { _ = db.Close() })
	ti := mustTable(t, db)

	txBuf := make(map[string]*walBufEntry)
	if _, err := ti.Insert(map[string]interface{}{
		"id":    "pending-1",
		"slug":  "pending-1",
		"title": "Pending",
		"genre": "drama",
	}, txBuf); err != nil {
		t.Fatalf("buffered insert: %v", err)
	}

	row, err := ti.Get("pending-1")
	if err != nil {
		t.Fatalf("get pending row: %v", err)
	}
	if row != nil {
		t.Fatal("expected pending buffered row to be hidden from point lookups")
	}
}

func TestDeleteCommitFailureRollsBackAndKeepsFiles(t *testing.T) {
	db := openTestDB(t, t.TempDir(), false, true)
	t.Cleanup(func() { _ = db.Close() })
	ti := mustTable(t, db)

	if _, err := ti.Insert(map[string]interface{}{
		"id":    "movie-1",
		"slug":  "movie-1",
		"title": "Movie",
		"genre": "drama",
	}, nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	fileDir := filepath.Join(db.GetDataDir(), "_files", "movies", "movie-1")
	if err := os.MkdirAll(fileDir, 0o755); err != nil {
		t.Fatalf("mkdir file dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fileDir, "poster.txt"), []byte("poster"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	testEnqueueCommitHook = func() error { return errors.New("commit failed") }
	t.Cleanup(func() { testEnqueueCommitHook = nil })

	ok, err := ti.Delete("movie-1", nil)
	if err == nil || ok {
		t.Fatalf("expected delete to fail before commit, got ok=%v err=%v", ok, err)
	}

	row, getErr := ti.Get("movie-1")
	if getErr != nil {
		t.Fatalf("get after failed delete: %v", getErr)
	}
	if row == nil {
		t.Fatal("expected row to be rolled back after failed delete commit")
	}
	if _, statErr := os.Stat(filepath.Join(fileDir, "poster.txt")); statErr != nil {
		t.Fatalf("expected row files to remain in place, stat err=%v", statErr)
	}
}

func TestArchiveCommitFailureLeavesLiveRowAndFiles(t *testing.T) {
	db := openReplayTestDBOrFatal(t, t.TempDir())
	defer func() { _ = db.Close() }()
	ti := db.GetTable("items")

	if _, err := ti.Insert(map[string]interface{}{
		"id": "id-base", "slug": "id-base", "title": "base", "value": float64(1),
	}, nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	fileDir := filepath.Join(db.GetDataDir(), "_files", "items", "id-base")
	if err := os.MkdirAll(fileDir, 0o755); err != nil {
		t.Fatalf("mkdir row files: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fileDir, "note.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write row file: %v", err)
	}

	testEnqueueCommitHook = func() error { return errors.New("commit failed") }
	t.Cleanup(func() { testEnqueueCommitHook = nil })

	record, err := ti.Archive("id-base", ArchiveOptions{}, nil)
	if err == nil || record != nil {
		t.Fatalf("expected archive commit to fail, got record=%v err=%v", record, err)
	}
	row, getErr := ti.Get("id-base")
	if getErr != nil {
		t.Fatalf("get after failed archive: %v", getErr)
	}
	if row == nil {
		t.Fatal("expected live row to remain after failed archive commit")
	}
	if _, statErr := os.Stat(filepath.Join(fileDir, "note.txt")); statErr != nil {
		t.Fatalf("expected live files to remain in place, stat err=%v", statErr)
	}
}

func openReplayTestDBOrFatal(t *testing.T, dataDir string) *Database {
	t.Helper()
	db, _, err := openReplayTestDB(dataDir)
	if err != nil {
		t.Fatalf("open replay test db: %v", err)
	}
	return db
}
