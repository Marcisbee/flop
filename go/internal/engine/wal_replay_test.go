package engine

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/marcisbee/flop/internal/schema"
)

func replayTestDefs() map[string]*schema.TableDef {
	fields := []schema.CompiledField{
		{Name: "id", Kind: schema.KindString, Required: true},
		{Name: "slug", Kind: schema.KindString, Required: true},
		{Name: "title", Kind: schema.KindString, Required: true},
		{Name: "value", Kind: schema.KindNumber},
	}
	return map[string]*schema.TableDef{
		"items": {
			Name:           "items",
			CompiledSchema: schema.NewCompiledSchema(fields),
			Indexes: []schema.IndexDef{
				{Fields: []string{"slug"}, Unique: true, Type: schema.IndexTypeHash},
			},
		},
	}
}

func openReplayTestDB(dataDir string) (*Database, *TableInstance, error) {
	db := NewDatabase(DatabaseConfig{DataDir: dataDir, SyncMode: "full"})
	if err := db.Open(replayTestDefs()); err != nil {
		return nil, nil, err
	}
	ti := db.GetTable("items")
	if ti == nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("items table missing")
	}
	return db, ti, nil
}

func TestWALReplayCommittedInsert(t *testing.T) {
	dataDir := t.TempDir()
	runReplayCrashHelper(t, dataDir, "insert", "insert_after_commit")

	db, ti, err := openReplayTestDB(dataDir)
	if err != nil {
		t.Fatalf("open after crash: %v", err)
	}
	defer func() { _ = db.Close() }()

	row, err := ti.Get("id-insert")
	if err != nil {
		t.Fatalf("get recovered row: %v", err)
	}
	if row == nil {
		t.Fatal("expected committed insert to be replayed")
	}
}

func TestWALReplayCommittedUpdate(t *testing.T) {
	dataDir := t.TempDir()
	db, ti, err := openReplayTestDB(dataDir)
	if err != nil {
		t.Fatalf("open seed db: %v", err)
	}
	if _, err := ti.Insert(map[string]interface{}{
		"id": "id-base", "slug": "id-base", "title": "base", "value": float64(1),
	}, nil); err != nil {
		_ = db.Close()
		t.Fatalf("seed insert: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}

	runReplayCrashHelper(t, dataDir, "update", "update_after_commit,update_slow_after_commit")

	db2, ti2, err := openReplayTestDB(dataDir)
	if err != nil {
		t.Fatalf("open after crash: %v", err)
	}
	defer func() { _ = db2.Close() }()

	row, err := ti2.Get("id-base")
	if err != nil {
		t.Fatalf("get recovered row: %v", err)
	}
	if row == nil {
		t.Fatal("expected base row")
	}
	if got := fmt.Sprintf("%v", row["title"]); got != "updated" {
		t.Fatalf("expected title=updated after replay, got %q", got)
	}
}

func TestWALReplayCommittedDelete(t *testing.T) {
	dataDir := t.TempDir()
	db, ti, err := openReplayTestDB(dataDir)
	if err != nil {
		t.Fatalf("open seed db: %v", err)
	}
	if _, err := ti.Insert(map[string]interface{}{
		"id": "id-base", "slug": "id-base", "title": "base", "value": float64(1),
	}, nil); err != nil {
		_ = db.Close()
		t.Fatalf("seed insert: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}

	runReplayCrashHelper(t, dataDir, "delete", "delete_after_commit")

	db2, ti2, err := openReplayTestDB(dataDir)
	if err != nil {
		t.Fatalf("open after crash: %v", err)
	}
	defer func() { _ = db2.Close() }()

	row, err := ti2.Get("id-base")
	if err != nil {
		t.Fatalf("get recovered row: %v", err)
	}
	if row != nil {
		t.Fatal("expected committed delete to be replayed")
	}
}

func TestWALReplayIgnoresUncommittedInsert(t *testing.T) {
	dataDir := t.TempDir()
	runReplayCrashHelper(t, dataDir, "insert", "insert_before_commit")

	db, ti, err := openReplayTestDB(dataDir)
	if err != nil {
		t.Fatalf("open after crash: %v", err)
	}
	defer func() { _ = db.Close() }()

	row, err := ti.Get("id-insert")
	if err != nil {
		t.Fatalf("get row: %v", err)
	}
	if row != nil {
		t.Fatal("did not expect uncommitted insert to be replayed")
	}
}

func runReplayCrashHelper(t *testing.T, dataDir, action, failpoint string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestWALReplayHelperProcess", "--", dataDir, action)
	cmd.Env = append(os.Environ(),
		"GO_WANT_WAL_HELPER=1",
		"FLOP_FAILPOINT="+failpoint,
		"FLOP_FAILPOINT_HIT=1",
		"FLOP_FAILPOINT_MODE=exit",
	)
	err := cmd.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("expected helper to crash with exit code, got err=%v", err)
	}
	if exitErr.ExitCode() != 197 {
		t.Fatalf("expected helper exit code 197, got %d", exitErr.ExitCode())
	}
}

func TestWALReplayHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_WAL_HELPER") != "1" {
		return
	}
	if len(os.Args) < 5 {
		os.Exit(2)
	}
	dataDir := os.Args[len(os.Args)-2]
	action := os.Args[len(os.Args)-1]

	db, ti, err := openReplayTestDB(dataDir)
	if err != nil {
		os.Exit(3)
	}

	switch action {
	case "insert":
		_, _ = ti.Insert(map[string]interface{}{
			"id": "id-insert", "slug": "id-insert", "title": "insert", "value": float64(1),
		}, nil)
	case "update":
		_, _ = ti.Update("id-base", map[string]interface{}{"title": "updated", "value": float64(2)}, nil)
	case "delete":
		_, _ = ti.Delete("id-base", nil)
	default:
		os.Exit(4)
	}

	// If failpoint did not trigger, close and return explicit error code.
	_ = db.Close()
	os.Exit(5)
}
