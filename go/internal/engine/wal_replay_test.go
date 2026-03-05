package engine

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/storage"
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

func TestWALReplaySkipsCheckpointedTransactions(t *testing.T) {
	dataDir := t.TempDir()
	db, _, err := openReplayTestDB(dataDir)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seed db: %v", err)
	}

	walPath := filepath.Join(dataDir, "items.wal")
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	mkInsert := func(id string) []byte {
		row := map[string]interface{}{
			"id":    id,
			"slug":  id,
			"title": "title-" + id,
			"value": float64(1),
		}
		return storage.SerializeRow(row, replayTestDefs()["items"].CompiledSchema, 1)
	}

	tx1 := wal.BeginTransaction()
	tx2 := wal.BeginTransaction()
	records := [][]byte{
		wal.BuildBeginRecord(tx1),
		wal.BuildRecord(tx1, storage.WALOpInsert, mkInsert("id-old")),
		wal.BuildBeginRecord(tx2),
		wal.BuildRecord(tx2, storage.WALOpInsert, mkInsert("id-new")),
	}
	if err := wal.FlushBatch(records, []uint32{tx1, tx2}); err != nil {
		_ = wal.Close()
		t.Fatalf("flush batch: %v", err)
	}

	entries, err := wal.Replay()
	if err != nil {
		_ = wal.Close()
		t.Fatalf("replay for commit lsn lookup: %v", err)
	}
	commitLSN := storage.FindCommittedTxLSN(entries)
	if commitLSN[tx1] == 0 || commitLSN[tx2] == 0 {
		_ = wal.Close()
		t.Fatalf("missing commit lsns for tx1/tx2: %v", commitLSN)
	}
	if !(commitLSN[tx1] < commitLSN[tx2]) {
		_ = wal.Close()
		t.Fatalf("expected tx1 commit lsn < tx2 commit lsn: %d >= %d", commitLSN[tx1], commitLSN[tx2])
	}
	if err := wal.SetCheckpointLSN(commitLSN[tx1]); err != nil {
		_ = wal.Close()
		t.Fatalf("set checkpoint lsn: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	db2, ti2, err := openReplayTestDB(dataDir)
	if err != nil {
		t.Fatalf("open recovery db: %v", err)
	}
	defer func() { _ = db2.Close() }()

	oldRow, err := ti2.Get("id-old")
	if err != nil {
		t.Fatalf("get id-old: %v", err)
	}
	if oldRow != nil {
		t.Fatalf("expected checkpointed tx row to be skipped, got=%v", oldRow)
	}

	newRow, err := ti2.Get("id-new")
	if err != nil {
		t.Fatalf("get id-new: %v", err)
	}
	if newRow == nil {
		t.Fatal("expected post-checkpoint tx row to be replayed")
	}
	if got := fmt.Sprintf("%v", newRow["id"]); got != "id-new" {
		t.Fatalf("unexpected id-new row: %q", got)
	}
}

func TestWALReplaySkipsWhenPageLSNAhead(t *testing.T) {
	dataDir := t.TempDir()
	db, ti, err := openReplayTestDB(dataDir)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	inserted, err := ti.Insert(map[string]interface{}{
		"id": "id-1", "slug": "id-1", "title": "old", "value": float64(1),
	}, nil)
	if err != nil || inserted == nil {
		t.Fatalf("seed insert: %v", err)
	}

	ptr, ok := ti.primaryIndex.Get("id-1")
	if !ok {
		t.Fatalf("missing pointer for id-1")
	}
	page, err := ti.tableFile.GetPage(ptr.PageNumber)
	if err != nil {
		t.Fatalf("get page: %v", err)
	}
	page.SetPageLSN(900)
	ti.tableFile.MarkPageDirty(ptr.PageNumber)
	if err := ti.tableFile.Flush(); err != nil {
		t.Fatalf("flush page lsn: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seed db: %v", err)
	}

	walPath := filepath.Join(dataDir, "items.wal")
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	row := map[string]interface{}{
		"id": "id-1", "slug": "id-1", "title": "new", "value": float64(2),
	}
	serialized := storage.SerializeRow(row, replayTestDefs()["items"].CompiledSchema, 1)

	tx := wal.BeginTransaction()
	begin := wal.BuildBeginRecord(tx)
	updateRecord, updateLSN := wal.BuildRecordWithLSN(tx, storage.WALOpUpdate, serialized)
	if updateLSN == 0 {
		t.Fatalf("expected non-zero update lsn")
	}
	if updateLSN >= 900 {
		t.Fatalf("test expects updateLSN < 900, got %d", updateLSN)
	}
	if err := wal.FlushBatch([][]byte{begin, updateRecord}, []uint32{tx}); err != nil {
		_ = wal.Close()
		t.Fatalf("flush wal update: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	db2, ti2, err := openReplayTestDB(dataDir)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = db2.Close() }()

	got, err := ti2.Get("id-1")
	if err != nil {
		t.Fatalf("get id-1: %v", err)
	}
	if got == nil {
		t.Fatalf("expected id-1 row")
	}
	if title := fmt.Sprintf("%v", got["title"]); title != "old" {
		t.Fatalf("expected replay skip due pageLSN (title old), got %q", title)
	}
}
