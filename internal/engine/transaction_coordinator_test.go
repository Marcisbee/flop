package engine

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/storage"
)

func coordinatedTestDefs() map[string]*schema.TableDef {
	fields := []schema.CompiledField{
		{Name: "id", Kind: schema.KindString, Required: true},
		{Name: "value", Kind: schema.KindString},
	}
	return map[string]*schema.TableDef{
		"left": {
			Name:           "left",
			CompiledSchema: schema.NewCompiledSchema(fields),
		},
		"right": {
			Name:           "right",
			CompiledSchema: schema.NewCompiledSchema(fields),
		},
	}
}

func TestOpenRejectsInvalidSyncMode(t *testing.T) {
	db := NewDatabase(DatabaseConfig{DataDir: t.TempDir(), SyncMode: "sometimes"})
	if err := db.Open(coordinatedTestDefs()); err == nil {
		t.Fatal("expected invalid sync mode to be rejected")
	}
}

func TestRecoveryIgnoresPreparedCrossTableTransaction(t *testing.T) {
	testRecoveryCrossTableDecision(t, transactionPrepared, false)
}

func TestRecoveryIgnoresAbortedCrossTableTransaction(t *testing.T) {
	testRecoveryCrossTableDecision(t, transactionAborted, false)
}

func TestRecoveryAppliesCommittedCrossTableTransaction(t *testing.T) {
	testRecoveryCrossTableDecision(t, transactionCommitted, true)
}

func testRecoveryCrossTableDecision(t *testing.T, state string, wantRows bool) {
	t.Helper()
	dataDir := t.TempDir()
	defs := coordinatedTestDefs()

	db := NewDatabase(DatabaseConfig{DataDir: dataDir, SyncMode: "full"})
	if err := db.Open(defs); err != nil {
		t.Fatalf("open seed db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seed db: %v", err)
	}

	tables := map[string][]uint32{}
	for _, tableName := range []string{"left", "right"} {
		wal, err := storage.OpenWAL(filepath.Join(dataDir, tableName+".wal"))
		if err != nil {
			t.Fatalf("open %s WAL: %v", tableName, err)
		}
		txID := wal.BeginTransaction()
		row := storage.SerializeRow(map[string]interface{}{
			"id": "row-1", "value": tableName,
		}, defs[tableName].CompiledSchema, 1)
		if err := wal.FlushBatch([][]byte{
			wal.BuildBeginRecord(txID),
			wal.BuildRecord(txID, storage.WALOpInsert, row),
		}, []uint32{txID}); err != nil {
			_ = wal.Close()
			t.Fatalf("flush %s WAL: %v", tableName, err)
		}
		if err := wal.Fsync(); err != nil {
			_ = wal.Close()
			t.Fatalf("sync %s WAL: %v", tableName, err)
		}
		if err := wal.Close(); err != nil {
			t.Fatalf("close %s WAL: %v", tableName, err)
		}
		tables[tableName] = []uint32{txID}
	}

	journal := transactionJournal{
		Version: transactionJournalVersion,
		Transactions: []coordinatedTransaction{{
			ID:     "test-transaction",
			State:  state,
			Tables: tables,
		}},
	}
	db = NewDatabase(DatabaseConfig{DataDir: dataDir, SyncMode: "full"})
	db.txJournal = journal
	if err := db.persistTransactionJournalLocked(); err != nil {
		t.Fatalf("persist transaction journal: %v", err)
	}

	reopened := NewDatabase(DatabaseConfig{DataDir: dataDir, SyncMode: "full"})
	if err := reopened.Open(defs); err != nil {
		t.Fatalf("open recovery db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	for _, tableName := range []string{"left", "right"} {
		row, err := reopened.GetTable(tableName).Get("row-1")
		if err != nil {
			t.Fatalf("get %s row: %v", tableName, err)
		}
		if wantRows && row == nil {
			t.Fatalf("expected committed row in %s", tableName)
		}
		if !wantRows && row != nil {
			t.Fatalf("unexpected aborted row in %s: %v", tableName, row)
		}
	}
}

// Acknowledged concurrent commits must survive without a clean close. If the
// transaction log cannot persist, none of that transaction may reappear on reopen.
func TestConcurrentTransactionCrashRecovery(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(fmt.Sprintf("durability_failure=%t", failure), func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestConcurrentTransactionCrashHelper$")
			cmd.Env = append(os.Environ(), "FLOP_TX_CRASH_DIR="+dir, "FLOP_TX_CRASH_FAILURE="+strconv.FormatBool(failure))
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("child: %v\n%s", err, out)
			}
			db := NewDatabase(DatabaseConfig{DataDir: dir, SyncMode: "full"})
			if err := db.Open(coordinatedTestDefs()); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			}()
			for i := 0; i < 64; i++ {
				for _, name := range []string{"left", "right"} {
					row, err := db.GetTable(name).Get(strconv.Itoa(i))
					if err != nil {
						t.Fatal(err)
					}
					if failure {
						if row != nil {
							t.Fatalf("failed transaction %d recovered in %s", i, name)
						}
					} else if row == nil || row["value"] != name {
						t.Fatalf("acknowledged transaction %d missing or incorrect in %s: %v", i, name, row)
					}
				}
			}
		})
	}
}

func TestConcurrentTransactionCrashHelper(t *testing.T) {
	dir := os.Getenv("FLOP_TX_CRASH_DIR")
	if dir == "" {
		return
	}
	db := NewDatabase(DatabaseConfig{DataDir: dir, SyncMode: "full"})
	if err := db.Open(coordinatedTestDefs()); err != nil {
		t.Fatal(err)
	}
	failure := os.Getenv("FLOP_TX_CRASH_FAILURE") == "true"
	if failure {
		if err := db.txDecisions.Close(); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(64)
	done.Add(64)
	for i := 0; i < 64; i++ {
		go func(i int) {
			defer done.Done()
			buffers := make(map[string]*walBufEntry)
			for _, name := range []string{"left", "right"} {
				if _, err := db.GetTable(name).Insert(map[string]interface{}{"id": strconv.Itoa(i), "value": name}, buffers); err != nil {
					t.Error(err)
					ready.Done()
					return
				}
			}
			ready.Done()
			<-start
			err := db.EnqueueCommit(buffers)
			if failure {
				if err == nil {
					t.Error("acknowledged failed transaction log")
				}
				for _, name := range []string{"left", "right"} {
					if e := db.GetTable(name).RollbackInsert(strconv.Itoa(i)); e != nil {
						t.Error(e)
					}
				}
				db.DiscardTransaction(buffers)
			} else if err != nil {
				t.Error(err)
			}
		}(i)
	}
	ready.Wait()
	close(start)
	done.Wait()
	if t.Failed() {
		os.Exit(1)
	}
	// Deliberately omit Close/checkpoint: only acknowledged WAL decisions survive.
	os.Exit(0)
}

// Recovery must merge ordinary WAL writes with global redo in operation order,
// even if one table checkpointed before the process stopped.
func TestTransactionRedoInterleavesWithOrdinaryWAL(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%t", legacy), func(t *testing.T) { testTransactionRedoInterleave(t, legacy) })
	}
}

func testTransactionRedoInterleave(t *testing.T, legacy bool) {
	t.Helper()
	dir := t.TempDir()
	if legacy {
		seed := NewDatabase(DatabaseConfig{DataDir: dir, SyncMode: "full"})
		if err := seed.Open(coordinatedTestDefs()); err != nil {
			t.Fatal(err)
		}
		if err := seed.Close(); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"left", "right"} {
			path := filepath.Join(dir, name+".wal")
			header, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			binary.LittleEndian.PutUint32(header[4:8], 1)
			if err := os.WriteFile(path, header, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestTransactionRedoInterleaveHelper$")
	cmd.Env = append(os.Environ(), "FLOP_REDO_INTERLEAVE_DIR="+dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v\n%s", err, out)
	}
	for reopen := 0; reopen < 2; reopen++ {
		db := NewDatabase(DatabaseConfig{DataDir: dir, SyncMode: "full"})
		if err := db.Open(coordinatedTestDefs()); err != nil {
			t.Fatal(err)
		}
		for name, want := range map[string]string{"left": "tx-last", "right": "ordinary-last"} {
			row, err := db.GetTable(name).Get("same")
			if err != nil || row == nil || row["value"] != want {
				t.Fatalf("reopen %d %s = %v, %v; want %s", reopen, name, row, err, want)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTransactionRedoInterleaveHelper(t *testing.T) {
	dir := os.Getenv("FLOP_REDO_INTERLEAVE_DIR")
	if dir == "" {
		return
	}
	db := NewDatabase(DatabaseConfig{DataDir: dir, SyncMode: "full"})
	if err := db.Open(coordinatedTestDefs()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"left", "right"} {
		if _, err := db.GetTable(name).Insert(map[string]interface{}{"id": "same", "value": "seed"}, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, value := range []string{"tx-first", "tx-last"} {
		buffers := map[string]*walBufEntry{}
		for _, name := range []string{"left", "right"} {
			if _, err := db.GetTable(name).Update("same", map[string]interface{}{"value": value}, buffers); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.EnqueueCommit(buffers); err != nil {
			t.Fatal(err)
		}
		if value == "tx-first" {
			if _, err := db.GetTable("left").Update("same", map[string]interface{}{"value": "ordinary-middle"}, nil); err != nil {
				t.Fatal(err)
			}
			if err := db.GetTable("left").Checkpoint(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := db.GetTable("right").Update("same", map[string]interface{}{"value": "ordinary-last"}, nil); err != nil {
		t.Fatal(err)
	}
	os.Exit(0)
}

func TestOpenRejectsMissingRedoLog(t *testing.T) {
	dir := t.TempDir()
	db := NewDatabase(DatabaseConfig{DataDir: dir, SyncMode: "full"})
	if err := db.Open(coordinatedTestDefs()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "_transactions.wal")); err != nil {
		t.Fatal(err)
	}
	db = NewDatabase(DatabaseConfig{DataDir: dir, SyncMode: "full"})
	if err := db.Open(coordinatedTestDefs()); err == nil {
		db.Close()
		t.Fatal("opened upgraded database without its redo log")
	}
}
