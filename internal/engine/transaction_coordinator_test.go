package engine

import (
	"path/filepath"
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
