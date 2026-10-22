package engine

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Exercise real crash recovery: Close would itself flush the live pages.
func TestCheckpointUncommittedTransactionRepro(t *testing.T) {
	if dir := os.Getenv("FLOP_CHECKPOINT_TX_REPRO"); dir != "" {
		db := NewDatabase(DatabaseConfig{DataDir: dir, SyncMode: "full"})
		if err := db.Open(coordinatedTestDefs()); err != nil {
			t.Fatal(err)
		}
		committed := make(map[string]*walBufEntry)
		for _, name := range []string{"left", "right"} {
			if _, err := db.GetTable(name).Insert(map[string]interface{}{"id": "committed", "value": "original"}, committed); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.EnqueueCommit(committed); err != nil {
			t.Fatal(err)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		buf := make(map[string]*walBufEntry)
		left := db.GetTable("left")
		switch os.Getenv("FLOP_CHECKPOINT_TX_OP") {
		case "insert":
			if _, err := left.Insert(map[string]interface{}{"id": "uncommitted", "value": "left"}, buf); err != nil {
				t.Fatal(err)
			}
		case "update":
			if _, err := left.Update("committed", map[string]interface{}{"value": "uncommitted"}, buf); err != nil {
				t.Fatal(err)
			}
		case "delete":
			if _, err := left.Delete("committed", buf); err != nil {
				t.Fatal(err)
			}
		}
		// A separate acknowledged cross-table commit must remain recoverable even
		// when checkpointing is deferred by the earlier buffered transaction.
		later := make(map[string]*walBufEntry)
		for _, name := range []string{"left", "right"} {
			if _, err := db.GetTable(name).Insert(map[string]interface{}{"id": "later", "value": "durable"}, later); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.EnqueueCommit(later); err != nil {
			t.Fatal(err)
		}
		if os.Getenv("FLOP_CHECKPOINT_TX_SCOPE") == "table" {
			for _, name := range []string{"left", "right"} {
				if err := db.GetTable(name).Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
		} else if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	}
	for _, scope := range []string{"database", "table"} {
		for _, op := range []string{"insert", "update", "delete"} {
			t.Run(scope+"/"+op, func(t *testing.T) {
				dir := t.TempDir()
				cmd := exec.Command(os.Args[0], "-test.run=^TestCheckpointUncommittedTransactionRepro$")
				cmd.Env = append(os.Environ(), "FLOP_CHECKPOINT_TX_REPRO="+dir, "FLOP_CHECKPOINT_TX_OP="+op, "FLOP_CHECKPOINT_TX_SCOPE="+scope)
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("child: %v %s", err, out)
				}
				db := NewDatabase(DatabaseConfig{DataDir: dir, SyncMode: "full"})
				if err := db.Open(coordinatedTestDefs()); err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				for _, name := range []string{"left", "right"} {
					for id, want := range map[string]string{"committed": "original", "later": "durable", "uncommitted": ""} {
						row, err := db.GetTable(name).Get(id)
						if err != nil {
							t.Fatal(err)
						}
						if want == "" {
							if row != nil {
								t.Errorf("uncommitted row survived in %s: %v", name, row)
							}
						} else if row == nil || row["value"] != want {
							t.Errorf("%s/%s: got %v, want value %s", name, id, row, want)
						}
					}
				}
			})
		}
	}
}

func TestSnapshotRetriesBufferedTransaction(t *testing.T) {
	db := NewDatabase(DatabaseConfig{DataDir: t.TempDir(), SyncMode: "full"})
	if err := db.Open(coordinatedTestDefs()); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	buf := make(map[string]*walBufEntry)
	if _, err := db.GetTable("left").Insert(map[string]interface{}{"id": "pending"}, buf); err != nil {
		t.Fatal(err)
	}
	called := false
	snapshotDir := t.TempDir()
	snapshot := func(dir string) error {
		called = true
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
			if err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(snapshotDir, entry.Name()), data, 0600); err != nil {
				return err
			}
		}
		return nil
	}
	if err := db.WithConsistentSnapshot(snapshot); err == nil || called {
		t.Fatal("snapshot must refuse buffered mutations before invoking the reader")
	}
	if _, err := db.GetTable("right").Insert(map[string]interface{}{"id": "pending"}, buf); err != nil {
		t.Fatal(err)
	}
	if err := db.EnqueueCommit(buf); err != nil {
		t.Fatal(err)
	}
	if err := db.WithConsistentSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("snapshot did not resume after commit")
	}
	restored := NewDatabase(DatabaseConfig{DataDir: snapshotDir, SyncMode: "full"})
	if err := restored.Open(coordinatedTestDefs()); err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	for _, name := range []string{"left", "right"} {
		row, err := restored.GetTable(name).Get("pending")
		if err != nil || row == nil {
			t.Fatalf("snapshot lost committed %s row: %v, %v", name, row, err)
		}
	}
}
