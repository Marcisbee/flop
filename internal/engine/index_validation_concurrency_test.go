package engine

import (
	"fmt"
	"sync"
	"testing"
)

// Materialized refreshes validate indexes while requests and other refreshes
// mutate the same table. Both operations must finish and preserve the rows.
func TestIndexRepairConcurrentWrites(t *testing.T) {
	db := openTestDB(t, t.TempDir(), false, true)
	defer db.Close()
	table := mustTable(t, db)
	seedMovies(t, table, 64)
	start := make(chan struct{})
	errors := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 500; i++ {
			if err := table.RepairIndexesIfNeeded(); err != nil {
				errors <- err
				return
			}
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 500; i++ {
			_, err := table.Insert(map[string]interface{}{
				"id": fmt.Sprintf("new-%d", i), "slug": fmt.Sprintf("new-slug-%d", i),
				"title": "Concurrent movie", "genre": "action",
			}, nil)
			if err != nil {
				errors <- err
				return
			}
		}
	}()
	close(start)
	workers.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	if got := table.Count(); got != 564 {
		t.Fatalf("row count = %d, want 564", got)
	}
	if err := table.RepairIndexesIfNeeded(); err != nil {
		t.Fatal(err)
	}
}
