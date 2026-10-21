package engine

import (
	"strings"
	"sync"
	"testing"
)

func TestConditionalUpdateHasOneWinner(t *testing.T) {
	for _, replacement := range []string{"short", strings.Repeat("large", 300)} {
		t.Run(toString(len(replacement)), func(t *testing.T) {
			db := openTestDB(t, t.TempDir(), false, true)
			defer db.Close()
			table := mustTable(t, db)
			if _, err := table.Insert(map[string]interface{}{"id": "id-1", "slug": "slug-1", "title": "original", "genre": "action"}, nil); err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			start := make(chan struct{})
			results := make(chan error, 16)
			for i := 0; i < 16; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					_, err := table.UpdateIf("id-1", map[string]interface{}{"title": replacement}, func(row map[string]interface{}) bool { return row["title"] == "original" })
					results <- err
				}()
			}
			close(start)
			wg.Wait()
			close(results)
			successes := 0
			for err := range results {
				if err == nil {
					successes++
				}
			}
			if successes != 1 {
				t.Fatalf("conditional update had %d winners", successes)
			}
			row, err := table.Get("id-1")
			if err != nil || row["title"] != replacement {
				t.Fatalf("winner was lost: %v", err)
			}
		})
	}
}
