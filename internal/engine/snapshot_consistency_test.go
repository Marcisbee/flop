package engine

import (
	"testing"
	"time"
)

func TestConsistentSnapshotBlocksConcurrentWrites(t *testing.T) {
	db := openTestDB(t, t.TempDir(), false, false)
	defer func() { _ = db.Close() }()
	table := mustTable(t, db)

	entered := make(chan struct{})
	release := make(chan struct{})
	snapshotDone := make(chan error, 1)
	go func() {
		snapshotDone <- db.WithConsistentSnapshot(func(string) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	writeDone := make(chan error, 1)
	go func() {
		_, err := table.Insert(map[string]interface{}{
			"id": "movie-1", "slug": "movie-1", "title": "Movie", "genre": "drama",
		}, nil)
		writeDone <- err
	}()

	select {
	case err := <-writeDone:
		t.Fatalf("write completed during snapshot: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-snapshotDone; err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("write after snapshot: %v", err)
	}
}
