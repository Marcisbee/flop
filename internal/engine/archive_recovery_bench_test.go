package engine

import (
	"fmt"
	"os"
	"testing"
)

// Measures reopening after an acknowledged archive operation and an abrupt exit.
// Fixture creation, copying, and close are excluded; recovered state is checked.
func BenchmarkArchiveRecovery(b *testing.B) {
	for _, action := range []string{"archive", "restore"} {
		for _, count := range []int{100, 10000} {
			b.Run(fmt.Sprintf("%s/%d", action, count), func(b *testing.B) {
				b.StopTimer()
				fixture := b.TempDir()
				db, ti, err := openReplayTestDB(fixture)
				if err != nil {
					b.Fatal(err)
				}
				for i := 0; i < count; i++ {
					id := fmt.Sprintf("row-%06d", i)
					if _, err := ti.Insert(map[string]interface{}{"id": id, "slug": id, "title": id}, nil); err != nil {
						b.Fatal(err)
					}
					if _, err := ti.Archive(id, ArchiveOptions{}, nil); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := ti.Insert(map[string]interface{}{"id": "id-base", "slug": "id-base", "title": "base"}, nil); err != nil {
					b.Fatal(err)
				}
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
				changedID := runReplayCrashHelper(b, fixture, action, "unused")
				for i := 0; i < b.N; i++ {
					dir := b.TempDir()
					if err := os.CopyFS(dir, os.DirFS(fixture)); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
					db, ti, err = openReplayTestDB(dir)
					b.StopTimer()
					if err != nil {
						b.Fatal(err)
					}
					record, _, err := ti.GetArchived(changedID)
					if err != nil {
						b.Fatal(err)
					}
					if (action == "restore" && record != nil) || (action == "archive" && record == nil) {
						b.Fatalf("incorrect archive state after %s recovery", action)
					}
					_, total, err := ti.ScanArchived(0, 0)
					want := count - 1
					if action == "archive" {
						want = count + 1
					}
					if err != nil || total != want {
						b.Fatalf("archive count=%d want=%d err=%v", total, want, err)
					}
					if err := db.Close(); err != nil {
						b.Fatal(err)
					}
					if err := os.RemoveAll(dir); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
