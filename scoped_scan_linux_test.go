package flop

import (
	"fmt"
	"strings"
	"syscall"
	"testing"
)

func BenchmarkScopedTraversal(b *testing.B) {
	app := New(Config{DataDir: b.TempDir(), SyncMode: "normal"})
	Define(app, "posts", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.String("owner").Required()
		s.String("body")
		s.Access(TableAccess{Read: func(c *TableReadCtx) bool { return c.Row["owner"] == c.Auth.ID }})
	})
	db, err := app.Open()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	raw := db.Table("posts")
	for start := 0; start < 100014; start += 1000 {
		rows := make([]map[string]any, 0, 1000)
		for i := start; i < start+1000 && i < 100014; i++ {
			owner := "other"
			if i%10 == 0 {
				owner = "reader"
			}
			rows = append(rows, map[string]any{"id": fmt.Sprintf("p%06d", i), "owner": owner, "body": strings.Repeat("x", 1024)})
		}
		if _, err := raw.InsertMany(rows, 1000); err != nil {
			b.Fatal(err)
		}
	}
	scoped := db.trackedAccessor(nil, &AuthContext{ID: "reader"}).Table("posts")
	b.Run("Count", func(b *testing.B) {
		b.ReportAllocs()
		var before, after syscall.Rusage
		syscall.Getrusage(syscall.RUSAGE_SELF, &before)
		defer func() {
			syscall.Getrusage(syscall.RUSAGE_SELF, &after)
			cpu := func(r syscall.Rusage) float64 {
				return float64(r.Utime.Sec+r.Stime.Sec)*1e9 + float64(r.Utime.Usec+r.Stime.Usec)*1e3
			}
			b.ReportMetric((cpu(after)-cpu(before))/float64(b.N), "cpu-ns/op")
		}()
		for b.Loop() {
			if n := scoped.Count(); n != 10002 {
				b.Fatalf("count=%d", n)
			}
		}
	})
	for _, tc := range []struct {
		name          string
		limit, offset int
	}{{"ScanAll", 10002, 0}, {"ScanDeep", 20, 9000}, {"ScanEarly", 20, 0}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			var before, after syscall.Rusage
			syscall.Getrusage(syscall.RUSAGE_SELF, &before)
			defer func() {
				syscall.Getrusage(syscall.RUSAGE_SELF, &after)
				cpu := func(r syscall.Rusage) float64 {
					return float64(r.Utime.Sec+r.Stime.Sec)*1e9 + float64(r.Utime.Usec+r.Stime.Usec)*1e3
				}
				b.ReportMetric((cpu(after)-cpu(before))/float64(b.N), "cpu-ns/op")
			}()
			for b.Loop() {
				rows, err := scoped.Scan(tc.limit, tc.offset)
				if err != nil || len(rows) != tc.limit {
					b.Fatalf("rows=%d err=%v", len(rows), err)
				}
				if rows[0]["id"] != fmt.Sprintf("p%06d", tc.offset*10) {
					b.Fatal("unexpected first row", rows[0]["id"])
				}
			}
		})
	}
}
