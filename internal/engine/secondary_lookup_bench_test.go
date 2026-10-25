package engine

import (
	"fmt"
	"github.com/marcisbee/flop/internal/schema"
	"testing"
)

// BenchmarkMappedSecondaryLookup includes row loading and decoding, after a
// durable close/reopen, to distinguish index savings from query-level savings.
func BenchmarkMappedSecondaryLookup(b *testing.B) {
	const groups, postings = 512, 64
	for _, hit := range []bool{false, true} {
		b.Run(fmt.Sprintf("hit=%t", hit), func(b *testing.B) {
			dir := b.TempDir()
			defs := map[string]*schema.TableDef{"items": {
				Name: "items",
				CompiledSchema: schema.NewCompiledSchema([]schema.CompiledField{
					{Name: "id", Kind: schema.KindString, Required: true},
					{Name: "owner", Kind: schema.KindString},
					{Name: "title", Kind: schema.KindString},
				}),
				Indexes: []schema.IndexDef{{Fields: []string{"owner"}, Type: schema.IndexTypeHash}},
			}}
			db := NewDatabase(DatabaseConfig{DataDir: dir, SyncMode: "full"})
			if err := db.Open(defs); err != nil {
				b.Fatal(err)
			}
			table := db.GetTable("items")
			keys := make([]string, groups)
			for i := range keys {
				keys[i] = fmt.Sprintf("owner-%08d", i*2)
				for j := 0; j < postings; j++ {
					_, err := table.Insert(map[string]interface{}{"id": fmt.Sprintf("%d-%d", i, j), "owner": keys[i], "title": "An ordinary post"}, nil)
					if err != nil {
						b.Fatal(err)
					}
				}
			}
			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
			db = NewDatabase(DatabaseConfig{DataDir: dir, SyncMode: "full"})
			if err := db.Open(defs); err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := db.Close(); err != nil {
					b.Error(err)
				}
			})
			table = db.GetTable("items")
			wantRows, wantTotal := 10, postings
			if !hit {
				wantRows, wantTotal = 0, 0
				for i := range keys {
					keys[i] = fmt.Sprintf("owner-%08d", i*2+1)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := keys[(i*7919)%groups]
				rows, total, used := table.LookupByField("owner", key, 10, 0)
				if !used || total != wantTotal || len(rows) != wantRows {
					b.Fatalf("lookup returned %d/%d, index=%t", len(rows), total, used)
				}
				for _, row := range rows {
					if row["owner"] != key {
						b.Fatal("wrong owner")
					}
				}
			}
		})
	}
}
