package engine

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/marcisbee/flop/internal/schema"
)

// BenchmarkRowIO exercises ordinary primary-key reads and durable updates.
// Both shapes use the same database settings; arrays represent multi-reference
// fields, while scalar rows provide a control for unaffected workloads.
func BenchmarkRowIO(b *testing.B) {
	for _, arrays := range []bool{false, true} {
		name := "Scalar"
		if arrays {
			name = "Arrays"
		}
		for _, operation := range []string{"Get", "Insert", "Update"} {
			b.Run(name+"/"+operation, func(b *testing.B) {
				fields := []schema.CompiledField{{Name: "id", Kind: schema.KindString, Required: true}, {Name: "title", Kind: schema.KindString}}
				row := map[string]interface{}{"id": "row-1", "title": "ordinary row"}
				if arrays {
					fields = append(fields, schema.CompiledField{Name: "refs", Kind: schema.KindRefMulti})
					refs := make([]interface{}, 64)
					for i := range refs {
						refs[i] = fmt.Sprintf("reference-%03d", i)
					}
					row["refs"] = refs
				}
				db := NewDatabase(DatabaseConfig{DataDir: b.TempDir(), SyncMode: "full"})
				if err := db.Open(map[string]*schema.TableDef{"items": {Name: "items", CompiledSchema: schema.NewCompiledSchema(fields)}}); err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() {
					if err := db.Close(); err != nil {
						b.Error(err)
					}
				})
				table := db.GetTable("items")
				if _, err := table.Insert(row, nil); err != nil {
					b.Fatal(err)
				}
				updates := map[string]interface{}{"title": "updated row"}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var got map[string]interface{}
					var err error
					if operation == "Get" {
						got, err = table.Get("row-1")
					} else if operation == "Insert" {
						row["id"] = "insert-" + strconv.Itoa(i)
						got, err = table.Insert(row, nil)
					} else {
						got, err = table.Update("row-1", updates, nil)
					}
					if err != nil || got == nil {
						b.Fatalf("operation failed: %v", err)
					}
				}
			})
		}
	}
}
