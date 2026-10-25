package storage

import (
	"fmt"
	"github.com/marcisbee/flop/internal/schema"
	"path/filepath"
	"testing"
)

// BenchmarkMultiIndexLookup measures the public index boundary after a restart,
// including copying results for callers. In-memory indexes are the control.
func BenchmarkMultiIndexLookup(b *testing.B) {
	for _, postings := range []int{1, 64} {
		for _, mapped := range []bool{false, true} {
			for _, hit := range []bool{false, true} {
				b.Run(fmt.Sprintf("postings=%d/mapped=%t/hit=%t", postings, mapped, hit), func(b *testing.B) {
					const count = 4096
					idx := NewMultiIndex()
					keys := make([]string, count)
					for i := range keys {
						keys[i] = fmt.Sprintf("tenant/shared/prefix/%08d", i*2)
						for j := 0; j < postings; j++ {
							idx.Add(keys[i], schema.RowPointer{PageNumber: uint32(i + 1), SlotIndex: uint16(j)})
						}
					}
					if mapped {
						path := filepath.Join(b.TempDir(), "index.smidx")
						if err := WriteMappedMultiIndexFile(path, idx); err != nil {
							b.Fatal(err)
						}
						var err error
						idx, err = ReadMappedMultiIndexFile(path)
						if err != nil {
							b.Fatal(err)
						}
					}
					b.Cleanup(idx.Clear)
					want := postings
					if !hit {
						want = 0
						for i := range keys {
							keys[i] = fmt.Sprintf("tenant/shared/prefix/%08d", i*2+1)
						}
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						got := idx.GetAll(keys[(i*7919)%count])
						if len(got) != want {
							b.Fatalf("got %d postings, want %d", len(got), want)
						}
					}
				})
			}
		}
	}
}
