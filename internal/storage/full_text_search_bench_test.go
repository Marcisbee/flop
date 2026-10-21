package storage

import (
	"fmt"
	"testing"
)

// Measure the public search path, including candidate collection and ranking.
func BenchmarkFullTextPrefixSearch(b *testing.B) {
	idx := NewFullTextIndex()
	for i := 0; i < 16384; i++ {
		idx.Index(fmt.Sprint(i), fmt.Sprintf("term%03d group%03d", i%128, i%256))
	}
	idx.Finalize()
	for _, query := range []string{"term000", "term", "term group"} {
		b.Run(query, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if got := idx.Search(query, 8); len(got) != 8 {
					b.Fatalf("got %d results", len(got))
				}
			}
		})
	}
}
