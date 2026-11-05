package flop

import "testing"

// Measures the conversion performed by typed Table Get/Scan and Insert,
// independently of disk latency. Both sides use the same record and helpers.
func BenchmarkTypedJSONConversion(b *testing.B) {
	type record struct {
		ID      string         `json:"id"`
		Title   string         `json:"title"`
		Enabled bool           `json:"enabled"`
		Score   float64        `json:"score"`
		Tags    []string       `json:"tags"`
		Meta    map[string]any `json:"meta"`
	}
	value := record{ID: "record-123", Title: "Ordinary typed Flop record", Enabled: true, Score: 42.5, Tags: []string{"flop", "json", "example"}, Meta: map[string]any{"editor": "user-42", "count": 12, "public": true}}
	row, err := valueToRowMap(value)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("Read", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var got record
			if err := rowMapToValue(row, &got); err != nil {
				b.Fatal(err)
			}
			if got.ID != value.ID || len(got.Tags) != 3 {
				b.Fatal("record changed")
			}
		}
	})
	b.Run("Write", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			got, err := valueToRowMap(value)
			if err != nil {
				b.Fatal(err)
			}
			if got["id"] != value.ID {
				b.Fatal("record changed")
			}
		}
	})
}
