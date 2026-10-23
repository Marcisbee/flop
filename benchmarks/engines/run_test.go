package engines

import "testing"

func TestHistogramReportsKnownTailWithinOnePercent(t *testing.T) {
	var first, second histogram
	for i := 1; i <= 1000; i++ {
		if i <= 500 {
			first.add(float64(i))
		} else {
			second.add(float64(i))
		}
	}
	first.merge(&second)
	got := first.quantiles()
	for i, want := range []float64{500, 990, 999} {
		if got[i] < want || got[i] > want*1.01 {
			t.Fatalf("percentile %d: got %v, expected [%v,%v]", i, got[i], want, want*1.01)
		}
	}
	var empty histogram
	if empty.quantiles() != nil {
		t.Fatal("empty histogram must not report zero-latency successes")
	}
}
