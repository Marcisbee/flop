package storage

import (
	"fmt"
	"math"
	"testing"
)

func TestTokenizeTexts(t *testing.T) {
	tests := []struct {
		input string
		want  map[string]struct{}
	}{
		{"Hello World", map[string]struct{}{"hello": {}, "world": {}}},
		{"Fast & Furious", map[string]struct{}{"fast": {}, "and": {}, "furious": {}}},
		{"a", map[string]struct{}{"a": {}}},
		{"  ", map[string]struct{}{}},
		{"hello hello", map[string]struct{}{"hello": {}}},
		{"C-3PO", map[string]struct{}{"c": {}, "3po": {}}},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := tokenizeTexts(tt.input)
			gotSet := make(map[string]struct{}, len(got))
			for _, tok := range got {
				gotSet[tok] = struct{}{}
			}
			if len(gotSet) != len(tt.want) {
				t.Fatalf("got %v, want %v", gotSet, tt.want)
			}
			for tok := range tt.want {
				if _, ok := gotSet[tok]; !ok {
					t.Fatalf("missing token %q in %v", tok, gotSet)
				}
			}
		})
	}
}

func TestTokenizeTextsWithFreq(t *testing.T) {
	freqs := tokenizeTextsWithFreq("the quick brown fox the fox")
	if freqs["the"] != 2 {
		t.Fatalf("expected the=2, got %d", freqs["the"])
	}
	if freqs["fox"] != 2 {
		t.Fatalf("expected fox=2, got %d", freqs["fox"])
	}
	if freqs["quick"] != 1 {
		t.Fatalf("expected quick=1, got %d", freqs["quick"])
	}
}

func TestIndexAndSearchBasic(t *testing.T) {
	idx := NewFullTextIndex()
	idx.Index("doc1", "Harry Potter and the Chamber of Secrets")
	idx.Index("doc2", "Harry Potter and the Goblet of Fire")
	idx.Index("doc3", "The Matrix")

	results := idx.Search("harry potter", 10)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d: %v", len(results), results)
	}
	// Both Harry Potter docs should be returned
	has := make(map[string]bool)
	for _, pk := range results {
		has[pk] = true
	}
	if !has["doc1"] || !has["doc2"] {
		t.Fatalf("expected doc1 and doc2, got %v", results)
	}
}

func TestSearchPrefixMatching(t *testing.T) {
	idx := NewFullTextIndex()
	idx.Index("doc1", "Harry Potter")
	idx.Index("doc2", "Harrison Ford")
	idx.Index("doc3", "The Matrix")

	// "harr" should match both Harry and Harrison
	results := idx.Search("harr", 10)
	if len(results) != 2 {
		t.Fatalf("expected 2 results for prefix 'harr', got %d: %v", len(results), results)
	}

	// "harry p" should match only Harry Potter
	results = idx.Search("harry p", 10)
	if len(results) != 1 || results[0] != "doc1" {
		t.Fatalf("expected [doc1] for 'harry p', got %v", results)
	}
}

func TestSearchNoMatch(t *testing.T) {
	idx := NewFullTextIndex()
	idx.Index("doc1", "Hello World")

	results := idx.Search("xyz", 10)
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestSearchEmpty(t *testing.T) {
	idx := NewFullTextIndex()
	results := idx.Search("test", 10)
	if results != nil {
		t.Fatalf("expected nil, got %v", results)
	}
}

func TestSearchAmpersand(t *testing.T) {
	idx := NewFullTextIndex()
	idx.Index("doc1", "Fast & Furious")
	idx.Index("doc2", "Fast and Furious 2")

	// "fast and furious" should match both (& is normalized to "and")
	results := idx.Search("fast and furious", 10)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d: %v", len(results), results)
	}
}

func TestBM25Ranking(t *testing.T) {
	idx := NewFullTextIndex()
	// doc1 has "potter" twice — should rank higher for query "potter"
	idx.Index("doc1", "Potter the potter")
	// doc2 has "potter" once in a longer doc
	idx.Index("doc2", "Harry Potter and the Chamber of Secrets and more words here")
	// doc3 doesn't match
	idx.Index("doc3", "The Matrix Reloaded")

	results := idx.Search("potter", 10)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d: %v", len(results), results)
	}
	// doc1 should rank first: higher tf, shorter doc
	if results[0] != "doc1" {
		t.Fatalf("expected doc1 first (higher tf, shorter doc), got %v", results)
	}
}

func TestBM25MultiTokenRanking(t *testing.T) {
	idx := NewFullTextIndex()
	idx.Index("doc1", "Fast and Furious")
	idx.Index("doc2", "Fast Company")
	idx.Index("doc3", "Furious Seven")

	results := idx.Search("fast furious", 10)
	// doc1 matches both tokens; doc2 and doc3 match only one each
	if len(results) != 1 {
		// Only doc1 has both "fast" AND "furious"
		t.Fatalf("expected 1 result, got %d: %v", len(results), results)
	}
	if results[0] != "doc1" {
		t.Fatalf("expected doc1, got %v", results)
	}
}

func TestSearchLimit(t *testing.T) {
	idx := NewFullTextIndex()
	for i := 0; i < 100; i++ {
		idx.Index(fmt.Sprintf("doc%d", i), fmt.Sprintf("Test Movie %d", i))
	}

	results := idx.Search("test", 5)
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
}

func TestDeleteRemovesFromSearch(t *testing.T) {
	idx := NewFullTextIndex()
	idx.Index("doc1", "Harry Potter")
	idx.Index("doc2", "Harry Styles")

	idx.Delete("doc1")

	results := idx.Search("harry", 10)
	if len(results) != 1 || results[0] != "doc2" {
		t.Fatalf("expected [doc2] after delete, got %v", results)
	}
}

func TestReindexUpdatesDocLen(t *testing.T) {
	idx := NewFullTextIndex()
	idx.Index("doc1", "short")
	idx.Index("doc1", "a much longer replacement title with many words")

	// Should still be searchable
	results := idx.Search("replacement", 10)
	if len(results) != 1 || results[0] != "doc1" {
		t.Fatalf("expected [doc1], got %v", results)
	}
	// Old text shouldn't match
	results = idx.Search("short", 10)
	if len(results) != 0 {
		t.Fatalf("expected 0 results for old text, got %v", results)
	}
}

func TestClear(t *testing.T) {
	idx := NewFullTextIndex()
	idx.Index("doc1", "Hello World")
	idx.Clear()

	results := idx.Search("hello", 10)
	if results != nil {
		t.Fatalf("expected nil after clear, got %v", results)
	}

	stats := idx.Stats()
	if stats.DocCount != 0 || stats.TokenCount != 0 {
		t.Fatalf("expected 0 counts after clear, got docs=%d tokens=%d", stats.DocCount, stats.TokenCount)
	}
}

func TestStats(t *testing.T) {
	idx := NewFullTextIndex()
	idx.Index("doc1", "Hello World")
	idx.Index("doc2", "Hello There")

	stats := idx.Stats()
	if stats.DocCount != 2 {
		t.Fatalf("expected 2 docs, got %d", stats.DocCount)
	}
	if stats.TokenCount != 3 { // hello, world, there
		t.Fatalf("expected 3 tokens, got %d", stats.TokenCount)
	}
	if stats.PostingEntries == 0 {
		t.Fatal("expected non-zero posting entries")
	}
	if stats.EstimatedPayloadBytes == 0 {
		t.Fatal("expected non-zero payload estimate")
	}
}

func TestDocLenTracking(t *testing.T) {
	idx := NewFullTextIndex()
	idx.Index("doc1", "one two three")
	idx.Index("doc2", "alpha beta")

	idx.mu.RLock()
	total := idx.totalDocLen
	dl1 := idx.docLens[idx.docByPK["doc1"]]
	dl2 := idx.docLens[idx.docByPK["doc2"]]
	idx.mu.RUnlock()

	if dl1 != 3 {
		t.Fatalf("expected doc1 len=3, got %d", dl1)
	}
	if dl2 != 2 {
		t.Fatalf("expected doc2 len=2, got %d", dl2)
	}
	if total != 5 {
		t.Fatalf("expected totalDocLen=5, got %d", total)
	}

	// Delete doc1, totalDocLen should decrease
	idx.Delete("doc1")
	idx.mu.RLock()
	total = idx.totalDocLen
	idx.mu.RUnlock()

	if total != 2 {
		t.Fatalf("expected totalDocLen=2 after delete, got %d", total)
	}
}

func TestStrideDocTerms(t *testing.T) {
	idx := NewFullTextIndex()
	idx.Index("doc1", "hello hello world")

	idx.mu.RLock()
	docID := idx.docByPK["doc1"]
	terms := idx.docTerms[docID]
	idx.mu.RUnlock()

	// stride-2: should have 2 entries (hello, world) × 2 = 4 uint32s
	if len(terms) != 4 {
		t.Fatalf("expected 4 entries (2 terms × stride-2), got %d: %v", len(terms), terms)
	}

	// Find hello's frequency (should be 2)
	helloFreq := uint32(0)
	worldFreq := uint32(0)
	idx.mu.RLock()
	helloID := idx.tokenIDByText["hello"]
	worldID := idx.tokenIDByText["world"]
	idx.mu.RUnlock()

	for i := 0; i < len(terms); i += 2 {
		if terms[i] == helloID {
			helloFreq = terms[i+1]
		}
		if terms[i] == worldID {
			worldFreq = terms[i+1]
		}
	}
	if helloFreq != 2 {
		t.Fatalf("expected hello freq=2, got %d", helloFreq)
	}
	if worldFreq != 1 {
		t.Fatalf("expected world freq=1, got %d", worldFreq)
	}
}

func TestBM25ScoreFunction(t *testing.T) {
	// Basic sanity: higher tf should produce higher score
	s1 := bm25Score(1.0, 10, 1000, 5.0, 5.0)
	s2 := bm25Score(3.0, 10, 1000, 5.0, 5.0)
	if s2 <= s1 {
		t.Fatalf("expected higher tf to produce higher score: tf=1 got %.4f, tf=3 got %.4f", s1, s2)
	}

	// Lower df (rarer term) should produce higher score
	sRare := bm25Score(1.0, 5, 1000, 5.0, 5.0)
	sCommon := bm25Score(1.0, 500, 1000, 5.0, 5.0)
	if sRare <= sCommon {
		t.Fatalf("expected rarer term to score higher: df=5 got %.4f, df=500 got %.4f", sRare, sCommon)
	}

	// Shorter doc should score higher (same tf)
	sShort := bm25Score(1.0, 10, 1000, 3.0, 5.0)
	sLong := bm25Score(1.0, 10, 1000, 10.0, 5.0)
	if sShort <= sLong {
		t.Fatalf("expected shorter doc to score higher: dl=3 got %.4f, dl=10 got %.4f", sShort, sLong)
	}

	// Score should be positive for valid inputs
	if s1 <= 0 {
		t.Fatalf("expected positive score, got %.4f", s1)
	}

	// Score should be finite
	if math.IsNaN(s1) || math.IsInf(s1, 0) {
		t.Fatalf("expected finite score, got %.4f", s1)
	}
}

func TestTopKHeap(t *testing.T) {
	h := make(topKHeap, 0, 4)

	topK(&h, 3, scoredDoc{docID: 1, score: 1.0})
	topK(&h, 3, scoredDoc{docID: 2, score: 3.0})
	topK(&h, 3, scoredDoc{docID: 3, score: 2.0})
	topK(&h, 3, scoredDoc{docID: 4, score: 5.0}) // should evict score=1.0
	topK(&h, 3, scoredDoc{docID: 5, score: 0.5}) // should be rejected

	if h.Len() != 3 {
		t.Fatalf("expected heap size 3, got %d", h.Len())
	}

	// Min should be 2.0 (the lowest of {3.0, 2.0, 5.0})
	if h[0].score != 2.0 {
		t.Fatalf("expected min score 2.0, got %.1f", h[0].score)
	}
}

func TestSearchResultsDescendingScore(t *testing.T) {
	idx := NewFullTextIndex()
	// Create docs with different relevance for "test"
	idx.Index("rare", "test")                                    // short doc, test is the only word
	idx.Index("medium", "test something else here")              // medium doc
	idx.Index("diluted", "a b c d e f g h i j k l m n test o p") // long doc, test buried

	results := idx.Search("test", 10)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d: %v", len(results), results)
	}
	// "rare" should be first — shortest doc with highest tf density
	if results[0] != "rare" {
		t.Fatalf("expected 'rare' first (shortest doc), got %v", results)
	}
}

func TestSearchMultiPrefixAllTokensMustMatch(t *testing.T) {
	idx := NewFullTextIndex()
	idx.Index("doc1", "alpha beta gamma")
	idx.Index("doc2", "alpha delta")
	idx.Index("doc3", "beta gamma")

	// "alpha beta" requires both
	results := idx.Search("alpha beta", 10)
	if len(results) != 1 || results[0] != "doc1" {
		t.Fatalf("expected [doc1], got %v", results)
	}

	// "alpha nonexistent" should return nothing
	results = idx.Search("alpha nonexistent", 10)
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %v", results)
	}
}

func TestExactMatchBoost(t *testing.T) {
	idx := NewFullTextIndex()
	// "Fast" has the exact word "fast". "Fastpitch" only prefix-matches.
	// Both are single-word titles (dl=1), so without the boost "Fastpitch"
	// would rank higher due to its much rarer IDF.
	idx.Index("fast-movie", "Fast")
	idx.Index("fastpitch", "Fastpitch")
	idx.Index("fastball", "Fastball")
	idx.Index("fastlife", "Fastlife")

	results := idx.Search("fast", 10)
	if len(results) == 0 {
		t.Fatal("expected results")
	}
	if results[0] != "fast-movie" {
		t.Fatalf("expected 'fast-movie' first (exact match boost), got %v", results)
	}
}

func TestExactMatchBoostMultiToken(t *testing.T) {
	idx := NewFullTextIndex()
	idx.Index("fast-furious", "Fast and Furious")
	idx.Index("fastball-fury", "Fastball Fury")

	// "fast furious" — both tokens are exact dictionary words
	results := idx.Search("fast furious", 10)
	if len(results) == 0 {
		t.Fatal("expected results")
	}
	if results[0] != "fast-furious" {
		t.Fatalf("expected 'fast-furious' first, got %v", results)
	}
}

func TestDeleteThenReinsert(t *testing.T) {
	idx := NewFullTextIndex()
	idx.Index("doc1", "original text")
	idx.Delete("doc1")
	idx.Index("doc1", "new text")

	results := idx.Search("original", 10)
	if len(results) != 0 {
		t.Fatalf("expected 0 results for 'original', got %v", results)
	}

	results = idx.Search("new", 10)
	if len(results) != 1 || results[0] != "doc1" {
		t.Fatalf("expected [doc1] for 'new', got %v", results)
	}
}
