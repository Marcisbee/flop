package storage

import (
	"container/heap"
	"math"
	"sort"
	"strings"
	"sync"
	"unicode"
)

// FullTextIndex is an in-memory inverted index optimized for memory:
// tokenID -> sorted doc IDs, with token dictionary and docID -> primary key mapping.
// A sorted token list enables fast O(log n) prefix lookups for autocomplete.
type FullTextIndex struct {
	mu sync.RWMutex

	postings map[uint32][]uint32
	docTerms [][]uint32
	docByPK  map[string]uint32
	docPKs   []string

	tokenIDByText map[string]uint32
	sortedTokens  []string // sorted word list for binary-search prefix lookup
	tokensDirty   bool     // true when sortedTokens needs rebuild

	docLens     []uint16 // document length (token count) per docID
	totalDocLen uint64   // sum of all document lengths (for avgdl)

	nextDocID   uint32
	nextTokenID uint32
}

type FullTextStats struct {
	TokenCount            int
	DocCount              int
	PostingEntries        int
	EstimatedPayloadBytes uint64
}

func NewFullTextIndex() *FullTextIndex {
	return &FullTextIndex{
		postings:      make(map[uint32][]uint32),
		docTerms:      make([][]uint32, 1),
		docByPK:       make(map[string]uint32),
		docPKs:        make([]string, 1),
		tokenIDByText: make(map[string]uint32),
		docLens:       make([]uint16, 1),
		nextDocID:     1,
		nextTokenID:   1,
	}
}

// Stats returns structural counts and a lower-bound payload estimate.
// The estimate excludes Go runtime map/slice overhead and allocator metadata.
func (f *FullTextIndex) Stats() FullTextStats {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var postingEntries int
	var payload uint64
	for _, docs := range f.postings {
		postingEntries += len(docs)
		payload += uint64(4 * len(docs))
	}
	for term := range f.tokenIDByText {
		payload += uint64(len(term))
	}
	for i := 1; i < len(f.docTerms); i++ {
		termIDs := f.docTerms[i]
		payload += uint64(4 * len(termIDs))
	}
	for pk := range f.docByPK {
		payload += uint64(len(pk) + 4)
	}
	payload += uint64(2 * len(f.docLens)) // docLens: uint16 per doc

	return FullTextStats{
		TokenCount:            len(f.tokenIDByText),
		DocCount:              len(f.docByPK),
		PostingEntries:        postingEntries,
		EstimatedPayloadBytes: payload,
	}
}

// Index stores (or replaces) the indexed text for a row primary key.
// docTerms uses stride-2 layout: [tokID, freq, tokID, freq, ...].
func (f *FullTextIndex) Index(pk string, texts ...string) {
	if pk == "" {
		return
	}

	freqs := tokenizeTextsWithFreq(texts...)

	f.mu.Lock()
	defer f.mu.Unlock()

	docID, exists := f.docByPK[pk]
	if !exists {
		docID = f.nextDocID
		f.nextDocID++
		f.ensureDocSlotLocked(docID)
		f.docByPK[pk] = docID
		f.docPKs[docID] = pk
	}

	f.removeDocTokensLocked(docID)

	termFreqs := make([]uint32, 0, len(freqs)*2)
	var docLen uint16
	for token, freq := range freqs {
		tokID := f.internTokenIDLocked(token)
		termFreqs = append(termFreqs, tokID, uint32(freq))
		f.postings[tokID] = addSortedUnique(f.postings[tokID], docID)
		docLen += freq
	}
	f.ensureDocSlotLocked(docID)
	f.docTerms[docID] = termFreqs
	f.docLens[docID] = docLen
	f.totalDocLen += uint64(docLen)
}

func (f *FullTextIndex) Delete(pk string) {
	if pk == "" {
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	docID, exists := f.docByPK[pk]
	if !exists {
		return
	}

	f.removeDocTokensLocked(docID)
	delete(f.docByPK, pk)
	if int(docID) < len(f.docPKs) {
		f.docPKs[docID] = ""
	}
	if int(docID) < len(f.docTerms) {
		f.docTerms[docID] = nil
	}
}

// Clear removes all indexed data.
func (f *FullTextIndex) Clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.postings = make(map[uint32][]uint32)
	f.docTerms = make([][]uint32, 1)
	f.docByPK = make(map[string]uint32)
	f.docPKs = make([]string, 1)
	f.tokenIDByText = make(map[string]uint32)
	f.sortedTokens = nil
	f.tokensDirty = false
	f.docLens = make([]uint16, 1)
	f.totalDocLen = 0
	f.nextDocID = 1
	f.nextTokenID = 1
}

// Search returns primary keys that match all query tokens, ranked by BM25.
// Each query token is prefix-matched against the token dictionary so that
// incomplete words work like autocomplete (e.g. "galact" matches "galactic").
func (f *FullTextIndex) Search(query string, limit int) []string {
	tokens := tokenizeTexts(query)
	if len(tokens) == 0 {
		return nil
	}
	if limit <= 0 {
		limit = 100
	}

	f.ensureSortedTokens()

	f.mu.RLock()
	defer f.mu.RUnlock()

	if len(tokens) == 1 {
		return f.searchSinglePrefix(tokens[0], limit)
	}
	return f.searchMultiPrefix(tokens, limit)
}

// Finalize eagerly rebuilds the sorted token list after bulk indexing,
// so the first Search() call doesn't pay the sorting cost.
func (f *FullTextIndex) Finalize() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rebuildSortedTokensIfNeeded()
}

// ensureSortedTokens rebuilds the sorted token list under a write lock if needed.
func (f *FullTextIndex) ensureSortedTokens() {
	f.mu.RLock()
	dirty := f.tokensDirty || len(f.sortedTokens) != len(f.tokenIDByText)
	f.mu.RUnlock()
	if !dirty {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rebuildSortedTokensIfNeeded()
}

// maxPrefixExpandSingle caps how many dictionary words a single-token prefix
// query expands to. For broad prefixes like "h" (matching thousands of words),
// we keep only the rarest terms (lowest df → highest IDF) to bound work while
// preserving the most relevant results.
const maxPrefixExpandSingle = 256

// searchSinglePrefix handles single-token queries with BM25 scoring.
func (f *FullTextIndex) searchSinglePrefix(prefix string, limit int) []string {
	termInfos := f.prefixTermInfos(prefix)
	if len(termInfos) == 0 {
		return nil
	}

	// For broad prefixes, keep only the rarest terms (highest IDF),
	// but always include the exact-match term if it exists.
	if len(termInfos) > maxPrefixExpandSingle {
		exactID, hasExact := f.tokenIDByText[prefix]
		sort.Slice(termInfos, func(i, j int) bool {
			return termInfos[i].df < termInfos[j].df
		})
		termInfos = termInfos[:maxPrefixExpandSingle]
		if hasExact {
			found := false
			for _, ti := range termInfos {
				if ti.id == exactID {
					found = true
					break
				}
			}
			if !found {
				// Replace the last (highest df) entry with the exact match.
				termInfos[maxPrefixExpandSingle-1] = prefixTermInfo{id: exactID, df: len(f.postings[exactID])}
			}
		}
	}

	// Build union from selected terms only.
	candidates := f.termInfoDocIDs(termInfos)
	if len(candidates) == 0 {
		return nil
	}

	numDocs := len(f.docByPK)
	if numDocs == 0 {
		return nil
	}
	avgdl := float64(f.totalDocLen) / float64(numDocs)

	// Build termID → df map for O(1) lookup during scoring.
	termDFs := make(map[uint32]int, len(termInfos))
	for _, ti := range termInfos {
		termDFs[ti.id] = ti.df
	}

	// Exact match boost: if the query token is an exact dictionary word, boost it.
	exactIDs := make(map[uint32]struct{}, 1)
	if id, ok := f.tokenIDByText[prefix]; ok {
		exactIDs[id] = struct{}{}
	}

	h := make(topKHeap, 0, limit+1)
	for _, docID := range candidates {
		if int(docID) >= len(f.docPKs) || f.docPKs[docID] == "" {
			continue
		}
		dl := float64(f.docLens[docID])
		score := f.scoreDoc(docID, termDFs, exactIDs, numDocs, dl, avgdl)
		if score > 0 {
			topK(&h, limit, scoredDoc{docID: docID, score: score})
		}
	}

	// Extract results in descending score order.
	out := make([]string, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		doc := heap.Pop(&h).(scoredDoc)
		out[i] = f.docPKs[doc.docID]
	}
	return out
}

// searchMultiPrefix handles multi-token queries with BM25 scoring.
// It builds a full doc-ID union only for the most selective token,
// filters candidates against remaining tokens, then scores with BM25.
func (f *FullTextIndex) searchMultiPrefix(tokens []string, limit int) []string {
	// Count matching dictionary words per token to find selectivity.
	type ranked struct {
		token string
		count int
	}
	ranked_ := make([]ranked, 0, len(tokens))
	for _, token := range tokens {
		c := f.prefixWordCount(token)
		if c == 0 {
			return nil
		}
		ranked_ = append(ranked_, ranked{token, c})
	}
	sort.Slice(ranked_, func(i, j int) bool {
		return ranked_[i].count < ranked_[j].count
	})

	// Build full union for the most selective token → candidates.
	candidates := f.prefixDocIDs(ranked_[0].token)
	if len(candidates) == 0 {
		return nil
	}

	// Filter candidates using term-ID sets for remaining tokens.
	for i := 1; i < len(ranked_); i++ {
		termIDs := f.prefixTermIDSet(ranked_[i].token)
		n := 0
		for _, docID := range candidates {
			if f.docHasAnyTerm(docID, termIDs) {
				candidates[n] = docID
				n++
			}
		}
		candidates = candidates[:n]
		if n == 0 {
			return nil
		}
	}

	numDocs := len(f.docByPK)
	if numDocs == 0 {
		return nil
	}
	avgdl := float64(f.totalDocLen) / float64(numDocs)

	// Build combined termID → df map across all query tokens for per-term scoring.
	termDFs := make(map[uint32]int)
	for _, token := range tokens {
		for _, ti := range f.prefixTermInfos(token) {
			termDFs[ti.id] = ti.df
		}
	}

	// Exact match boost for query tokens that are exact dictionary words.
	exactIDs := make(map[uint32]struct{}, len(tokens))
	for _, token := range tokens {
		if id, ok := f.tokenIDByText[token]; ok {
			exactIDs[id] = struct{}{}
		}
	}

	// Score each surviving candidate: each dictionary term contributes
	// independently with its own IDF.
	h := make(topKHeap, 0, limit+1)
	for _, docID := range candidates {
		if int(docID) >= len(f.docPKs) || f.docPKs[docID] == "" {
			continue
		}
		dl := float64(f.docLens[docID])
		score := f.scoreDoc(docID, termDFs, exactIDs, numDocs, dl, avgdl)
		if score > 0 {
			topK(&h, limit, scoredDoc{docID: docID, score: score})
		}
	}

	// Extract results in descending score order.
	out := make([]string, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		doc := heap.Pop(&h).(scoredDoc)
		out[i] = f.docPKs[doc.docID]
	}
	return out
}

// prefixWordCount returns how many dictionary words match the prefix.
func (f *FullTextIndex) prefixWordCount(prefix string) int {
	start := sort.SearchStrings(f.sortedTokens, prefix)
	count := 0
	for i := start; i < len(f.sortedTokens); i++ {
		if !strings.HasPrefix(f.sortedTokens[i], prefix) {
			break
		}
		count++
	}
	return count
}

// prefixDocIDs returns the sorted union of all doc IDs for dictionary words
// matching the prefix.
func (f *FullTextIndex) prefixDocIDs(prefix string) []uint32 {
	start := sort.SearchStrings(f.sortedTokens, prefix)
	var merged []uint32
	for i := start; i < len(f.sortedTokens); i++ {
		if !strings.HasPrefix(f.sortedTokens[i], prefix) {
			break
		}
		if list := f.postings[f.tokenIDByText[f.sortedTokens[i]]]; len(list) > 0 {
			merged = unionSorted(merged, list)
		}
	}
	return merged
}

// termInfoDocIDs returns the sorted union of doc IDs for the given term infos.
func (f *FullTextIndex) termInfoDocIDs(infos []prefixTermInfo) []uint32 {
	var merged []uint32
	for _, ti := range infos {
		if list := f.postings[ti.id]; len(list) > 0 {
			merged = unionSorted(merged, list)
		}
	}
	return merged
}

// prefixTermIDSet returns the set of term IDs for all dictionary words
// matching the prefix.
func (f *FullTextIndex) prefixTermIDSet(prefix string) map[uint32]struct{} {
	start := sort.SearchStrings(f.sortedTokens, prefix)
	ids := make(map[uint32]struct{})
	for i := start; i < len(f.sortedTokens); i++ {
		if !strings.HasPrefix(f.sortedTokens[i], prefix) {
			break
		}
		ids[f.tokenIDByText[f.sortedTokens[i]]] = struct{}{}
	}
	return ids
}

// docHasAnyTerm checks whether a document contains any term in the set.
// docTerms uses stride-2: [tokID, freq, tokID, freq, ...].
func (f *FullTextIndex) docHasAnyTerm(docID uint32, termIDs map[uint32]struct{}) bool {
	if int(docID) >= len(f.docTerms) {
		return false
	}
	terms := f.docTerms[docID]
	for i := 0; i < len(terms); i += 2 {
		if _, ok := termIDs[terms[i]]; ok {
			return true
		}
	}
	return false
}


// rebuildSortedTokensIfNeeded lazily rebuilds the sorted token list.
func (f *FullTextIndex) rebuildSortedTokensIfNeeded() {
	if !f.tokensDirty && len(f.sortedTokens) == len(f.tokenIDByText) {
		return
	}
	f.sortedTokens = make([]string, 0, len(f.tokenIDByText))
	for word := range f.tokenIDByText {
		f.sortedTokens = append(f.sortedTokens, word)
	}
	sort.Strings(f.sortedTokens)
	f.tokensDirty = false
}

func (f *FullTextIndex) internTokenIDLocked(token string) uint32 {
	if id, ok := f.tokenIDByText[token]; ok {
		return id
	}
	id := f.nextTokenID
	f.nextTokenID++
	f.tokenIDByText[token] = id
	f.tokensDirty = true
	return id
}

func (f *FullTextIndex) removeDocTokensLocked(docID uint32) {
	if int(docID) >= len(f.docTerms) {
		return
	}
	oldTerms := f.docTerms[docID]
	if len(oldTerms) == 0 {
		return
	}
	// stride-2: [tokID, freq, tokID, freq, ...]
	for i := 0; i < len(oldTerms); i += 2 {
		termID := oldTerms[i]
		list := removeSortedValue(f.postings[termID], docID)
		if len(list) == 0 {
			delete(f.postings, termID)
			continue
		}
		f.postings[termID] = list
	}
	f.docTerms[docID] = nil
	if int(docID) < len(f.docLens) {
		f.totalDocLen -= uint64(f.docLens[docID])
		f.docLens[docID] = 0
	}
}

func (f *FullTextIndex) ensureDocSlotLocked(docID uint32) {
	need := int(docID) + 1
	if need <= len(f.docTerms) {
		return
	}
	grow := need - len(f.docTerms)
	f.docTerms = append(f.docTerms, make([][]uint32, grow)...)
	f.docPKs = append(f.docPKs, make([]string, grow)...)
	f.docLens = append(f.docLens, make([]uint16, grow)...)
}

func addSortedUnique(in []uint32, id uint32) []uint32 {
	n := len(in)
	if n == 0 {
		return []uint32{id}
	}
	if id > in[n-1] {
		return append(in, id)
	}

	pos := sort.Search(n, func(i int) bool { return in[i] >= id })
	if pos < n && in[pos] == id {
		return in
	}

	in = append(in, 0)
	copy(in[pos+1:], in[pos:])
	in[pos] = id
	return in
}

func removeSortedValue(in []uint32, id uint32) []uint32 {
	n := len(in)
	if n == 0 {
		return in
	}
	pos := sort.Search(n, func(i int) bool { return in[i] >= id })
	if pos >= n || in[pos] != id {
		return in
	}
	return append(in[:pos], in[pos+1:]...)
}

func unionSorted(a, b []uint32) []uint32 {
	if len(a) == 0 {
		return append([]uint32(nil), b...)
	}
	if len(b) == 0 {
		return a
	}
	out := make([]uint32, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		default:
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}

// BM25 parameters.
const (
	bm25K1 = 1.2
	bm25B  = 0.75
	// exactMatchBoost multiplies BM25 contribution when a document contains
	// the exact query token (not just a prefix match). This ensures "Fast"
	// ranks above "Fastpitch" when the user types "fast".
	exactMatchBoost = 4.0
)

// bm25Score computes BM25 for a single query term in a single document.
func bm25Score(tf float64, df int, numDocs int, dl float64, avgdl float64) float64 {
	idf := math.Log(1 + (float64(numDocs)-float64(df)+0.5)/(float64(df)+0.5))
	tfNorm := (tf * (bm25K1 + 1)) / (tf + bm25K1*(1-bm25B+bm25B*(dl/avgdl)))
	return idf * tfNorm
}

// scoredDoc pairs a doc ID with its BM25 score.
type scoredDoc struct {
	docID uint32
	score float64
}

// topKHeap is a min-heap of scoredDoc for top-K selection.
type topKHeap []scoredDoc

func (h topKHeap) Len() int            { return len(h) }
func (h topKHeap) Less(i, j int) bool   { return h[i].score < h[j].score }
func (h topKHeap) Swap(i, j int)        { h[i], h[j] = h[j], h[i] }
func (h *topKHeap) Push(x any)          { *h = append(*h, x.(scoredDoc)) }
func (h *topKHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// topK inserts a scored doc into the min-heap, evicting the lowest if full.
func topK(h *topKHeap, k int, doc scoredDoc) {
	if h.Len() < k {
		heap.Push(h, doc)
	} else if doc.score > (*h)[0].score {
		(*h)[0] = doc
		heap.Fix(h, 0)
	}
}

// prefixTermInfo holds a term ID and its document frequency for prefix matches.
type prefixTermInfo struct {
	id uint32
	df int
}

// prefixTermInfos returns term IDs and their document frequencies for words matching prefix.
func (f *FullTextIndex) prefixTermInfos(prefix string) []prefixTermInfo {
	start := sort.SearchStrings(f.sortedTokens, prefix)
	var infos []prefixTermInfo
	for i := start; i < len(f.sortedTokens); i++ {
		word := f.sortedTokens[i]
		if !strings.HasPrefix(word, prefix) {
			break
		}
		tokID := f.tokenIDByText[word]
		infos = append(infos, prefixTermInfo{id: tokID, df: len(f.postings[tokID])})
	}
	return infos
}

// scoreDoc computes the BM25 score for a document by summing per-term
// contributions. Each dictionary term contributes independently with its
// own IDF. Terms that exactly match a query token get an extra boost so
// that "Fast" ranks above "Fastpitch" for query "fast".
func (f *FullTextIndex) scoreDoc(docID uint32, termDFs map[uint32]int, exactIDs map[uint32]struct{}, numDocs int, dl, avgdl float64) float64 {
	if int(docID) >= len(f.docTerms) {
		return 0
	}
	terms := f.docTerms[docID]
	var total float64
	for i := 0; i < len(terms); i += 2 {
		if df, ok := termDFs[terms[i]]; ok {
			s := bm25Score(float64(terms[i+1]), df, numDocs, dl, avgdl)
			if _, exact := exactIDs[terms[i]]; exact {
				s *= exactMatchBoost
			}
			total += s
		}
	}
	return total
}

func tokenizeTexts(texts ...string) []string {
	if len(texts) == 0 {
		return nil
	}

	const minTokenLen = 1
	seen := make(map[string]struct{}, 16)
	var token []rune

	flush := func() {
		if len(token) < minTokenLen {
			token = token[:0]
			return
		}
		seen[string(token)] = struct{}{}
		token = token[:0]
	}

	for _, text := range texts {
		for _, r := range text {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				token = append(token, unicode.ToLower(r))
				continue
			}
			if r == '&' {
				flush()
				seen["and"] = struct{}{}
				continue
			}
			flush()
		}
		flush()
	}

	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	return out
}

// tokenizeTextsWithFreq is like tokenizeTexts but returns token frequencies.
// Used during indexing to populate stride-2 docTerms.
func tokenizeTextsWithFreq(texts ...string) map[string]uint16 {
	if len(texts) == 0 {
		return nil
	}

	const minTokenLen = 1
	freqs := make(map[string]uint16, 16)
	var token []rune

	flush := func() {
		if len(token) < minTokenLen {
			token = token[:0]
			return
		}
		s := string(token)
		freqs[s]++
		token = token[:0]
	}

	for _, text := range texts {
		for _, r := range text {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				token = append(token, unicode.ToLower(r))
				continue
			}
			if r == '&' {
				flush()
				freqs["and"]++
				continue
			}
			flush()
		}
		flush()
	}

	return freqs
}

