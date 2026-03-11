package flop

import (
	"container/heap"
	"math"
	"sort"
	"strings"
	"sync"
	"unicode"
)

// FTSIndex is an in-memory inverted index with BM25 ranking and prefix matching.
// Each token maps to a sorted posting list of doc IDs. A sorted token dictionary
// enables O(log n) prefix lookups for autocomplete-style search.
type FTSIndex struct {
	mu sync.RWMutex

	postings map[uint32][]uint32 // tokenID -> sorted doc IDs
	docTerms [][]uint32          // docID -> stride-2 [tokID, freq, ...]
	docByID  map[uint64]uint32   // row ID -> internal doc ID
	docIDs   []uint64            // internal doc ID -> row ID

	tokenIDByText map[string]uint32
	sortedTokens  []string // sorted word list for binary-search prefix lookup
	tokensDirty   bool

	docLens     []uint16 // document length (token count) per docID
	totalDocLen uint64

	nextDocID   uint32
	nextTokenID uint32
}

// NewFTSIndex creates a new full-text search index.
func NewFTSIndex() *FTSIndex {
	return &FTSIndex{
		postings:      make(map[uint32][]uint32),
		docTerms:      make([][]uint32, 1),
		docByID:       make(map[uint64]uint32),
		docIDs:        make([]uint64, 1),
		tokenIDByText: make(map[string]uint32),
		docLens:       make([]uint16, 1),
		nextDocID:     1,
		nextTokenID:   1,
	}
}

// Index stores (or replaces) the indexed text for a row ID.
func (f *FTSIndex) Index(rowID uint64, texts ...string) {
	if rowID == 0 {
		return
	}

	freqs := tokenizeWithFreq(texts...)

	f.mu.Lock()
	defer f.mu.Unlock()

	docID, exists := f.docByID[rowID]
	if !exists {
		docID = f.nextDocID
		f.nextDocID++
		f.ensureDocSlot(docID)
		f.docByID[rowID] = docID
		f.docIDs[docID] = rowID
	}

	f.removeDocTokens(docID)

	termFreqs := make([]uint32, 0, len(freqs)*2)
	var docLen uint16
	for token, freq := range freqs {
		tokID := f.internTokenID(token)
		termFreqs = append(termFreqs, tokID, uint32(freq))
		f.postings[tokID] = addSortedUnique(f.postings[tokID], docID)
		docLen += freq
	}
	f.ensureDocSlot(docID)
	f.docTerms[docID] = termFreqs
	f.docLens[docID] = docLen
	f.totalDocLen += uint64(docLen)
}

// Delete removes a row from the index.
func (f *FTSIndex) Delete(rowID uint64) {
	if rowID == 0 {
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	docID, exists := f.docByID[rowID]
	if !exists {
		return
	}

	f.removeDocTokens(docID)
	delete(f.docByID, rowID)
	if int(docID) < len(f.docIDs) {
		f.docIDs[docID] = 0
	}
	if int(docID) < len(f.docTerms) {
		f.docTerms[docID] = nil
	}
}

// Search returns row IDs matching the query, ranked by BM25 with prefix matching.
// Each query token is prefix-matched against the token dictionary so that
// incomplete words work like autocomplete (e.g. "galact" matches "galactic").
func (f *FTSIndex) Search(query string, limit int) []uint64 {
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

// Finalize eagerly rebuilds the sorted token list after bulk indexing.
func (f *FTSIndex) Finalize() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rebuildSortedTokens()
}

// --- internal ---

const maxPrefixExpand = 256

func (f *FTSIndex) searchSinglePrefix(prefix string, limit int) []uint64 {
	termInfos := f.prefixTermInfos(prefix)
	if len(termInfos) == 0 {
		return nil
	}

	if len(termInfos) > maxPrefixExpand {
		exactID, hasExact := f.tokenIDByText[prefix]
		sort.Slice(termInfos, func(i, j int) bool {
			return termInfos[i].df < termInfos[j].df
		})
		termInfos = termInfos[:maxPrefixExpand]
		if hasExact {
			found := false
			for _, ti := range termInfos {
				if ti.id == exactID {
					found = true
					break
				}
			}
			if !found {
				termInfos[maxPrefixExpand-1] = ftsTermInfo{id: exactID, df: len(f.postings[exactID])}
			}
		}
	}

	candidates := f.termInfoDocIDs(termInfos)
	if len(candidates) == 0 {
		return nil
	}

	numDocs := len(f.docByID)
	if numDocs == 0 {
		return nil
	}
	avgdl := float64(f.totalDocLen) / float64(numDocs)

	termDFs := make(map[uint32]int, len(termInfos))
	for _, ti := range termInfos {
		termDFs[ti.id] = ti.df
	}

	exactIDs := make(map[uint32]struct{}, 1)
	if id, ok := f.tokenIDByText[prefix]; ok {
		exactIDs[id] = struct{}{}
	}

	h := make(ftsHeap, 0, limit+1)
	for _, docID := range candidates {
		if int(docID) >= len(f.docIDs) || f.docIDs[docID] == 0 {
			continue
		}
		dl := float64(f.docLens[docID])
		score := f.scoreDoc(docID, termDFs, exactIDs, numDocs, dl, avgdl)
		if score > 0 {
			ftsTopK(&h, limit, ftsScored{docID: docID, score: score})
		}
	}

	out := make([]uint64, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		doc := heap.Pop(&h).(ftsScored)
		out[i] = f.docIDs[doc.docID]
	}
	return out
}

func (f *FTSIndex) searchMultiPrefix(tokens []string, limit int) []uint64 {
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

	candidates := f.prefixDocIDs(ranked_[0].token)
	if len(candidates) == 0 {
		return nil
	}

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

	numDocs := len(f.docByID)
	if numDocs == 0 {
		return nil
	}
	avgdl := float64(f.totalDocLen) / float64(numDocs)

	termDFs := make(map[uint32]int)
	for _, token := range tokens {
		for _, ti := range f.prefixTermInfos(token) {
			termDFs[ti.id] = ti.df
		}
	}

	exactIDs := make(map[uint32]struct{}, len(tokens))
	for _, token := range tokens {
		if id, ok := f.tokenIDByText[token]; ok {
			exactIDs[id] = struct{}{}
		}
	}

	h := make(ftsHeap, 0, limit+1)
	for _, docID := range candidates {
		if int(docID) >= len(f.docIDs) || f.docIDs[docID] == 0 {
			continue
		}
		dl := float64(f.docLens[docID])
		score := f.scoreDoc(docID, termDFs, exactIDs, numDocs, dl, avgdl)
		if score > 0 {
			ftsTopK(&h, limit, ftsScored{docID: docID, score: score})
		}
	}

	out := make([]uint64, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		doc := heap.Pop(&h).(ftsScored)
		out[i] = f.docIDs[doc.docID]
	}
	return out
}

func (f *FTSIndex) prefixWordCount(prefix string) int {
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

func (f *FTSIndex) prefixDocIDs(prefix string) []uint32 {
	start := sort.SearchStrings(f.sortedTokens, prefix)
	var merged []uint32
	for i := start; i < len(f.sortedTokens); i++ {
		if !strings.HasPrefix(f.sortedTokens[i], prefix) {
			break
		}
		if list := f.postings[f.tokenIDByText[f.sortedTokens[i]]]; len(list) > 0 {
			merged = ftsUnionSorted(merged, list)
		}
	}
	return merged
}

func (f *FTSIndex) termInfoDocIDs(infos []ftsTermInfo) []uint32 {
	var merged []uint32
	for _, ti := range infos {
		if list := f.postings[ti.id]; len(list) > 0 {
			merged = ftsUnionSorted(merged, list)
		}
	}
	return merged
}

func (f *FTSIndex) prefixTermIDSet(prefix string) map[uint32]struct{} {
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

type ftsTermInfo struct {
	id uint32
	df int
}

func (f *FTSIndex) prefixTermInfos(prefix string) []ftsTermInfo {
	start := sort.SearchStrings(f.sortedTokens, prefix)
	var infos []ftsTermInfo
	for i := start; i < len(f.sortedTokens); i++ {
		word := f.sortedTokens[i]
		if !strings.HasPrefix(word, prefix) {
			break
		}
		tokID := f.tokenIDByText[word]
		infos = append(infos, ftsTermInfo{id: tokID, df: len(f.postings[tokID])})
	}
	return infos
}

func (f *FTSIndex) docHasAnyTerm(docID uint32, termIDs map[uint32]struct{}) bool {
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

// BM25 parameters.
const (
	ftsBM25K1         = 1.2
	ftsBM25B          = 0.75
	ftsExactBoost     = 4.0
)

func ftsBM25Score(tf float64, df int, numDocs int, dl float64, avgdl float64) float64 {
	idf := math.Log(1 + (float64(numDocs)-float64(df)+0.5)/(float64(df)+0.5))
	tfNorm := (tf * (ftsBM25K1 + 1)) / (tf + ftsBM25K1*(1-ftsBM25B+ftsBM25B*(dl/avgdl)))
	return idf * tfNorm
}

func (f *FTSIndex) scoreDoc(docID uint32, termDFs map[uint32]int, exactIDs map[uint32]struct{}, numDocs int, dl, avgdl float64) float64 {
	if int(docID) >= len(f.docTerms) {
		return 0
	}
	terms := f.docTerms[docID]
	var total float64
	for i := 0; i < len(terms); i += 2 {
		if df, ok := termDFs[terms[i]]; ok {
			s := ftsBM25Score(float64(terms[i+1]), df, numDocs, dl, avgdl)
			if _, exact := exactIDs[terms[i]]; exact {
				s *= ftsExactBoost
			}
			total += s
		}
	}
	return total
}

// sorted token management

func (f *FTSIndex) ensureSortedTokens() {
	f.mu.RLock()
	dirty := f.tokensDirty || len(f.sortedTokens) != len(f.tokenIDByText)
	f.mu.RUnlock()
	if !dirty {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rebuildSortedTokens()
}

func (f *FTSIndex) rebuildSortedTokens() {
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

func (f *FTSIndex) internTokenID(token string) uint32 {
	if id, ok := f.tokenIDByText[token]; ok {
		return id
	}
	id := f.nextTokenID
	f.nextTokenID++
	f.tokenIDByText[token] = id
	f.tokensDirty = true
	return id
}

func (f *FTSIndex) removeDocTokens(docID uint32) {
	if int(docID) >= len(f.docTerms) {
		return
	}
	oldTerms := f.docTerms[docID]
	if len(oldTerms) == 0 {
		return
	}
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

func (f *FTSIndex) ensureDocSlot(docID uint32) {
	need := int(docID) + 1
	if need <= len(f.docTerms) {
		return
	}
	grow := need - len(f.docTerms)
	f.docTerms = append(f.docTerms, make([][]uint32, grow)...)
	f.docIDs = append(f.docIDs, make([]uint64, grow)...)
	f.docLens = append(f.docLens, make([]uint16, grow)...)
}

// top-K heap

type ftsScored struct {
	docID uint32
	score float64
}

type ftsHeap []ftsScored

func (h ftsHeap) Len() int            { return len(h) }
func (h ftsHeap) Less(i, j int) bool  { return h[i].score < h[j].score }
func (h ftsHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *ftsHeap) Push(x any)         { *h = append(*h, x.(ftsScored)) }
func (h *ftsHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func ftsTopK(h *ftsHeap, k int, doc ftsScored) {
	if h.Len() < k {
		heap.Push(h, doc)
	} else if doc.score > (*h)[0].score {
		(*h)[0] = doc
		heap.Fix(h, 0)
	}
}

// sorted set helpers

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

func ftsUnionSorted(a, b []uint32) []uint32 {
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

// tokenization

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

func tokenizeWithFreq(texts ...string) map[string]uint16 {
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

// Tokenize splits text into normalized tokens (exported for compatibility).
func Tokenize(text string) []string {
	return tokenizeTexts(text)
}
