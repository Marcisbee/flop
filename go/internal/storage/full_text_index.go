package storage

import (
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

	return FullTextStats{
		TokenCount:            len(f.tokenIDByText),
		DocCount:              len(f.docByPK),
		PostingEntries:        postingEntries,
		EstimatedPayloadBytes: payload,
	}
}

// Index stores (or replaces) the indexed text for a row primary key.
func (f *FullTextIndex) Index(pk string, texts ...string) {
	if pk == "" {
		return
	}

	tokens := tokenizeTexts(texts...)

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

	tokenIDs := make([]uint32, 0, len(tokens))
	for _, token := range tokens {
		tokID := f.internTokenIDLocked(token)
		tokenIDs = append(tokenIDs, tokID)
		f.postings[tokID] = addSortedUnique(f.postings[tokID], docID)
	}
	f.ensureDocSlotLocked(docID)
	f.docTerms[docID] = tokenIDs
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
	f.nextDocID = 1
	f.nextTokenID = 1
}

// maxPrefixWordsSingle caps how many dictionary words a single-token prefix
// query expands to, keeping the round-robin cursor count small.
const maxPrefixWordsSingle = 32

// Search returns primary keys that match all query tokens.
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

// searchSinglePrefix handles single-token queries by round-robining across
// matching words' posting lists for fast, diverse results.
func (f *FullTextIndex) searchSinglePrefix(prefix string, limit int) []string {
	lists := f.prefixPostingLists(prefix, maxPrefixWordsSingle)
	if len(lists) == 0 {
		return nil
	}

	// Round-robin: take one doc from each matching word per round.
	// Gives diversity (results from different words) and stops early.
	seen := make(map[uint32]struct{}, limit*2)
	out := make([]string, 0, limit)
	cursors := make([]int, len(lists))
	for len(out) < limit {
		progress := false
		for i, list := range lists {
			for cursors[i] < len(list) {
				docID := list[cursors[i]]
				cursors[i]++
				if _, ok := seen[docID]; ok {
					continue
				}
				seen[docID] = struct{}{}
				if int(docID) < len(f.docPKs) {
					if pk := f.docPKs[docID]; pk != "" {
						out = append(out, pk)
						progress = true
						if len(out) >= limit {
							return out
						}
					}
				}
				break // move to next word
			}
		}
		if !progress {
			break
		}
	}
	return out
}

// searchMultiPrefix handles multi-token queries. It builds a full doc-ID union
// only for the most selective token (fewest matching dictionary words), then
// filters candidates against remaining tokens via their docTerms — avoiding
// expensive unions for broad prefixes like single-letter tokens.
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

	out := make([]string, 0, min(len(candidates), limit))
	for _, docID := range candidates {
		if int(docID) >= len(f.docPKs) {
			continue
		}
		if pk := f.docPKs[docID]; pk != "" {
			out = append(out, pk)
			if len(out) >= limit {
				break
			}
		}
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
func (f *FullTextIndex) docHasAnyTerm(docID uint32, termIDs map[uint32]struct{}) bool {
	if int(docID) >= len(f.docTerms) {
		return false
	}
	for _, tid := range f.docTerms[docID] {
		if _, ok := termIDs[tid]; ok {
			return true
		}
	}
	return false
}

// prefixPostingLists returns the posting lists for dictionary words matching
// the prefix, found via binary search. Capped to maxWords.
func (f *FullTextIndex) prefixPostingLists(prefix string, maxWords int) [][]uint32 {
	start := sort.SearchStrings(f.sortedTokens, prefix)

	var lists [][]uint32
	for i := start; i < len(f.sortedTokens) && len(lists) < maxWords; i++ {
		word := f.sortedTokens[i]
		if !strings.HasPrefix(word, prefix) {
			break
		}
		if list := f.postings[f.tokenIDByText[word]]; len(list) > 0 {
			lists = append(lists, list)
		}
	}
	return lists
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
	for _, termID := range oldTerms {
		list := removeSortedValue(f.postings[termID], docID)
		if len(list) == 0 {
			delete(f.postings, termID)
			continue
		}
		f.postings[termID] = list
	}
	f.docTerms[docID] = nil
}

func (f *FullTextIndex) ensureDocSlotLocked(docID uint32) {
	need := int(docID) + 1
	if need <= len(f.docTerms) {
		return
	}
	grow := need - len(f.docTerms)
	f.docTerms = append(f.docTerms, make([][]uint32, grow)...)
	f.docPKs = append(f.docPKs, make([]string, grow)...)
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

func intersectSorted(a, b []uint32) []uint32 {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	out := make([]uint32, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}
	return out
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

