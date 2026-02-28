package storage

import (
	"sort"
	"strings"
	"sync"
	"unicode"
)

// FullTextIndex is an in-memory inverted index optimized for memory:
// tokenID -> sorted doc IDs, with token dictionary and docID -> primary key mapping.
type FullTextIndex struct {
	mu sync.RWMutex

	postings map[uint32][]uint32
	docTerms [][]uint32
	docByPK  map[string]uint32
	docPKs   []string

	tokenIDByText map[string]uint32

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
	f.nextDocID = 1
	f.nextTokenID = 1
}

// Search returns primary keys that match all query tokens.
func (f *FullTextIndex) Search(query string, limit int) []string {
	tokens := tokenizeTexts(query)
	if len(tokens) == 0 {
		return nil
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	lists := make([][]uint32, 0, len(tokens))
	for _, token := range tokens {
		tokenID, ok := f.tokenIDByText[token]
		if !ok {
			return nil
		}
		list := f.postings[tokenID]
		if len(list) == 0 {
			return nil
		}
		lists = append(lists, list)
	}

	sort.Slice(lists, func(i, j int) bool {
		return len(lists[i]) < len(lists[j])
	})

	candidates := append([]uint32(nil), lists[0]...)
	for i := 1; i < len(lists); i++ {
		candidates = intersectSorted(candidates, lists[i])
		if len(candidates) == 0 {
			return nil
		}
	}

	out := make([]string, 0, len(candidates))
	for _, docID := range candidates {
		if int(docID) >= len(f.docPKs) {
			continue
		}
		pk := f.docPKs[docID]
		if pk == "" {
			continue
		}
		out = append(out, pk)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func (f *FullTextIndex) internTokenIDLocked(token string) uint32 {
	if id, ok := f.tokenIDByText[token]; ok {
		return id
	}
	id := f.nextTokenID
	f.nextTokenID++
	f.tokenIDByText[token] = id
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

func tokenizeTexts(texts ...string) []string {
	if len(texts) == 0 {
		return nil
	}

	const minTokenLen = 2
	seen := make(map[string]struct{}, 16)
	var token []rune

	flush := func() {
		if len(token) < minTokenLen {
			token = token[:0]
			return
		}
		t := string(token)
		if _, stop := defaultStopWords[t]; !stop {
			seen[t] = struct{}{}
		}
		token = token[:0]
	}

	for _, text := range texts {
		for _, r := range text {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				token = append(token, unicode.ToLower(r))
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

var defaultStopWords = func() map[string]struct{} {
	words := []string{
		"a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
		"has", "he", "in", "is", "it", "its", "of", "on", "or", "that",
		"the", "to", "was", "were", "will", "with",
	}
	m := make(map[string]struct{}, len(words))
	for _, w := range words {
		m[strings.ToLower(w)] = struct{}{}
	}
	return m
}()
