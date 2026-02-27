package storage

import (
	"sort"
	"strings"
	"sync"
	"unicode"
)

// FullTextIndex is an in-memory inverted index optimized for low memory:
// token -> sorted doc IDs, with doc ID -> primary key mapping.
type FullTextIndex struct {
	mu sync.RWMutex

	postings map[string][]uint32
	docTerms map[uint32][]string
	docByPK  map[string]uint32
	pkByDoc  map[uint32]string
	nextDoc  uint32
}

func NewFullTextIndex() *FullTextIndex {
	return &FullTextIndex{
		postings: make(map[string][]uint32),
		docTerms: make(map[uint32][]string),
		docByPK:  make(map[string]uint32),
		pkByDoc:  make(map[uint32]string),
		nextDoc:  1,
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
		docID = f.nextDoc
		f.nextDoc++
		f.docByPK[pk] = docID
		f.pkByDoc[docID] = pk
	}

	f.removeDocTokensLocked(docID)
	f.docTerms[docID] = tokens

	for _, token := range tokens {
		f.postings[token] = addSortedUnique(f.postings[token], docID)
	}
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
	delete(f.pkByDoc, docID)
	delete(f.docTerms, docID)
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
		list := f.postings[token]
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
		pk := f.pkByDoc[docID]
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

func (f *FullTextIndex) removeDocTokensLocked(docID uint32) {
	oldTokens := f.docTerms[docID]
	if len(oldTokens) == 0 {
		return
	}
	for _, token := range oldTokens {
		list := removeSortedValue(f.postings[token], docID)
		if len(list) == 0 {
			delete(f.postings, token)
			continue
		}
		f.postings[token] = list
	}
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
	sort.Strings(out)
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
