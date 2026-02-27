package engine

import (
	"sort"
	"strings"
	"sync"
	"unicode"
)

// AutocompleteEntry is a reusable search entry for in-memory autocomplete indexes.
type AutocompleteEntry struct {
	Key  string
	Text string
	Data map[string]interface{}
}

type autocompleteItem struct {
	norm          string
	normNoArticle string
	raw           string
	rawNoArticle  string
	entry         AutocompleteEntry
}

// AutocompleteIndex provides fast in-memory prefix search with forgiving
// matching for word-prefix, substring, and ordered-token phrase input.
type AutocompleteIndex struct {
	mu    sync.RWMutex
	items []autocompleteItem
	byKey map[string]autocompleteItem
}

// NewAutocompleteIndex builds a new autocomplete index.
func NewAutocompleteIndex(entries []AutocompleteEntry) *AutocompleteIndex {
	idx := &AutocompleteIndex{byKey: make(map[string]autocompleteItem, len(entries))}
	idx.Add(entries)
	return idx
}

// Add inserts or replaces entries by key.
func (a *AutocompleteIndex) Add(entries []AutocompleteEntry) {
	if len(entries) == 0 {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.byKey == nil {
		a.byKey = make(map[string]autocompleteItem, len(entries))
	}

	for _, entry := range entries {
		if entry.Key == "" || entry.Text == "" {
			continue
		}
		raw := normalizeAutocompleteRaw(entry.Text)
		norm := normalizeAutocomplete(entry.Text)
		item := autocompleteItem{
			norm:          norm,
			normNoArticle: removeLeadingArticle(norm),
			raw:           raw,
			rawNoArticle:  removeLeadingArticle(raw),
			entry: AutocompleteEntry{
				Key:  entry.Key,
				Text: entry.Text,
				Data: cloneAutocompleteData(entry.Data),
			},
		}
		a.byKey[entry.Key] = item
	}

	a.items = a.items[:0]
	a.items = make([]autocompleteItem, 0, len(a.byKey))
	for _, item := range a.byKey {
		a.items = append(a.items, item)
	}

	sort.Slice(a.items, func(i, j int) bool {
		if a.items[i].norm == a.items[j].norm {
			return a.items[i].entry.Key < a.items[j].entry.Key
		}
		return a.items[i].norm < a.items[j].norm
	})
}

// Query returns up to limit matching entries.
func (a *AutocompleteIndex) Query(prefix string, limit int) []AutocompleteEntry {
	norm := normalizeAutocomplete(prefix)
	if norm == "" {
		return []AutocompleteEntry{}
	}
	rawNorm := normalizeAutocompleteRaw(prefix)
	if rawNorm == "" {
		rawNorm = norm
	}
	queryTokens := strings.Fields(norm)
	rawQueryTokens := strings.Fields(rawNorm)
	multiTokenQuery := len(queryTokens) > 1
	useOrderedTokenFallback := shouldUseOrderedTokenFallback(queryTokens)

	a.mu.RLock()
	defer a.mu.RUnlock()

	if len(a.items) == 0 {
		return []AutocompleteEntry{}
	}
	if limit <= 0 {
		limit = 10
	}

	start := sort.Search(len(a.items), func(i int) bool {
		return a.items[i].norm >= norm
	})

	seen := make(map[string]struct{}, limit*2)
	out := make([]AutocompleteEntry, 0, limit)
	appendMatch := func(item autocompleteItem) {
		key := item.entry.Key
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		out = append(out, AutocompleteEntry{
			Key:  item.entry.Key,
			Text: item.entry.Text,
			Data: cloneAutocompleteData(item.entry.Data),
		})
	}

	for i := start; i < len(a.items) && len(out) < limit; i++ {
		item := a.items[i]
		if !strings.HasPrefix(item.norm, norm) {
			break
		}
		appendMatch(item)
	}

	if len(out) < limit {
		for _, item := range a.items {
			match := strings.HasPrefix(item.normNoArticle, norm) || strings.Contains(item.norm, norm)
			if !match {
				match = strings.HasPrefix(item.rawNoArticle, rawNorm) || strings.Contains(item.raw, rawNorm)
			}
			if !match && !multiTokenQuery {
				match = hasWordPrefix(item.norm, norm) || hasWordPrefix(item.raw, rawNorm)
			}
			if !match && useOrderedTokenFallback {
				match = hasOrderedTokenPrefixMatchTokens(item.norm, queryTokens)
				if !match {
					match = hasOrderedTokenPrefixMatchTokens(item.raw, rawQueryTokens)
				}
			}
			if match {
				appendMatch(item)
				if len(out) >= limit {
					break
				}
			}
		}
	}

	return out
}

func cloneAutocompleteData(in map[string]interface{}) map[string]interface{} {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func normalizeAutocomplete(s string) string {
	norm := normalizeAutocompleteRaw(s)
	if norm == "" {
		return ""
	}

	// Canonicalize Roman sequel numerals to Arabic numbers so:
	// "Mortal Kombat II" and "Mortal Kombat 2" normalize identically.
	tokens := strings.Fields(norm)
	for i := range tokens {
		if arabic, ok := romanNumeralArabic(tokens[i]); ok {
			tokens[i] = arabic
		}
	}
	return strings.Join(tokens, " ")
}

func normalizeAutocompleteRaw(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	lastSpace := true
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			lastSpace = false
			continue
		}
		if !lastSpace {
			b.WriteByte(' ')
			lastSpace = true
		}
	}
	return strings.TrimSpace(b.String())
}

func romanNumeralArabic(token string) (string, bool) {
	switch token {
	case "ii":
		return "2", true
	case "iii":
		return "3", true
	case "iv":
		return "4", true
	case "v":
		return "5", true
	case "vi":
		return "6", true
	case "vii":
		return "7", true
	case "viii":
		return "8", true
	case "ix":
		return "9", true
	case "x":
		return "10", true
	case "xi":
		return "11", true
	case "xii":
		return "12", true
	case "xiii":
		return "13", true
	case "xiv":
		return "14", true
	case "xv":
		return "15", true
	case "xvi":
		return "16", true
	case "xvii":
		return "17", true
	case "xviii":
		return "18", true
	case "xix":
		return "19", true
	case "xx":
		return "20", true
	default:
		return "", false
	}
}

func removeLeadingArticle(s string) string {
	s = strings.TrimSpace(s)
	for _, prefix := range []string{"the ", "a ", "an "} {
		if strings.HasPrefix(s, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(s, prefix))
		}
	}
	return s
}

func hasWordPrefix(text, query string) bool {
	if text == "" || query == "" {
		return false
	}
	for _, word := range strings.Fields(text) {
		if strings.HasPrefix(word, query) {
			return true
		}
	}
	return false
}

func hasOrderedTokenPrefixMatchTokens(text string, queryTokens []string) bool {
	if text == "" {
		return false
	}

	titleTokens := strings.Fields(text)
	if len(titleTokens) == 0 || len(queryTokens) == 0 {
		return false
	}

	ti := 0
	for _, q := range queryTokens {
		found := false
		for ti < len(titleTokens) {
			if strings.HasPrefix(titleTokens[ti], q) {
				found = true
				ti++
				break
			}
			ti++
		}
		if !found {
			return false
		}
	}
	return true
}

func shouldUseOrderedTokenFallback(queryTokens []string) bool {
	if len(queryTokens) < 2 || len(queryTokens) > 7 {
		return false
	}
	shortTokens := 0
	for _, token := range queryTokens {
		if len(token) <= 1 {
			shortTokens++
		}
	}
	// Queries like "a b c d e" are too ambiguous and expensive to run
	// through ordered token matching across the full catalog.
	if shortTokens == len(queryTokens) {
		return false
	}
	if shortTokens*2 > len(queryTokens) {
		return false
	}
	return true
}
