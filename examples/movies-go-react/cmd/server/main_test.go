package main

import "testing"

func TestNormalizeSearchRomanNumerals(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "Mortal Kombat II", want: "mortal kombat 2"},
		{in: "Mortal Kombat 2", want: "mortal kombat 2"},
		{in: "Rocky VI", want: "rocky 6"},
		{in: "Rocky 6", want: "rocky 6"},
		{in: "Sherlock Holmes: A Game of Shadows II", want: "sherlock holmes a game of shadows 2"},
		// Keep single-letter pronoun/title token untouched.
		{in: "I, Robot", want: "i robot"},
	}

	for _, tc := range tests {
		got := normalizeSearch(tc.in)
		if got != tc.want {
			t.Fatalf("normalizeSearch(%q)=%q want %q", tc.in, got, tc.want)
		}
	}
}

func TestShouldUseOrderedTokenFallback(t *testing.T) {
	cases := []struct {
		name   string
		tokens []string
		want   bool
	}{
		{name: "single token", tokens: []string{"sherlock"}, want: false},
		{name: "natural phrase", tokens: []string{"sherlock", "holmes", "game", "shadow"}, want: true},
		{name: "all one-char noise", tokens: []string{"a", "b", "c", "d", "e"}, want: false},
		{name: "too many one-char tokens", tokens: []string{"ab", "c", "d"}, want: false},
	}
	for _, tc := range cases {
		got := shouldUseOrderedTokenFallback(tc.tokens)
		if got != tc.want {
			t.Fatalf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

func TestOrderedTokenPrefixMatchStillWorks(t *testing.T) {
	title := normalizeSearch("Sherlock Holmes: A Game of Shadows")
	queryTokens := []string{"sherlock", "holmes", "game", "shadow"}
	if !hasOrderedTokenPrefixMatchTokens(title, queryTokens) {
		t.Fatalf("expected ordered token prefix match for %q and %+v", title, queryTokens)
	}
}
