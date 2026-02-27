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
