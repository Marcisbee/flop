package server

import (
	"strings"
	"testing"
)

// Regression: deeply nested filter groups recursed the parser without a
// depth limit, exhausting the goroutine stack and crashing the process.
func TestParseFilterRejectsDeepNesting(t *testing.T) {
	expr := strings.Repeat("(", 200000) + "id=1" + strings.Repeat(")", 200000)
	if _, _, err := ParseFilter(expr); err == nil {
		t.Fatal("deeply nested filter accepted, want depth-limit error")
	} else if !strings.Contains(err.Error(), "nesting depth") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Moderate nesting below the cap still parses.
func TestParseFilterAllowsReasonableNesting(t *testing.T) {
	expr := strings.Repeat("(", 8) + "id=1" + strings.Repeat(")", 8)
	groups, _, err := ParseFilter(expr)
	if err != nil {
		t.Fatalf("nested filter rejected: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("expected one group, got %d", len(groups))
	}
}
