package server

import (
	"fmt"
	"net/url"
	"testing"
)

func TestScannerScan(t *testing.T) {
	type output struct {
		error bool
		print string
	}
	testScenarios := []struct {
		text    string
		expects []output
	}{
		// whitespace
		{"   ", []output{{false, "{<nil> whitespace    }"}}},
		{"test 123", []output{{false, "{<nil> identifier test}"}, {false, "{<nil> whitespace  }"}, {false, "{<nil> number 123}"}}},
		// identifier
		{`test`, []output{{false, `{<nil> identifier test}`}}},
		{`@test.123:c`, []output{{false, `{<nil> identifier @test.123:c}`}}},
		{`_test_a.123`, []output{{false, `{<nil> identifier _test_a.123}`}}},
		// number
		{`123`, []output{{false, `{<nil> number 123}`}}},
		{`-123`, []output{{false, `{<nil> number -123}`}}},
		{`-123.456`, []output{{false, `{<nil> number -123.456}`}}},
		{`123.456`, []output{{false, `{<nil> number 123.456}`}}},
		// text
		{`""`, []output{{false, `{<nil> text }`}}},
		{`''`, []output{{false, `{<nil> text }`}}},
		{`'test'`, []output{{false, `{<nil> text test}`}}},
		{`'te\'st'`, []output{{false, `{<nil> text te'st}`}}},
		{`"te\"st"`, []output{{false, `{<nil> text te"st}`}}},
		// join types
		{`&& ||`, []output{{false, `{<nil> join &&}`}, {false, `{<nil> whitespace  }`}, {false, `{<nil> join ||}`}}},
		// expression signs
		{`= != ~ !~ > >= < <=`, []output{
			{false, `{<nil> sign =}`},
			{false, `{<nil> whitespace  }`},
			{false, `{<nil> sign !=}`},
			{false, `{<nil> whitespace  }`},
			{false, `{<nil> sign ~}`},
			{false, `{<nil> whitespace  }`},
			{false, `{<nil> sign !~}`},
			{false, `{<nil> whitespace  }`},
			{false, `{<nil> sign >}`},
			{false, `{<nil> whitespace  }`},
			{false, `{<nil> sign >=}`},
			{false, `{<nil> whitespace  }`},
			{false, `{<nil> sign <}`},
			{false, `{<nil> whitespace  }`},
			{false, `{<nil> sign <=}`},
		}},
	}

	for _, scenario := range testScenarios {
		t.Run(scenario.text, func(t *testing.T) {
			s := NewScanner([]byte(scenario.text))

			for j, expect := range scenario.expects {
				token, err := s.Scan()

				hasErr := err != nil
				if expect.error != hasErr {
					t.Errorf("[%d] Expected hasErr %v, got %v: %v (%v)", j, expect.error, hasErr, err, token)
				}

				tokenPrint := fmt.Sprintf("%v", token)
				if tokenPrint != expect.print {
					t.Errorf("[%d] Expected token %s, got %s", j, expect.print, tokenPrint)
				}
			}

			lastToken, err := s.Scan()
			if err != nil || lastToken.Type != TokenEOF {
				t.Fatalf("Expected EOF token, got %v (%v)", lastToken, err)
			}
		})
	}
}

func TestParse(t *testing.T) {
	scenarios := []struct {
		input         string
		expectedError bool
		expectedPrint string
	}{
		{`> 1`, true, "[]"},
		{`a >`, true, "[]"},
		{`a > >`, true, "[]"},
		{`a ! 1`, true, "[]"},
		{`a || 1`, true, "[]"},
		{`a && 1`, true, "[]"},
		{`test > 1 &&`, true, `[]`},
		{`|| test = 1`, true, `[]`},
		{`test = 1 && ||`, true, "[]"},
		{`test = 1 && a`, true, "[]"},
		// valid simple expression and sign operators check
		{`1=12`, false, `[{{{<nil> number 1} = {<nil> number 12}} &&}]`},
		{`   1    =    12    `, false, `[{{{<nil> number 1} = {<nil> number 12}} &&}]`},
		{`"demo" != test`, false, `[{{{<nil> text demo} != {<nil> identifier test}} &&}]`},
		{`a~1`, false, `[{{{<nil> identifier a} ~ {<nil> number 1}} &&}]`},
		{`a !~ 1`, false, `[{{{<nil> identifier a} !~ {<nil> number 1}} &&}]`},
		{`test>12`, false, `[{{{<nil> identifier test} > {<nil> number 12}} &&}]`},
		{`test > 12`, false, `[{{{<nil> identifier test} > {<nil> number 12}} &&}]`},
		{`test >="test"`, false, `[{{{<nil> identifier test} >= {<nil> text test}} &&}]`},
		{`1<="test"`, false, `[{{{<nil> number 1} <= {<nil> text test}} &&}]`},
		// valid parenthesis
		{`(a=1)`, false, `[{[{{{<nil> identifier a} = {<nil> number 1}} &&}] &&}]`},
		{`a=1 || 2!=3`, false, `[{{{<nil> identifier a} = {<nil> number 1}} &&} {{{<nil> number 2} != {<nil> number 3}} ||}]`},
		{`a=1 && 2!=3`, false, `[{{{<nil> identifier a} = {<nil> number 1}} &&} {{{<nil> number 2} != {<nil> number 3}} &&}]`},
		{`(a=1 && 2!=3) || "b"=a`, false, `[{[{{{<nil> identifier a} = {<nil> number 1}} &&} {{{<nil> number 2} != {<nil> number 3}} &&}] &&} {{{<nil> text b} = {<nil> identifier a}} ||}]`},
	}

	for i, scenario := range scenarios {
		t.Run(fmt.Sprintf("s%d:%s", i, scenario.input), func(t *testing.T) {
			v, err := Parse(scenario.input)

			if scenario.expectedError && err == nil {
				t.Fatalf("Expected error, got nil (%q)", scenario.input)
			}

			if !scenario.expectedError && err != nil {
				t.Fatalf("Did not expect error, got %q (%q).", err, scenario.input)
			}

			vPrint := fmt.Sprintf("%v", v)

			if vPrint != scenario.expectedPrint {
				t.Fatalf("Expected %s, got %s", scenario.expectedPrint, vPrint)
			}
		})
	}
}

func TestFilterComparisons(t *testing.T) {
	tests := []struct {
		expr string
		row  map[string]interface{}
		want bool
	}{
		// String equality (case-insensitive)
		{`name="alice"`, map[string]interface{}{"name": "Alice"}, true},
		{`name="alice"`, map[string]interface{}{"name": "bob"}, false},

		// Contains (like)
		{`email~"@gmail.com"`, map[string]interface{}{"email": "user@gmail.com"}, true},
		{`email~"@gmail.com"`, map[string]interface{}{"email": "user@yahoo.com"}, false},

		// Not-like
		{`email!~"@gmail.com"`, map[string]interface{}{"email": "user@yahoo.com"}, true},
		{`email!~"@gmail.com"`, map[string]interface{}{"email": "user@gmail.com"}, false},

		// Numeric comparisons
		{`age>=18`, map[string]interface{}{"age": float64(18)}, true},
		{`age>=18`, map[string]interface{}{"age": float64(17)}, false},
		{`age>=18`, map[string]interface{}{"age": int32(25)}, true},
		{`count>0`, map[string]interface{}{"count": float64(0)}, false},
		{`count>0`, map[string]interface{}{"count": float64(1)}, true},
		{`count<10`, map[string]interface{}{"count": float64(5)}, true},
		{`count<=5`, map[string]interface{}{"count": float64(5)}, true},
		{`count<=5`, map[string]interface{}{"count": float64(6)}, false},

		// Not-equal
		{`status!="active"`, map[string]interface{}{"status": "inactive"}, true},
		{`status!="active"`, map[string]interface{}{"status": "active"}, false},

		// Numeric not-equal
		{`count!=0`, map[string]interface{}{"count": float64(0)}, false},
		{`count!=0`, map[string]interface{}{"count": float64(1)}, true},

		// Missing/nil fields
		{`missing!="x"`, map[string]interface{}{}, true},
		{`missing="x"`, map[string]interface{}{}, false},
		{`missing>=0`, map[string]interface{}{}, false},

		// Negative numbers
		{`score>-5`, map[string]interface{}{"score": float64(0)}, true},
		{`score>-5`, map[string]interface{}{"score": float64(-10)}, false},

		// Decimal numbers
		{`ratio>=3.14`, map[string]interface{}{"ratio": float64(3.14)}, true},
		{`ratio>=3.14`, map[string]interface{}{"ratio": float64(3.13)}, false},

		// Single-quoted strings
		{`name='alice'`, map[string]interface{}{"name": "Alice"}, true},
	}

	for _, tc := range tests {
		fn, err := ParseAndEvalFilter(tc.expr)
		if err != nil {
			t.Errorf("parse error for %q: %v", tc.expr, err)
			continue
		}
		got := fn(tc.row)
		if got != tc.want {
			t.Errorf("%s with %v => %v, want %v", tc.expr, tc.row, got, tc.want)
		}
	}
}

func TestFilterLogical(t *testing.T) {
	tests := []struct {
		expr string
		row  map[string]interface{}
		want bool
	}{
		// AND
		{`name="a" && age>=5`, map[string]interface{}{"name": "a", "age": float64(5)}, true},
		{`name="a" && age>=5`, map[string]interface{}{"name": "a", "age": float64(4)}, false},
		{`name="a" && age>=5`, map[string]interface{}{"name": "b", "age": float64(10)}, false},

		// OR
		{`name="a" || name="b"`, map[string]interface{}{"name": "a"}, true},
		{`name="a" || name="b"`, map[string]interface{}{"name": "b"}, true},
		{`name="a" || name="b"`, map[string]interface{}{"name": "c"}, false},

		// fexpr is left-to-right, no operator precedence
		// a="1" || b="2" && c="3" is parsed as: [{a="1", &&}, {b="2", ||}, {c="3", &&}]
		// Which means: a="1" OR (b="2" AND c="3")
		{`a="1" || b="2" && c="3"`, map[string]interface{}{"a": "1", "b": "x", "c": "x"}, true},
		{`a="1" || b="2" && c="3"`, map[string]interface{}{"a": "x", "b": "2", "c": "3"}, true},
		{`a="1" || b="2" && c="3"`, map[string]interface{}{"a": "x", "b": "2", "c": "x"}, false},

		// Parentheses override grouping
		{`(a="1" || b="2") && c="3"`, map[string]interface{}{"a": "1", "c": "3"}, true},
		{`(a="1" || b="2") && c="3"`, map[string]interface{}{"a": "1", "c": "x"}, false},

		// Complex expression
		{`(displayName="anne" && email~"@gmail.com") || likeCount>=5`,
			map[string]interface{}{"displayName": "anne", "email": "anne@gmail.com", "likeCount": float64(2)}, true},
		{`(displayName="anne" && email~"@gmail.com") || likeCount>=5`,
			map[string]interface{}{"displayName": "bob", "email": "bob@yahoo.com", "likeCount": float64(10)}, true},
		{`(displayName="anne" && email~"@gmail.com") || likeCount>=5`,
			map[string]interface{}{"displayName": "bob", "email": "bob@yahoo.com", "likeCount": float64(3)}, false},
	}

	for _, tc := range tests {
		fn, err := ParseAndEvalFilter(tc.expr)
		if err != nil {
			t.Errorf("parse error for %q: %v", tc.expr, err)
			continue
		}
		got := fn(tc.row)
		if got != tc.want {
			t.Errorf("%s with %v => %v, want %v", tc.expr, tc.row, got, tc.want)
		}
	}
}

func TestFilterExactTwitterScenario(t *testing.T) {
	expr := `handle="marcis"`
	fn, err := ParseAndEvalFilter(expr)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	row := map[string]interface{}{
		"id":          "abc123",
		"email":       "marcis@example.com",
		"password":    "[REDACTED]",
		"handle":      "marcis",
		"displayName": "Marcis",
		"bio":         "",
		"avatarUrl":   "",
		"roles":       []interface{}{"admin"},
		"createdAt":   float64(1700000000),
	}

	if !fn(row) {
		t.Errorf("expected handle=\"marcis\" to match row with handle=%q", row["handle"])
	}

	row2 := map[string]interface{}{
		"id":     "xyz789",
		"handle": "other",
	}
	if fn(row2) {
		t.Error("expected handle=\"marcis\" to NOT match row with handle=other")
	}
}

func TestFilterURLDecoding(t *testing.T) {
	rawQuery := "page=1&limit=50&filter=handle%3D%22marcis%22"
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	filterExpr := values.Get("filter")
	if filterExpr != `handle="marcis"` {
		t.Fatalf("URL decoding: expected handle=\"marcis\", got %q", filterExpr)
	}

	fn, err := ParseAndEvalFilter(filterExpr)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	row := map[string]interface{}{"handle": "marcis", "id": "abc"}
	if !fn(row) {
		t.Error("filter should match row with handle=marcis")
	}
}

func TestFilterParseErrors(t *testing.T) {
	bad := []string{
		`name=`,          // missing value
		`name="unclosed`, // unterminated string
		``,               // empty
		`&&`,             // no operands
		`name="a" &&`,    // trailing operator
	}
	for _, expr := range bad {
		_, err := ParseAndEvalFilter(expr)
		if err == nil {
			t.Errorf("expected error for %q, got nil", expr)
		}
	}
}
