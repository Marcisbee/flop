package server

import (
	"fmt"
	"strconv"
	"strings"
)

// toFloat converts a row value to float64 for numeric comparison.
func toFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

// evalExpr evaluates a single Expr against a row.
func evalExpr(e Expr, row map[string]interface{}) bool {
	// The left side is the field name (identifier), right side is the value.
	field := e.Left.Literal
	v, exists := row[field]
	if !exists || v == nil {
		return e.Op == SignNeq
	}

	// Determine if the right operand is numeric.
	var numVal float64
	var isNum bool
	if e.Right.Type == TokenNumber {
		n, err := strconv.ParseFloat(e.Right.Literal, 64)
		if err == nil {
			numVal = n
			isNum = true
		}
	}

	if isNum {
		if fv, ok := toFloat(v); ok {
			switch e.Op {
			case SignEq:
				return fv == numVal
			case SignNeq:
				return fv != numVal
			case SignGt:
				return fv > numVal
			case SignGte:
				return fv >= numVal
			case SignLt:
				return fv < numVal
			case SignLte:
				return fv <= numVal
			}
		}
		return e.Op == SignNeq
	}

	// String comparison
	sv := fmt.Sprint(v)
	strVal := e.Right.Literal
	switch e.Op {
	case SignEq:
		return strings.EqualFold(sv, strVal)
	case SignNeq:
		return !strings.EqualFold(sv, strVal)
	case SignLike:
		return strings.Contains(strings.ToLower(sv), strings.ToLower(strVal))
	case SignNlike:
		return !strings.Contains(strings.ToLower(sv), strings.ToLower(strVal))
	case SignGt:
		return sv > strVal
	case SignGte:
		return sv >= strVal
	case SignLt:
		return sv < strVal
	case SignLte:
		return sv <= strVal
	}
	return false
}

// evalGroups evaluates a slice of ExprGroup against a row.
// Groups are joined: items with JoinAnd are collected into an AND-clause,
// and JoinOr separates OR-clauses.
func evalGroups(groups []ExprGroup, row map[string]interface{}) bool {
	// fexpr represents: a=1 && b=2 || c=3 as:
	//   [{a=1, &&}, {b=2, &&}, {c=3, ||}]
	// The join on each item indicates what comes BEFORE it (except the first which defaults to &&).
	// So || on c=3 means "this starts a new OR branch".
	// We evaluate by collecting AND runs and OR-ing between them.
	currentAnd := true
	result := false

	for _, g := range groups {
		var val bool
		switch item := g.Item.(type) {
		case Expr:
			val = evalExpr(item, row)
		case []ExprGroup:
			val = evalGroups(item, row)
		default:
			continue
		}

		if g.Join == JoinOr {
			// Previous AND-chain is done, OR it into result
			result = result || currentAnd
			// Start new AND-chain with this value
			currentAnd = val
		} else {
			// AND into current chain
			currentAnd = currentAnd && val
		}
	}

	return result || currentAnd
}

// ParseAndEvalFilter parses a filter expression and returns a reusable predicate.
func ParseAndEvalFilter(expr string) (func(map[string]interface{}) bool, error) {
	groups, err := Parse(expr)
	if err != nil {
		return nil, err
	}

	return func(row map[string]interface{}) bool {
		return evalGroups(groups, row)
	}, nil
}
