package server

import (
	"bytes"
	"fmt"
	"strings"
	"unicode/utf8"
)

// eof represents a marker rune for the end of the reader.
const eof = rune(0)

// JoinOp represents a join type operator.
type JoinOp string

// supported join type operators
const (
	JoinAnd JoinOp = "&&"
	JoinOr  JoinOp = "||"
)

// SignOp represents an expression sign operator.
type SignOp string

// supported expression sign operators
const (
	SignEq    SignOp = "="
	SignNeq   SignOp = "!="
	SignLike  SignOp = "~"
	SignNlike SignOp = "!~"
	SignLt    SignOp = "<"
	SignLte   SignOp = "<="
	SignGt    SignOp = ">"
	SignGte   SignOp = ">="

	// array/any operators
	SignAnyEq    SignOp = "?="
	SignAnyNeq   SignOp = "?!="
	SignAnyLike  SignOp = "?~"
	SignAnyNlike SignOp = "?!~"
	SignAnyLt    SignOp = "?<"
	SignAnyLte   SignOp = "?<="
	SignAnyGt    SignOp = "?>"
	SignAnyGte   SignOp = "?>="
)

// TokenType represents a Token type.
type TokenType string

// token type constants
const (
	TokenUnexpected TokenType = "unexpected"
	TokenEOF        TokenType = "eof"
	TokenWS         TokenType = "whitespace"
	TokenJoin       TokenType = "join"
	TokenSign       TokenType = "sign"
	TokenIdentifier TokenType = "identifier" // variable, column name, placeholder, etc.
	TokenFunction   TokenType = "function"   // function
	TokenNumber     TokenType = "number"
	TokenText       TokenType = "text"  // ' or " quoted string
	TokenGroup      TokenType = "group" // groupped/nested tokens
	TokenComment    TokenType = "comment"
)

// Token represents a single scanned literal (one or more combined runes).
type Token struct {
	Meta    interface{}
	Type    TokenType
	Literal string
}

// NewScanner creates and returns a new scanner instance loaded with the specified data.
func NewScanner(data []byte) *Scanner {
	return &Scanner{
		data:         data,
		maxFuncDepth: 3,
	}
}

// Scanner represents a filter and lexical scanner.
type Scanner struct {
	data         []byte
	pos          int
	maxFuncDepth int
}

// Scan reads and returns the next available token value from the scanner's buffer.
func (s *Scanner) Scan() (Token, error) {
	ch := s.read()

	if ch == eof {
		return Token{Type: TokenEOF, Literal: ""}, nil
	}

	if isWhitespaceRune(ch) {
		s.unread()
		return s.scanWhitespace()
	}

	if isGroupStartRune(ch) {
		s.unread()
		return s.scanGroup()
	}

	if isIdentifierStartRune(ch) {
		s.unread()
		return s.scanIdentifier(s.maxFuncDepth)
	}

	if isNumberStartRune(ch) {
		s.unread()
		return s.scanNumber()
	}

	if isTextStartRune(ch) {
		s.unread()
		return s.scanText(false)
	}

	if isSignStartRune(ch) {
		s.unread()
		return s.scanSign()
	}

	if isJoinStartRune(ch) {
		s.unread()
		return s.scanJoin()
	}

	if isCommentStartRune(ch) {
		s.unread()
		return s.scanComment()
	}

	return Token{Type: TokenUnexpected, Literal: string(ch)}, fmt.Errorf("unexpected character %q", ch)
}

// scanWhitespace consumes all contiguous whitespace runes.
func (s *Scanner) scanWhitespace() (Token, error) {
	var buf bytes.Buffer

	for {
		ch := s.read()

		if ch == eof {
			break
		}

		if !isWhitespaceRune(ch) {
			s.unread()
			break
		}

		buf.WriteRune(ch)
	}

	return Token{Type: TokenWS, Literal: buf.String()}, nil
}

// scanNumber consumes all contiguous digit runes.
func (s *Scanner) scanNumber() (Token, error) {
	var buf bytes.Buffer

	var hadDot bool

	for {
		ch := s.read()

		if ch == eof {
			break
		}

		if !isDigitRune(ch) &&
			(ch != '-' || buf.Len() != 0) &&
			(ch != '.' || hadDot) {
			s.unread()
			break
		}

		buf.WriteRune(ch)

		if ch == '.' {
			hadDot = true
		}
	}

	total := buf.Len()
	literal := buf.String()

	var err error
	if (total == 1 && literal[0] == '-') || literal[0] == '.' || literal[total-1] == '.' {
		err = fmt.Errorf("invalid number %q", literal)
	}

	return Token{Type: TokenNumber, Literal: buf.String()}, err
}

// scanText consumes all contiguous quoted text runes.
func (s *Scanner) scanText(preserveQuotes bool) (Token, error) {
	var buf bytes.Buffer

	firstCh := s.read()
	buf.WriteRune(firstCh)
	var prevCh rune
	var hasMatchingQuotes bool

	for {
		ch := s.read()

		if ch == eof {
			break
		}

		buf.WriteRune(ch)

		if ch == firstCh && prevCh != '\\' {
			hasMatchingQuotes = true
			break
		}

		prevCh = ch
	}

	literal := buf.String()

	var err error
	if !hasMatchingQuotes {
		err = fmt.Errorf("invalid quoted text %q", literal)
	} else if !preserveQuotes {
		literal = literal[1 : len(literal)-1]
		firstChStr := string(firstCh)
		literal = strings.ReplaceAll(literal, `\`+firstChStr, firstChStr)
	}

	return Token{Type: TokenText, Literal: literal}, err
}

// scanComment consumes all contiguous single line comment runes.
func (s *Scanner) scanComment() (Token, error) {
	var buf bytes.Buffer

	if !isCommentStartRune(s.read()) || !isCommentStartRune(s.read()) {
		return Token{Type: TokenComment}, ErrInvalidComment
	}

	for i := 0; ; i++ {
		ch := s.read()

		if ch == eof || ch == '\n' {
			break
		}

		buf.WriteRune(ch)
	}

	return Token{Type: TokenComment, Literal: strings.TrimSpace(buf.String())}, nil
}

// scanIdentifier consumes all contiguous ident runes.
func (s *Scanner) scanIdentifier(funcDepth int) (Token, error) {
	var buf bytes.Buffer

	buf.WriteRune(s.read())

	for {
		ch := s.read()

		if ch == eof {
			break
		}

		if ch == '(' {
			funcName := buf.String()
			if funcDepth <= 0 {
				return Token{Type: TokenFunction, Literal: funcName}, fmt.Errorf("max nested function arguments reached (max: %d)", s.maxFuncDepth)
			}
			if !isValidIdentifier(funcName) {
				return Token{Type: TokenFunction, Literal: funcName}, fmt.Errorf("invalid function name %q", funcName)
			}
			s.unread()
			return s.scanFunctionArgs(funcName, funcDepth)
		}

		if !isLetterRune(ch) && !isDigitRune(ch) && !isIdentifierCombineRune(ch) && ch != '_' {
			s.unread()
			break
		}

		buf.WriteRune(ch)
	}

	literal := buf.String()

	var err error
	if !isValidIdentifier(literal) {
		err = fmt.Errorf("invalid identifier %q", literal)
	}

	return Token{Type: TokenIdentifier, Literal: literal}, err
}

// scanSign consumes all contiguous sign operator runes.
func (s *Scanner) scanSign() (Token, error) {
	var buf bytes.Buffer

	for {
		ch := s.read()

		if ch == eof {
			break
		}

		if !isSignStartRune(ch) {
			s.unread()
			break
		}

		buf.WriteRune(ch)
	}

	literal := buf.String()

	var err error
	if !isSignOperator(literal) {
		err = fmt.Errorf("invalid sign operator %q", literal)
	}

	return Token{Type: TokenSign, Literal: literal}, err
}

// scanJoin consumes all contiguous join operator runes.
func (s *Scanner) scanJoin() (Token, error) {
	var buf bytes.Buffer

	for {
		ch := s.read()

		if ch == eof {
			break
		}

		if !isJoinStartRune(ch) {
			s.unread()
			break
		}

		buf.WriteRune(ch)
	}

	literal := buf.String()

	var err error
	if !isJoinOperator(literal) {
		err = fmt.Errorf("invalid join operator %q", literal)
	}

	return Token{Type: TokenJoin, Literal: literal}, err
}

// scanGroup consumes all runes within a group/parenthesis.
func (s *Scanner) scanGroup() (Token, error) {
	var buf bytes.Buffer

	firstChar := s.read()
	openGroups := 1

	for {
		ch := s.read()

		if ch == eof {
			break
		}

		if isGroupStartRune(ch) {
			openGroups++
			buf.WriteRune(ch)
		} else if isTextStartRune(ch) {
			s.unread()
			t, err := s.scanText(true)
			if err != nil {
				buf.WriteString(t.Literal)
				return Token{Type: TokenGroup, Literal: buf.String()}, err
			}

			buf.WriteString(t.Literal)
		} else if ch == ')' {
			openGroups--

			if openGroups <= 0 {
				break
			} else {
				buf.WriteRune(ch)
			}
		} else {
			buf.WriteRune(ch)
		}
	}

	literal := buf.String()

	var err error
	if !isGroupStartRune(firstChar) || openGroups > 0 {
		err = fmt.Errorf("invalid formatted group - missing %d closing bracket(s)", openGroups)
	}

	return Token{Type: TokenGroup, Literal: literal}, err
}

// scanFunctionArgs consumes all contiguous function call runes.
func (s *Scanner) scanFunctionArgs(funcName string, funcDepth int) (Token, error) {
	var args []Token

	var expectComma, isComma, isClosed bool

	ch := s.read()
	if ch != '(' {
		return Token{Type: TokenFunction, Literal: funcName}, fmt.Errorf("invalid or incomplete function call %q", funcName)
	}

	for {
		ch := s.read()

		if ch == eof {
			break
		}

		if ch == ')' {
			isClosed = true
			break
		}

		if isWhitespaceRune(ch) {
			_, err := s.scanWhitespace()
			if err != nil {
				return Token{Type: TokenFunction, Literal: funcName, Meta: args}, fmt.Errorf("failed to scan whitespaces in function %q: %w", funcName, err)
			}
			continue
		}

		if isCommentStartRune(ch) {
			s.unread()
			_, err := s.scanComment()
			if err != nil {
				return Token{Type: TokenFunction, Literal: funcName, Meta: args}, fmt.Errorf("failed to scan comment in function %q: %w", funcName, err)
			}
			continue
		}

		isComma = ch == ','

		if expectComma && !isComma {
			return Token{Type: TokenFunction, Literal: funcName, Meta: args}, fmt.Errorf("expected comma after the last argument in function %q", funcName)
		}

		if !expectComma && isComma {
			return Token{Type: TokenFunction, Literal: funcName, Meta: args}, fmt.Errorf("unexpected comma in function %q", funcName)
		}

		expectComma = false

		if isComma {
			continue
		}

		if isIdentifierStartRune(ch) {
			s.unread()
			t, err := s.scanIdentifier(funcDepth - 1)
			if err != nil {
				return Token{Type: TokenFunction, Literal: funcName, Meta: args}, fmt.Errorf("invalid identifier argument %q in function %q: %w", t.Literal, funcName, err)
			}
			args = append(args, t)
			expectComma = true
		} else if isNumberStartRune(ch) {
			s.unread()
			t, err := s.scanNumber()
			if err != nil {
				return Token{Type: TokenFunction, Literal: funcName, Meta: args}, fmt.Errorf("invalid number argument %q in function %q: %w", t.Literal, funcName, err)
			}
			args = append(args, t)
			expectComma = true
		} else if isTextStartRune(ch) {
			s.unread()
			t, err := s.scanText(false)
			if err != nil {
				return Token{Type: TokenFunction, Literal: funcName, Meta: args}, fmt.Errorf("invalid text argument %q in function %q: %w", t.Literal, funcName, err)
			}
			args = append(args, t)
			expectComma = true
		} else {
			return Token{Type: TokenFunction, Literal: funcName, Meta: args}, fmt.Errorf("unsupported argument character %q in function %q", ch, funcName)
		}
	}

	if !isClosed {
		return Token{Type: TokenFunction, Literal: funcName, Meta: args}, fmt.Errorf("invalid or incomplete function %q (expected ')')", funcName)
	}

	return Token{Type: TokenFunction, Literal: funcName, Meta: args}, nil
}

func (s *Scanner) unread() {
	if s.pos > 0 {
		s.pos = s.pos - 1
	}
}

func (s *Scanner) read() rune {
	if s.pos >= len(s.data) {
		return eof
	}

	ch, n := utf8.DecodeRune(s.data[s.pos:])
	s.pos += n

	return ch
}

// Lexical helpers:

func isWhitespaceRune(ch rune) bool { return ch == ' ' || ch == '\t' || ch == '\n' }

func isLetterRune(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isDigitRune(ch rune) bool {
	return (ch >= '0' && ch <= '9')
}

func isTextStartRune(ch rune) bool {
	return ch == '\'' || ch == '"'
}

func isNumberStartRune(ch rune) bool {
	return ch == '-' || isDigitRune(ch)
}

func isSignStartRune(ch rune) bool {
	return ch == '=' ||
		ch == '?' ||
		ch == '!' ||
		ch == '>' ||
		ch == '<' ||
		ch == '~'
}

func isJoinStartRune(ch rune) bool {
	return ch == '&' || ch == '|'
}

func isGroupStartRune(ch rune) bool {
	return ch == '('
}

func isCommentStartRune(ch rune) bool {
	return ch == '/'
}

func isIdentifierStartRune(ch rune) bool {
	return isLetterRune(ch) || isIdentifierSpecialStartRune(ch)
}

func isIdentifierSpecialStartRune(ch rune) bool {
	return ch == '@' || ch == '_' || ch == '#'
}

func isIdentifierCombineRune(ch rune) bool {
	return ch == '.' || ch == ':'
}

func isSignOperator(literal string) bool {
	switch SignOp(literal) {
	case
		SignEq,
		SignNeq,
		SignLt,
		SignLte,
		SignGt,
		SignGte,
		SignLike,
		SignNlike,
		SignAnyEq,
		SignAnyNeq,
		SignAnyLike,
		SignAnyNlike,
		SignAnyLt,
		SignAnyLte,
		SignAnyGt,
		SignAnyGte:
		return true
	}

	return false
}

func isJoinOperator(literal string) bool {
	switch JoinOp(literal) {
	case
		JoinAnd,
		JoinOr:
		return true
	}

	return false
}

func isValidIdentifier(literal string) bool {
	length := len(literal)

	return (
	// doesn't end with combine rune
	!isIdentifierCombineRune(rune(literal[length-1])) &&
		// is not just a special start rune
		(length != 1 || !isIdentifierSpecialStartRune(rune(literal[0]))))
}
