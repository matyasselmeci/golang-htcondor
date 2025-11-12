package config

import (
	"strings"
	"testing"
)

func TestLexerMultiLine(t *testing.T) {
	input := `A = first
B = second
C = third
`
	lex := NewLexer(strings.NewReader(input))

	tokens := []struct {
		expectedToken int
		expectedLit   string
	}{
		{IDENT, "A"},
		{ASSIGN, "first"},
		{IDENT, "B"},
		{ASSIGN, "second"},
		{IDENT, "C"},
		{ASSIGN, "third"},
		{EOF, ""},
	}

	for i, tt := range tokens {
		tok := lex.NextToken()
		if tok.Token != tt.expectedToken {
			t.Errorf("token[%d]: expected %d, got %d (lit=%q)", i, tt.expectedToken, tok.Token, tok.Lit)
		}
		if tt.expectedLit != "" && tok.Lit != tt.expectedLit {
			t.Errorf("token[%d]: expected lit %q, got %q", i, tt.expectedLit, tok.Lit)
		}
	}
}
