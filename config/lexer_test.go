package config

import (
	"strings"
	"testing"
)

func TestLexerBasicTokens(t *testing.T) {
	input := "FOO = bar"
	lex := NewLexer(strings.NewReader(input))

	tok := lex.NextToken()
	if tok.Token != IDENT || tok.Lit != "FOO" {
		t.Errorf("Expected IDENT FOO, got %d %q", tok.Token, tok.Lit)
	}

	tok = lex.NextToken()
	if tok.Token != ASSIGN {
		t.Errorf("Expected ASSIGN, got %d", tok.Token)
	}
	if tok.Lit != "bar" {
		t.Errorf("Expected value 'bar', got %q", tok.Lit)
	}
}

func TestLexerMacroExpansion(t *testing.T) {
	input := "FOO = $(BAR)"
	lex := NewLexer(strings.NewReader(input))

	tok := lex.NextToken()
	if tok.Token != IDENT || tok.Lit != "FOO" {
		t.Errorf("Expected IDENT FOO, got %d %q", tok.Token, tok.Lit)
	}

	tok = lex.NextToken()
	if tok.Token != ASSIGN {
		t.Errorf("Expected ASSIGN, got %d", tok.Token)
	}

	if tok.Lit != "$(BAR)" {
		t.Errorf("Expected $(BAR), got %q", tok.Lit)
	}
}

func TestLexerNestedMacros(t *testing.T) {
	input := "$(FOO:$(BAR))"
	lex := NewLexer(strings.NewReader(input))

	tok := lex.NextToken()
	if tok.Token != STRING {
		t.Errorf("Expected STRING, got %d", tok.Token)
	}

	if tok.Lit != "$(FOO:$(BAR))" {
		t.Errorf("Expected $(FOO:$(BAR)), got %q", tok.Lit)
	}
}

func TestLexerComments(t *testing.T) {
	input := `
# This is a comment
FOO = bar # inline comment
	`
	lex := NewLexer(strings.NewReader(input))

	tok := lex.NextToken()
	if tok.Token != IDENT || tok.Lit != "FOO" {
		t.Errorf("Expected IDENT FOO, got %d %q", tok.Token, tok.Lit)
	}
}

func TestLexerLineContinuation(t *testing.T) {
	input := "FOO = this is a \\\nvery long value"
	lex := NewLexer(strings.NewReader(input))

	tok := lex.NextToken()
	if tok.Token != IDENT || tok.Lit != "FOO" {
		t.Errorf("Expected IDENT FOO, got %d %q", tok.Token, tok.Lit)
	}

	tok = lex.NextToken()
	if tok.Token != ASSIGN {
		t.Errorf("Expected ASSIGN, got %d", tok.Token)
	}

	expected := "this is a very long value"
	if tok.Lit != expected {
		t.Errorf("Expected %q, got %q", expected, tok.Lit)
	}
}

func TestLexerKeywords(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"if", IF},
		{"elif", ELIF},
		{"else", ELSE},
		{"endif", ENDIF},
		{"defined", DEFINED},
		{"version", VERSION},
		{"include", INCLUDE},
		{"use", USE},
		{"true", TRUE},
		{"false", FALSE},
		{"yes", YES},
		{"no", NO},
	}

	for _, tt := range tests {
		lex := NewLexer(strings.NewReader(tt.input))
		tok := lex.NextToken()

		if tok.Token != tt.expected {
			t.Errorf("input=%q: expected token=%d, got=%d",
				tt.input, tt.expected, tok.Token)
		}
	}
}

func TestLexerSubsystemVariables(t *testing.T) {
	input := "MASTER.LOWPORT = 20000"
	lex := NewLexer(strings.NewReader(input))

	tok := lex.NextToken()
	if tok.Token != IDENT || tok.Lit != "MASTER.LOWPORT" {
		t.Errorf("Expected IDENT MASTER.LOWPORT, got %d %q", tok.Token, tok.Lit)
	}

	tok = lex.NextToken()
	if tok.Token != ASSIGN {
		t.Errorf("Expected ASSIGN, got %d", tok.Token)
	}

	if tok.Lit != "20000" {
		t.Errorf("Expected 20000, got %q", tok.Lit)
	}
}

func TestLexerStrings(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`"hello world"`, "hello world"},
		{`'single quotes'`, "single quotes"},
		{`"escaped \"quote\""`, `escaped "quote"`},
	}

	for _, tt := range tests {
		lex := NewLexer(strings.NewReader(tt.input))
		tok := lex.NextToken()

		if tok.Token != STRING {
			t.Errorf("input=%q: expected STRING, got=%d", tt.input, tok.Token)
		}

		if tok.Lit != tt.expected {
			t.Errorf("input=%q: expected %q, got %q",
				tt.input, tt.expected, tok.Lit)
		}
	}
}

func TestLexerNumbers(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"123", "123"},
		{"456.789", "456.789"},
		{"-42", "-42"},
		{"-3.14", "-3.14"},
	}

	for _, tt := range tests {
		lex := NewLexer(strings.NewReader(tt.input))
		tok := lex.NextToken()

		if tok.Token != NUMBER {
			t.Errorf("input=%q: expected NUMBER, got=%d", tt.input, tok.Token)
		}

		if tok.Lit != tt.expected {
			t.Errorf("input=%q: expected %q, got=%q",
				tt.input, tt.expected, tok.Lit)
		}
	}
}

func TestLexerComplexValue(t *testing.T) {
	input := "ADMIN_MACHINES = condor.cs.wisc.edu, $(HOSTNAME), \\\n    stork.cs.wisc.edu"
	lex := NewLexer(strings.NewReader(input))

	tok := lex.NextToken()
	if tok.Token != IDENT || tok.Lit != "ADMIN_MACHINES" {
		t.Errorf("Expected IDENT ADMIN_MACHINES, got %d %q", tok.Token, tok.Lit)
	}

	tok = lex.NextToken()
	if tok.Token != ASSIGN {
		t.Errorf("Expected ASSIGN, got %d", tok.Token)
	}

	// Line continuation removes the backslash-newline and trailing whitespace before it,
	// then replaces with a single space. Leading whitespace on the continuation line is skipped.
	expected := "condor.cs.wisc.edu, $(HOSTNAME), stork.cs.wisc.edu"
	if tok.Lit != expected {
		t.Errorf("Expected %q, got %q", expected, tok.Lit)
	}
}
