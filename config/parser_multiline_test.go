package config

import (
	"strings"
	"testing"
)

func TestParserMultipleLines(t *testing.T) {
	input := `A = first
B = second
C = third
`
	lex := NewLexer(strings.NewReader(input))
	stmts, err := Parse(lex)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if len(stmts) != 3 {
		t.Fatalf("Expected 3 statements, got %d", len(stmts))
		for i, stmt := range stmts {
			t.Logf("Statement %d: %T %+v", i, stmt, stmt)
		}
	}

	expected := []struct {
		name  string
		value string
	}{
		{"A", "first"},
		{"B", "second"},
		{"C", "third"},
	}

	for i, exp := range expected {
		assign, ok := stmts[i].(*Assignment)
		if !ok {
			t.Errorf("Statement %d: expected Assignment, got %T", i, stmts[i])
			continue
		}
		if assign.Name != exp.name {
			t.Errorf("Statement %d: expected name %q, got %q", i, exp.name, assign.Name)
		}
		if assign.Value != exp.value {
			t.Errorf("Statement %d: expected value %q, got %q", i, exp.value, assign.Value)
		}
	}
}
