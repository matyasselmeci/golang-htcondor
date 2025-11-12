package config

import (
	"strings"
	"testing"
)

// Helper function to parse queue statements
func parseQueueStatements(input string) ([]Statement, error) {
	lexer := NewLexer(strings.NewReader(input))
	return Parse(lexer)
}

func TestQueueSimple(t *testing.T) {
	input := `
queue
`
	stmts, err := parseQueueStatements(input)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if len(stmts) != 1 {
		t.Fatalf("Expected 1 statement, got %d", len(stmts))
	}

	queueStmt, ok := stmts[0].(*QueueStatement)
	if !ok {
		t.Fatalf("Expected QueueStatement, got %T", stmts[0])
	}

	if queueStmt.Count != 1 {
		t.Errorf("Expected count 1, got %d", queueStmt.Count)
	}
}

func TestQueueWithCount(t *testing.T) {
	input := `
queue 10
`
	stmts, err := parseQueueStatements(input)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	queueStmt := stmts[0].(*QueueStatement)
	if queueStmt.Count != 10 {
		t.Errorf("Expected count 10, got %d", queueStmt.Count)
	}
}

func TestQueueFromFile(t *testing.T) {
	input := `
queue var1, var2 from inputfile.txt
`
	stmts, err := parseQueueStatements(input)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	queueStmt := stmts[0].(*QueueStatement)
	if len(queueStmt.VarNames) != 2 {
		t.Errorf("Expected 2 var names, got %d", len(queueStmt.VarNames))
	}
	if queueStmt.VarNames[0] != "var1" {
		t.Errorf("Expected var1, got %s", queueStmt.VarNames[0])
	}
	if queueStmt.VarNames[1] != "var2" {
		t.Errorf("Expected var2, got %s", queueStmt.VarNames[1])
	}
	if queueStmt.File != "inputfile.txt" {
		t.Errorf("Expected inputfile.txt, got %s", queueStmt.File)
	}
}

func TestQueueCountFromFile(t *testing.T) {
	input := `
queue 5 var from data.csv
`
	stmts, err := parseQueueStatements(input)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	queueStmt := stmts[0].(*QueueStatement)
	if queueStmt.Count != 5 {
		t.Errorf("Expected count 5, got %d", queueStmt.Count)
	}
	if queueStmt.File != "data.csv" {
		t.Errorf("Expected data.csv, got %s", queueStmt.File)
	}
}

func TestQueueIn(t *testing.T) {
	input := `
queue item in (apple, banana, cherry)
`
	stmts, err := parseQueueStatements(input)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	queueStmt := stmts[0].(*QueueStatement)
	if len(queueStmt.VarNames) != 1 || queueStmt.VarNames[0] != "item" {
		t.Errorf("Expected var name 'item', got %v", queueStmt.VarNames)
	}
	if len(queueStmt.Items) != 3 {
		t.Fatalf("Expected 3 items, got %d", len(queueStmt.Items))
	}
	expectedItems := []string{"apple", "banana", "cherry"}
	for i, expected := range expectedItems {
		if queueStmt.Items[i] != expected {
			t.Errorf("Item %d: expected %s, got %s", i, expected, queueStmt.Items[i])
		}
	}
}

func TestQueueCountIn(t *testing.T) {
	input := `
queue 2 var in (foo, bar)
`
	stmts, err := parseQueueStatements(input)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	queueStmt := stmts[0].(*QueueStatement)
	if queueStmt.Count != 2 {
		t.Errorf("Expected count 2, got %d", queueStmt.Count)
	}
	if len(queueStmt.Items) != 2 {
		t.Errorf("Expected 2 items, got %d", len(queueStmt.Items))
	}
}

func TestQueueMatching(t *testing.T) {
	input := `
queue matching files *.dat
`
	stmts, err := parseQueueStatements(input)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	queueStmt := stmts[0].(*QueueStatement)
	// For matching, the pattern is stored in File field
	if queueStmt.File == "" {
		t.Error("Expected File field to contain pattern")
	}
}

func TestQueueMatchingWithCount(t *testing.T) {
	input := `
queue 3 matching files
`
	stmts, err := parseQueueStatements(input)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	queueStmt := stmts[0].(*QueueStatement)
	if queueStmt.Count != 3 {
		t.Errorf("Expected count 3, got %d", queueStmt.Count)
	}
	if queueStmt.File != "files" {
		t.Errorf("Expected pattern 'files', got %s", queueStmt.File)
	}
}
