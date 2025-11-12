package config

import (
	"strings"
	"testing"
)

func TestErrorAsAttributeName(t *testing.T) {
	// Test that "error = value" is treated as an assignment, not a directive
	input := `
error = error.txt
warning = warning.log
`
	cfg, err := NewFromReader(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	errorVal, ok := cfg.Get("error")
	if !ok {
		t.Fatal("Expected 'error' attribute to be set")
	}
	if errorVal != "error.txt" {
		t.Errorf("Expected error='error.txt', got '%s'", errorVal)
	}

	warningVal, ok := cfg.Get("warning")
	if !ok {
		t.Fatal("Expected 'warning' attribute to be set")
	}
	if warningVal != "warning.log" {
		t.Errorf("Expected warning='warning.log', got '%s'", warningVal)
	}
}

func TestErrorAsDirective(t *testing.T) {
	// Test that "error: message" is treated as a directive
	input := `
error: This is an error message
`
	_, err := NewFromReader(strings.NewReader(input))
	if err == nil {
		t.Fatal("Expected error directive to cause an error")
	}

	// Check that the error message contains our text
	if !strings.Contains(err.Error(), "This is an error message") {
		t.Errorf("Expected error message to contain 'This is an error message', got: %v", err)
	}
}

func TestErrorWithSpaces(t *testing.T) {
	// Test that "error = value" with spaces works
	input := `
error  =  spaced.txt
`
	cfg, err := NewFromReader(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	errorVal, ok := cfg.Get("error")
	if !ok {
		t.Fatal("Expected 'error' attribute to be set")
	}
	if errorVal != "spaced.txt" {
		t.Errorf("Expected error='spaced.txt', got '%s'", errorVal)
	}
}
