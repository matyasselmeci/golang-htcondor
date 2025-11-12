package config

import (
	"testing"
)

func TestEvaluateConditionDefined(t *testing.T) {
	cfg := &Config{
		values: map[string]string{
			"FOO": "bar",
		},
		evaluating: make(map[string]bool),
	}

	tests := []struct {
		condition string
		expected  bool
	}{
		{"defined(FOO)", true},
		{"defined(BAR)", false},
		{"defined(NONEXISTENT)", false},
	}

	for _, tt := range tests {
		result, err := cfg.evaluateCondition(tt.condition)
		if err != nil {
			t.Errorf("%s failed: %v", tt.condition, err)
			continue
		}

		if result != tt.expected {
			t.Errorf("%s: expected %v, got %v", tt.condition, tt.expected, result)
		}
	}
}

func TestEvaluateConditionTruthy(t *testing.T) {
	cfg := &Config{
		values:     make(map[string]string),
		evaluating: make(map[string]bool),
	}

	tests := []struct {
		value    string
		expected bool
	}{
		{"true", true},
		{"TRUE", true},
		{"yes", true},
		{"YES", true},
		{"1", true},
		{"on", true},
		{"false", false},
		{"FALSE", false},
		{"no", false},
		{"NO", false},
		{"0", false},
		{"off", false},
		{"", false},
		{"anything", true}, // Non-empty strings are truthy
	}

	for _, tt := range tests {
		result := cfg.isTruthy(tt.value)
		if result != tt.expected {
			t.Errorf("isTruthy(%q): expected %v, got %v", tt.value, tt.expected, result)
		}
	}
}

func TestEvaluateConditionComparison(t *testing.T) {
	cfg := &Config{
		values: map[string]string{
			"NUM": "42",
			"STR": "hello",
		},
		evaluating: make(map[string]bool),
	}

	tests := []struct {
		condition string
		expected  bool
	}{
		{"$(NUM) == 42", true},
		{"$(NUM) != 43", true},
		{"$(NUM) > 40", true},
		{"$(NUM) < 50", true},
		{"$(NUM) >= 42", true},
		{"$(NUM) <= 42", true},
		{"$(STR) == hello", true},
		{"$(STR) != world", true},
	}

	for _, tt := range tests {
		result, err := cfg.evaluateCondition(tt.condition)
		if err != nil {
			t.Errorf("%s failed: %v", tt.condition, err)
			continue
		}

		if result != tt.expected {
			t.Errorf("%s: expected %v, got %v", tt.condition, tt.expected, result)
		}
	}
}

func TestEvaluateConditionLogical(t *testing.T) {
	cfg := &Config{
		values: map[string]string{
			"FOO": "bar",
			"NUM": "42",
		},
		evaluating: make(map[string]bool),
	}

	tests := []struct {
		condition string
		expected  bool
	}{
		{"defined(FOO) && defined(NUM)", true},
		{"defined(FOO) && defined(BAR)", false},
		{"defined(FOO) || defined(BAR)", true},
		{"defined(BAZ) || defined(QUX)", false},
		{"!defined(BAR)", true},
		{"!defined(FOO)", false},
	}

	for _, tt := range tests {
		result, err := cfg.evaluateCondition(tt.condition)
		if err != nil {
			t.Errorf("%s failed: %v", tt.condition, err)
			continue
		}

		if result != tt.expected {
			t.Logf("Condition: %q", tt.condition)
			t.Logf("Config values: %v", cfg.values)
			t.Errorf("%s: expected %v, got %v", tt.condition, tt.expected, result)
		}
	}
}

func TestEvaluateVersionCondition(t *testing.T) {
	cfg := &Config{
		values: map[string]string{
			"CONDOR_VERSION": "9.0.0",
		},
		evaluating: make(map[string]bool),
	}

	tests := []struct {
		condition string
		expected  bool
	}{
		{"version >= 8.0.0", true},
		{"version > 8.0.0", true},
		{"version < 10.0.0", true},
		{"version <= 9.0.0", true},
		{"version == 9.0.0", true},
		{"version != 8.0.0", true},
		{"version >= 10.0.0", false},
	}

	for _, tt := range tests {
		result, err := cfg.evaluateCondition(tt.condition)
		if err != nil {
			t.Errorf("%s failed: %v", tt.condition, err)
			continue
		}

		if result != tt.expected {
			t.Errorf("%s: expected %v, got %v", tt.condition, tt.expected, result)
		}
	}
}

func TestCompareVersions(t *testing.T) {
	tests := []struct {
		v1       string
		v2       string
		expected int
	}{
		{"9.0.0", "8.0.0", 1},
		{"8.0.0", "9.0.0", -1},
		{"9.0.0", "9.0.0", 0},
		{"9.0.1", "9.0.0", 1},
		{"9.0", "9.0.0", 0},
		{"10.0.0", "9.9.9", 1},
		{"$CondorVersion: 9.0.0", "9.0.0", 0},
	}

	for _, tt := range tests {
		result := compareVersions(tt.v1, tt.v2)
		if result != tt.expected {
			t.Errorf("compareVersions(%q, %q): expected %d, got %d", tt.v1, tt.v2, tt.expected, result)
		}
	}
}
