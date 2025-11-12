package htcondor

import (
	"context"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

func TestNewSchedd(t *testing.T) {
	schedd := NewSchedd("test_schedd", "schedd.example.com", 9618)
	if schedd == nil {
		t.Fatal("NewSchedd returned nil")
	}
	if schedd.name != "test_schedd" {
		t.Errorf("Expected name 'test_schedd', got '%s'", schedd.name)
	}
	if schedd.address != "schedd.example.com" {
		t.Errorf("Expected address 'schedd.example.com', got '%s'", schedd.address)
	}
	if schedd.port != 9618 {
		t.Errorf("Expected port 9618, got %d", schedd.port)
	}
}

func TestScheddQuery(t *testing.T) {
	schedd := NewSchedd("test_schedd", "schedd.example.com", 9618)
	ctx := context.Background()

	_, err := schedd.Query(ctx, "Owner == \"user\"", []string{"ClusterId", "ProcId"})
	if err == nil {
		t.Error("Expected error for unimplemented method")
	}
}

func TestScheddSubmit(t *testing.T) {
	schedd := NewSchedd("test_schedd", "schedd.example.com", 9618)
	ctx := context.Background()

	ad := classad.New()
	_ = ad.Set("Cmd", "/bin/echo")
	_ = ad.Set("Args", "hello")
	_, err := schedd.Submit(ctx, ad)
	if err == nil {
		t.Error("Expected error for unimplemented method")
	}
}

func TestScheddAct(t *testing.T) {
	schedd := NewSchedd("test_schedd", "schedd.example.com", 9618)
	ctx := context.Background()

	err := schedd.Act(ctx, "remove", "ClusterId == 1")
	if err == nil {
		t.Error("Expected error for unimplemented method")
	}
}

func TestScheddEdit(t *testing.T) {
	schedd := NewSchedd("test_schedd", "schedd.example.com", 9618)
	ctx := context.Background()

	err := schedd.Edit(ctx, "ClusterId == 1", "JobPrio", "10")
	if err == nil {
		t.Error("Expected error for unimplemented method")
	}
}
