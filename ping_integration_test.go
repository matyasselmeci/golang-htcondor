package htcondor

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestCollectorPingIntegration tests the Collector.Ping method against a real HTCondor instance
func TestCollectorPingIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup mini HTCondor instance
	harness := setupCondorHarness(t)

	t.Logf("HTCondor instance started with collector at: %s", harness.GetCollectorAddr())

	// Parse collector address - HTCondor uses "sinful strings" like <127.0.0.1:9618?addrs=...>
	// Extract the host:port from within the angle brackets
	addr := harness.GetCollectorAddr()
	addr = strings.TrimPrefix(addr, "<")
	if idx := strings.Index(addr, "?"); idx > 0 {
		addr = addr[:idx] // Remove query parameters
	}
	addr = strings.TrimSuffix(addr, ">")

	// Create a Collector client
	collector := NewCollector(addr)

	// Test ping operation
	t.Run("PingCollector", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		result, err := collector.Ping(ctx)
		if err != nil {
			t.Fatalf("Failed to ping collector: %v", err)
		}

		// Verify we got a result
		if result == nil {
			t.Fatal("Expected non-nil ping result")
		}

		// Log the result
		t.Logf("Ping Result:")
		t.Logf("  Auth Method: %s", result.AuthMethod)
		t.Logf("  User: %s", result.User)
		t.Logf("  Session ID: %s", result.SessionID)
		t.Logf("  Valid Commands: %s", result.ValidCommands)
		t.Logf("  Encryption: %v", result.Encryption)
		t.Logf("  Authentication: %v", result.Authentication)

		// Verify that we got authentication information
		// In the test environment, we should get some form of authentication
		if result.AuthMethod == "" {
			t.Error("Expected non-empty authentication method")
		}

		// Note: User may be empty if using anonymous authentication (CLAIMTOBE)
		// but that's acceptable for testing

		// Verify authentication was performed (should be true)
		if !result.Authentication {
			t.Error("Expected authentication to be true")
		}
	})
}

// TestScheddPingIntegration tests the Schedd.Ping method against a real HTCondor instance
func TestScheddPingIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup mini HTCondor instance
	harness := setupCondorHarness(t)

	t.Logf("HTCondor instance started with collector at: %s", harness.GetCollectorAddr())

	// Parse collector address
	collectorAddr := harness.GetCollectorAddr()
	collectorAddr = strings.TrimPrefix(collectorAddr, "<")
	if idx := strings.Index(collectorAddr, "?"); idx > 0 {
		collectorAddr = collectorAddr[:idx]
	}
	collectorAddr = strings.TrimSuffix(collectorAddr, ">")

	// Create a Collector client to discover the schedd
	collector := NewCollector(collectorAddr)

	// Query for schedd to get its address
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	scheddAds, err := collector.QueryAds(ctx, "Schedd", "")
	if err != nil {
		t.Fatalf("Failed to query schedd ads: %v", err)
	}

	if len(scheddAds) == 0 {
		t.Fatal("No schedd ads found")
	}

	// Extract schedd address from the ad
	scheddAd := scheddAds[0]
	myAddressExpr, ok := scheddAd.Lookup("MyAddress")
	if !ok {
		t.Fatal("Schedd ad missing MyAddress attribute")
	}

	scheddAddr := strings.TrimSpace(myAddressExpr.String())
	scheddAddr = strings.Trim(scheddAddr, "\"")
	scheddAddr = strings.TrimPrefix(scheddAddr, "<")
	scheddAddr = strings.TrimSuffix(scheddAddr, ">")

	// Get schedd name
	scheddName := harness.scheddName

	t.Logf("Schedd address: %s, name: %s", scheddAddr, scheddName)

	// Create a Schedd client
	schedd := NewSchedd(scheddName, scheddAddr)

	// Test ping operation
	t.Run("PingSchedd", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		result, err := schedd.Ping(ctx)
		if err != nil {
			t.Fatalf("Failed to ping schedd: %v", err)
		}

		// Verify we got a result
		if result == nil {
			t.Fatal("Expected non-nil ping result")
		}

		// Log the result
		t.Logf("Ping Result:")
		t.Logf("  Auth Method: %s", result.AuthMethod)
		t.Logf("  User: %s", result.User)
		t.Logf("  Session ID: %s", result.SessionID)
		t.Logf("  Valid Commands: %s", result.ValidCommands)
		t.Logf("  Encryption: %v", result.Encryption)
		t.Logf("  Authentication: %v", result.Authentication)

		// Verify that we got authentication information
		if result.AuthMethod == "" {
			t.Error("Expected non-empty authentication method")
		}

		// Verify authentication was performed
		if !result.Authentication {
			t.Error("Expected authentication to be true")
		}
	})

	// Test multiple pings in sequence (session caching)
	t.Run("MultiplePings", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

			result, err := schedd.Ping(ctx)
			cancel()

			if err != nil {
				t.Fatalf("Ping %d failed: %v", i+1, err)
			}

			if result == nil {
				t.Fatalf("Ping %d returned nil result", i+1)
			}

			t.Logf("Ping %d successful - Auth: %s, User: %s", i+1, result.AuthMethod, result.User)
		}
	})
}
