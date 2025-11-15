package htcondor

import (
	"context"
	"sync"
	"testing"

	"github.com/bbockelm/cedar/commands"
)

func TestGlobalDefaultConfig(t *testing.T) {
	// Test that getDefaultConfig is thread-safe
	t.Run("ThreadSafety", func(_ *testing.T) {
		// Reset global config
		globalDefaultConfig.Store(nil)

		var wg sync.WaitGroup
		numGoroutines := 10

		// Launch multiple goroutines trying to get default config
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				cfg := getDefaultConfig()
				// Config may be nil if HTCondor is not installed
				// Just checking that we don't panic
				_ = cfg
			}()
		}

		wg.Wait()
	})

	// Test ReloadDefaultConfig
	t.Run("ReloadDefaultConfig", func(t *testing.T) {
		// Reset global config
		globalDefaultConfig.Store(nil)

		// First load
		cfg1 := getDefaultConfig()

		// Reload
		ReloadDefaultConfig()
		cfg2 := getDefaultConfig()

		// Both should be nil or both should be non-nil
		// (depends on whether HTCondor config exists)
		if (cfg1 == nil) != (cfg2 == nil) {
			t.Errorf("Reload changed nil state: cfg1=%v cfg2=%v", cfg1 != nil, cfg2 != nil)
		}
	})

	// Test GetSecurityConfigOrDefault with nil config
	t.Run("GetSecurityConfigOrDefaultWithNilConfig", func(t *testing.T) {
		// Reset global config
		globalDefaultConfig.Store(nil)

		ctx := context.Background()
		secConfig, err := GetSecurityConfigOrDefault(ctx, nil, commands.QUERY_JOB_ADS, "CLIENT", "schedd.example.com")

		if err != nil {
			t.Fatalf("GetSecurityConfigOrDefault failed: %v", err)
		}

		if secConfig == nil {
			t.Fatal("Expected non-nil SecurityConfig")
		}

		// Should have default values
		if secConfig.Command != commands.QUERY_JOB_ADS {
			t.Errorf("Expected command %d, got %d", commands.QUERY_JOB_ADS, secConfig.Command)
		}

		if secConfig.PeerName != "schedd.example.com" {
			t.Errorf("Expected PeerName 'schedd.example.com', got %s", secConfig.PeerName)
		}

		// Should have default auth methods
		if len(secConfig.AuthMethods) == 0 {
			t.Error("Expected non-empty AuthMethods")
		}
	})

	// Test that global config is used when available
	t.Run("GlobalConfigPrecedence", func(t *testing.T) {
		// This test just verifies the logic flows correctly
		// Actual behavior depends on whether HTCondor config exists

		ctx := context.Background()

		// Try with explicit nil config - should use global default
		secConfig1, err := GetSecurityConfigOrDefault(ctx, nil, commands.QUERY_JOB_ADS, "CLIENT", "schedd1.example.com")
		if err != nil {
			t.Fatalf("GetSecurityConfigOrDefault failed: %v", err)
		}
		if secConfig1 == nil {
			t.Fatal("Expected non-nil SecurityConfig")
		}

		// Verify it returns a valid config
		if secConfig1.Command != commands.QUERY_JOB_ADS {
			t.Errorf("Expected command %d, got %d", commands.QUERY_JOB_ADS, secConfig1.Command)
		}
	})

	// Test thread-safe concurrent access to GetSecurityConfigOrDefault
	t.Run("ConcurrentGetSecurityConfigOrDefault", func(t *testing.T) {
		// Reset global config
		globalDefaultConfig.Store(nil)

		var wg sync.WaitGroup
		numGoroutines := 20
		ctx := context.Background()

		// Launch multiple goroutines trying to get security config
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				secConfig, err := GetSecurityConfigOrDefault(ctx, nil, commands.QUERY_JOB_ADS, "CLIENT", "schedd.example.com")
				if err != nil {
					t.Errorf("Goroutine %d: GetSecurityConfigOrDefault failed: %v", id, err)
					return
				}
				if secConfig == nil {
					t.Errorf("Goroutine %d: Expected non-nil SecurityConfig", id)
				}
			}(i)
		}

		wg.Wait()
	})
}
