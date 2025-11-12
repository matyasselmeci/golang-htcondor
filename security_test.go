package htcondor

import (
	"strings"
	"testing"

	"github.com/bbockelm/cedar/security"
	"github.com/bbockelm/golang-htcondor/config"
)

func TestGetSecurityConfig_Defaults(t *testing.T) {
	// Test with empty configuration (should use defaults)
	cfgReader := strings.NewReader("")
	cfg, err := config.NewFromReader(cfgReader)
	if err != nil {
		t.Fatalf("Failed to create config: %v", err)
	}

	secConfig, err := GetSecurityConfig(cfg, 60000, "CLIENT")
	if err != nil {
		t.Fatalf("GetSecurityConfig failed: %v", err)
	}

	// Check defaults
	if secConfig.Authentication != security.SecurityOptional {
		t.Errorf("Expected Authentication=SecurityOptional, got %v", secConfig.Authentication)
	}
	if secConfig.Encryption != security.SecurityOptional {
		t.Errorf("Expected Encryption=SecurityOptional, got %v", secConfig.Encryption)
	}
	if secConfig.Integrity != security.SecurityOptional {
		t.Errorf("Expected Integrity=SecurityOptional, got %v", secConfig.Integrity)
	}

	// Check default auth methods (FS, IDTOKENS)
	if len(secConfig.AuthMethods) != 2 {
		t.Errorf("Expected 2 default auth methods, got %d", len(secConfig.AuthMethods))
	}

	// Check default crypto method (AES)
	if len(secConfig.CryptoMethods) != 1 {
		t.Errorf("Expected 1 default crypto method, got %d", len(secConfig.CryptoMethods))
	}
	if len(secConfig.CryptoMethods) > 0 && secConfig.CryptoMethods[0] != security.CryptoAES {
		t.Errorf("Expected CryptoAES, got %v", secConfig.CryptoMethods[0])
	}
}

func TestGetSecurityConfig_ClientSettings(t *testing.T) {
	configText := `
SEC_CLIENT_AUTHENTICATION = REQUIRED
SEC_CLIENT_ENCRYPTION = PREFERRED
SEC_CLIENT_INTEGRITY = OPTIONAL
SEC_CLIENT_AUTHENTICATION_METHODS = SSL,TOKEN
SEC_CLIENT_CRYPTO_METHODS = AES
`
	cfgReader := strings.NewReader(configText)
	cfg, err := config.NewFromReader(cfgReader)
	if err != nil {
		t.Fatalf("Failed to create config: %v", err)
	}

	secConfig, err := GetSecurityConfig(cfg, 60000, "CLIENT")
	if err != nil {
		t.Fatalf("GetSecurityConfig failed: %v", err)
	}

	// Check CLIENT-specific settings
	if secConfig.Authentication != security.SecurityRequired {
		t.Errorf("Expected Authentication=SecurityRequired, got %v", secConfig.Authentication)
	}
	if secConfig.Encryption != security.SecurityPreferred {
		t.Errorf("Expected Encryption=SecurityPreferred, got %v", secConfig.Encryption)
	}
	if secConfig.Integrity != security.SecurityOptional {
		t.Errorf("Expected Integrity=SecurityOptional, got %v", secConfig.Integrity)
	}

	// Check auth methods
	if len(secConfig.AuthMethods) != 2 {
		t.Errorf("Expected 2 auth methods, got %d", len(secConfig.AuthMethods))
	}
	if secConfig.AuthMethods[0] != security.AuthSSL {
		t.Errorf("Expected first auth method to be SSL, got %v", secConfig.AuthMethods[0])
	}
	if secConfig.AuthMethods[1] != security.AuthToken {
		t.Errorf("Expected second auth method to be TOKEN, got %v", secConfig.AuthMethods[1])
	}

	// Check crypto methods
	if len(secConfig.CryptoMethods) != 1 {
		t.Errorf("Expected 1 crypto method, got %d", len(secConfig.CryptoMethods))
	}
	if secConfig.CryptoMethods[0] != security.CryptoAES {
		t.Errorf("Expected CryptoAES, got %v", secConfig.CryptoMethods[0])
	}
}

func TestGetSecurityConfig_DefaultFallback(t *testing.T) {
	// Test that CLIENT context falls back to DEFAULT when CLIENT settings are not specified
	configText := `
SEC_DEFAULT_AUTHENTICATION = REQUIRED
SEC_DEFAULT_ENCRYPTION = NEVER
SEC_DEFAULT_AUTHENTICATION_METHODS = KERBEROS,SSL
SEC_DEFAULT_CRYPTO_METHODS = BLOWFISH,3DES
`
	cfgReader := strings.NewReader(configText)
	cfg, err := config.NewFromReader(cfgReader)
	if err != nil {
		t.Fatalf("Failed to create config: %v", err)
	}

	secConfig, err := GetSecurityConfig(cfg, 60000, "CLIENT")
	if err != nil {
		t.Fatalf("GetSecurityConfig failed: %v", err)
	}

	// Check that DEFAULT settings are used
	if secConfig.Authentication != security.SecurityRequired {
		t.Errorf("Expected Authentication=SecurityRequired from DEFAULT, got %v", secConfig.Authentication)
	}
	if secConfig.Encryption != security.SecurityNever {
		t.Errorf("Expected Encryption=SecurityNever from DEFAULT, got %v", secConfig.Encryption)
	}

	// Check auth methods from DEFAULT
	if len(secConfig.AuthMethods) != 2 {
		t.Errorf("Expected 2 auth methods from DEFAULT, got %d", len(secConfig.AuthMethods))
	}

	// Check crypto methods from DEFAULT
	if len(secConfig.CryptoMethods) != 2 {
		t.Errorf("Expected 2 crypto methods from DEFAULT, got %d", len(secConfig.CryptoMethods))
	}
}

func TestGetSecurityConfig_SSLCertificates(t *testing.T) {
	configText := `
SEC_CLIENT_AUTHENTICATION_METHODS = SSL
AUTH_SSL_CLIENT_CERTFILE = /path/to/cert.pem
AUTH_SSL_CLIENT_KEYFILE = /path/to/key.pem
AUTH_SSL_CLIENT_CAFILE = /path/to/ca.pem
`
	cfgReader := strings.NewReader(configText)
	cfg, err := config.NewFromReader(cfgReader)
	if err != nil {
		t.Fatalf("Failed to create config: %v", err)
	}

	secConfig, err := GetSecurityConfig(cfg, 60000, "CLIENT")
	if err != nil {
		t.Fatalf("GetSecurityConfig failed: %v", err)
	}

	// Check SSL certificate paths
	if secConfig.CertFile != "/path/to/cert.pem" {
		t.Errorf("Expected CertFile=/path/to/cert.pem, got %s", secConfig.CertFile)
	}
	if secConfig.KeyFile != "/path/to/key.pem" {
		t.Errorf("Expected KeyFile=/path/to/key.pem, got %s", secConfig.KeyFile)
	}
	if secConfig.CAFile != "/path/to/ca.pem" {
		t.Errorf("Expected CAFile=/path/to/ca.pem, got %s", secConfig.CAFile)
	}
}

func TestGetSecurityConfig_TokenDirectory(t *testing.T) {
	configText := `
SEC_CLIENT_AUTHENTICATION_METHODS = TOKEN,IDTOKENS
SEC_TOKEN_DIRECTORY = /custom/token/dir
`
	cfgReader := strings.NewReader(configText)
	cfg, err := config.NewFromReader(cfgReader)
	if err != nil {
		t.Fatalf("Failed to create config: %v", err)
	}

	secConfig, err := GetSecurityConfig(cfg, 60000, "CLIENT")
	if err != nil {
		t.Fatalf("GetSecurityConfig failed: %v", err)
	}

	// Check token directory
	if secConfig.TokenDir != "/custom/token/dir" {
		t.Errorf("Expected TokenDir=/custom/token/dir, got %s", secConfig.TokenDir)
	}
}

func TestGetSecurityConfig_MultipleAuthMethods(t *testing.T) {
	configText := `
SEC_CLIENT_AUTHENTICATION_METHODS = SSL,KERBEROS,PASSWORD,FS,IDTOKENS,SCITOKENS,TOKEN,ANONYMOUS
`
	cfgReader := strings.NewReader(configText)
	cfg, err := config.NewFromReader(cfgReader)
	if err != nil {
		t.Fatalf("Failed to create config: %v", err)
	}

	secConfig, err := GetSecurityConfig(cfg, 60000, "CLIENT")
	if err != nil {
		t.Fatalf("GetSecurityConfig failed: %v", err)
	}

	// Check that all methods are mapped
	expectedCount := 8 // SSL, KERBEROS, PASSWORD, FS, IDTOKENS, SCITOKENS, TOKEN, ANONYMOUS (as AuthNone)
	if len(secConfig.AuthMethods) != expectedCount {
		t.Errorf("Expected %d auth methods, got %d: %v", expectedCount, len(secConfig.AuthMethods), secConfig.AuthMethods)
	}

	// Verify specific methods
	hasSSL := false
	hasKerberos := false
	hasToken := false
	for _, method := range secConfig.AuthMethods {
		if method == security.AuthSSL {
			hasSSL = true
		}
		if method == security.AuthKerberos {
			hasKerberos = true
		}
		if method == security.AuthToken {
			hasToken = true
		}
	}

	if !hasSSL {
		t.Error("Expected SSL authentication method")
	}
	if !hasKerberos {
		t.Error("Expected Kerberos authentication method")
	}
	if !hasToken {
		t.Error("Expected Token authentication method")
	}
}

func TestGetSecurityConfig_MultipleCryptoMethods(t *testing.T) {
	configText := `
SEC_CLIENT_CRYPTO_METHODS = AES,BLOWFISH,3DES
`
	cfgReader := strings.NewReader(configText)
	cfg, err := config.NewFromReader(cfgReader)
	if err != nil {
		t.Fatalf("Failed to create config: %v", err)
	}

	secConfig, err := GetSecurityConfig(cfg, 60000, "CLIENT")
	if err != nil {
		t.Fatalf("GetSecurityConfig failed: %v", err)
	}

	// Check that all crypto methods are mapped
	if len(secConfig.CryptoMethods) != 3 {
		t.Errorf("Expected 3 crypto methods, got %d", len(secConfig.CryptoMethods))
	}

	// Verify order is preserved
	if secConfig.CryptoMethods[0] != security.CryptoAES {
		t.Errorf("Expected first crypto method to be AES, got %v", secConfig.CryptoMethods[0])
	}
	if secConfig.CryptoMethods[1] != security.CryptoBlowfish {
		t.Errorf("Expected second crypto method to be Blowfish, got %v", secConfig.CryptoMethods[1])
	}
	if secConfig.CryptoMethods[2] != security.Crypto3DES {
		t.Errorf("Expected third crypto method to be 3DES, got %v", secConfig.CryptoMethods[2])
	}
}

func TestMapSecurityLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected security.SecurityLevel
	}{
		{"REQUIRED", security.SecurityRequired},
		{"required", security.SecurityRequired},
		{"PREFERRED", security.SecurityPreferred},
		{"OPTIONAL", security.SecurityOptional},
		{"NEVER", security.SecurityNever},
		{"  REQUIRED  ", security.SecurityRequired}, // with whitespace
		{"invalid", security.SecurityOptional},      // unknown defaults to optional
		{"", security.SecurityOptional},             // empty defaults to optional
	}

	for _, tt := range tests {
		result := mapSecurityLevel(tt.input)
		if result != tt.expected {
			t.Errorf("mapSecurityLevel(%q) = %v, expected %v", tt.input, result, tt.expected)
		}
	}
}

func TestMapAuthMethods(t *testing.T) {
	tests := []struct {
		input         string
		expectedCount int
		hasSSL        bool
		hasToken      bool
	}{
		{"SSL,TOKEN", 2, true, true},
		{"ssl,token", 2, true, true},  // case insensitive
		{"SSL, TOKEN", 2, true, true}, // with spaces
		{"", 0, false, false},         // empty string
		{"SSL,KERBEROS,PASSWORD,FS", 4, true, false},
		{"ANONYMOUS", 1, false, false}, // ANONYMOUS maps to AuthNone
	}

	for _, tt := range tests {
		result := mapAuthMethods(tt.input)
		if len(result) != tt.expectedCount {
			t.Errorf("mapAuthMethods(%q) returned %d methods, expected %d", tt.input, len(result), tt.expectedCount)
		}

		hasSSL := false
		hasToken := false
		for _, method := range result {
			if method == security.AuthSSL {
				hasSSL = true
			}
			if method == security.AuthToken {
				hasToken = true
			}
		}

		if hasSSL != tt.hasSSL {
			t.Errorf("mapAuthMethods(%q) hasSSL=%v, expected %v", tt.input, hasSSL, tt.hasSSL)
		}
		if hasToken != tt.hasToken {
			t.Errorf("mapAuthMethods(%q) hasToken=%v, expected %v", tt.input, hasToken, tt.hasToken)
		}
	}
}

func TestMapCryptoMethods(t *testing.T) {
	tests := []struct {
		input         string
		expectedCount int
		expected      []security.CryptoMethod
	}{
		{"AES", 1, []security.CryptoMethod{security.CryptoAES}},
		{"AES,BLOWFISH", 2, []security.CryptoMethod{security.CryptoAES, security.CryptoBlowfish}},
		{"aes,blowfish,3des", 3, []security.CryptoMethod{security.CryptoAES, security.CryptoBlowfish, security.Crypto3DES}},
		{"", 0, []security.CryptoMethod{}},
	}

	for _, tt := range tests {
		result := mapCryptoMethods(tt.input)
		if len(result) != tt.expectedCount {
			t.Errorf("mapCryptoMethods(%q) returned %d methods, expected %d", tt.input, len(result), tt.expectedCount)
		}

		for i, expected := range tt.expected {
			if i >= len(result) || result[i] != expected {
				t.Errorf("mapCryptoMethods(%q)[%d] = %v, expected %v", tt.input, i, result[i], expected)
			}
		}
	}
}

func TestGetSecurityConfig_READContext(t *testing.T) {
	// Test with a different context
	configText := `
SEC_READ_AUTHENTICATION = NEVER
SEC_READ_ENCRYPTION = REQUIRED
SEC_DEFAULT_AUTHENTICATION = REQUIRED
`
	cfgReader := strings.NewReader(configText)
	cfg, err := config.NewFromReader(cfgReader)
	if err != nil {
		t.Fatalf("Failed to create config: %v", err)
	}

	secConfig, err := GetSecurityConfig(cfg, 60000, "READ")
	if err != nil {
		t.Fatalf("GetSecurityConfig failed: %v", err)
	}

	// READ context should use READ-specific settings
	if secConfig.Authentication != security.SecurityNever {
		t.Errorf("Expected Authentication=SecurityNever from READ context, got %v", secConfig.Authentication)
	}
	if secConfig.Encryption != security.SecurityRequired {
		t.Errorf("Expected Encryption=SecurityRequired from READ context, got %v", secConfig.Encryption)
	}
}
