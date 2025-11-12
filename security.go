package htcondor

import (
	"fmt"
	"strings"

	"github.com/bbockelm/cedar/security"
	"github.com/bbockelm/golang-htcondor/config"
)

// GetSecurityConfig creates a SecurityConfig from HTCondor configuration.
// It reads security-related parameters like SEC_CLIENT_AUTHENTICATION, SEC_DEFAULT_AUTHENTICATION,
// SEC_CLIENT_AUTHENTICATION_METHODS, etc., and maps them to the cedar SecurityConfig struct.
//
// The function follows HTCondor's security configuration pattern:
//   - SEC_<context>_<feature> where context is CLIENT, READ, WRITE, etc.
//   - Falls back to SEC_DEFAULT_* if context-specific settings are not found
//   - Supports REQUIRED, PREFERRED, OPTIONAL, NEVER security levels
//   - Supports multiple authentication methods (SSL, KERBEROS, TOKEN, etc.)
//   - Supports multiple encryption methods (AES, BLOWFISH, 3DES)
//
// Parameters:
//   - cfg: HTCondor configuration object
//   - command: The command to be executed (from cedar/commands package)
//   - context: Security context ("CLIENT", "READ", "WRITE", "ADMINISTRATOR", etc.)
//
// Returns:
//   - *security.SecurityConfig: Cedar security configuration
//   - error: Any configuration error encountered
//
// Deficiencies (to be addressed in follow-up):
//   - SSL certificate paths (AUTH_SSL_CLIENT_CERTFILE, etc.) not yet mapped
//   - Token directory locations (SEC_TOKEN_DIRECTORY, etc.) not yet mapped
//   - Authorization settings (ALLOW_READ, DENY_WRITE, etc.) are separate from SecurityConfig
//   - Context-specific overrides beyond CLIENT not yet fully implemented
//   - NEGOTIATION security level not yet mapped
func GetSecurityConfig(cfg *config.Config, command int, context string) (*security.SecurityConfig, error) {
	if context == "" {
		context = "CLIENT"
	}

	secConfig := &security.SecurityConfig{
		Command: command,
	}

	// Get authentication level
	authLevel := getSecurityLevel(cfg, context, "AUTHENTICATION")
	secConfig.Authentication = mapSecurityLevel(authLevel)

	// Get encryption level
	encLevel := getSecurityLevel(cfg, context, "ENCRYPTION")
	secConfig.Encryption = mapSecurityLevel(encLevel)

	// Get integrity level
	intLevel := getSecurityLevel(cfg, context, "INTEGRITY")
	secConfig.Integrity = mapSecurityLevel(intLevel)

	// Get authentication methods
	authMethods := getSecurityMethods(cfg, context, "AUTHENTICATION_METHODS")
	secConfig.AuthMethods = mapAuthMethods(authMethods)

	// Get crypto methods
	cryptoMethods := getSecurityMethods(cfg, context, "CRYPTO_METHODS")
	secConfig.CryptoMethods = mapCryptoMethods(cryptoMethods)

	// Get SSL certificate/key paths if SSL authentication is enabled
	for _, method := range secConfig.AuthMethods {
		if method == security.AuthSSL {
			if certFile, ok := cfg.Get("AUTH_SSL_CLIENT_CERTFILE"); ok {
				secConfig.CertFile = certFile
			}
			if keyFile, ok := cfg.Get("AUTH_SSL_CLIENT_KEYFILE"); ok {
				secConfig.KeyFile = keyFile
			}
			if caFile, ok := cfg.Get("AUTH_SSL_CLIENT_CAFILE"); ok {
				secConfig.CAFile = caFile
			}
			break
		}
	}

	// Get token file/directory if token authentication is enabled
	for _, method := range secConfig.AuthMethods {
		if method == security.AuthToken || method == security.AuthIDTokens || method == security.AuthSciTokens {
			if tokenDir, ok := cfg.Get("SEC_TOKEN_DIRECTORY"); ok {
				secConfig.TokenDir = tokenDir
			}
			// Note: TokenFile is typically used for single-token scenarios
			// In practice, HTCondor usually uses TokenDir with multiple tokens
			break
		}
	}

	return secConfig, nil
}

// getSecurityLevel retrieves a security level setting with context and default fallback
// For example: SEC_CLIENT_AUTHENTICATION, falling back to SEC_DEFAULT_AUTHENTICATION
func getSecurityLevel(cfg *config.Config, context, feature string) string {
	// Try context-specific setting first
	contextKey := fmt.Sprintf("SEC_%s_%s", context, feature)
	if value, ok := cfg.Get(contextKey); ok {
		return value
	}

	// Fall back to DEFAULT setting
	defaultKey := fmt.Sprintf("SEC_DEFAULT_%s", feature)
	if value, ok := cfg.Get(defaultKey); ok {
		return value
	}

	// Return HTCondor's default
	switch feature {
	case "AUTHENTICATION":
		return "OPTIONAL"
	case "ENCRYPTION":
		return "OPTIONAL"
	case "INTEGRITY":
		return "OPTIONAL"
	case "NEGOTIATION":
		return "PREFERRED"
	default:
		return "OPTIONAL"
	}
}

// getSecurityMethods retrieves a comma-separated list of security methods
// For example: SEC_CLIENT_AUTHENTICATION_METHODS, falling back to SEC_DEFAULT_AUTHENTICATION_METHODS
func getSecurityMethods(cfg *config.Config, context, feature string) string {
	// Try context-specific setting first
	contextKey := fmt.Sprintf("SEC_%s_%s", context, feature)
	if value, ok := cfg.Get(contextKey); ok {
		return value
	}

	// Fall back to DEFAULT setting
	defaultKey := fmt.Sprintf("SEC_DEFAULT_%s", feature)
	if value, ok := cfg.Get(defaultKey); ok {
		return value
	}

	// Return HTCondor's default based on platform
	switch feature {
	case "AUTHENTICATION_METHODS":
		return getDefaultAuthMethods()
	case "CRYPTO_METHODS":
		return "AES" // HTCondor 9.0+ default
	}

	return ""
}

// getDefaultAuthMethods returns platform-appropriate default authentication methods
func getDefaultAuthMethods() string {
	// Unix/Linux/macOS default: FS, IDTOKENS, KERBEROS
	// Windows default: NTSSPI, IDTOKENS, KERBEROS
	// We'll use Unix default since this is primarily targeting Unix systems
	// Note: HTCondor also includes FS_REMOTE by default, but cedar maps it to FS
	return "FS,IDTOKENS"
}

// mapSecurityLevel converts HTCondor security level string to cedar SecurityLevel
func mapSecurityLevel(level string) security.SecurityLevel {
	switch strings.ToUpper(strings.TrimSpace(level)) {
	case "REQUIRED":
		return security.SecurityRequired
	case "PREFERRED":
		return security.SecurityPreferred
	case "OPTIONAL":
		return security.SecurityOptional
	case "NEVER":
		return security.SecurityNever
	default:
		return security.SecurityOptional
	}
}

// mapAuthMethods converts comma-separated HTCondor auth methods to cedar AuthMethod slice
func mapAuthMethods(methods string) []security.AuthMethod {
	if methods == "" {
		return []security.AuthMethod{}
	}

	var result []security.AuthMethod
	for _, method := range strings.Split(methods, ",") {
		method = strings.ToUpper(strings.TrimSpace(method))
		switch method {
		case "SSL":
			result = append(result, security.AuthSSL)
		case "KERBEROS":
			result = append(result, security.AuthKerberos)
		case "PASSWORD":
			result = append(result, security.AuthPassword)
		case "FS":
			result = append(result, security.AuthFS)
		case "FS_REMOTE":
			// Cedar doesn't have FS_REMOTE as separate method, map to FS
			result = append(result, security.AuthFS)
		case "IDTOKENS":
			result = append(result, security.AuthIDTokens)
		case "SCITOKENS":
			result = append(result, security.AuthSciTokens)
		case "TOKEN":
			result = append(result, security.AuthToken)
		case "NTSSPI":
			// NTSSPI not in cedar's current auth methods (Windows-specific)
			// Skip for now
		case "MUNGE":
			// MUNGE not in cedar's current auth methods
			// Skip for now
		case "CLAIMTOBE":
			// CLAIMTOBE not in cedar's current auth methods
			// Skip for now
		case "ANONYMOUS":
			// Map ANONYMOUS to AuthNone
			result = append(result, security.AuthNone)
		}
	}

	return result
}

// mapCryptoMethods converts comma-separated HTCondor crypto methods to cedar CryptoMethod slice
func mapCryptoMethods(methods string) []security.CryptoMethod {
	if methods == "" {
		return []security.CryptoMethod{}
	}

	var result []security.CryptoMethod
	for _, method := range strings.Split(methods, ",") {
		method = strings.ToUpper(strings.TrimSpace(method))
		switch method {
		case "AES":
			result = append(result, security.CryptoAES)
		case "BLOWFISH":
			result = append(result, security.CryptoBlowfish)
		case "3DES":
			result = append(result, security.Crypto3DES)
		}
	}

	return result
}
