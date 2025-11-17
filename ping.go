package htcondor

import (
	"context"
	"fmt"

	"github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/security"
)

// PingResult contains the result of a ping operation
type PingResult struct {
	// AuthMethod is the authentication method that was negotiated
	AuthMethod string
	// User is the authenticated username
	User string
	// SessionID is the session identifier
	SessionID string
	// ValidCommands is a string describing which commands are authorized
	ValidCommands string
	// Encryption indicates whether encryption was negotiated
	Encryption bool
	// Authentication indicates whether authentication was performed
	Authentication bool
}

// Ping performs a ping operation against the collector daemon
// This is similar to condor_ping and provides information about authentication
// and authorization. It's useful for health checks and debugging security settings.
func (c *Collector) Ping(ctx context.Context) (*PingResult, error) {
	// Establish connection using cedar client
	htcondorClient, err := client.ConnectToAddress(ctx, c.address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to collector: %w", err)
	}
	defer func() {
		if cerr := htcondorClient.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close connection: %w", cerr)
		}
	}()

	// Get CEDAR stream from client
	cedarStream := htcondorClient.GetStream()

	// Get SecurityConfig for DC_AUTHENTICATE command
	secConfig, err := GetSecurityConfigOrDefault(ctx, nil, int(commands.DC_AUTHENTICATE), "CLIENT", c.address)
	if err != nil {
		return nil, fmt.Errorf("failed to create security config: %w", err)
	}

	// Perform security handshake using DC_AUTHENTICATE
	auth := security.NewAuthenticator(secConfig, cedarStream)
	negotiation, err := auth.ClientHandshake(ctx)
	if err != nil {
		return nil, fmt.Errorf("ping handshake failed: %w", err)
	}

	// Convert negotiation result to PingResult
	result := &PingResult{
		AuthMethod:     string(negotiation.NegotiatedAuth),
		User:           negotiation.User,
		SessionID:      negotiation.SessionId,
		ValidCommands:  negotiation.ValidCommands,
		Encryption:     negotiation.Encryption,
		Authentication: negotiation.Authentication,
	}

	return result, nil
}

// Ping performs a ping operation against the schedd daemon
// This is similar to condor_ping and provides information about authentication
// and authorization. It's useful for health checks and debugging security settings.
func (s *Schedd) Ping(ctx context.Context) (*PingResult, error) {
	// Establish connection using cedar client
	htcondorClient, err := client.ConnectToAddress(ctx, s.address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to schedd at %s: %w", s.address, err)
	}
	defer func() {
		if cerr := htcondorClient.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close connection: %w", cerr)
		}
	}()

	// Get CEDAR stream from client
	cedarStream := htcondorClient.GetStream()

	// Get SecurityConfig for DC_AUTHENTICATE command
	secConfig, err := GetSecurityConfigOrDefault(ctx, nil, int(commands.DC_AUTHENTICATE), "CLIENT", s.address)
	if err != nil {
		return nil, fmt.Errorf("failed to create security config: %w", err)
	}

	// Perform security handshake using DC_AUTHENTICATE
	auth := security.NewAuthenticator(secConfig, cedarStream)
	negotiation, err := auth.ClientHandshake(ctx)
	if err != nil {
		return nil, fmt.Errorf("ping handshake failed: %w", err)
	}

	// Convert negotiation result to PingResult
	result := &PingResult{
		AuthMethod:     string(negotiation.NegotiatedAuth),
		User:           negotiation.User,
		SessionID:      negotiation.SessionId,
		ValidCommands:  negotiation.ValidCommands,
		Encryption:     negotiation.Encryption,
		Authentication: negotiation.Authentication,
	}

	return result, nil
}
