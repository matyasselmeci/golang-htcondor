package htcondor

import (
	"context"
	"fmt"
	"net"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
	"github.com/bbockelm/cedar/security"
	"github.com/bbockelm/cedar/stream"
)

// Collector represents an HTCondor collector daemon
type Collector struct {
	address string
	port    int
}

// NewCollector creates a new Collector instance
func NewCollector(address string, port int) *Collector {
	return &Collector{
		address: address,
		port:    port,
	}
}

// QueryAds queries the collector for daemon advertisements
// adType specifies the type of ads to query (e.g., "StartdAd", "ScheddAd")
// constraint is a ClassAd constraint expression
func (c *Collector) QueryAds(ctx context.Context, adType string, constraint string) ([]*classad.ClassAd, error) {
	// Establish TCP connection
	addr := net.JoinHostPort(c.address, fmt.Sprintf("%d", c.port))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to collector: %w", err)
	}
	defer conn.Close()

	// Create CEDAR stream
	cedarStream := stream.NewStream(conn)

	// Determine the command based on ad type
	cmd := getCommandForAdType(adType)

	// Perform security handshake
	secConfig := &security.SecurityConfig{
		Command:        int(cmd),
		AuthMethods:    []security.AuthMethod{security.AuthSSL, security.AuthToken},
		Authentication: security.SecurityOptional,
		CryptoMethods:  []security.CryptoMethod{security.CryptoAES},
		Encryption:     security.SecurityOptional,
		Integrity:      security.SecurityOptional,
	}

	auth := security.NewAuthenticator(secConfig, cedarStream)
	_, err = auth.ClientHandshake()
	if err != nil {
		return nil, fmt.Errorf("security handshake failed: %w", err)
	}

	// Create query ClassAd
	queryAd := createQueryAd(adType, constraint)

	// Create message and send query
	queryMsg := message.NewMessageForStream(cedarStream)
	err = queryMsg.PutClassAd(queryAd)
	if err != nil {
		return nil, fmt.Errorf("failed to add query ClassAd to message: %w", err)
	}

	err = queryMsg.FlushFrame(true)
	if err != nil {
		return nil, fmt.Errorf("failed to send query message: %w", err)
	}

	// Process response ads
	responseMsg := message.NewMessageFromStream(cedarStream)
	var ads []*classad.ClassAd

	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ads, ctx.Err()
		default:
		}

		// Read "more" flag
		more, err := responseMsg.GetInt32()
		if err != nil {
			return ads, fmt.Errorf("failed to read 'more' flag: %w", err)
		}

		if more == 0 {
			break
		}

		// Read ClassAd
		ad, err := responseMsg.GetClassAd()
		if err != nil {
			return ads, fmt.Errorf("failed to read ClassAd: %w", err)
		}

		ads = append(ads, ad)
	}

	return ads, nil
}

// getCommandForAdType maps ad type to HTCondor command
func getCommandForAdType(adType string) commands.CommandType {
	switch adType {
	case "StartdAd", "Machine":
		return commands.QUERY_STARTD_ADS
	case "ScheddAd", "Schedd":
		return commands.QUERY_SCHEDD_ADS
	case "MasterAd", "Master":
		return commands.QUERY_MASTER_ADS
	case "SubmitterAd", "Submitter":
		return commands.QUERY_SUBMITTOR_ADS
	case "LicenseAd", "License":
		return commands.QUERY_LICENSE_ADS
	case "CollectorAd", "Collector":
		return commands.QUERY_COLLECTOR_ADS
	case "NegotiatorAd", "Negotiator":
		return commands.QUERY_NEGOTIATOR_ADS
	default:
		// Default to generic query
		return commands.QUERY_STARTD_ADS
	}
}

// createQueryAd creates a ClassAd for querying ads
func createQueryAd(adType string, constraint string) *classad.ClassAd {
	ad := classad.New()

	// Set MyType and TargetType as required by HTCondor query protocol
	_ = ad.Set("MyType", "Query")

	// Set TargetType based on ad type
	targetType := getTargetTypeForAdType(adType)
	_ = ad.Set("TargetType", targetType)

	// Set Requirements
	if constraint == "" {
		_ = ad.Set("Requirements", true)
	} else {
		_ = ad.Set("Requirements", constraint)
	}

	return ad
}

// getTargetTypeForAdType maps ad type to TargetType
func getTargetTypeForAdType(adType string) string {
	switch adType {
	case "StartdAd", "Machine":
		return "Machine"
	case "ScheddAd", "Schedd":
		return "Scheduler"
	case "MasterAd", "Master":
		return "DaemonMaster"
	case "SubmitterAd", "Submitter":
		return "Submitter"
	case "NegotiatorAd", "Negotiator":
		return "Negotiator"
	case "CollectorAd", "Collector":
		return "Collector"
	default:
		return adType
	}
}

// Advertise sends an advertisement to the collector
func (c *Collector) Advertise(ctx context.Context, ad *classad.ClassAd, command string) error {
	// TODO: Implement advertisement using cedar protocol
	return fmt.Errorf("not implemented")
}

// LocateDaemon locates a daemon by querying the collector
func (c *Collector) LocateDaemon(ctx context.Context, daemonType string, name string) (*DaemonLocation, error) {
	// TODO: Implement daemon location logic
	return nil, fmt.Errorf("not implemented")
}

// DaemonLocation represents the location information for a daemon
type DaemonLocation struct {
	Name    string
	Address net.IP
	Port    int
	Pool    string
}
