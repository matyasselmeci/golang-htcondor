package htcondor

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
	"github.com/bbockelm/cedar/security"
	"github.com/bbockelm/cedar/stream"
)

// Schedd represents an HTCondor schedd daemon
type Schedd struct {
	name    string
	address string
	port    int
}

// NewSchedd creates a new Schedd instance
func NewSchedd(name string, address string, port int) *Schedd {
	return &Schedd{
		name:    name,
		address: address,
		port:    port,
	}
}

// Query queries the schedd for job advertisements
// constraint is a ClassAd constraint expression (use "true" to get all jobs)
// projection is a list of attributes to return (use nil to get all attributes)
func (s *Schedd) Query(ctx context.Context, constraint string, projection []string) ([]*classad.ClassAd, error) {
	return s.queryWithAuth(ctx, constraint, projection, false)
}

// queryWithAuth performs the actual query with optional authentication
func (s *Schedd) queryWithAuth(ctx context.Context, constraint string, projection []string, useAuth bool) ([]*classad.ClassAd, error) {
	// Establish TCP connection
	addr := fmt.Sprintf("%s:%d", s.address, s.port)
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to schedd: %w", err)
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close connection: %w", cerr)
		}
	}()

	// Create CEDAR stream
	cedarStream := stream.NewStream(conn)

	// Determine command
	cmd := commands.QUERY_JOB_ADS
	if useAuth {
		cmd = commands.QUERY_JOB_ADS_WITH_AUTH
	}

	// Perform security handshake
	secConfig := &security.SecurityConfig{
		Command:        cmd,
		AuthMethods:    []security.AuthMethod{security.AuthSSL, security.AuthToken},
		Authentication: security.SecurityOptional,
		CryptoMethods:  []security.CryptoMethod{security.CryptoAES},
		Encryption:     security.SecurityOptional,
		Integrity:      security.SecurityOptional,
	}

	auth := security.NewAuthenticator(secConfig, cedarStream)
	_, err = auth.ClientHandshake(ctx)
	if err != nil {
		return nil, fmt.Errorf("security handshake failed: %w", err)
	}

	// Create query request ClassAd
	requestAd := createJobQueryAd(constraint, projection)

	// Send query
	queryMsg := message.NewMessageForStream(cedarStream)
	err = queryMsg.PutClassAd(ctx, requestAd)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize query ClassAd: %w", err)
	}

	err = queryMsg.FinishMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to send query: %w", err)
	}

	// Receive response ads
	var jobAds []*classad.ClassAd

	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return jobAds, ctx.Err()
		default:
		}

		// Create a new message for each response ClassAd
		responseMsg := message.NewMessageFromStream(cedarStream)

		// Read ClassAd
		ad, err := responseMsg.GetClassAd(ctx)
		if err != nil {
			return jobAds, fmt.Errorf("failed to read ClassAd: %w", err)
		}

		// Check if this is the final ad (Owner == 0)
		if ownerVal, ok := ad.EvaluateAttrInt("Owner"); ok && ownerVal == 0 {
			// This is the final ad - check for errors
			if errCode, ok := ad.EvaluateAttrInt("ErrorCode"); ok && errCode != 0 {
				errMsg := "unknown error"
				if errStr, ok := ad.EvaluateAttrString("ErrorString"); ok {
					errMsg = errStr
				}
				return jobAds, fmt.Errorf("schedd query error %d: %s", errCode, errMsg)
			}
			// Success - final ad received (may contain summary information)
			break
		}

		// This is a job ad - append to results
		jobAds = append(jobAds, ad)
	}

	return jobAds, nil
}

// createJobQueryAd creates a request ClassAd for querying jobs
func createJobQueryAd(constraint string, projection []string) *classad.ClassAd {
	ad := classad.New()

	// Set constraint (use "true" if empty)
	if constraint == "" {
		constraint = "true"
	}
	// Parse constraint as an expression
	constraintExpr, err := classad.ParseExpr(constraint)
	if err != nil {
		// If parsing fails, use a simple "true" expression
		constraintExpr, _ = classad.ParseExpr("true")
	}
	ad.InsertExpr("Requirements", constraintExpr)

	// Set projection (newline-separated list of attributes)
	if len(projection) > 0 {
		projectionStr := strings.Join(projection, " ")
		_ = ad.Set("Projection", projectionStr)
	}

	return ad
}

// Submit submits a job to the schedd using an HTCondor submit file
// submitFileContent is the content of an HTCondor submit file
// Returns the cluster ID as a string
func (s *Schedd) Submit(ctx context.Context, submitFileContent string) (string, error) {
	// Parse the submit file
	submitFile, err := ParseSubmitFile(strings.NewReader(submitFileContent))
	if err != nil {
		return "", fmt.Errorf("failed to parse submit file: %w", err)
	}

	// Create QMGMT connection
	qmgmt, err := NewQmgmtConnection(ctx, s.address, s.port)
	if err != nil {
		return "", fmt.Errorf("failed to connect to schedd: %w", err)
	}
	defer func() {
		if cerr := qmgmt.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close connection: %w", cerr)
		}
	}()

	// Set up error handling to abort transaction on failure
	var submissionErr error
	defer func() {
		if submissionErr != nil {
			_ = qmgmt.AbortTransaction(ctx)
		}
	}()

	// Get authenticated user from the QMGMT connection
	owner := qmgmt.authenticatedUser
	if owner == "" {
		submissionErr = fmt.Errorf("no authenticated user")
		return "", submissionErr
	}

	// Set effective owner
	if err := qmgmt.SetEffectiveOwner(ctx, owner); err != nil {
		submissionErr = fmt.Errorf("failed to set effective owner: %w", err)
		return "", submissionErr
	}

	// Create new cluster
	clusterID, err := qmgmt.NewCluster(ctx)
	if err != nil {
		submissionErr = fmt.Errorf("failed to create cluster: %w", err)
		return "", submissionErr
	}

	// Generate job ads from the submit file
	submitResult, err := submitFile.Submit(clusterID)
	if err != nil {
		submissionErr = fmt.Errorf("failed to generate job ads: %w", err)
		return "", submissionErr
	}

	// Submit each proc
	for i, procAd := range submitResult.ProcAds {
		procID, err := qmgmt.NewProc(ctx, clusterID)
		if err != nil {
			submissionErr = fmt.Errorf("failed to create proc %d: %w", i, err)
			return "", submissionErr
		}

		// Send job attributes
		if err := qmgmt.SendJobAttributes(ctx, clusterID, procID, procAd); err != nil {
			submissionErr = fmt.Errorf("failed to set attributes for proc %d: %w", i, err)
			return "", submissionErr
		}
	}

	// Commit transaction
	if err := qmgmt.CommitTransaction(ctx); err != nil {
		submissionErr = fmt.Errorf("failed to commit transaction: %w", err)
		return "", submissionErr
	}

	return fmt.Sprintf("%d", clusterID), nil
}

// Act performs an action on a job (e.g., remove, hold, release)
func (s *Schedd) Act(_ context.Context, _ string, _ string) error {
	// TODO: Implement job action using cedar protocol
	return fmt.Errorf("not implemented")
}

// Edit modifies job attributes
func (s *Schedd) Edit(_ context.Context, _ string, _ string, _ string) error {
	// TODO: Implement job edit using cedar protocol
	return fmt.Errorf("not implemented")
}
