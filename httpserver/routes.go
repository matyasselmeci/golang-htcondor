package httpserver

import (
	"net/http"
)

// setupRoutes sets up all HTTP routes
func (s *Server) setupRoutes(mux *http.ServeMux) {
	// OpenAPI schema
	mux.HandleFunc("/openapi.json", s.handleOpenAPISchema)

	// Job management endpoints
	mux.HandleFunc("/api/v1/jobs", s.handleJobs)
	mux.HandleFunc("/api/v1/jobs/", s.handleJobsPath) // Pattern with trailing slash catches /api/v1/jobs/* paths

	// Collector endpoints
	mux.HandleFunc("/api/v1/collector/", s.handleCollectorPath) // Pattern with trailing slash catches /api/v1/collector/* paths

	// Metrics endpoint (if enabled)
	if s.prometheusExporter != nil {
		mux.HandleFunc("/metrics", s.handleMetrics)
	}
}
