package api

import (
	"bm-lrs/pkg/route"
	"fmt"
	"log"
	"net/http"
)

// APIServer represents the REST API server
type APIServer struct {
	repo   *route.LRSRouteRepository
	port   int
	server *http.Server
}

// NewAPIServer creates a new API server instance
func NewAPIServer(repo *route.LRSRouteRepository, port int) *APIServer {
	return &APIServer{
		repo: repo,
		port: port,
	}
}

// Start starts the REST API server
func (s *APIServer) Start() error {
	handler := NewAPIHandler(s.repo)

	mux := http.NewServeMux()

	// Register routes
	mux.HandleFunc("/api/v1/calculate-mvalue", handler.CalculateMValueHandler)

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: mux,
	}

	log.Printf("Starting REST API server on port %d", s.port)
	return s.server.ListenAndServe()
}

// Stop stops the REST API server
func (s *APIServer) Stop() error {
	if s.server != nil {
		return s.server.Close()
	}
	return nil
}
