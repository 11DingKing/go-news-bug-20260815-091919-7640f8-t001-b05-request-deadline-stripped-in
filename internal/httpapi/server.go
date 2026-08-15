// Package httpapi exposes the ledger's HTTP API and orchestrates graceful
// shutdown: stop accepting, drain in-flight requests, drain the pipeline, and
// close the store.
package httpapi

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"example.com/ledgerd/internal/ingest"
	"example.com/ledgerd/internal/retention"
	"example.com/ledgerd/internal/store"
)

// Config configures the HTTP server.
type Config struct {
	Port         int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func (c Config) withDefaults() Config {
	if c.Port == 0 {
		c.Port = 54278
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = 5 * time.Second
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 10 * time.Second
	}
	return c
}

// Server is the HTTP front end.
type Server struct {
	cfg       Config
	store     store.Store
	pipeline  *ingest.Pipeline
	retention *retention.Engine
	http      *http.Server
	logger    *log.Logger
}

// NewServer wires the HTTP routes to the store, pipeline, and retention
// engine.
func NewServer(cfg Config, st store.Store, p *ingest.Pipeline, r *retention.Engine) *Server {
	cfg = cfg.withDefaults()
	s := &Server{cfg: cfg, store: st, pipeline: p, retention: r, logger: log.Default()}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/jobs/{jobID}/events", s.handlePostEvent)
	mux.HandleFunc("GET /api/v1/jobs/{jobID}", s.handleGetJob)
	mux.HandleFunc("POST /api/v1/erasure", s.handleErasure)
	mux.HandleFunc("GET /admin/status", s.handleStatus)

	s.http = &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      s.recoverMiddleware(mux),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}
	return s
}

// Addr returns the listen address.
func (s *Server) Addr() string { return s.http.Addr }

// Handler returns the root HTTP handler (used by tests).
func (s *Server) Handler() http.Handler { return s.http.Handler }

// ListenAndServe blocks serving HTTP until the server is shut down.
func (s *Server) ListenAndServe() error {
	return s.http.ListenAndServe()
}

// Shutdown performs an orderly shutdown: stop accepting, drain in-flight HTTP
// handlers, drain the ingest pipeline, then close the store.
func (s *Server) Shutdown(ctx context.Context) error {
	if err := s.http.Shutdown(ctx); err != nil {
		return err
	}
	if err := s.pipeline.Shutdown(); err != nil {
		return err
	}
	return s.store.Close()
}

func (s *Server) recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.logger.Printf("panic serving %s %s: %v", r.Method, r.URL.Path, rec)
				writeError(w, http.StatusInternalServerError, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
