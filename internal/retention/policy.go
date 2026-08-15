// Package retention implements the background retention engine: archiving jobs
// past their active window, hard-deleting archived jobs past their retention
// window, and compliance erasure by job or tenant.
package retention

import (
	"context"
	"time"

	"example.com/ledgerd/internal/model"
	"example.com/ledgerd/internal/store"
)

// Config configures the retention engine's windows and cadence.
type Config struct {
	ActiveWindow    time.Duration
	RetentionWindow time.Duration
	ArchiveInterval time.Duration
	ReapInterval    time.Duration
}

func (c Config) withDefaults() Config {
	if c.ActiveWindow <= 0 {
		c.ActiveWindow = 24 * time.Hour
	}
	if c.RetentionWindow <= 0 {
		c.RetentionWindow = 7 * 24 * time.Hour
	}
	if c.ArchiveInterval <= 0 {
		c.ArchiveInterval = time.Minute
	}
	if c.ReapInterval <= 0 {
		c.ReapInterval = time.Minute
	}
	return c
}

// Engine runs the retention background tasks.
type Engine struct {
	cfg   Config
	store store.Store
}

// New creates a retention engine.
func New(cfg Config, st store.Store) *Engine {
	return &Engine{cfg: cfg.withDefaults(), store: st}
}

// ArchiveScan archives every active job whose last activity predates the
// active window. Returns the number of jobs archived.
func (e *Engine) ArchiveScan(ctx context.Context) (int, error) {
	cutoff := time.Now().Add(-e.cfg.ActiveWindow)
	n := 0
	for _, job := range e.store.ListActive() {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		if job.LastActive.Before(cutoff) {
			if err := e.store.Archive(job.ID, e.cfg.ActiveWindow, e.cfg.RetentionWindow); err != nil {
				return n, err
			}
			n++
		}
	}
	return n, nil
}

// Reap hard-deletes archived jobs whose retention window has expired, writing
// a tombstone and audit entry for each. Returns the number of jobs erased.
func (e *Engine) Reap(ctx context.Context) (int, error) {
	now := time.Now()
	n := 0
	for _, job := range e.store.ListArchived() {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		if now.After(job.Retention.ExpiresAt) {
			if _, err := e.store.Erase(model.ErasureRequest{JobID: job.ID}); err != nil {
				return n, err
			}
			n++
		}
	}
	return n, nil
}

// Erase performs a compliance deletion by jobID or tenant. Idempotent.
func (e *Engine) Erase(ctx context.Context, req model.ErasureRequest) (int, error) {
	return e.store.Erase(req)
}

// Run executes the archive and reap scans on their configured intervals until
// ctx is cancelled.
func (e *Engine) Run(ctx context.Context) {
	archive := time.NewTicker(e.cfg.ArchiveInterval)
	reap := time.NewTicker(e.cfg.ReapInterval)
	defer archive.Stop()
	defer reap.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-archive.C:
			_, _ = e.ArchiveScan(ctx)
		case <-reap.C:
			_, _ = e.Reap(ctx)
		}
	}
}
