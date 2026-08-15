// Command ledgerd runs the job ledger and retention governance service for an
// AI compute centre: it ingests high-concurrency job lifecycle events,
// persists them to an append-only WAL, archives stale jobs, and supports
// auditable compliance erasure.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"example.com/ledgerd/internal/config"
	"example.com/ledgerd/internal/httpapi"
	"example.com/ledgerd/internal/ingest"
	"example.com/ledgerd/internal/reconcile"
	"example.com/ledgerd/internal/retention"
	"example.com/ledgerd/internal/store"
)

func main() {
	logger := log.New(os.Stderr, "ledgerd: ", log.LstdFlags)
	cfg := config.Load()
	logger.Printf("starting on :%d (data dir %s)", cfg.Port, cfg.DataDir)

	st, err := store.New(cfg.DataDir)
	if err != nil {
		logger.Fatalf("open store: %v", err)
	}

	// Reconcile: rebuild the index from the WAL before serving traffic.
	if err := reconcileFromWAL(st, logger); err != nil {
		logger.Fatalf("reconcile: %v", err)
	}

	p := ingest.New(ingest.Config{
		QueueSize:      cfg.QueueSize,
		Workers:        cfg.Workers,
		EnqueueTimeout: cfg.EnqueueTimeout,
	}, st)

	ret := retention.New(retention.Config{
		ActiveWindow:    cfg.ActiveWindow,
		RetentionWindow: cfg.RetentionWindow,
		ArchiveInterval: cfg.ArchiveInterval,
		ReapInterval:    cfg.ReapInterval,
	}, st)

	retCtx, retCancel := context.WithCancel(context.Background())
	defer retCancel()
	go ret.Run(retCtx)
	go snapshotLoop(retCtx, st, cfg.DataDir, cfg.ArchiveInterval, logger)

	srv := httpapi.NewServer(httpapi.Config{
		Port:         cfg.Port,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}, st, p, ret)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("server: %v", err)
		}
	case sig := <-sigCh:
		logger.Printf("received %v, shutting down", sig)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Printf("shutdown error: %v", err)
	}
	retCancel()
	logger.Printf("shutdown complete")
}

func reconcileFromWAL(st store.Store, logger *log.Logger) error {
	data, err := st.WALData()
	if err != nil {
		return err
	}
	n, err := reconcile.Replay(st, data)
	if err != nil {
		return err
	}
	if n > 0 {
		logger.Printf("reconciled %d events from WAL", n)
	}
	return nil
}

func snapshotLoop(ctx context.Context, st store.Store, dir string, interval time.Duration, logger *log.Logger) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			data, err := st.Snapshot()
			if err != nil {
				logger.Printf("snapshot: %v", err)
				continue
			}
			if err := os.WriteFile(filepath.Join(dir, "snapshot.json"), data, 0o600); err != nil {
				logger.Printf("snapshot write: %v", err)
			}
		}
	}
}
