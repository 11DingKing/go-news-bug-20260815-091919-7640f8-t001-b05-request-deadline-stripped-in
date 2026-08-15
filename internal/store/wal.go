package store

import (
	"context"
	"encoding/json"
	"os"
	"sync"

	"example.com/ledgerd/internal/model"
)

// WAL is an append-only write-ahead log. Records are newline-delimited JSON
// with the newline written as a separator *between* records rather than as a
// trailing terminator, so the final record may legitimately lack a trailing
// newline. Every append is fsync'd before returning, so a completed append is
// durable.
type WAL struct {
	path    string
	f       *os.File
	mu      sync.Mutex
	records int64
}

func openWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &WAL{path: path, f: f}, nil
}

// Append durably writes a single event record. The context is checked before
// and after the write so a caller that exceeded its deadline learns about it
// instead of silently continuing.
func (w *WAL) Append(ctx context.Context, ev model.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.records > 0 {
		if _, err := w.f.Write([]byte("\n")); err != nil {
			return err
		}
	}
	if _, err := w.f.Write(data); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.records++
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// ReadAll returns the raw bytes of the WAL, used by the reconcile package to
// rebuild the index on startup.
func (w *WAL) ReadAll() ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Sync(); err != nil {
		return nil, err
	}
	return os.ReadFile(w.path)
}

// Close flushes and closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}
