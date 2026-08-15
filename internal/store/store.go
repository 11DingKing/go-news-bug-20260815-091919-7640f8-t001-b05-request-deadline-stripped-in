// Package store implements the durable, append-only persistence layer: a WAL
// plus an in-memory index, an archive area, and an auditable tombstone log.
// No external database is used.
package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"example.com/ledgerd/internal/model"
)

// Sentinel errors returned by the store. Callers must classify these with
// errors.Is, never with ==, because intermediate layers (the ingest pipeline)
// wrap store errors with %w before they reach the HTTP handler.
var (
	ErrDuplicate             = errors.New("store: duplicate event")
	ErrJobErased             = errors.New("store: job erased")
	ErrJobNotFound           = errors.New("store: job not found")
	ErrNotActive             = errors.New("store: job not active")
	ErrErasureTargetRequired = errors.New("store: erasure target required")
	ErrClosed                = errors.New("store: closed")
)

// Store is the persistence interface consumed by the ingest, retention, and
// HTTP layers. Keeping the interface at the consumer boundary makes the store
// replaceable in tests (e.g. a gated or slow store to exercise concurrency).
type Store interface {
	Append(ctx context.Context, ev model.Event) error
	Restore(ev model.Event) error
	GetJob(jobID string) (model.Job, bool)
	ListActive() []model.Job
	ListArchived() []model.Job
	Archive(jobID string, activeWindow, retentionWindow time.Duration) error
	Erase(req model.ErasureRequest) (int, error)
	WALData() ([]byte, error)
	Snapshot() ([]byte, error)
	Stats() Stats
	Close() error
}

// Stats is a point-in-time snapshot of store health.
type Stats struct {
	Active     int
	Archived   int
	Persisted  uint64
	Duplicates uint64
	Erased     uint64
	Tombstones int
}

type seqKey struct {
	jobID string
	seq   uint64
}

// ledgerStore is the concrete implementation of the persistence layer.
type ledgerStore struct {
	mu sync.RWMutex

	jobs         map[string]*model.Job
	seen         map[seqKey]struct{}
	tombstones   []model.Tombstone
	tombstoneSet map[string]model.Tombstone

	persisted  uint64
	duplicates uint64
	erased     uint64

	wal    *WAL
	dir    string
	closed bool
}

const tombstonesFile = "tombstones.log"

// New opens (or creates) the store rooted at dir.
func New(dir string) (Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	wal, err := openWAL(filepath.Join(dir, "ledger.wal"))
	if err != nil {
		return nil, err
	}
	s := &ledgerStore{
		jobs:         map[string]*model.Job{},
		seen:         map[seqKey]struct{}{},
		tombstoneSet: map[string]model.Tombstone{},
		wal:          wal,
		dir:          dir,
	}
	if err := s.loadTombstones(); err != nil {
		wal.Close()
		return nil, err
	}
	return s, nil
}

// Append durably persists an event and updates the index. It is idempotent:
// a repeated (jobID, seq) returns ErrDuplicate without writing again, and a
// late event for an erased job returns ErrJobErased so deleted data is never
// resurrected.
func (s *ledgerStore) Append(ctx context.Context, ev model.Event) error {
	if err := ev.Validate(); err != nil {
		return fmt.Errorf("store: invalid event: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}
	key := seqKey{ev.JobID, ev.Seq}
	if _, ok := s.seen[key]; ok {
		s.duplicates++
		return ErrDuplicate
	}
	if _, ok := s.tombstoneSet[ev.JobID]; ok {
		return ErrJobErased
	}
	if err := s.wal.Append(ctx, ev); err != nil {
		return err
	}
	s.seen[key] = struct{}{}
	s.upsertJob(ev)
	s.persisted++
	return nil
}

// Restore rebuilds the index from a replayed WAL event without writing the
// WAL again. It is idempotent: events already in the index return ErrDuplicate
// and events for erased jobs return ErrJobErased, both of which the reconcile
// loop skips.
func (s *ledgerStore) Restore(ev model.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.tombstoneSet[ev.JobID]; ok {
		return ErrJobErased
	}
	key := seqKey{ev.JobID, ev.Seq}
	if _, ok := s.seen[key]; ok {
		return ErrDuplicate
	}
	s.seen[key] = struct{}{}
	s.upsertJob(ev)
	return nil
}

// GetJob returns a copy of the job's current state.
func (s *ledgerStore) GetJob(jobID string) (model.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[jobID]
	if !ok {
		return model.Job{}, false
	}
	return cloneJob(job), true
}

// ListActive returns copies of all active jobs.
func (s *ledgerStore) ListActive() []model.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]model.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		if j.Status == model.StatusActive {
			out = append(out, cloneJob(j))
		}
	}
	return out
}

// ListArchived returns copies of all archived jobs.
func (s *ledgerStore) ListArchived() []model.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]model.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		if j.Status == model.StatusArchived {
			out = append(out, cloneJob(j))
		}
	}
	return out
}

// Erase performs a compliance deletion by jobID or tenant. Tombstones are
// persisted before the index is mutated so a persistence failure leaves the
// index untouched and the operation retryable. Repeated erasure is idempotent.
func (s *ledgerStore) Erase(req model.ErasureRequest) (int, error) {
	if req.JobID == "" && req.Tenant == "" {
		return 0, ErrErasureTargetRequired
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var targets []*model.Job
	if req.JobID != "" {
		if job, ok := s.jobs[req.JobID]; ok && job.Status != model.StatusErased {
			targets = append(targets, job)
		}
	} else {
		for _, job := range s.jobs {
			if job.Tenant == req.Tenant && job.Status != model.StatusErased {
				targets = append(targets, job)
			}
		}
	}
	if len(targets) == 0 {
		return 0, nil
	}

	now := time.Now()
	newTombs := make([]model.Tombstone, 0, len(targets))
	for _, job := range targets {
		newTombs = append(newTombs, model.Tombstone{
			JobID:    job.ID,
			Tenant:   job.Tenant,
			ErasedAt: now,
			Reason:   "compliance_erasure",
		})
	}
	for _, t := range newTombs {
		if err := s.persistTombstone(t); err != nil {
			return 0, err
		}
	}
	for i, job := range targets {
		t := newTombs[i]
		s.tombstones = append(s.tombstones, t)
		s.tombstoneSet[job.ID] = t
		job.Status = model.StatusErased
		delete(s.jobs, job.ID)
		s.erased++
	}
	return len(targets), nil
}

// WALData returns the raw bytes of the WAL for reconcile-driven recovery.
func (s *ledgerStore) WALData() ([]byte, error) {
	return s.wal.ReadAll()
}

// Snapshot serialises the full index (jobs and tombstones) as a point-in-time
// checkpoint written periodically by the background compaction task.
func (s *ledgerStore) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := snapshotData{
		Jobs:       make([]model.Job, 0, len(s.jobs)),
		Tombstones: append([]model.Tombstone(nil), s.tombstones...),
	}
	for _, j := range s.jobs {
		snap.Jobs = append(snap.Jobs, cloneJob(j))
	}
	return json.Marshal(snap)
}

// Stats reports current store health.
func (s *ledgerStore) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var active, archived int
	for _, j := range s.jobs {
		switch j.Status {
		case model.StatusActive:
			active++
		case model.StatusArchived:
			archived++
		}
	}
	return Stats{
		Active:     active,
		Archived:   archived,
		Persisted:  s.persisted,
		Duplicates: s.duplicates,
		Erased:     s.erased,
		Tombstones: len(s.tombstones),
	}
}

// Close flushes and closes the WAL. It is idempotent.
func (s *ledgerStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.wal.Close()
}

func (s *ledgerStore) upsertJob(ev model.Event) {
	job, ok := s.jobs[ev.JobID]
	if !ok {
		job = &model.Job{
			ID:        ev.JobID,
			Tenant:    ev.Tenant,
			Status:    model.StatusActive,
			CreatedAt: ev.ClientTime,
		}
		s.jobs[ev.JobID] = job
	}
	if ev.Seq > job.LastSeq {
		job.LastSeq = ev.Seq
	}
	if ev.ClientTime.After(job.LastActive) {
		job.LastActive = ev.ClientTime
	}
	if ev.Type == model.EventFinished {
		job.Finished = true
	}
}

func cloneJob(j *model.Job) model.Job {
	c := *j
	if j.Retention != nil {
		r := *j.Retention
		c.Retention = &r
	}
	return c
}

func (s *ledgerStore) loadTombstones() error {
	path := filepath.Join(s.dir, tombstonesFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, line := range bytes.Split(data, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var t model.Tombstone
		if err := json.Unmarshal(line, &t); err != nil {
			return fmt.Errorf("store: corrupt tombstone record: %w", err)
		}
		s.tombstones = append(s.tombstones, t)
		s.tombstoneSet[t.JobID] = t
	}
	return nil
}

func (s *ledgerStore) persistTombstone(t model.Tombstone) error {
	f, err := os.OpenFile(filepath.Join(s.dir, tombstonesFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := json.Marshal(t)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := f.Write(data); err != nil {
		return err
	}
	return f.Sync()
}

type snapshotData struct {
	Jobs       []model.Job       `json:"jobs"`
	Tombstones []model.Tombstone `json:"tombstones"`
}
