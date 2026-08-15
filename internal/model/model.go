// Package model defines the domain types shared across the job ledger service:
// lifecycle events, job state, retention metadata, and erasure records.
package model

import (
	"encoding/json"
	"errors"
	"time"
)

// EventType enumerates the lifecycle events an agent reports for a job.
type EventType string

const (
	EventStarted    EventType = "started"
	EventCheckpoint EventType = "checkpoint"
	EventFinished   EventType = "finished"
	EventMetrics    EventType = "metrics"
)

// Valid reports whether t is a known event type.
func (t EventType) Valid() bool {
	switch t {
	case EventStarted, EventCheckpoint, EventFinished, EventMetrics:
		return true
	default:
		return false
	}
}

// Event is a single lifecycle report for a job. Seq is monotonic per job and,
// together with JobID, forms the idempotency key used to reject duplicate
// (network-retried) reports.
type Event struct {
	JobID      string          `json:"job_id"`
	Tenant     string          `json:"tenant"`
	Seq        uint64          `json:"seq"`
	Type       EventType       `json:"type"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	ClientTime time.Time       `json:"client_time"`
}

// Validate returns an error describing the first invalid field, if any.
func (e Event) Validate() error {
	if e.JobID == "" {
		return errors.New("job_id is required")
	}
	if e.Tenant == "" {
		return errors.New("tenant is required")
	}
	if e.Seq == 0 {
		return errors.New("seq must be >= 1")
	}
	if !e.Type.Valid() {
		return errors.New("unknown event type")
	}
	if e.ClientTime.IsZero() {
		return errors.New("client_time is required")
	}
	return nil
}

// JobStatus is the lifecycle state of a job in the ledger.
type JobStatus string

const (
	StatusActive   JobStatus = "active"
	StatusArchived JobStatus = "archived"
	StatusErased   JobStatus = "erased"
)

// RetentionInfo describes the retention windows and archival timestamps that
// the retention engine attaches to an archived job.
type RetentionInfo struct {
	ActiveWindow    time.Duration `json:"active_window_ns"`
	RetentionWindow time.Duration `json:"retention_window_ns"`
	ArchivedAt      time.Time     `json:"archived_at"`
	ExpiresAt       time.Time     `json:"expires_at"`
}

// Job is the in-memory index entry for one job.
type Job struct {
	ID         string
	Tenant     string
	Status     JobStatus
	LastSeq    uint64
	LastActive time.Time
	Finished   bool
	Retention  *RetentionInfo
	CreatedAt  time.Time
}

// ErasureRequest is a compliance-deletion request targeting either a single
// job or every job of a tenant.
type ErasureRequest struct {
	JobID  string `json:"job_id,omitempty"`
	Tenant string `json:"tenant,omitempty"`
}

// Validate enforces exactly-one-of semantics.
func (r ErasureRequest) Validate() error {
	if r.JobID == "" && r.Tenant == "" {
		return errors.New("one of job_id or tenant is required")
	}
	if r.JobID != "" && r.Tenant != "" {
		return errors.New("job_id and tenant are mutually exclusive")
	}
	return nil
}

// Tombstone is the durable, auditable marker that a job has been erased. Its
// presence guarantees a late-arriving event cannot resurrect the job.
type Tombstone struct {
	JobID    string    `json:"job_id"`
	Tenant   string    `json:"tenant"`
	ErasedAt time.Time `json:"erased_at"`
	Reason   string    `json:"reason"`
}
