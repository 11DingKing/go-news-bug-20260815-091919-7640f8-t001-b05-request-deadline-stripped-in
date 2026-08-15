package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"example.com/ledgerd/internal/ingest"
	"example.com/ledgerd/internal/model"
	"example.com/ledgerd/internal/store"
)

// postEventRequest is the body of POST /api/v1/jobs/{jobID}/events.
type postEventRequest struct {
	Tenant     string          `json:"tenant"`
	Seq        uint64          `json:"seq"`
	Type       model.EventType `json:"type"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	ClientTime time.Time       `json:"client_time"`
}

func (r postEventRequest) toEvent(jobID string) model.Event {
	return model.Event{
		JobID:      jobID,
		Tenant:     r.Tenant,
		Seq:        r.Seq,
		Type:       r.Type,
		Payload:    r.Payload,
		ClientTime: r.ClientTime,
	}
}

func (s *Server) handlePostEvent(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")
	if jobID == "" {
		writeError(w, http.StatusBadRequest, "job_id is required")
		return
	}
	var req postEventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	ev := req.toEvent(jobID)
	if err := ev.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	err := s.pipeline.Ingest(r.Context(), ev)
	s.writeEventResult(w, err)
}

// writeEventResult maps the pipeline/store outcome to an HTTP status code.
func (s *Server) writeEventResult(w http.ResponseWriter, err error) {
	switch {
	case err == nil:
		w.WriteHeader(http.StatusAccepted)
	case errors.Is(err, store.ErrDuplicate):
		// Idempotent retry: report success, never a server error.
		w.WriteHeader(http.StatusAccepted)
	case errors.Is(err, ingest.ErrQueueFull):
		writeError(w, http.StatusServiceUnavailable, "queue full")
	case errors.Is(err, ingest.ErrShuttingDown):
		writeError(w, http.StatusServiceUnavailable, "shutting down")
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusGatewayTimeout, "deadline exceeded")
	case errors.Is(err, store.ErrJobErased):
		writeError(w, http.StatusConflict, "job erased")
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

type jobResponse struct {
	JobID      string     `json:"job_id"`
	Tenant     string     `json:"tenant"`
	Status     string     `json:"status"`
	LastSeq    uint64     `json:"last_seq"`
	LastActive time.Time  `json:"last_active"`
	Finished   bool       `json:"finished"`
	ArchivedAt *time.Time `json:"archived_at,omitempty"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")
	job, ok := s.store.GetJob(jobID)
	if !ok {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	resp := jobResponse{
		JobID:      job.ID,
		Tenant:     job.Tenant,
		Status:     string(job.Status),
		LastSeq:    job.LastSeq,
		LastActive: job.LastActive,
		Finished:   job.Finished,
	}
	if job.Status == model.StatusArchived {
		// Archived jobs must carry retention metadata; dereference it.
		resp.ArchivedAt = &job.Retention.ArchivedAt
		resp.ExpiresAt = &job.Retention.ExpiresAt
	}
	writeJSON(w, http.StatusOK, resp)
}

type erasureResponse struct {
	Erased int `json:"erased"`
}

func (s *Server) handleErasure(w http.ResponseWriter, r *http.Request) {
	var req model.ErasureRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	n, err := s.retention.Erase(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "erasure failed")
		return
	}
	writeJSON(w, http.StatusOK, erasureResponse{Erased: n})
}

type statusResponse struct {
	QueueDepth int    `json:"queue_depth"`
	Workers    int    `json:"workers"`
	Accepted   uint64 `json:"accepted"`
	Processed  uint64 `json:"processed"`
	Failed     uint64 `json:"failed"`
	Active     int    `json:"active_jobs"`
	Archived   int    `json:"archived_jobs"`
	Persisted  uint64 `json:"persisted"`
	Duplicates uint64 `json:"duplicates"`
	Erased     uint64 `json:"erased"`
	Tombstones int    `json:"tombstones"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ps := s.pipeline.Stats()
	st := s.store.Stats()
	writeJSON(w, http.StatusOK, statusResponse{
		QueueDepth: ps.QueueDepth,
		Workers:    ps.Workers,
		Accepted:   ps.Accepted,
		Processed:  ps.Processed,
		Failed:     ps.Failed,
		Active:     st.Active,
		Archived:   st.Archived,
		Persisted:  st.Persisted,
		Duplicates: st.Duplicates,
		Erased:     st.Erased,
		Tombstones: st.Tombstones,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
