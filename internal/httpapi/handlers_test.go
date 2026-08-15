package httpapi_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"example.com/ledgerd/internal/httpapi"
	"example.com/ledgerd/internal/ingest"
	"example.com/ledgerd/internal/retention"
	"example.com/ledgerd/internal/store"
)

func newTestServer(t *testing.T) (store.Store, *httpapi.Server) {
	t.Helper()
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	p := ingest.New(ingest.Config{QueueSize: 32, Workers: 2, EnqueueTimeout: time.Second}, st)
	t.Cleanup(func() { _ = p.Shutdown() })
	ret := retention.New(retention.Config{ActiveWindow: time.Hour, RetentionWindow: time.Hour}, st)
	srv := httpapi.NewServer(httpapi.Config{Port: 54278}, st, p, ret)
	return st, srv
}

func doRequest(srv *httpapi.Server, method, path, body string) *httptest.ResponseRecorder {
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rdr)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	return rr
}

// TestIdempotentRetryIsNotServerError verifies that a duplicate (network
// retried) event report is treated as idempotent success, never a 500.
func TestIdempotentRetryIsNotServerError(t *testing.T) {
	st, srv := newTestServer(t)

	body := `{"tenant":"tenant-a","seq":1,"type":"started","client_time":"2026-08-15T00:00:00Z"}`
	path := "/api/v1/jobs/job-1/events"

	rr1 := doRequest(srv, http.MethodPost, path, body)
	if rr1.Code != http.StatusAccepted {
		t.Fatalf("first report: want 202, got %d (body %s)", rr1.Code, rr1.Body.String())
	}

	rr2 := doRequest(srv, http.MethodPost, path, body)
	if rr2.Code != http.StatusAccepted {
		t.Fatalf("idempotent retry: want 202, got %d (body %s)", rr2.Code, rr2.Body.String())
	}

	if got := st.Stats().Persisted; got != 1 {
		t.Fatalf("persisted %d, want 1 (duplicate must not be rewritten)", got)
	}
}

// TestPostEventValidatesInput verifies input validation returns 400 for
// malformed requests.
func TestPostEventValidatesInput(t *testing.T) {
	_, srv := newTestServer(t)

	cases := []struct {
		name string
		body string
		want int
	}{
		{"missing tenant", `{"seq":1,"type":"started","client_time":"2026-08-15T00:00:00Z"}`, http.StatusBadRequest},
		{"zero seq", `{"tenant":"t","seq":0,"type":"started","client_time":"2026-08-15T00:00:00Z"}`, http.StatusBadRequest},
		{"unknown type", `{"tenant":"t","seq":1,"type":"bogus","client_time":"2026-08-15T00:00:00Z"}`, http.StatusBadRequest},
		{"malformed json", `{not json`, http.StatusBadRequest},
	}
	for _, c := range cases {
		rr := doRequest(srv, http.MethodPost, "/api/v1/jobs/job-1/events", c.body)
		if rr.Code != c.want {
			t.Errorf("%s: want %d, got %d", c.name, c.want, rr.Code)
		}
	}
}

// TestGetJobStatusAndRetention verifies the query path returns status and
// retention info for an archived job.
func TestGetJobStatusAndRetention(t *testing.T) {
	st, srv := newTestServer(t)

	body := `{"tenant":"tenant-a","seq":1,"type":"started","client_time":"2026-08-15T00:00:00Z"}`
	if rr := doRequest(srv, http.MethodPost, "/api/v1/jobs/job-9/events", body); rr.Code != http.StatusAccepted {
		t.Fatalf("post: want 202, got %d", rr.Code)
	}
	if err := st.Archive("job-9", time.Hour, 2*time.Hour); err != nil {
		t.Fatalf("archive: %v", err)
	}

	rr := doRequest(srv, http.MethodGet, "/api/v1/jobs/job-9", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("get: want 200, got %d", rr.Code)
	}
	var resp struct {
		Status     string     `json:"status"`
		ArchivedAt *time.Time `json:"archived_at"`
		ExpiresAt  *time.Time `json:"expires_at"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != "archived" {
		t.Fatalf("status %q, want archived", resp.Status)
	}
	if resp.ArchivedAt == nil || resp.ExpiresAt == nil {
		t.Fatalf("archived job missing retention timestamps: %+v", resp)
	}
}

// TestAdminStatusReportsHealth verifies the admin status endpoint exposes
// pipeline and store health counters.
func TestAdminStatusReportsHealth(t *testing.T) {
	st, srv := newTestServer(t)

	body := `{"tenant":"tenant-a","seq":1,"type":"started","client_time":"2026-08-15T00:00:00Z"}`
	if rr := doRequest(srv, http.MethodPost, "/api/v1/jobs/job-5/events", body); rr.Code != http.StatusAccepted {
		t.Fatalf("post: want 202, got %d", rr.Code)
	}

	rr := doRequest(srv, http.MethodGet, "/admin/status", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d", rr.Code)
	}
	var resp struct {
		Workers   int    `json:"workers"`
		Persisted uint64 `json:"persisted"`
		Active    int    `json:"active_jobs"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Workers < 1 {
		t.Fatalf("workers %d, want >= 1", resp.Workers)
	}
	if resp.Persisted != 1 {
		t.Fatalf("persisted %d, want 1", resp.Persisted)
	}
	if resp.Active != 1 {
		t.Fatalf("active %d, want 1", resp.Active)
	}
	if got := st.Stats().Persisted; got != resp.Persisted {
		t.Fatalf("status persisted %d != store persisted %d", resp.Persisted, got)
	}
}
