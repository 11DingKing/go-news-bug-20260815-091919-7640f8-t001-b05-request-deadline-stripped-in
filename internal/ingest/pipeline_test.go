package ingest_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"example.com/ledgerd/internal/ingest"
	"example.com/ledgerd/internal/model"
	"example.com/ledgerd/internal/store"
)

func newTestStore(t *testing.T) store.Store {
	t.Helper()
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func testEvent(i int) model.Event {
	return model.Event{
		JobID:      fmt.Sprintf("job-%d", i),
		Tenant:     "tenant-a",
		Seq:        1,
		Type:       model.EventStarted,
		ClientTime: time.Now(),
	}
}

// gatedStore blocks every Append until the gate is closed, letting a test hold
// a worker mid-write while the queue fills up.
type gatedStore struct {
	store.Store
	gate chan struct{}
}

func (g *gatedStore) Append(ctx context.Context, ev model.Event) error {
	<-g.gate
	return g.Store.Append(ctx, ev)
}

func (g *gatedStore) release() { close(g.gate) }

// slowStore delays each Append, honouring the context so a deadline can abort
// the write.
type slowStore struct {
	store.Store
	delay time.Duration
}

func (s *slowStore) Append(ctx context.Context, ev model.Event) error {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.Store.Append(ctx, ev)
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

// TestGracefulShutdownDrainsAllAcceptedEvents verifies that a graceful
// shutdown drains and persists every event already accepted into the queue
// before the workers exit.
func TestGracefulShutdownDrainsAllAcceptedEvents(t *testing.T) {
	base := newTestStore(t)
	gated := &gatedStore{Store: base, gate: make(chan struct{})}
	p := ingest.New(ingest.Config{QueueSize: 64, Workers: 1, EnqueueTimeout: time.Second}, gated)

	const n = 40
	results := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			results <- p.Ingest(context.Background(), testEvent(i))
		}(i)
	}

	waitUntil(t, 5*time.Second, func() bool { return p.Accepted() >= n })

	shutdownDone := make(chan struct{})
	go func() {
		_ = p.Shutdown()
		close(shutdownDone)
	}()

	gated.release()

	select {
	case <-shutdownDone:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not complete in time")
	}

	if got := base.Stats().Persisted; got != n {
		t.Fatalf("persisted %d of %d accepted events", got, n)
	}
	for i := 0; i < n; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("ingest %d returned error: %v", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for ingest %d ack", i)
		}
	}
}

// TestDeadlinePropagatesThroughPipeline verifies that a client deadline is
// honoured all the way through the worker to the store write, aborting a slow
// write instead of letting it run to completion.
func TestDeadlinePropagatesThroughPipeline(t *testing.T) {
	base := newTestStore(t)
	slow := &slowStore{Store: base, delay: 200 * time.Millisecond}
	p := ingest.New(ingest.Config{QueueSize: 8, Workers: 1, EnqueueTimeout: time.Second}, slow)
	defer p.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := p.Ingest(ctx, testEvent(0))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context.DeadlineExceeded, got %v", err)
	}

	// Give a buggy worker time to finish a slow write that should have been
	// aborted; the event must still not be persisted.
	time.Sleep(300 * time.Millisecond)
	if got := base.Stats().Persisted; got != 0 {
		t.Fatalf("deadline exceeded but event was still persisted (persisted=%d)", got)
	}
}

// TestIngestQueueFullReturnsBackpressure verifies bounded backpressure: a full
// queue blocks for a bounded time and then returns ErrQueueFull.
func TestIngestQueueFullReturnsBackpressure(t *testing.T) {
	base := newTestStore(t)
	gated := &gatedStore{Store: base, gate: make(chan struct{})}
	p := ingest.New(ingest.Config{QueueSize: 2, Workers: 1, EnqueueTimeout: 50 * time.Millisecond}, gated)

	go func() { _ = p.Ingest(context.Background(), testEvent(0)) }()
	go func() { _ = p.Ingest(context.Background(), testEvent(1)) }()
	go func() { _ = p.Ingest(context.Background(), testEvent(2)) }()

	waitUntil(t, time.Second, func() bool { return p.Accepted() >= 3 })

	err := p.Ingest(context.Background(), testEvent(3))
	if !errors.Is(err, ingest.ErrQueueFull) {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}

	// Clean up: release the gated worker before draining so shutdown completes.
	gated.release()
	_ = p.Shutdown()
}

// TestIngestDeduplicatesByJobSeq verifies (jobID, seq) idempotency: a repeated
// event is not written again and surfaces ErrDuplicate.
func TestIngestDeduplicatesByJobSeq(t *testing.T) {
	base := newTestStore(t)
	p := ingest.New(ingest.Config{QueueSize: 8, Workers: 2, EnqueueTimeout: time.Second}, base)
	defer p.Shutdown()

	ev := testEvent(0)
	if err := p.Ingest(context.Background(), ev); err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	err := p.Ingest(context.Background(), ev)
	if !errors.Is(err, store.ErrDuplicate) {
		t.Fatalf("retry: want ErrDuplicate (idempotent), got %v", err)
	}
	st := base.Stats()
	if st.Persisted != 1 {
		t.Fatalf("persisted %d, want 1", st.Persisted)
	}
	if st.Duplicates != 1 {
		t.Fatalf("duplicates %d, want 1", st.Duplicates)
	}
}

// TestShutdownIsIdempotent verifies shutdown can be called repeatedly and that
// ingestion is rejected after shutdown.
func TestShutdownIsIdempotent(t *testing.T) {
	base := newTestStore(t)
	p := ingest.New(ingest.Config{QueueSize: 8, Workers: 2, EnqueueTimeout: time.Second}, base)

	if err := p.Ingest(context.Background(), testEvent(0)); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := p.Shutdown(); err != nil {
		t.Fatalf("first shutdown: %v", err)
	}
	if err := p.Shutdown(); err != nil {
		t.Fatalf("second shutdown should be idempotent: %v", err)
	}
	err := p.Ingest(context.Background(), testEvent(1))
	if !errors.Is(err, ingest.ErrShuttingDown) {
		t.Fatalf("want ErrShuttingDown after shutdown, got %v", err)
	}
}
