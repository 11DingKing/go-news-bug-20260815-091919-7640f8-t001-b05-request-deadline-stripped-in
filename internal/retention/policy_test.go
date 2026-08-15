package retention_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/ledgerd/internal/model"
	"example.com/ledgerd/internal/retention"
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

// TestEraseArchivedOrphanJob verifies that a job archived without ever
// receiving a Finished event still carries full retention metadata, so the
// reaper can erase it without a nil-pointer panic.
func TestEraseArchivedOrphanJob(t *testing.T) {
	st := newTestStore(t)
	e := retention.New(retention.Config{
		ActiveWindow:    time.Hour,
		RetentionWindow: time.Millisecond,
		ArchiveInterval: time.Minute,
		ReapInterval:    time.Minute,
	}, st)
	ctx := context.Background()

	// Orphan job: started + checkpoint, but no finished event.
	if err := st.Append(ctx, model.Event{JobID: "orphan-1", Tenant: "t", Seq: 1, Type: model.EventStarted, ClientTime: time.Now()}); err != nil {
		t.Fatalf("append started: %v", err)
	}
	if err := st.Append(ctx, model.Event{JobID: "orphan-1", Tenant: "t", Seq: 2, Type: model.EventCheckpoint, ClientTime: time.Now()}); err != nil {
		t.Fatalf("append checkpoint: %v", err)
	}

	if err := st.Archive("orphan-1", time.Hour, time.Millisecond); err != nil {
		t.Fatalf("archive: %v", err)
	}

	time.Sleep(5 * time.Millisecond)

	n, err := e.Reap(ctx)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if n != 1 {
		t.Fatalf("reap erased %d, want 1", n)
	}
	if _, ok := st.GetJob("orphan-1"); ok {
		t.Fatal("orphan job still present after reap")
	}
	if got := st.Stats().Tombstones; got != 1 {
		t.Fatalf("tombstones %d, want 1", got)
	}
}

// TestArchiveMovesActiveToArchived verifies the active -> archived transition
// attaches retention metadata and that double archiving is rejected.
func TestArchiveMovesActiveToArchived(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	if err := st.Append(ctx, model.Event{JobID: "j", Tenant: "t", Seq: 1, Type: model.EventStarted, ClientTime: time.Now()}); err != nil {
		t.Fatalf("append started: %v", err)
	}
	if err := st.Append(ctx, model.Event{JobID: "j", Tenant: "t", Seq: 2, Type: model.EventFinished, ClientTime: time.Now()}); err != nil {
		t.Fatalf("append finished: %v", err)
	}

	if err := st.Archive("j", time.Hour, 2*time.Hour); err != nil {
		t.Fatalf("archive: %v", err)
	}

	job, ok := st.GetJob("j")
	if !ok {
		t.Fatal("job missing after archive")
	}
	if job.Status != model.StatusArchived {
		t.Fatalf("status %q, want archived", job.Status)
	}
	if job.Retention == nil || job.Retention.ArchivedAt.IsZero() || job.Retention.ExpiresAt.IsZero() {
		t.Fatal("archived job missing retention metadata")
	}

	if err := st.Archive("j", time.Hour, 2*time.Hour); err == nil {
		t.Fatal("second archive should fail (already archived)")
	}
}

// TestEraseIsIdempotentAndWritesTombstone verifies tenant erasure is
// idempotent, writes tombstones, and prevents a late event from resurrecting
// the erased data.
func TestEraseIsIdempotentAndWritesTombstone(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	if err := st.Append(ctx, model.Event{JobID: "j1", Tenant: "t", Seq: 1, Type: model.EventStarted, ClientTime: time.Now()}); err != nil {
		t.Fatalf("append j1: %v", err)
	}
	if err := st.Append(ctx, model.Event{JobID: "j2", Tenant: "t", Seq: 1, Type: model.EventStarted, ClientTime: time.Now()}); err != nil {
		t.Fatalf("append j2: %v", err)
	}

	n, err := st.Erase(model.ErasureRequest{Tenant: "t"})
	if err != nil {
		t.Fatalf("erase: %v", err)
	}
	if n != 2 {
		t.Fatalf("erased %d, want 2", n)
	}

	n2, err := st.Erase(model.ErasureRequest{Tenant: "t"})
	if err != nil {
		t.Fatalf("second erase: %v", err)
	}
	if n2 != 0 {
		t.Fatalf("second erase %d, want 0 (idempotent)", n2)
	}
	if got := st.Stats().Tombstones; got != 2 {
		t.Fatalf("tombstones %d, want 2", got)
	}

	err = st.Append(ctx, model.Event{JobID: "j1", Tenant: "t", Seq: 2, Type: model.EventCheckpoint, ClientTime: time.Now()})
	if !errors.Is(err, store.ErrJobErased) {
		t.Fatalf("late event for erased job: want ErrJobErased, got %v", err)
	}
}
