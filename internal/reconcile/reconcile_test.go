package reconcile_test

import (
	"context"
	"testing"
	"time"

	"example.com/ledgerd/internal/model"
	"example.com/ledgerd/internal/reconcile"
	"example.com/ledgerd/internal/store"
)

func event(seq uint64) model.Event {
	return model.Event{
		JobID:      "job-1",
		Tenant:     "tenant-a",
		Seq:        seq,
		Type:       model.EventStarted,
		ClientTime: time.Now(),
	}
}

// TestReplayRecoversEveryPersistedEvent verifies that WAL replay restores
// every persisted event, whether or not the final record ends with a newline.
func TestReplayRecoversEveryPersistedEvent(t *testing.T) {
	const n = 10

	// Case 1: natural WAL, records separated by newlines with no trailing
	// newline after the final record.
	stA, err := store.New(t.TempDir())
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	for i := uint64(1); i <= n; i++ {
		if err := stA.Append(context.Background(), event(i)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	wal, err := stA.WALData()
	if err != nil {
		t.Fatalf("WALData: %v", err)
	}
	stA.Close()

	stB, err := store.New(t.TempDir())
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	defer stB.Close()
	got, err := reconcile.Replay(stB, wal)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if got != n {
		t.Fatalf("replayed %d, want %d", got, n)
	}
	job, ok := stB.GetJob("job-1")
	if !ok {
		t.Fatal("job-1 missing after replay")
	}
	if job.LastSeq != n {
		t.Fatalf("last seq %d, want %d (final event lost)", job.LastSeq, n)
	}

	// Case 2: a WAL that ends with a trailing newline must also recover fully.
	walWithNewline := append(append([]byte(nil), wal...), '\n')
	stC, err := store.New(t.TempDir())
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	defer stC.Close()
	got2, err := reconcile.Replay(stC, walWithNewline)
	if err != nil {
		t.Fatalf("replay (trailing newline): %v", err)
	}
	if got2 != n {
		t.Fatalf("replayed %d (trailing newline), want %d", got2, n)
	}
}

// TestReplaySkipsDuplicateEvents verifies replay is idempotent: replaying the
// same WAL a second time restores zero new events and does not error.
func TestReplaySkipsDuplicateEvents(t *testing.T) {
	stA, err := store.New(t.TempDir())
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	for i := uint64(1); i <= 5; i++ {
		if err := stA.Append(context.Background(), event(i)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	wal, err := stA.WALData()
	if err != nil {
		t.Fatalf("WALData: %v", err)
	}
	stA.Close()

	stB, err := store.New(t.TempDir())
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	defer stB.Close()

	n1, err := reconcile.Replay(stB, wal)
	if err != nil {
		t.Fatalf("first replay: %v", err)
	}
	if n1 != 5 {
		t.Fatalf("first replay restored %d, want 5", n1)
	}

	n2, err := reconcile.Replay(stB, wal)
	if err != nil {
		t.Fatalf("second replay: %v", err)
	}
	if n2 != 0 {
		t.Fatalf("second replay should restore 0 new events, got %d", n2)
	}
}
