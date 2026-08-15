// Package ingest implements the bounded ingestion pipeline: a bounded queue
// and a bounded worker pool that deduplicate, persist, and index lifecycle
// events under backpressure.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"example.com/ledgerd/internal/model"
	"example.com/ledgerd/internal/store"
)

// Sentinel errors returned by the pipeline.
var (
	ErrQueueFull    = errors.New("ingest: queue full")
	ErrShuttingDown = errors.New("ingest: shutting down")
)

// Config configures the pipeline's bounds.
type Config struct {
	QueueSize           int
	Workers             int
	EnqueueTimeout      time.Duration
	EnqueuePollInterval time.Duration
}

func (c Config) withDefaults() Config {
	if c.QueueSize <= 0 {
		c.QueueSize = 1024
	}
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.EnqueueTimeout <= 0 {
		c.EnqueueTimeout = 100 * time.Millisecond
	}
	if c.EnqueuePollInterval <= 0 {
		c.EnqueuePollInterval = time.Millisecond
	}
	return c
}

type workItem struct {
	ctx context.Context
	ev  model.Event
	ack chan error
}

// Pipeline is a bounded ingestion pipeline: a bounded queue feeding a bounded
// worker pool. Workers deduplicate via the store, persist to the WAL, and
// report the outcome back to the caller.
type Pipeline struct {
	cfg   Config
	store store.Store

	queue  chan workItem
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu        sync.Mutex
	closed    bool
	accepted  uint64
	processed uint64
	failed    uint64

	shutdownOnce sync.Once
}

// New starts cfg.Workers workers draining the bounded queue.
func New(cfg Config, st store.Store) *Pipeline {
	cfg = cfg.withDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pipeline{
		cfg:    cfg,
		store:  st,
		queue:  make(chan workItem, cfg.QueueSize),
		ctx:    ctx,
		cancel: cancel,
	}
	for i := 0; i < cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

// Ingest accepts an event for durable persistence, applying bounded
// backpressure. It returns once the event has been enqueued and processed by a
// worker; the returned error carries any store error (including ErrDuplicate)
// wrapped with %w so callers can classify it with errors.Is.
func (p *Pipeline) Ingest(ctx context.Context, ev model.Event) error {
	item := workItem{ctx: ctx, ev: ev, ack: make(chan error, 1)}
	if err := p.enqueue(ctx, item); err != nil {
		return err
	}
	select {
	case err := <-item.ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// enqueue places item into the bounded queue, applying backpressure and
// observing shutdown. The closed flag and the channel send are both guarded by
// p.mu so a send never races with the queue being closed by Shutdown.
func (p *Pipeline) enqueue(ctx context.Context, item workItem) error {
	deadline := time.Now().Add(p.cfg.EnqueueTimeout)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return ErrShuttingDown
		}
		select {
		case p.queue <- item:
			p.accepted++
			p.mu.Unlock()
			return nil
		default:
			p.mu.Unlock()
		}
		if time.Now().After(deadline) {
			return ErrQueueFull
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(p.cfg.EnqueuePollInterval):
		}
	}
}

func (p *Pipeline) worker() {
	defer p.wg.Done()
	for {
		select {
		case item, ok := <-p.queue:
			if !ok {
				return
			}
			p.process(item)
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *Pipeline) process(item workItem) {
	// The worker forwards the request-scoped context so a client deadline is
	// honoured all the way down to the store write.
	err := p.store.Append(item.ctx, item.ev)
	if err != nil {
		err = fmt.Errorf("ingest %s#%d: %w", item.ev.JobID, item.ev.Seq, err)
		p.mu.Lock()
		p.failed++
		p.mu.Unlock()
	} else {
		p.mu.Lock()
		p.processed++
		p.mu.Unlock()
	}
	item.ack <- err
}

// Shutdown stops accepting new events, drains the queue, waits for workers to
// exit, and finally cancels the pipeline context. It is idempotent.
func (p *Pipeline) Shutdown() error {
	p.shutdownOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		close(p.queue)
		p.mu.Unlock()
		p.wg.Wait()
		p.cancel()
	})
	return nil
}

// Stats reports pipeline health for the admin endpoint.
func (p *Pipeline) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{
		QueueDepth: len(p.queue),
		Workers:    p.cfg.Workers,
		Accepted:   p.accepted,
		Processed:  p.processed,
		Failed:     p.failed,
	}
}

// Accepted reports the number of events accepted into the queue.
func (p *Pipeline) Accepted() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.accepted
}

// Stats is the pipeline health snapshot.
type Stats struct {
	QueueDepth int
	Workers    int
	Accepted   uint64
	Processed  uint64
	Failed     uint64
}
