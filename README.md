# Job Ledger & Retention Governance Service

A self-built (原创 0-1) Go backend service that keeps the job-run ledger for an
AI compute centre. During peak load, many compute-node agents concurrently
report job lifecycle events (start / checkpoint / finish / metrics). The
service must persist those events durably under bursty, high-concurrency
writes, archive jobs once they leave the active window, and support auditable
compliance deletion — all without any external database.

## Business purpose

- **Ingest** (`internal/ingest`): a bounded queue + bounded worker pool accepts
  events with backpressure. When the queue is full, callers are blocked for a
  bounded time and then get `503`; no unbounded growth, no silent drops.
  Duplicate (network-retried) reports are deduplicated idempotently by
  `(jobID, seq)`.
- **Store** (`internal/store`): an append-only WAL + in-memory index + archive
  area + auditable tombstone log, implemented with the standard library only.
  A crash is recovered by replaying the WAL.
- **Retention** (`internal/retention`): background tasks archive jobs past the
  active window (`active -> archived`) and hard-delete archived jobs past the
  retention window, writing a tombstone + audit entry each time.
- **Erasure** (`internal/httpapi` + `internal/store`): compliance deletion by
  jobID or tenant; idempotent, audited, and a late event can never resurrect a
  deleted job.
- **Reconcile** (`internal/reconcile`): replays the WAL on startup to rebuild
  the index, skipping already-indexed duplicates and erased jobs.

## Layout

```
cmd/ledgerd           entrypoint, signal handling, graceful shutdown
internal/config       env-driven configuration with defaults
internal/model        domain types (events, jobs, retention, tombstones)
internal/store        WAL + index + archive + tombstones (persistence)
internal/ingest       bounded pipeline (queue + workers, backpressure)
internal/retention    archive scan, reaper, compliance erasure
internal/reconcile    WAL replay / index rebuild
internal/httpapi      HTTP routes + shutdown orchestration
```

## Build & run

Requires Go 1.26. No external dependencies, no database, no network services.

```sh
go build ./...
go run ./cmd/ledgerd
```

The service listens on a fixed port **54278** (override with `LEDGERD_PORT`,
but the default and the documented convention is always 54278). Data is kept
under `./data` (override with `LEDGERD_DATA_DIR`). All other tunables
(`LEDGERD_QUEUE_SIZE`, `LEDGERD_WORKERS`, `LEDGERD_ACTIVE_WINDOW`, …) have
production-safe defaults in `internal/config`.

## HTTP API

| Method | Path | Description |
| --- | --- | --- |
| POST | `/api/v1/jobs/{jobID}/events` | Report a lifecycle event. `202` = accepted; idempotent retries return `202`, never `500`. `503` on backpressure. |
| GET  | `/api/v1/jobs/{jobID}` | Job status (`active`/`archived`/`erased`) and retention info. |
| POST | `/api/v1/erasure` | Compliance deletion by `{"job_id": ...}` or `{"tenant": ...}`. |
| GET  | `/admin/status` | Queue depth, worker count, persisted/duplicate/erased counters. |

Example event body:

```json
{"tenant":"cluster-a","seq":1,"type":"started","payload":{"node":"n-7"},"client_time":"2026-08-15T00:00:00Z"}
```

## Graceful shutdown

On `SIGINT`/`SIGTERM` the process: stops accepting connections, drains
in-flight requests, drains the ingest queue (every event already accepted is
persisted), waits for background tasks, then closes the WAL.

## Tests

```sh
go mod verify
go build ./...
go test -count=1 ./...
```

The test suite covers the ingest happy path and backpressure, idempotent
deduplication, archive state transitions, erasure idempotency + tombstones,
WAL replay (including a final record without a trailing newline), graceful
shutdown draining, and request-deadline propagation. No test needs a running
database, container, account, or network.
