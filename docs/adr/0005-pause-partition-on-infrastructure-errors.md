# 5. Pause the partition on infrastructure errors; dead-letter only poison records

**Status:** accepted

## Context

The consumer retried a failing record a few times and then dead-lettered it. That is right for a poison record (it can't be decoded, or the handler rejects it every time), but wrong when the record is fine and something it depends on is down: during a Postgres outage every record would be retried briefly and then dead-lettered, emptying the topic into the DLQ and leaving someone to replay it by hand.

## Decision

Classify the error once a record has used its attempts:

- **Infrastructure:** Postgres connection failures and timeouts, network errors, Postgres error classes `08` (connection) and `53` (insufficient resources), `57P01`-`57P03` (server shutting down or not accepting connections), and any handler error wrapping `consumer.ErrUnavailable`. The consumer pauses fetching for that partition only and keeps the failing record and the records fetched behind it. It retries the head of that backlog once per wait, starting at `Backoff` and doubling up to `MaxPause` (30s), while other partitions keep being polled. When the head succeeds, the backlog is drained in order and fetching resumes.
- **Everything else** (decode errors, missing `event_id`, constraint violations, plain handler errors) is a problem with the record and still goes to the DLQ after `MaxAttempts`.

## Consequences

- An outage no longer drains the topic into the DLQ, and order within a partition is kept through it.
- A handler bug that fails every record with an error marked `ErrUnavailable` stalls its partition instead of dead-lettering. That is visible as consumer lag, which is the signal to watch.
- If a paused partition is revoked in a rebalance, its backlog may still be retried here while the new owner reads the same records from the committed offset. Both are safe because the side effect and the `event_id` marker commit together.
- Pausing is per consumer instance and in memory; a restart re-reads from the last committed offset, which is never past the paused record.
