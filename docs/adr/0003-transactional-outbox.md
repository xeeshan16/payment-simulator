# 3. Publish events through a transactional outbox, consume idempotently

**Status:** accepted

## Context

Every payment outcome must produce exactly one effective event for downstream consumers. Writing to Postgres and Kafka directly from the request ("dual write") fails in both directions: commit then crash loses the event; publish then rollback announces a payment that doesn't exist.

## Decision

- The payment transaction inserts the event into `outbox_events`. The event exists if and only if the payment does.
- A relay claims unpublished rows with `FOR UPDATE SKIP LOCKED`, produces them synchronously (`acks=all`, idempotent producer), then marks them published in the same transaction that holds the claim. If producing fails the transaction rolls back and the rows are retried.
- Records are keyed by payment id, so all events for one payment land on one partition in order. Each record carries an `event-id` header.
- Consumers insert `(consumer, event_id)` into `processed_events` in the same transaction as their side effect. A redelivered event finds the row and does nothing.
- A record that can't be decoded, or still fails after a bounded number of attempts, is written to `payments.events.dlq` with the error and its original topic, partition and offset. Its offset is then committed so one bad record can't stall the partition.

## Consequences

- Delivery is at-least-once: a crash after Kafka acknowledges a batch but before the "published" update commits sends that batch again. The consumer deduplication makes the *effect* exactly-once. This is why deduplication lives on the consumer, not the producer.
- Several relays can run (one per instance) without double-publishing in the normal path, since a claimed row is invisible to other relays.
- Ordering is only guaranteed per payment. Two relays can publish events for *different* payments in either order, which is fine because consumers never depend on cross-payment order.
- Polling adds up to one poll interval (200 ms) of latency. Change data capture (Debezium reading the WAL) would remove polling but adds a component to operate; worth it at higher volume.
- The outbox grows forever in this slice. Pruning published rows is on the roadmap.
- Infrastructure errors in a consumer (database down) are retried and then dead-lettered like any other failure. A production consumer would pause the partition on infrastructure errors and dead-letter only poison messages.
