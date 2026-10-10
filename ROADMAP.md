# Roadmap

Small, independently shippable tasks, roughly in priority order. Each should land as one pull request with tests, and leave `go test -race ./...` green.

## Done

- [x] Accounts with opening balances funded by a per-currency system account
- [x] Synchronous transfers with idempotency keys, lock ordering and a double-entry ledger
- [x] Transactional outbox, relay with `SKIP LOCKED`, franz-go publisher
- [x] Idempotent consumer with retries and a dead-letter topic
- [x] Invariant checks after every test; race-detector CI against real Postgres

## Next

### Measure it
- [ ] `cmd/loadgen`: concurrent transfers over HTTP with configurable accounts, workers and hot-account skew; reports throughput and p50/p95/p99 latency
- [ ] Publish a results table in the README with the hardware and settings used (measured numbers only)
- [ ] Prometheus metrics: request latency histogram, payments by status, outbox lag (oldest unpublished event age), consumer retries and DLQ count

### Operability
- [ ] OpenTelemetry tracing, with trace context propagated through the outbox into Kafka headers and continued in the consumer
- [ ] Prune published outbox rows older than a retention window
- [ ] Expire idempotency keys after 24 hours
- [ ] Statement timeout on idempotency-key waits; return `409 Conflict` instead of waiting indefinitely
- [ ] Consumer: pause the partition on infrastructure errors, dead-letter only poison messages
- [ ] DLQ replay tool: re-publish selected dead-lettered records to the main topic

### Payments domain
- [ ] Refunds: full and partial, as new journals referencing the original payment; never mutate the original entries
- [ ] Two-phase payments: authorize (hold funds) then capture or void, with an expiry for stale holds
- [ ] Reconciliation job: recompute balances from the ledger and report drift; simulate a settlement file and match it
- [ ] Pagination: `GET /v1/accounts/{id}/payments` with keyset pagination

### Quality
- [ ] OpenAPI spec for the HTTP API, checked in CI against the handlers
- [ ] Fuzz test for request validation and the idempotency hash
- [ ] Unit tests for the HTTP layer with a fake service (no database), alongside the existing integration tests
