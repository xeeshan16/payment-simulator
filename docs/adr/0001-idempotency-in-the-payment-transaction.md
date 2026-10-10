# 1. Claim the idempotency key inside the payment transaction

**Status:** accepted

## Context

Clients retry `POST /v1/payments` after timeouts and network errors. A retry must never move money twice, and concurrent retries (a client firing the same request from two threads, or a load balancer retrying) must resolve to a single payment.

Common alternatives:

1. **Check, then act** (`SELECT` the key; if absent, process and insert). Two concurrent requests can both see "absent". Unsafe without extra locking.
2. **Separate idempotency store** (e.g. Redis `SET NX` with a TTL, then process in Postgres). Fast, but the key and the payment commit in different systems, so a crash between them leaves a claimed key with no payment or a payment with no key. Needs an "in progress" state, a recovery path and a lock expiry tuned to the slowest request.
3. **Claim the key in the same database transaction as the transfer.**

## Decision

Option 3. The request runs `INSERT INTO idempotency_keys … ON CONFLICT (key) DO NOTHING` first, inside the transfer's transaction, and stores the response on the key before commit.

- If the insert succeeds, this request owns the key and does the work.
- If another in-flight transaction already inserted the key, Postgres makes this insert wait on the unique index until that transaction ends. If it committed, this request reads the stored response; if it rolled back, this insert succeeds and the request does the work itself.

A SHA-256 of the normalized request is stored with the key. Reusing a key with a different request returns `422` rather than silently returning someone else's response.

## Consequences

- No "in progress" state, no lock TTLs and no cross-system recovery: key and payment are atomic.
- Only *decided* outcomes bind a key. Validation errors, unknown accounts and currency mismatches roll the transaction back, so the client can fix the request and retry with the same key. Insufficient funds is a decided outcome (see ADR 4) and is replayed like a success.
- A retry of a slow request holds a database connection while it waits. Acceptable at this scale; a production system would add a statement timeout and return `409` instead of waiting indefinitely.
- Keys never expire. Expiry after a fixed window (24 hours is a common choice in public payment APIs) is on the roadmap.
