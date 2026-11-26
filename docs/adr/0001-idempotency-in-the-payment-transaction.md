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
- A retry of a slow request holds a database connection while it waits, so the wait is bounded. Before the claim the transaction sets a local `lock_timeout` (`PAYSIM_IDEMPOTENCY_KEY_WAIT`, default 5s); if the first request has not finished by then, Postgres cancels the claim with `55P03`, the transaction rolls back and the client gets `409 idempotency_key_in_progress`. Nothing is written, so retrying later with the same key is safe. `lock_timeout` is used rather than `statement_timeout` because it limits only time spent waiting for a lock, not a slow but progressing statement. It is reset right after a successful claim: contention on an account's row lock is ordinary load, not a duplicate request, and is waited for as before.
- Keys expire after 24 hours (`IdempotencyKeyTTL`), a common window in public payment APIs. The claim is an `INSERT … ON CONFLICT (key) DO UPDATE … WHERE created_at < now() - TTL`: an expired key is taken over by the new request under the same row lock, so expiry needs no separate cleanup step to be correct and concurrent requests on an expired key still produce one payment. A background pruner deletes expired keys in batches only to keep the table bounded; payments are never deleted. A retry after the window is processed as a new payment, so clients must not retry a request more than 24 hours later and expect a replay.
