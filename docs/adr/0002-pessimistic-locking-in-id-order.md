# 2. Pessimistic row locks, always taken in account-id order

**Status:** accepted

## Context

A transfer reads two balances and writes both. Under concurrency this must not overdraw an account, lose an update, or deadlock when A→B and B→A run at the same time.

Options considered:

- **`SERIALIZABLE` isolation with retries.** Correct, but under contention on hot accounts most transactions abort and retry, and every caller needs a retry loop.
- **Optimistic concurrency** (a version column, `UPDATE … WHERE version = $v`). Works well when conflicts are rare; hot accounts turn into retry storms.
- **`SELECT … FOR UPDATE`** on both rows at `READ COMMITTED`. Conflicting transfers queue instead of failing. The risk is deadlock when two transactions lock the same pair in opposite orders.

## Decision

Lock both account rows with `SELECT … FOR UPDATE`, always in ascending id order regardless of transfer direction. A `CHECK (balance >= 0)` on customer accounts backs up the application-level funds check.

## Consequences

- Opposite transfers wait for each other instead of deadlocking. This is verified: removing the ordering makes `TestConcurrentTransfersPreserveInvariants` fail with `deadlock detected (40P01)`.
- Throughput on a single hot account is bounded by how fast one transaction holds the lock. The fix for that, if it ever matters, is to shard the hot account into sub-balances, not to change isolation level.
- Opening balances lock the per-currency funding account. That serializes account creation with an opening balance, which is fine for a simulator but would need the same sharding in a high-volume system.
