-- Lets the key pruner find keys past IdempotencyKeyTTL without scanning the
-- table.
CREATE INDEX idempotency_keys_created_at ON idempotency_keys (created_at);
