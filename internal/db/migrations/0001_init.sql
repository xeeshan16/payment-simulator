-- Accounts hold a cached balance; the ledger is the source of truth and
-- the invariant tests check the two always agree.
CREATE TABLE accounts (
    id         UUID PRIMARY KEY,
    owner      TEXT        NOT NULL,
    currency   CHAR(3)     NOT NULL,
    kind       TEXT        NOT NULL DEFAULT 'customer' CHECK (kind IN ('customer', 'funding')),
    balance    BIGINT      NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Only the system funding account may go negative: it is the other side
    -- of every opening balance, so the whole ledger always sums to zero.
    CONSTRAINT customer_balance_non_negative CHECK (kind = 'funding' OR balance >= 0)
);

-- One funding account per currency.
CREATE UNIQUE INDEX accounts_one_funding_per_currency ON accounts (currency) WHERE kind = 'funding';

CREATE TABLE payments (
    id             UUID PRIMARY KEY,
    from_account   UUID        NOT NULL REFERENCES accounts (id),
    to_account     UUID        NOT NULL REFERENCES accounts (id),
    amount         BIGINT      NOT NULL CHECK (amount > 0),
    currency       CHAR(3)     NOT NULL,
    reference      TEXT        NOT NULL DEFAULT '',
    status         TEXT        NOT NULL CHECK (status IN ('completed', 'rejected')),
    failure_reason TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (from_account <> to_account)
);

-- Double-entry journal. Every journal (a payment, or an account's opening
-- balance) is a set of entries that sums to zero.
CREATE TABLE ledger_entries (
    id         BIGSERIAL PRIMARY KEY,
    journal_id UUID        NOT NULL,
    account_id UUID        NOT NULL REFERENCES accounts (id),
    amount     BIGINT      NOT NULL CHECK (amount <> 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ledger_entries_account ON ledger_entries (account_id);
CREATE INDEX ledger_entries_journal ON ledger_entries (journal_id);

-- A key is bound to the hash of the request that first used it and to the
-- response that request produced, so a retry gets the original answer.
CREATE TABLE idempotency_keys (
    key           TEXT PRIMARY KEY,
    request_hash  TEXT        NOT NULL,
    payment_id    UUID REFERENCES payments (id),
    response_body JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional outbox: written in the same transaction as the payment,
-- published to Kafka afterwards by the relay.
CREATE TABLE outbox_events (
    id           BIGSERIAL PRIMARY KEY,
    event_id     UUID        NOT NULL UNIQUE,
    topic        TEXT        NOT NULL,
    msg_key      TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_events_unpublished ON outbox_events (id) WHERE published_at IS NULL;

-- Consumer-side deduplication: Kafka delivery is at-least-once.
CREATE TABLE processed_events (
    consumer     TEXT        NOT NULL,
    event_id     UUID        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

-- Side effect of the sample consumer: one notification per account touched
-- by a completed payment.
CREATE TABLE notifications (
    id         BIGSERIAL PRIMARY KEY,
    event_id   UUID        NOT NULL,
    payment_id UUID        NOT NULL,
    account_id UUID        NOT NULL,
    message    TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
