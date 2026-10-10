# 4. Insufficient funds is a recorded rejection, not an error

**Status:** accepted

## Context

When the payer can't cover a transfer, the API could return an error (rolling everything back) or record a payment with status `rejected`.

## Decision

Record it: the payment row is written with `status = rejected` and `failure_reason = insufficient_funds`, no ledger entries are created, a `payment.rejected` event goes through the outbox, and the response is bound to the idempotency key.

## Consequences

- A retry of a declined payment gets the same decline, even if the account has been topped up since. The client must use a new key to try again, which matches what a payer expects: that attempt was declined, and a new attempt is a new payment.
- Declines are visible to downstream systems (fraud signals, customer notifications) through the same event stream as successes.
- Request errors that are the *caller's* fault (malformed input, unknown account, wrong currency) remain errors and do not consume the key (see ADR 1).
