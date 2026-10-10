package payment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xeeshan16/payment-simulator/internal/id"
)

// MaxIdempotencyKeyLen bounds the Idempotency-Key header.
const MaxIdempotencyKeyLen = 255

// Service implements the payment use cases on top of Postgres.
type Service struct {
	pool  *pgxpool.Pool
	topic string
}

// NewService returns a Service that writes outbox events for topic.
func NewService(pool *pgxpool.Pool, topic string) *Service {
	return &Service{pool: pool, topic: topic}
}

// OpenAccount creates a customer account. A non-zero opening balance is
// journalled against the currency's funding account, so money is never
// created without a matching entry.
func (s *Service) OpenAccount(ctx context.Context, req OpenAccountRequest) (Account, error) {
	if err := req.normalize(); err != nil {
		return Account{}, err
	}
	acc := Account{ID: id.New(), Owner: req.Owner, Currency: req.Currency, Balance: req.OpeningBalance}

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx,
			`INSERT INTO accounts (id, owner, currency, balance) VALUES ($1, $2, $3, $4) RETURNING created_at`,
			acc.ID, acc.Owner, acc.Currency, acc.Balance,
		).Scan(&acc.CreatedAt); err != nil {
			return fmt.Errorf("insert account: %w", err)
		}
		if req.OpeningBalance == 0 {
			return nil
		}
		fundingID, err := lockFundingAccount(ctx, tx, acc.Currency)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE accounts SET balance = balance - $1 WHERE id = $2`, req.OpeningBalance, fundingID,
		); err != nil {
			return fmt.Errorf("debit funding account: %w", err)
		}
		return insertJournal(ctx, tx, id.New(), fundingID, acc.ID, req.OpeningBalance)
	})
	if err != nil {
		return Account{}, err
	}
	return acc, nil
}

// CreatePayment transfers money between two accounts exactly once per
// idempotency key.
//
// The key is claimed with INSERT ... ON CONFLICT DO NOTHING inside the same
// transaction as the transfer. A concurrent request with the same key blocks
// on that insert until the first transaction finishes; if it committed, the
// second request sees the stored response and returns it (replayed=true); if
// it rolled back, the second request claims the key and does the work itself.
// Either way exactly one payment exists per key.
func (s *Service) CreatePayment(ctx context.Context, key string, req CreatePaymentRequest) (p Payment, replayed bool, err error) {
	if key == "" || len(key) > MaxIdempotencyKeyLen {
		return Payment{}, false, invalid("Idempotency-Key header is required (1-%d characters)", MaxIdempotencyKeyLen)
	}
	if err := req.normalize(); err != nil {
		return Payment{}, false, err
	}
	hash := requestHash(req)

	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`INSERT INTO idempotency_keys (key, request_hash) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`,
			key, hash)
		if err != nil {
			return fmt.Errorf("claim idempotency key: %w", err)
		}
		if tag.RowsAffected() == 0 {
			p, err = loadReplay(ctx, tx, key, hash)
			replayed = err == nil
			return err
		}

		p, err = s.transfer(ctx, tx, req)
		if err != nil {
			return err
		}
		body, err := json.Marshal(p)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx,
			`UPDATE idempotency_keys SET payment_id = $2, response_body = $3 WHERE key = $1`,
			key, p.ID, body)
		return err
	})
	if err != nil {
		return Payment{}, false, err
	}
	return p, replayed, nil
}

func loadReplay(ctx context.Context, tx pgx.Tx, key, hash string) (Payment, error) {
	var storedHash string
	var body []byte
	if err := tx.QueryRow(ctx,
		`SELECT request_hash, response_body FROM idempotency_keys WHERE key = $1`, key,
	).Scan(&storedHash, &body); err != nil {
		return Payment{}, fmt.Errorf("load idempotency key: %w", err)
	}
	if storedHash != hash {
		return Payment{}, ErrIdempotencyKeyReused
	}
	if body == nil {
		// The key and its response are written in one transaction, so a
		// committed key without a response means the data was edited by hand.
		return Payment{}, fmt.Errorf("idempotency key %q has no stored response", key)
	}
	var p Payment
	if err := json.Unmarshal(body, &p); err != nil {
		return Payment{}, fmt.Errorf("decode stored response: %w", err)
	}
	return p, nil
}

// transfer locks both accounts in a fixed order (by id) so two opposite
// transfers between the same pair of accounts cannot deadlock, then either
// moves the money or records a rejection. Both outcomes get an outbox event.
func (s *Service) transfer(ctx context.Context, tx pgx.Tx, req CreatePaymentRequest) (Payment, error) {
	type locked struct {
		currency string
		balance  int64
	}
	ids := []string{req.FromAccount, req.ToAccount}
	if ids[1] < ids[0] {
		ids[0], ids[1] = ids[1], ids[0]
	}
	accounts := make(map[string]locked, 2)
	for _, accountID := range ids {
		var a locked
		err := tx.QueryRow(ctx,
			`SELECT currency, balance FROM accounts WHERE id = $1 AND kind = 'customer' FOR UPDATE`, accountID,
		).Scan(&a.currency, &a.balance)
		if errors.Is(err, pgx.ErrNoRows) {
			return Payment{}, fmt.Errorf("%w: account %s", ErrNotFound, accountID)
		}
		if err != nil {
			return Payment{}, fmt.Errorf("lock account: %w", err)
		}
		if a.currency != req.Currency {
			return Payment{}, fmt.Errorf("%w: account %s holds %s", ErrCurrencyMismatch, accountID, a.currency)
		}
		accounts[accountID] = a
	}

	p := Payment{
		ID:          id.New(),
		FromAccount: req.FromAccount,
		ToAccount:   req.ToAccount,
		Amount:      req.Amount,
		Currency:    req.Currency,
		Reference:   req.Reference,
		Status:      StatusCompleted,
	}
	if accounts[req.FromAccount].balance < req.Amount {
		p.Status = StatusRejected
		p.FailureReason = ReasonInsufficientFunds
	} else {
		if _, err := tx.Exec(ctx,
			`UPDATE accounts SET balance = balance - $2 WHERE id = $1`, req.FromAccount, req.Amount,
		); err != nil {
			return Payment{}, fmt.Errorf("debit: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE accounts SET balance = balance + $2 WHERE id = $1`, req.ToAccount, req.Amount,
		); err != nil {
			return Payment{}, fmt.Errorf("credit: %w", err)
		}
	}

	var reason *string
	if p.FailureReason != "" {
		reason = &p.FailureReason
	}
	if err := tx.QueryRow(ctx,
		`INSERT INTO payments (id, from_account, to_account, amount, currency, reference, status, failure_reason)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING created_at`,
		p.ID, p.FromAccount, p.ToAccount, p.Amount, p.Currency, p.Reference, string(p.Status), reason,
	).Scan(&p.CreatedAt); err != nil {
		return Payment{}, fmt.Errorf("insert payment: %w", err)
	}

	if p.Status == StatusCompleted {
		if err := insertJournal(ctx, tx, p.ID, p.FromAccount, p.ToAccount, p.Amount); err != nil {
			return Payment{}, err
		}
	}
	if err := s.enqueue(ctx, tx, p); err != nil {
		return Payment{}, err
	}
	return p, nil
}

func (s *Service) enqueue(ctx context.Context, tx pgx.Tx, p Payment) error {
	ev := Event{EventID: id.New(), Type: EventPaymentCompleted, OccurredAt: p.CreatedAt, Payment: p}
	if p.Status == StatusRejected {
		ev.Type = EventPaymentRejected
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	// Keyed by payment id: every event for one payment lands on the same
	// partition, so consumers see a payment's events in order.
	_, err = tx.Exec(ctx,
		`INSERT INTO outbox_events (event_id, topic, msg_key, payload) VALUES ($1, $2, $3, $4)`,
		ev.EventID, s.topic, p.ID, payload)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

// insertJournal writes a balanced pair of entries: debit `from`, credit `to`.
func insertJournal(ctx context.Context, tx pgx.Tx, journalID, from, to string, amount int64) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO ledger_entries (journal_id, account_id, amount) VALUES ($1, $2, $3), ($1, $4, $5)`,
		journalID, from, -amount, to, amount)
	if err != nil {
		return fmt.Errorf("insert ledger entries: %w", err)
	}
	return nil
}

// lockFundingAccount returns the funding account for currency, creating it
// on first use, and holds its row lock until the transaction ends.
func lockFundingAccount(ctx context.Context, tx pgx.Tx, currency string) (string, error) {
	if _, err := tx.Exec(ctx,
		`INSERT INTO accounts (id, owner, currency, kind) VALUES ($1, 'system', $2, 'funding')
		 ON CONFLICT (currency) WHERE kind = 'funding' DO NOTHING`,
		id.New(), currency,
	); err != nil {
		return "", fmt.Errorf("ensure funding account: %w", err)
	}
	var fundingID string
	if err := tx.QueryRow(ctx,
		`SELECT id FROM accounts WHERE kind = 'funding' AND currency = $1 FOR UPDATE`, currency,
	).Scan(&fundingID); err != nil {
		return "", fmt.Errorf("lock funding account: %w", err)
	}
	return fundingID, nil
}

// GetPayment returns a payment by id.
func (s *Service) GetPayment(ctx context.Context, paymentID string) (Payment, error) {
	if !id.Valid(paymentID) {
		return Payment{}, ErrNotFound
	}
	var p Payment
	var status string
	var reason *string
	err := s.pool.QueryRow(ctx,
		`SELECT id, from_account, to_account, amount, currency, reference, status, failure_reason, created_at
		 FROM payments WHERE id = $1`, paymentID,
	).Scan(&p.ID, &p.FromAccount, &p.ToAccount, &p.Amount, &p.Currency, &p.Reference, &status, &reason, &p.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, ErrNotFound
	}
	if err != nil {
		return Payment{}, err
	}
	p.Status = Status(status)
	if reason != nil {
		p.FailureReason = *reason
	}
	return p, nil
}

// GetAccount returns a customer account by id.
func (s *Service) GetAccount(ctx context.Context, accountID string) (Account, error) {
	if !id.Valid(accountID) {
		return Account{}, ErrNotFound
	}
	var a Account
	err := s.pool.QueryRow(ctx,
		`SELECT id, owner, currency, balance, created_at FROM accounts WHERE id = $1 AND kind = 'customer'`, accountID,
	).Scan(&a.ID, &a.Owner, &a.Currency, &a.Balance, &a.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Account{}, ErrNotFound
	}
	return a, err
}

func requestHash(req CreatePaymentRequest) string {
	b, _ := json.Marshal(req) // fixed field order: deterministic for a normalized request
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
