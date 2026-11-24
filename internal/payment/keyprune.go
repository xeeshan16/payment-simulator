package payment

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// KeyPruner deletes idempotency keys once they are older than
// IdempotencyKeyTTL. Expiry itself does not depend on it (CreatePayment takes
// over an expired key on its own); it only stops the table growing forever.
// The payments a key was bound to are kept.
type KeyPruner struct {
	pool      *pgxpool.Pool
	batchSize int
	interval  time.Duration
	log       *slog.Logger
}

// NewKeyPruner returns a KeyPruner that deletes at most batchSize keys per
// statement, checking every interval. Bounded batches keep each transaction
// short when a large backlog has expired.
func NewKeyPruner(pool *pgxpool.Pool, batchSize int, interval time.Duration, log *slog.Logger) *KeyPruner {
	if batchSize <= 0 {
		batchSize = 1000
	}
	if interval <= 0 {
		interval = time.Minute
	}
	return &KeyPruner{pool: pool, batchSize: batchSize, interval: interval, log: log}
}

// PruneOnce deletes at most one batch of expired keys and returns how many it
// deleted. The expiry condition is repeated outside the subquery so that a key
// a concurrent request has just taken over (and so made fresh again) is
// re-checked after its row lock is released and left alone.
func (p *KeyPruner) PruneOnce(ctx context.Context) (int, error) {
	tag, err := p.pool.Exec(ctx,
		`DELETE FROM idempotency_keys
		 WHERE created_at < now() - make_interval(secs => $1)
		 AND key IN (
			SELECT key FROM idempotency_keys
			WHERE created_at < now() - make_interval(secs => $1)
			ORDER BY created_at LIMIT $2)`,
		IdempotencyKeyTTL.Seconds(), p.batchSize)
	if err != nil {
		return 0, fmt.Errorf("prune idempotency keys: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// Run prunes until ctx is cancelled. It deletes full batches back to back and
// sleeps only when no expired keys are left or a delete fails.
func (p *KeyPruner) Run(ctx context.Context) error {
	for {
		n, err := p.PruneOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			p.log.Warn("idempotency key prune failed; will retry", "err", err)
		}
		if n > 0 {
			p.log.Debug("pruned expired idempotency keys", "rows", n)
		}
		if n == p.batchSize && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(p.interval):
		}
	}
}
