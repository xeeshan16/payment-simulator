package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pruner deletes published outbox rows once they are older than a retention
// window. Unpublished rows are never deleted, however old: they are events
// the relay still owes to Kafka.
//
// A published row is no longer needed for delivery (the relay only reads
// unpublished rows, and consumers deduplicate on processed_events), so the
// retention window only decides how long events stay queryable in Postgres
// for debugging.
type Pruner struct {
	pool      *pgxpool.Pool
	retention time.Duration
	batchSize int
	interval  time.Duration
	log       *slog.Logger
}

// NewPruner returns a Pruner that removes rows published more than retention
// ago, at most batchSize rows per statement, checking every interval.
// Deleting in bounded batches keeps each transaction short, so pruning a large
// backlog never holds locks or bloats WAL in one go.
func NewPruner(pool *pgxpool.Pool, retention time.Duration, batchSize int, interval time.Duration, log *slog.Logger) *Pruner {
	if batchSize <= 0 {
		batchSize = 1000
	}
	if interval <= 0 {
		interval = time.Minute
	}
	return &Pruner{pool: pool, retention: retention, batchSize: batchSize, interval: interval, log: log}
}

// PruneOnce deletes at most one batch and returns how many rows it deleted.
// The cutoff uses the database clock, the same clock that set published_at.
func (p *Pruner) PruneOnce(ctx context.Context) (int, error) {
	tag, err := p.pool.Exec(ctx,
		`DELETE FROM outbox_events WHERE id IN (
			SELECT id FROM outbox_events
			WHERE published_at < now() - make_interval(secs => $1)
			ORDER BY published_at LIMIT $2)`,
		p.retention.Seconds(), p.batchSize)
	if err != nil {
		return 0, fmt.Errorf("prune outbox: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// Run prunes until ctx is cancelled. It deletes full batches back to back and
// sleeps only when nothing is left past the retention window or a delete
// fails.
func (p *Pruner) Run(ctx context.Context) error {
	for {
		n, err := p.PruneOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			p.log.Warn("outbox prune failed; will retry", "err", err)
		}
		if n > 0 {
			p.log.Debug("pruned published outbox events", "rows", n)
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
