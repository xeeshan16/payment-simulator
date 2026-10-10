// Package outbox publishes events written to the outbox table.
//
// Delivery is at-least-once: if the process dies after Kafka acknowledges a
// batch but before the transaction marking it published commits, the batch is
// sent again. Consumers deduplicate on event_id.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Message is one record to publish.
type Message struct {
	Topic   string
	Key     string
	Value   []byte
	EventID string
}

// Publisher sends a batch and returns only once every message is durably
// acknowledged, or returns an error.
type Publisher interface {
	Publish(ctx context.Context, msgs []Message) error
}

// Relay moves unpublished outbox rows to a Publisher.
type Relay struct {
	pool      *pgxpool.Pool
	pub       Publisher
	batchSize int
	interval  time.Duration
	log       *slog.Logger
}

// NewRelay returns a Relay. Several relays (one per instance) can run against
// the same table: rows are claimed with FOR UPDATE SKIP LOCKED, so each batch
// is owned by exactly one relay at a time.
func NewRelay(pool *pgxpool.Pool, pub Publisher, batchSize int, interval time.Duration, log *slog.Logger) *Relay {
	if batchSize <= 0 {
		batchSize = 100
	}
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	return &Relay{pool: pool, pub: pub, batchSize: batchSize, interval: interval, log: log}
}

// RunOnce publishes at most one batch and returns how many rows it published.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	published := 0
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT id, event_id, topic, msg_key, payload::text FROM outbox_events
			 WHERE published_at IS NULL ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, r.batchSize)
		if err != nil {
			return fmt.Errorf("claim batch: %w", err)
		}
		var ids []int64
		var msgs []Message
		for rows.Next() {
			var rowID int64
			var m Message
			var payload string
			if err := rows.Scan(&rowID, &m.EventID, &m.Topic, &m.Key, &payload); err != nil {
				rows.Close()
				return err
			}
			m.Value = []byte(payload)
			ids = append(ids, rowID)
			msgs = append(msgs, m)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(msgs) == 0 {
			return nil
		}
		if err := r.pub.Publish(ctx, msgs); err != nil {
			return fmt.Errorf("publish %d events: %w", len(msgs), err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE outbox_events SET published_at = now() WHERE id = ANY($1)`, ids,
		); err != nil {
			return fmt.Errorf("mark published: %w", err)
		}
		published = len(msgs)
		return nil
	})
	return published, err
}

// Run publishes continuously until ctx is cancelled. It drains full batches
// back to back and sleeps only when the table is caught up or a publish fails.
func (r *Relay) Run(ctx context.Context) error {
	for {
		n, err := r.RunOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			r.log.Warn("outbox relay batch failed; will retry", "err", err)
		}
		if n == r.batchSize && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(r.interval):
		}
	}
}
