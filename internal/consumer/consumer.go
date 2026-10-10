// Package consumer processes payment events from Kafka exactly once in
// effect: delivery is at-least-once, and each event's side effects commit in
// the same database transaction that records the event id as processed.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xeeshan16/payment-simulator/internal/id"
	"github.com/xeeshan16/payment-simulator/internal/payment"
)

// Handler applies an event's side effects inside tx. Returning an error rolls
// the transaction back, including the processed-event marker, so the event is
// retried.
type Handler func(ctx context.Context, tx pgx.Tx, ev payment.Event) error

// Config tunes retry and dead-lettering.
type Config struct {
	// Name identifies the consumer in processed_events; two different
	// consumers of the same topic deduplicate independently.
	Name        string
	DLQTopic    string
	MaxAttempts int
	Backoff     time.Duration
}

// Consumer reads records from a franz-go group client created with
// kgo.DisableAutoCommit and commits offsets only after records are handled.
type Consumer struct {
	client *kgo.Client
	pool   *pgxpool.Pool
	handle Handler
	cfg    Config
	log    *slog.Logger
}

// New returns a Consumer.
func New(client *kgo.Client, pool *pgxpool.Pool, handle Handler, cfg Config, log *slog.Logger) *Consumer {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.Backoff <= 0 {
		cfg.Backoff = 200 * time.Millisecond
	}
	return &Consumer{client: client, pool: pool, handle: handle, cfg: cfg, log: log}
}

// Run polls until ctx is cancelled or the client is closed. It returns an
// error only when a record could neither be handled nor dead-lettered; its
// offset is then left uncommitted so the record is redelivered after restart.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.Warn("fetch error", "topic", topic, "partition", partition, "err", err)
		})

		var done []*kgo.Record
		var runErr error
		fetches.EachRecord(func(rec *kgo.Record) {
			if runErr != nil {
				return
			}
			if err := c.Process(ctx, rec); err != nil {
				runErr = err
				return
			}
			done = append(done, rec)
		})
		if len(done) > 0 {
			if err := c.client.CommitRecords(ctx, done...); err != nil && ctx.Err() == nil {
				c.log.Warn("commit offsets failed; records will be redelivered and deduplicated", "err", err)
			}
		}
		if runErr != nil {
			return runErr
		}
	}
}

// Process handles one record: decode, then apply with retries. A record that
// cannot be decoded, or still fails after MaxAttempts, goes to the DLQ.
func (c *Consumer) Process(ctx context.Context, rec *kgo.Record) error {
	var ev payment.Event
	if err := json.Unmarshal(rec.Value, &ev); err != nil {
		return c.deadLetter(ctx, rec, fmt.Errorf("decode event: %w", err))
	}
	if !id.Valid(ev.EventID) {
		return c.deadLetter(ctx, rec, errors.New("event has no valid event_id"))
	}

	for attempt := 1; ; attempt++ {
		err := c.applyOnce(ctx, ev)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt >= c.cfg.MaxAttempts {
			return c.deadLetter(ctx, rec, fmt.Errorf("after %d attempts: %w", attempt, err))
		}
		c.log.Warn("event handling failed; retrying", "event_id", ev.EventID, "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.cfg.Backoff * time.Duration(attempt)):
		}
	}
}

func (c *Consumer) applyOnce(ctx context.Context, ev payment.Event) error {
	return pgx.BeginFunc(ctx, c.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
			c.cfg.Name, ev.EventID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil // duplicate delivery: already applied
		}
		return c.handle(ctx, tx, ev)
	})
}

func (c *Consumer) deadLetter(ctx context.Context, rec *kgo.Record, cause error) error {
	c.log.Error("dead-lettering record", "topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "err", cause)
	dlq := &kgo.Record{
		Topic: c.cfg.DLQTopic,
		Key:   rec.Key,
		Value: rec.Value,
		Headers: append(append([]kgo.RecordHeader(nil), rec.Headers...),
			kgo.RecordHeader{Key: "dlq-error", Value: []byte(cause.Error())},
			kgo.RecordHeader{Key: "dlq-original-topic", Value: []byte(rec.Topic)},
			kgo.RecordHeader{Key: "dlq-original-partition", Value: []byte(strconv.Itoa(int(rec.Partition)))},
			kgo.RecordHeader{Key: "dlq-original-offset", Value: []byte(strconv.FormatInt(rec.Offset, 10))},
		),
	}
	if err := c.client.ProduceSync(ctx, dlq).FirstErr(); err != nil {
		return fmt.Errorf("publish to dead-letter topic: %w (original error: %v)", err, cause)
	}
	return nil
}
