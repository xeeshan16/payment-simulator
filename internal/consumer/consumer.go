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
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xeeshan16/payment-simulator/internal/dlq"
	"github.com/xeeshan16/payment-simulator/internal/id"
	"github.com/xeeshan16/payment-simulator/internal/payment"
)

// Handler applies an event's side effects inside tx. Returning an error rolls
// the transaction back, including the processed-event marker, so the event is
// retried. Wrap ErrUnavailable into the error when a dependency the handler
// calls is down, so the record is held back instead of dead-lettered.
type Handler func(ctx context.Context, tx pgx.Tx, ev payment.Event) error

// ErrUnavailable marks an infrastructure failure: the record is fine, but
// something it depends on is not. Handlers wrap it into their errors (for
// example fmt.Errorf("send email: %w", consumer.ErrUnavailable)); database
// connection failures are recognised without it. Process wraps it into the
// error it returns when a record should be retried later rather than
// dead-lettered.
var ErrUnavailable = errors.New("dependency unavailable")

// Config tunes retry, pausing and dead-lettering.
type Config struct {
	// Name identifies the consumer in processed_events; two different
	// consumers of the same topic deduplicate independently.
	Name     string
	DLQTopic string
	// MaxAttempts is how many times a record is tried before it is
	// dead-lettered or, on an infrastructure error, its partition paused.
	MaxAttempts int
	Backoff     time.Duration
	// MaxPause caps the wait between retries of a paused partition. The wait
	// starts at Backoff and doubles on each failed retry.
	MaxPause time.Duration
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
	if cfg.MaxPause <= 0 {
		cfg.MaxPause = 30 * time.Second
	}
	return &Consumer{client: client, pool: pool, handle: handle, cfg: cfg, log: log}
}

// partitionKey identifies one partition of one topic.
type partitionKey struct {
	topic     string
	partition int32
}

// pausedPartition holds the records of a partition that stopped on an
// infrastructure error: the failing record first, then the records fetched
// behind it. Fetching is paused, so nothing newer arrives until the backlog
// has been drained.
type pausedPartition struct {
	backlog []*kgo.Record
	wait    time.Duration
	retryAt time.Time
}

// Run polls until ctx is cancelled or the client is closed. When a record
// keeps failing on an infrastructure error, its partition is paused and the
// record retried with growing waits, while other partitions keep flowing;
// order within the partition is preserved. Run returns an error only when a
// record could neither be handled nor dead-lettered; its offset is then left
// uncommitted so the record is redelivered after restart.
func (c *Consumer) Run(ctx context.Context) error {
	paused := make(map[partitionKey]*pausedPartition)
	for {
		var done []*kgo.Record
		var runErr error

		// process handles recs in order and returns the records from the first
		// one that hit an infrastructure error onwards, or nil. The first
		// record gets firstAttempts tries, the rest MaxAttempts each.
		process := func(recs []*kgo.Record, firstAttempts int) []*kgo.Record {
			for i, rec := range recs {
				if runErr != nil {
					return nil
				}
				attempts := c.cfg.MaxAttempts
				if i == 0 {
					attempts = firstAttempts
				}
				err := c.process(ctx, rec, attempts)
				if ctx.Err() != nil {
					return nil // shutting down; the record stays uncommitted
				}
				if errors.Is(err, ErrUnavailable) {
					return recs[i:]
				}
				if err != nil {
					runErr = err
					return nil
				}
				done = append(done, rec)
			}
			return nil
		}

		now := time.Now()
		for key, p := range paused {
			if now.Before(p.retryAt) {
				continue
			}
			// The head of the backlog already used its attempts when the
			// partition was paused, so each retry is a single attempt: a
			// paused partition must not hold up the others for long.
			if p.backlog = process(p.backlog, 1); len(p.backlog) > 0 {
				p.wait = min(2*p.wait, c.cfg.MaxPause)
				p.retryAt = time.Now().Add(p.wait)
				c.log.Warn("partition still unavailable; staying paused", "topic", key.topic, "partition", key.partition, "retry_in", p.wait)
				continue
			}
			delete(paused, key)
			c.client.ResumeFetchPartitions(map[string][]int32{key.topic: {key.partition}})
			c.log.Info("partition resumed", "topic", key.topic, "partition", key.partition)
		}

		if runErr == nil {
			fetches := c.poll(ctx, paused)
			if fetches.IsClientClosed() || ctx.Err() != nil {
				return nil
			}
			fetches.EachError(func(topic string, partition int32, err error) {
				if errors.Is(err, context.DeadlineExceeded) {
					return // poll woke up to retry a paused partition
				}
				c.log.Warn("fetch error", "topic", topic, "partition", partition, "err", err)
			})
			fetches.EachPartition(func(fp kgo.FetchTopicPartition) {
				if len(fp.Records) == 0 {
					return
				}
				key := partitionKey{fp.Topic, fp.Partition}
				if p, ok := paused[key]; ok {
					// Fetched before the pause took effect; keep it behind
					// the backlog so the partition stays in order.
					p.backlog = append(p.backlog, fp.Records...)
					return
				}
				rest := process(fp.Records, c.cfg.MaxAttempts)
				if len(rest) == 0 {
					return
				}
				c.client.PauseFetchPartitions(map[string][]int32{fp.Topic: {fp.Partition}})
				paused[key] = &pausedPartition{backlog: rest, wait: c.cfg.Backoff, retryAt: time.Now().Add(c.cfg.Backoff)}
				c.log.Warn("partition paused on infrastructure error", "topic", fp.Topic, "partition", fp.Partition, "offset", rest[0].Offset)
			})
		}

		if len(done) > 0 {
			if err := c.client.CommitRecords(ctx, done...); err != nil && ctx.Err() == nil {
				c.log.Warn("commit offsets failed; records will be redelivered and deduplicated", "err", err)
			}
		}
		if runErr != nil {
			return runErr
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

// poll fetches records, waking up when the earliest paused partition is due
// for a retry so a quiet topic cannot keep it waiting.
func (c *Consumer) poll(ctx context.Context, paused map[partitionKey]*pausedPartition) kgo.Fetches {
	if len(paused) == 0 {
		return c.client.PollFetches(ctx)
	}
	var next time.Time
	for _, p := range paused {
		if next.IsZero() || p.retryAt.Before(next) {
			next = p.retryAt
		}
	}
	pollCtx, cancel := context.WithDeadline(ctx, next)
	defer cancel()
	return c.client.PollFetches(pollCtx)
}

// Process handles one record: decode, then apply with retries. A record that
// cannot be decoded, or still fails after MaxAttempts, goes to the DLQ, unless
// the failure is an infrastructure error: then the returned error wraps
// ErrUnavailable and the record should be retried later, not dead-lettered.
func (c *Consumer) Process(ctx context.Context, rec *kgo.Record) error {
	return c.process(ctx, rec, c.cfg.MaxAttempts)
}

func (c *Consumer) process(ctx context.Context, rec *kgo.Record, maxAttempts int) error {
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
		if attempt >= maxAttempts {
			if isInfrastructure(err) {
				return fmt.Errorf("%w: event %s after %d attempts: %w", ErrUnavailable, ev.EventID, attempt, err)
			}
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

// isInfrastructure reports whether err means a dependency is down rather than
// that the record is bad: handler errors marked with ErrUnavailable, failures
// to reach Postgres, and Postgres errors about the connection or the server's
// resources. Anything else, including constraint violations, is treated as a
// problem with the record.
func isInfrastructure(err error) bool {
	if errors.Is(err, ErrUnavailable) || pgconn.SafeToRetry(err) || pgconn.Timeout(err) {
		return true
	}
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// 08: connection exception, 53: insufficient resources,
		// 57P01-57P03: server shutting down or not accepting connections.
		return strings.HasPrefix(pgErr.Code, "08") || strings.HasPrefix(pgErr.Code, "53") ||
			pgErr.Code == "57P01" || pgErr.Code == "57P02" || pgErr.Code == "57P03"
	}
	var netErr net.Error
	return errors.As(err, &netErr)
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
	dead := &kgo.Record{
		Topic: c.cfg.DLQTopic,
		Key:   rec.Key,
		Value: rec.Value,
		Headers: append(append([]kgo.RecordHeader(nil), rec.Headers...),
			kgo.RecordHeader{Key: dlq.HeaderError, Value: []byte(cause.Error())},
			kgo.RecordHeader{Key: dlq.HeaderOriginalTopic, Value: []byte(rec.Topic)},
			kgo.RecordHeader{Key: dlq.HeaderOriginalPartition, Value: []byte(strconv.Itoa(int(rec.Partition)))},
			kgo.RecordHeader{Key: dlq.HeaderOriginalOffset, Value: []byte(strconv.FormatInt(rec.Offset, 10))},
		),
	}
	if err := c.client.ProduceSync(ctx, dead).FirstErr(); err != nil {
		return fmt.Errorf("publish to dead-letter topic: %w (original error: %v)", err, cause)
	}
	return nil
}
