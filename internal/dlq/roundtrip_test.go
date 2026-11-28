package dlq_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xeeshan16/payment-simulator/internal/consumer"
	"github.com/xeeshan16/payment-simulator/internal/dlq"
	"github.com/xeeshan16/payment-simulator/internal/id"
	"github.com/xeeshan16/payment-simulator/internal/kafka"
	"github.com/xeeshan16/payment-simulator/internal/outbox"
	"github.com/xeeshan16/payment-simulator/internal/payment"
	"github.com/xeeshan16/payment-simulator/internal/testutil"
)

// An event dead-lettered by a handler bug is applied exactly once after the
// bug is fixed and the event replayed, even when it is replayed twice.
func TestDeadLetteredEventIsAppliedOnceAfterReplay(t *testing.T) {
	t.Parallel()
	const topic, dlqTopic = "payments.events", "payments.events.dlq"
	pool := testutil.NewDB(t)
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic, dlqTopic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)

	ev, sentinel := newEvent(t), newEvent(t)
	publish(t, cl, topic, ev)

	var fixed atomic.Bool
	handle := func(ctx context.Context, tx pgx.Tx, ev payment.Event) error {
		if !fixed.Load() {
			return errors.New("template rendering bug")
		}
		return consumer.Notifications(ctx, tx, ev)
	}
	group, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumerGroup("test-"+id.New()),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := consumer.New(group, pool, handle, consumer.Config{
		Name: "notifications", DLQTopic: dlqTopic, MaxAttempts: 2, Backoff: time.Millisecond,
	}, testutil.Logger(t))
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := c.Run(ctx); err != nil {
			t.Errorf("consumer stopped: %v", err)
		}
	}()
	t.Cleanup(func() { cancel(); group.Close(); <-done })

	opts := dlq.Options{DLQTopic: dlqTopic, Topic: topic, EventIDs: []string{ev.EventID}}
	waitFor(t, "event to be dead-lettered", func() bool {
		res, err := dlq.Replay(context.Background(), cl, dlq.Options{DLQTopic: dlqTopic, Topic: topic, All: true, DryRun: true})
		return err == nil && len(res.Matched) == 1
	})

	fixed.Store(true)
	for range 2 {
		res, err := dlq.Replay(context.Background(), cl, opts)
		if err != nil {
			t.Fatal(err)
		}
		if res.Published != 1 {
			t.Fatalf("replay published %d records, want 1", res.Published)
		}
	}

	// One partition: once a later event is processed, both replays have
	// been consumed.
	publish(t, cl, topic, sentinel)
	waitFor(t, "sentinel event to be processed", func() bool {
		return count(t, pool, `SELECT count(*) FROM processed_events WHERE event_id = $1`, sentinel.EventID) == 1
	})
	if n := count(t, pool, `SELECT count(*) FROM processed_events WHERE event_id = $1`, ev.EventID); n != 1 {
		t.Errorf("replayed event processed %d times, want 1", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM notifications WHERE event_id = $1`, ev.EventID); n != 2 {
		t.Errorf("replayed event produced %d notifications, want 2 (one per party, applied once)", n)
	}
}

func newEvent(t *testing.T) payment.Event {
	t.Helper()
	return payment.Event{
		EventID:    id.New(),
		Type:       payment.EventPaymentCompleted,
		OccurredAt: time.Now().UTC(),
		Payment: payment.Payment{
			ID: id.New(), FromAccount: id.New(), ToAccount: id.New(),
			Amount: 1_250, Currency: "AED", Status: payment.StatusCompleted,
		},
	}
}

// publish sends ev the way the outbox relay does, with its event-id header.
func publish(t *testing.T, cl *kgo.Client, topic string, ev payment.Event) {
	t.Helper()
	value, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	msg := outbox.Message{Topic: topic, Key: ev.Payment.FromAccount, EventID: ev.EventID, Value: value}
	if err := (kafka.Publisher{Client: cl}).Publish(context.Background(), []outbox.Message{msg}); err != nil {
		t.Fatal(err)
	}
}

func count(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
