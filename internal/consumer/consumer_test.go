package consumer_test

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
	"github.com/xeeshan16/payment-simulator/internal/id"
	"github.com/xeeshan16/payment-simulator/internal/payment"
	"github.com/xeeshan16/payment-simulator/internal/testutil"
)

const (
	topic = "payments.events"
	dlq   = "payments.events.dlq"
)

type harness struct {
	t       *testing.T
	pool    *pgxpool.Pool
	cluster *kfake.Cluster
	prod    *kgo.Client
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic, dlq))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	prod, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	return &harness{t: t, pool: testutil.NewDB(t), cluster: cluster, prod: prod}
}

func (h *harness) produce(values ...[]byte) {
	h.t.Helper()
	for _, v := range values {
		if err := h.prod.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Value: v}).FirstErr(); err != nil {
			h.t.Fatal(err)
		}
	}
}

// start runs a consumer until the test ends.
func (h *harness) start(handle consumer.Handler) {
	h.t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(h.cluster.ListenAddrs()...),
		kgo.ConsumerGroup("test-"+id.New()),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		h.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := consumer.New(cl, h.pool, handle, consumer.Config{
		Name: "notifications", DLQTopic: dlq, MaxAttempts: 3, Backoff: time.Millisecond,
	}, testutil.Logger(h.t))
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := c.Run(ctx); err != nil {
			h.t.Errorf("consumer stopped: %v", err)
		}
	}()
	h.t.Cleanup(func() { cancel(); cl.Close(); <-done })
}

// waitProcessed blocks until eventID has been recorded as processed.
func (h *harness) waitProcessed(eventID string) {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var ok bool
		if err := h.pool.QueryRow(context.Background(),
			`SELECT EXISTS (SELECT 1 FROM processed_events WHERE event_id = $1)`, eventID).Scan(&ok); err != nil {
			h.t.Fatal(err)
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	h.t.Fatalf("event %s was not processed", eventID)
}

func (h *harness) count(sql string, args ...any) int {
	h.t.Helper()
	var n int
	if err := h.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		h.t.Fatal(err)
	}
	return n
}

func event(t *testing.T, typ string) (payment.Event, []byte) {
	t.Helper()
	ev := payment.Event{
		EventID:    id.New(),
		Type:       typ,
		OccurredAt: time.Now().UTC(),
		Payment: payment.Payment{
			ID: id.New(), FromAccount: id.New(), ToAccount: id.New(),
			Amount: 1_250, Currency: "AED", Status: payment.StatusCompleted,
		},
	}
	if typ == payment.EventPaymentRejected {
		ev.Payment.Status, ev.Payment.FailureReason = payment.StatusRejected, payment.ReasonInsufficientFunds
	}
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	return ev, b
}

// The same event delivered twice (as the relay may do after a crash) must
// produce its side effects once.
func TestDuplicateDeliveryIsAppliedOnce(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	completed, completedJSON := event(t, payment.EventPaymentCompleted)
	_, rejectedJSON := event(t, payment.EventPaymentRejected)
	sentinel, sentinelJSON := event(t, payment.EventPaymentCompleted)

	h.produce(completedJSON, completedJSON, rejectedJSON, sentinelJSON)
	h.start(consumer.Notifications)
	h.waitProcessed(sentinel.EventID) // single partition: everything before it is done

	if n := h.count(`SELECT count(*) FROM notifications WHERE event_id = $1`, completed.EventID); n != 2 {
		t.Errorf("duplicate event produced %d notifications, want 2", n)
	}
	if n := h.count(`SELECT count(*) FROM notifications`); n != 5 {
		t.Errorf("total notifications = %d, want 5 (2 + 1 + 2)", n)
	}
}

// A transient failure is retried and the event is applied once; a record
// that cannot be decoded, or keeps failing, is dead-lettered without blocking
// the records behind it.
func TestRetriesAndDeadLetters(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	flaky, flakyJSON := event(t, payment.EventPaymentCompleted)
	broken, brokenJSON := event(t, payment.EventPaymentCompleted)
	sentinel, sentinelJSON := event(t, payment.EventPaymentCompleted)

	var flakyCalls atomic.Int32
	handle := func(ctx context.Context, tx pgx.Tx, ev payment.Event) error {
		switch ev.EventID {
		case flaky.EventID:
			if flakyCalls.Add(1) < 3 {
				return errors.New("downstream timeout")
			}
		case broken.EventID:
			return errors.New("permanent failure")
		}
		return consumer.Notifications(ctx, tx, ev)
	}

	h.produce([]byte(`{not json`), flakyJSON, brokenJSON, sentinelJSON)
	h.start(handle)
	h.waitProcessed(sentinel.EventID)

	if got := flakyCalls.Load(); got != 3 {
		t.Errorf("flaky handler called %d times, want 3", got)
	}
	if n := h.count(`SELECT count(*) FROM notifications WHERE event_id = $1`, flaky.EventID); n != 2 {
		t.Errorf("flaky event produced %d notifications, want 2", n)
	}
	if n := h.count(`SELECT count(*) FROM processed_events WHERE event_id = $1`, broken.EventID); n != 0 {
		t.Error("permanently failing event was marked processed")
	}

	dl := readDLQ(t, h.cluster, 2)
	if len(dl) != 2 {
		t.Fatalf("dead-letter topic has %d records, want 2", len(dl))
	}
	if string(dl[0].Value) != `{not json` || string(dl[1].Value) != string(brokenJSON) {
		t.Errorf("unexpected dead-lettered values: %q, %q", dl[0].Value, dl[1].Value)
	}
	for _, r := range dl {
		if header(r, "dlq-error") == "" || header(r, "dlq-original-topic") != topic {
			t.Errorf("dead-letter record missing diagnostic headers: %+v", r.Headers)
		}
	}
}

func readDLQ(t *testing.T, cluster *kfake.Cluster, want int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics(dlq),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var recs []*kgo.Record
	for len(recs) < want && ctx.Err() == nil {
		recs = append(recs, cl.PollFetches(ctx).Records()...)
	}
	return recs
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
