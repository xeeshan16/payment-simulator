package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xeeshan16/payment-simulator/internal/kafka"
	"github.com/xeeshan16/payment-simulator/internal/outbox"
	"github.com/xeeshan16/payment-simulator/internal/payment"
	"github.com/xeeshan16/payment-simulator/internal/testutil"
)

const topic = "payments.events"

// Four relays drain the same outbox concurrently. SKIP LOCKED must hand each
// row to exactly one of them, so every event reaches Kafka exactly once.
func TestConcurrentRelaysPublishEachEventOnce(t *testing.T) {
	t.Parallel()
	pool := testutil.NewDB(t)
	ctx := context.Background()
	svc := payment.NewService(pool, topic)

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, topic))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	alice, _ := svc.OpenAccount(ctx, payment.OpenAccountRequest{Owner: "alice", Currency: "AED", OpeningBalance: 1_000})
	bob, _ := svc.OpenAccount(ctx, payment.OpenAccountRequest{Owner: "bob", Currency: "AED"})
	const payments = 60 // the later ones are rejected for insufficient funds
	for i := range payments {
		if _, _, err := svc.CreatePayment(ctx, fmt.Sprintf("k%d", i), payment.CreatePaymentRequest{
			FromAccount: alice.ID, ToAccount: bob.ID, Amount: 25, Currency: "AED",
		}); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := outbox.NewRelay(pool, kafka.Publisher{Client: producer}, 7, time.Millisecond, testutil.Logger(t))
			for {
				n, err := r.RunOnce(ctx)
				if err != nil {
					t.Errorf("relay: %v", err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	var unpublished int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&unpublished); err != nil {
		t.Fatal(err)
	}
	if unpublished != 0 {
		t.Fatalf("%d events left unpublished", unpublished)
	}

	recs := consumeAll(t, cluster, topic, payments)
	seen := map[string]int{}
	for _, r := range recs {
		var eventID string
		for _, h := range r.Headers {
			if h.Key == kafka.HeaderEventID {
				eventID = string(h.Value)
			}
		}
		if eventID == "" {
			t.Fatalf("record at offset %d has no %s header", r.Offset, kafka.HeaderEventID)
		}
		seen[eventID]++
	}
	if len(recs) != payments || len(seen) != payments {
		t.Fatalf("got %d records with %d distinct event ids, want %d of each", len(recs), len(seen), payments)
	}
}

type failingPublisher struct{}

func (failingPublisher) Publish(context.Context, []outbox.Message) error {
	return errors.New("broker unavailable")
}

type capturingPublisher struct{ got []outbox.Message }

func (c *capturingPublisher) Publish(_ context.Context, msgs []outbox.Message) error {
	c.got = append(c.got, msgs...)
	return nil
}

// A failed publish rolls back the claim, so the same events are published
// once the broker is reachable again.
func TestFailedPublishIsRetried(t *testing.T) {
	t.Parallel()
	pool := testutil.NewDB(t)
	ctx := context.Background()
	svc := payment.NewService(pool, topic)
	alice, _ := svc.OpenAccount(ctx, payment.OpenAccountRequest{Owner: "alice", Currency: "AED", OpeningBalance: 100})
	bob, _ := svc.OpenAccount(ctx, payment.OpenAccountRequest{Owner: "bob", Currency: "AED"})
	for i := range 3 {
		if _, _, err := svc.CreatePayment(ctx, fmt.Sprintf("k%d", i), payment.CreatePaymentRequest{
			FromAccount: alice.ID, ToAccount: bob.ID, Amount: 10, Currency: "AED",
		}); err != nil {
			t.Fatal(err)
		}
	}

	down := outbox.NewRelay(pool, failingPublisher{}, 10, time.Millisecond, testutil.Logger(t))
	if n, err := down.RunOnce(ctx); err == nil || n != 0 {
		t.Fatalf("want error and 0 published, got n=%d err=%v", n, err)
	}

	pub := &capturingPublisher{}
	up := outbox.NewRelay(pool, pub, 10, time.Millisecond, testutil.Logger(t))
	if n, err := up.RunOnce(ctx); err != nil || n != 3 {
		t.Fatalf("want 3 published, got n=%d err=%v", n, err)
	}
	if n, err := up.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("second run republished: n=%d err=%v", n, err)
	}
	if len(pub.got) != 3 || pub.got[0].Topic != topic {
		t.Fatalf("unexpected messages: %+v", pub.got)
	}
}

func consumeAll(t *testing.T, cluster *kfake.Cluster, topic string, want int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var recs []*kgo.Record
	for len(recs) < want {
		f := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		recs = append(recs, f.Records()...)
	}
	// Give any unexpected duplicates a moment to show up.
	extra, cancelExtra := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancelExtra()
	recs = append(recs, cl.PollFetches(extra).Records()...)
	return recs
}
