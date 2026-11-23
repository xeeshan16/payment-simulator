package outbox_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xeeshan16/payment-simulator/internal/outbox"
	"github.com/xeeshan16/payment-simulator/internal/payment"
	"github.com/xeeshan16/payment-simulator/internal/testutil"
)

// Only rows published before the retention window are deleted. Recently
// published rows stay, and unpublished rows stay however old they are,
// because the relay still has to deliver them.
func TestPrunerDeletesOnlyPublishedRowsPastRetention(t *testing.T) {
	t.Parallel()
	pool := testutil.NewDB(t)
	ctx := context.Background()
	createPayments(t, pool, 5)

	// Publish the three oldest events, then age two of them past retention.
	relay := outbox.NewRelay(pool, &capturingPublisher{}, 3, time.Millisecond, testutil.Logger(t))
	if n, err := relay.RunOnce(ctx); err != nil || n != 3 {
		t.Fatalf("want 3 published, got n=%d err=%v", n, err)
	}
	exec(t, pool, `UPDATE outbox_events SET published_at = now() - interval '2 hours'
		WHERE id IN (SELECT id FROM outbox_events WHERE published_at IS NOT NULL ORDER BY id LIMIT 2)`)
	// An unpublished row that is old by creation time must survive.
	exec(t, pool, `UPDATE outbox_events SET created_at = now() - interval '30 days' WHERE published_at IS NULL`)

	p := outbox.NewPruner(pool, time.Hour, 100, time.Millisecond, testutil.Logger(t))
	if n, err := p.PruneOnce(ctx); err != nil || n != 2 {
		t.Fatalf("want 2 pruned, got n=%d err=%v", n, err)
	}
	if n, err := p.PruneOnce(ctx); err != nil || n != 0 {
		t.Fatalf("second prune deleted more: n=%d err=%v", n, err)
	}

	var published, unpublished int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE published_at IS NOT NULL),
		count(*) FILTER (WHERE published_at IS NULL) FROM outbox_events`).Scan(&published, &unpublished); err != nil {
		t.Fatal(err)
	}
	if published != 1 || unpublished != 2 {
		t.Fatalf("left %d published and %d unpublished rows, want 1 and 2", published, unpublished)
	}
}

// A backlog larger than one batch is removed in bounded deletes, and Run
// drains it without waiting an interval between full batches.
func TestPrunerDeletesInBatches(t *testing.T) {
	t.Parallel()
	pool := testutil.NewDB(t)
	ctx := context.Background()
	createPayments(t, pool, 7)

	relay := outbox.NewRelay(pool, &capturingPublisher{}, 100, time.Millisecond, testutil.Logger(t))
	if n, err := relay.RunOnce(ctx); err != nil || n != 7 {
		t.Fatalf("want 7 published, got n=%d err=%v", n, err)
	}
	exec(t, pool, `UPDATE outbox_events SET published_at = now() - interval '2 hours'`)

	p := outbox.NewPruner(pool, time.Hour, 3, time.Hour, testutil.Logger(t))
	if n, err := p.PruneOnce(ctx); err != nil || n != 3 {
		t.Fatalf("want a batch of 3, got n=%d err=%v", n, err)
	}

	// With a one-hour interval, Run can only finish the backlog in time by
	// deleting full batches back to back.
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- p.Run(runCtx) }()
	deadline := time.Now().Add(10 * time.Second)
	for countRows(t, pool) > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d rows left after 10s", countRows(t, pool))
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func createPayments(t *testing.T, pool *pgxpool.Pool, n int) {
	t.Helper()
	ctx := context.Background()
	svc := payment.NewService(pool, topic)
	alice, err := svc.OpenAccount(ctx, payment.OpenAccountRequest{Owner: "alice", Currency: "AED", OpeningBalance: 1_000})
	if err != nil {
		t.Fatal(err)
	}
	bob, err := svc.OpenAccount(ctx, payment.OpenAccountRequest{Owner: "bob", Currency: "AED"})
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if _, _, err := svc.CreatePayment(ctx, fmt.Sprintf("k%d", i), payment.CreatePaymentRequest{
			FromAccount: alice.ID, ToAccount: bob.ID, Amount: 10, Currency: "AED",
		}); err != nil {
			t.Fatal(err)
		}
	}
	if got := countRows(t, pool); got != n {
		t.Fatalf("outbox has %d rows, want %d", got, n)
	}
}

func countRows(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func exec(t *testing.T, pool *pgxpool.Pool, sql string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}
