package payment_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xeeshan16/payment-simulator/internal/payment"
	"github.com/xeeshan16/payment-simulator/internal/testutil"
)

// Only keys past the TTL are deleted, in bounded batches, and the payments
// they were bound to stay.
func TestKeyPrunerDeletesOnlyExpiredKeys(t *testing.T) {
	t.Parallel()
	svc, pool := setup(t)
	ctx := context.Background()
	alice := open(t, svc, "alice", "AED", 1_000)
	bob := open(t, svc, "bob", "AED", 0)
	for i := range 5 {
		if _, _, err := svc.CreatePayment(ctx, fmt.Sprintf("k%d", i), payment.CreatePaymentRequest{
			FromAccount: alice.ID, ToAccount: bob.ID, Amount: 10, Currency: "AED",
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"k0", "k1", "k2"} {
		ageKey(t, pool, key, payment.IdempotencyKeyTTL+time.Minute)
	}
	ageKey(t, pool, "k3", payment.IdempotencyKeyTTL-time.Minute)

	p := payment.NewKeyPruner(pool, 2, time.Millisecond, testutil.Logger(t))
	if n, err := p.PruneOnce(ctx); err != nil || n != 2 {
		t.Fatalf("want a batch of 2, got n=%d err=%v", n, err)
	}
	if n, err := p.PruneOnce(ctx); err != nil || n != 1 {
		t.Fatalf("want the last expired key, got n=%d err=%v", n, err)
	}
	if n, err := p.PruneOnce(ctx); err != nil || n != 0 {
		t.Fatalf("pruned unexpired keys: n=%d err=%v", n, err)
	}

	if got := keys(t, pool); fmt.Sprint(got) != "[k3 k4]" {
		t.Errorf("keys left: %v, want [k3 k4]", got)
	}
	var payments int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payments`).Scan(&payments); err != nil {
		t.Fatal(err)
	}
	if payments != 5 {
		t.Errorf("%d payments left, want 5", payments)
	}
	checkInvariants(t, pool)
}

// Run drains a backlog larger than one batch without waiting an interval
// between full batches.
func TestKeyPrunerRunDrainsBacklog(t *testing.T) {
	t.Parallel()
	svc, pool := setup(t)
	ctx := context.Background()
	alice := open(t, svc, "alice", "AED", 1_000)
	bob := open(t, svc, "bob", "AED", 0)
	for i := range 7 {
		key := fmt.Sprintf("k%d", i)
		if _, _, err := svc.CreatePayment(ctx, key, payment.CreatePaymentRequest{
			FromAccount: alice.ID, ToAccount: bob.ID, Amount: 10, Currency: "AED",
		}); err != nil {
			t.Fatal(err)
		}
		ageKey(t, pool, key, payment.IdempotencyKeyTTL+time.Hour)
	}

	// With a one-hour interval, Run can only finish in time by deleting full
	// batches back to back.
	p := payment.NewKeyPruner(pool, 3, time.Hour, testutil.Logger(t))
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- p.Run(runCtx) }()
	deadline := time.Now().Add(10 * time.Second)
	for len(keys(t, pool)) > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d keys left after 10s", len(keys(t, pool)))
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	checkInvariants(t, pool)
}

func keys(t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT key FROM idempotency_keys ORDER BY key`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
