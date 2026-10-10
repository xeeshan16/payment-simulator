package payment_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xeeshan16/payment-simulator/internal/payment"
	"github.com/xeeshan16/payment-simulator/internal/testutil"
)

const topic = "payments.events"

func setup(t *testing.T) (*payment.Service, *pgxpool.Pool) {
	t.Helper()
	pool := testutil.NewDB(t)
	return payment.NewService(pool, topic), pool
}

func open(t *testing.T, svc *payment.Service, owner, currency string, balance int64) payment.Account {
	t.Helper()
	acc, err := svc.OpenAccount(context.Background(), payment.OpenAccountRequest{
		Owner: owner, Currency: currency, OpeningBalance: balance,
	})
	if err != nil {
		t.Fatalf("open account %s: %v", owner, err)
	}
	return acc
}

func balance(t *testing.T, svc *payment.Service, accountID string) int64 {
	t.Helper()
	acc, err := svc.GetAccount(context.Background(), accountID)
	if err != nil {
		t.Fatalf("get account: %v", err)
	}
	return acc.Balance
}

func TestTransferMovesMoney(t *testing.T) {
	t.Parallel()
	svc, pool := setup(t)
	ctx := context.Background()
	alice := open(t, svc, "alice", "AED", 10_000)
	bob := open(t, svc, "bob", "AED", 0)

	p, replayed, err := svc.CreatePayment(ctx, "k1", payment.CreatePaymentRequest{
		FromAccount: alice.ID, ToAccount: bob.ID, Amount: 2_500, Currency: "aed", Reference: "rent",
	})
	if err != nil {
		t.Fatalf("create payment: %v", err)
	}
	if replayed {
		t.Fatal("first request reported as replayed")
	}
	if p.Status != payment.StatusCompleted || p.Currency != "AED" {
		t.Fatalf("unexpected payment: %+v", p)
	}
	if got := balance(t, svc, alice.ID); got != 7_500 {
		t.Errorf("alice balance = %d, want 7500", got)
	}
	if got := balance(t, svc, bob.ID); got != 2_500 {
		t.Errorf("bob balance = %d, want 2500", got)
	}

	stored, err := svc.GetPayment(ctx, p.ID)
	if err != nil {
		t.Fatalf("get payment: %v", err)
	}
	if stored.ID != p.ID || stored.Amount != 2_500 || stored.Status != payment.StatusCompleted {
		t.Errorf("stored payment = %+v", stored)
	}
	checkInvariants(t, pool)
}

func TestInsufficientFundsIsRecordedAsRejected(t *testing.T) {
	t.Parallel()
	svc, pool := setup(t)
	alice := open(t, svc, "alice", "AED", 100)
	bob := open(t, svc, "bob", "AED", 0)

	p, _, err := svc.CreatePayment(context.Background(), "k1", payment.CreatePaymentRequest{
		FromAccount: alice.ID, ToAccount: bob.ID, Amount: 101, Currency: "AED",
	})
	if err != nil {
		t.Fatalf("create payment: %v", err)
	}
	if p.Status != payment.StatusRejected || p.FailureReason != payment.ReasonInsufficientFunds {
		t.Fatalf("want rejected/insufficient_funds, got %s/%s", p.Status, p.FailureReason)
	}
	if got := balance(t, svc, alice.ID); got != 100 {
		t.Errorf("alice balance changed to %d", got)
	}
	checkInvariants(t, pool)
}

func TestErrorsDoNotConsumeTheIdempotencyKey(t *testing.T) {
	t.Parallel()
	svc, pool := setup(t)
	ctx := context.Background()
	alice := open(t, svc, "alice", "AED", 1_000)
	euro := open(t, svc, "eve", "EUR", 0)
	bob := open(t, svc, "bob", "AED", 0)

	_, _, err := svc.CreatePayment(ctx, "k1", payment.CreatePaymentRequest{
		FromAccount: alice.ID, ToAccount: euro.ID, Amount: 10, Currency: "AED",
	})
	if !errors.Is(err, payment.ErrCurrencyMismatch) {
		t.Fatalf("want ErrCurrencyMismatch, got %v", err)
	}
	_, _, err = svc.CreatePayment(ctx, "k2", payment.CreatePaymentRequest{
		FromAccount: alice.ID, ToAccount: "00000000-0000-7000-8000-000000000000", Amount: 10, Currency: "AED",
	})
	if !errors.Is(err, payment.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}

	// The failed attempt rolled back with its key, so a corrected request
	// may reuse it.
	p, replayed, err := svc.CreatePayment(ctx, "k1", payment.CreatePaymentRequest{
		FromAccount: alice.ID, ToAccount: bob.ID, Amount: 10, Currency: "AED",
	})
	if err != nil || replayed || p.Status != payment.StatusCompleted {
		t.Fatalf("retry with corrected request: payment=%+v replayed=%v err=%v", p, replayed, err)
	}
	checkInvariants(t, pool)
}

func TestValidation(t *testing.T) {
	t.Parallel()
	svc, _ := setup(t)
	a, b := "0190a1b2-0000-7000-8000-000000000001", "0190a1b2-0000-7000-8000-000000000002"
	cases := []struct {
		name string
		key  string
		req  payment.CreatePaymentRequest
	}{
		{"missing key", "", payment.CreatePaymentRequest{FromAccount: a, ToAccount: b, Amount: 1, Currency: "AED"}},
		{"bad from", "k", payment.CreatePaymentRequest{FromAccount: "x", ToAccount: b, Amount: 1, Currency: "AED"}},
		{"same account", "k", payment.CreatePaymentRequest{FromAccount: a, ToAccount: a, Amount: 1, Currency: "AED"}},
		{"zero amount", "k", payment.CreatePaymentRequest{FromAccount: a, ToAccount: b, Amount: 0, Currency: "AED"}},
		{"negative amount", "k", payment.CreatePaymentRequest{FromAccount: a, ToAccount: b, Amount: -5, Currency: "AED"}},
		{"bad currency", "k", payment.CreatePaymentRequest{FromAccount: a, ToAccount: b, Amount: 1, Currency: "dirham"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := svc.CreatePayment(context.Background(), tc.key, tc.req)
			var ve *payment.ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("want ValidationError, got %v", err)
			}
		})
	}
}

func TestRetryWithSameKeyReplaysOriginalResponse(t *testing.T) {
	t.Parallel()
	svc, pool := setup(t)
	ctx := context.Background()
	alice := open(t, svc, "alice", "AED", 1_000)
	bob := open(t, svc, "bob", "AED", 0)
	req := payment.CreatePaymentRequest{FromAccount: alice.ID, ToAccount: bob.ID, Amount: 300, Currency: "AED"}

	first, _, err := svc.CreatePayment(ctx, "retry-key", req)
	if err != nil {
		t.Fatal(err)
	}
	second, replayed, err := svc.CreatePayment(ctx, "retry-key", req)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed {
		t.Error("second request not reported as replayed")
	}
	if second.ID != first.ID || !second.CreatedAt.Equal(first.CreatedAt) || second.Status != first.Status {
		t.Errorf("replay differs from original:\n first=%+v\nsecond=%+v", first, second)
	}
	if got := balance(t, svc, alice.ID); got != 700 {
		t.Errorf("alice debited twice: balance %d", got)
	}
	checkInvariants(t, pool)
}

func TestReusingKeyForDifferentRequestFails(t *testing.T) {
	t.Parallel()
	svc, _ := setup(t)
	ctx := context.Background()
	alice := open(t, svc, "alice", "AED", 1_000)
	bob := open(t, svc, "bob", "AED", 0)

	if _, _, err := svc.CreatePayment(ctx, "k", payment.CreatePaymentRequest{
		FromAccount: alice.ID, ToAccount: bob.ID, Amount: 100, Currency: "AED",
	}); err != nil {
		t.Fatal(err)
	}
	_, _, err := svc.CreatePayment(ctx, "k", payment.CreatePaymentRequest{
		FromAccount: alice.ID, ToAccount: bob.ID, Amount: 999, Currency: "AED",
	})
	if !errors.Is(err, payment.ErrIdempotencyKeyReused) {
		t.Fatalf("want ErrIdempotencyKeyReused, got %v", err)
	}
}

// Many clients retrying the same request at the same moment must produce one
// payment and identical responses.
func TestConcurrentRequestsWithSameKeyCreateOnePayment(t *testing.T) {
	t.Parallel()
	svc, pool := setup(t)
	alice := open(t, svc, "alice", "AED", 1_000)
	bob := open(t, svc, "bob", "AED", 0)
	req := payment.CreatePaymentRequest{FromAccount: alice.ID, ToAccount: bob.ID, Amount: 400, Currency: "AED"}

	const n = 40
	ids := make([]string, n)
	errs := make([]error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			p, _, err := svc.CreatePayment(context.Background(), "same-key", req)
			ids[i], errs[i] = p.ID, err
		}()
	}
	close(start)
	wg.Wait()

	for i := range n {
		if errs[i] != nil {
			t.Fatalf("request %d: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("request %d got payment %s, request 0 got %s", i, ids[i], ids[0])
		}
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM payments`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("%d payments created, want 1", count)
	}
	if got := balance(t, svc, alice.ID); got != 600 {
		t.Errorf("alice balance %d, want 600", got)
	}
	checkInvariants(t, pool)
}

// Random transfers in both directions across a small set of accounts, from
// many goroutines at once. Opposite transfers between the same pair would
// deadlock without consistent lock ordering; overdrafts or lost updates
// would break the invariants.
func TestConcurrentTransfersPreserveInvariants(t *testing.T) {
	t.Parallel()
	svc, pool := setup(t)
	const accounts, workers, perWorker = 6, 16, 40
	ids := make([]string, accounts)
	for i := range accounts {
		ids[i] = open(t, svc, fmt.Sprintf("acct-%d", i), "AED", 5_000).ID
	}

	var wg sync.WaitGroup
	errCh := make(chan error, workers*perWorker)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 42))
			for i := range perWorker {
				from := rng.IntN(accounts)
				to := (from + 1 + rng.IntN(accounts-1)) % accounts
				_, _, err := svc.CreatePayment(context.Background(), fmt.Sprintf("w%d-%d", w, i),
					payment.CreatePaymentRequest{
						FromAccount: ids[from], ToAccount: ids[to],
						Amount: int64(1 + rng.IntN(2_500)), Currency: "AED",
					})
				if err != nil {
					errCh <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("transfer failed: %v", err)
	}

	var total, payments int64
	if err := pool.QueryRow(context.Background(),
		`SELECT (SELECT sum(balance) FROM accounts WHERE kind = 'customer'), (SELECT count(*) FROM payments)`,
	).Scan(&total, &payments); err != nil {
		t.Fatal(err)
	}
	if total != accounts*5_000 {
		t.Errorf("customer money total = %d, want %d", total, accounts*5_000)
	}
	if payments != workers*perWorker {
		t.Errorf("payments = %d, want %d", payments, workers*perWorker)
	}
	checkInvariants(t, pool)
}

// checkInvariants asserts the properties that must hold after any sequence
// of operations.
func checkInvariants(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	queries := []struct {
		what string
		sql  string
	}{
		{"currencies whose balances do not sum to zero",
			`SELECT count(*) FROM (SELECT currency FROM accounts GROUP BY currency HAVING sum(balance) <> 0) x`},
		{"accounts whose balance differs from their ledger",
			`SELECT count(*) FROM accounts a
			 WHERE a.balance <> COALESCE((SELECT sum(amount) FROM ledger_entries e WHERE e.account_id = a.id), 0)`},
		{"unbalanced journals",
			`SELECT count(*) FROM (SELECT journal_id FROM ledger_entries GROUP BY journal_id HAVING sum(amount) <> 0) x`},
		{"customer accounts with a negative balance",
			`SELECT count(*) FROM accounts WHERE kind = 'customer' AND balance < 0`},
		{"rejected payments with ledger entries",
			`SELECT count(*) FROM payments p JOIN ledger_entries e ON e.journal_id = p.id WHERE p.status = 'rejected'`},
		{"payments without exactly one outbox event",
			`SELECT count(*) FROM payments p
			 WHERE (SELECT count(*) FROM outbox_events o WHERE o.msg_key = p.id::text) <> 1`},
		{"payments not bound to an idempotency key",
			`SELECT count(*) FROM payments p WHERE NOT EXISTS (SELECT 1 FROM idempotency_keys k WHERE k.payment_id = p.id)`},
	}
	for _, q := range queries {
		var n int
		if err := pool.QueryRow(ctx, q.sql).Scan(&n); err != nil {
			t.Fatalf("invariant query %q: %v", q.what, err)
		}
		if n != 0 {
			t.Errorf("invariant violated: %d %s", n, q.what)
		}
	}
}
