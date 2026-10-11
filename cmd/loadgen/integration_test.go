package main

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xeeshan16/payment-simulator/internal/httpapi"
	"github.com/xeeshan16/payment-simulator/internal/payment"
	"github.com/xeeshan16/payment-simulator/internal/testutil"
)

// TestLoadAgainstRealService runs a short, contended load against the real
// service and checks that every response was a completed or rejected payment
// and that the ledger still balances.
func TestLoadAgainstRealService(t *testing.T) {
	pool := testutil.NewDB(t)
	srv := httptest.NewServer(httpapi.New(payment.NewService(pool, "payments.events"), testutil.Logger(t)))
	defer srv.Close()

	const accounts, opening, requests = 20, 1_000, 400
	cfg := config{
		baseURL: srv.URL, accounts: accounts, workers: 8, requests: requests,
		hotAccounts: 1, hotRatio: 0.8, openingBalance: opening, maxAmount: 300,
		currency: "AED", timeout: 10 * time.Second, seed: 1,
	}
	res, err := run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := res.outcomes["completed"] + res.outcomes["rejected"]; got != requests {
		t.Fatalf("completed+rejected = %d, want %d (outcomes %v)", got, requests, res.outcomes)
	}

	ctx := context.Background()
	var payments, customerTotal, unbalancedCurrencies int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payments`).Scan(&payments); err != nil {
		t.Fatal(err)
	}
	if payments != requests {
		t.Errorf("payments stored = %d, want %d", payments, requests)
	}
	if err := pool.QueryRow(ctx, `SELECT COALESCE(sum(balance), 0) FROM accounts WHERE kind = 'customer'`).Scan(&customerTotal); err != nil {
		t.Fatal(err)
	}
	if customerTotal != accounts*opening {
		t.Errorf("customer money total = %d, want %d", customerTotal, accounts*opening)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM (SELECT currency FROM accounts GROUP BY currency HAVING sum(balance) <> 0) x`,
	).Scan(&unbalancedCurrencies); err != nil {
		t.Fatal(err)
	}
	if unbalancedCurrencies != 0 {
		t.Errorf("%d currencies whose balances do not sum to zero", unbalancedCurrencies)
	}
}
