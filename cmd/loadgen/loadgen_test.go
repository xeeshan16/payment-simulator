package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	ms := func(ns ...int) []time.Duration {
		out := make([]time.Duration, len(ns))
		for i, n := range ns {
			out[i] = time.Duration(n) * time.Millisecond
		}
		return out
	}
	hundred := make([]int, 100)
	for i := range hundred {
		hundred[i] = i + 1
	}
	cases := []struct {
		name   string
		sorted []time.Duration
		p      float64
		want   time.Duration
	}{
		{"empty", nil, 50, 0},
		{"single", ms(7), 99, 7 * time.Millisecond},
		{"p50 of 1..100", ms(hundred...), 50, 50 * time.Millisecond},
		{"p95 of 1..100", ms(hundred...), 95, 95 * time.Millisecond},
		{"p99 of 1..100", ms(hundred...), 99, 99 * time.Millisecond},
		{"max of 1..100", ms(hundred...), 100, 100 * time.Millisecond},
		{"p50 rounds up to the next rank", ms(1, 2, 3), 50, 2 * time.Millisecond},
		{"tiny p is the minimum", ms(1, 2, 3), 0.1, 1 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := percentile(tc.sorted, tc.p); got != tc.want {
				t.Errorf("percentile(%v) = %v, want %v", tc.p, got, tc.want)
			}
		})
	}
}

// hotShare returns the fraction of picks that involve account 0.
func hotShare(t *testing.T, p picker, n int) float64 {
	t.Helper()
	r := rand.New(rand.NewPCG(1, 2))
	hits := 0
	for range n {
		from, to := p.pick(r)
		if from == to || from < 0 || to < 0 || from >= p.accounts || to >= p.accounts {
			t.Fatalf("pick() = (%d, %d) for %d accounts", from, to, p.accounts)
		}
		if from == 0 || to == 0 {
			hits++
		}
	}
	return float64(hits) / float64(n)
}

func TestPickerHonoursHotRatio(t *testing.T) {
	// Half the transfers pick account 0 as one side; the rest touch it with
	// probability 2/100 (either side, uniform). Expected share: 0.5 + 0.5*0.02.
	got := hotShare(t, picker{accounts: 100, hotAccounts: 1, hotRatio: 0.5}, 100_000)
	if got < 0.49 || got > 0.53 {
		t.Errorf("share of transfers touching the hot account = %.3f, want about 0.51", got)
	}
}

func TestPickerIsUniformWithoutHotAccounts(t *testing.T) {
	got := hotShare(t, picker{accounts: 100}, 100_000)
	if got < 0.015 || got > 0.025 {
		t.Errorf("share of transfers touching account 0 = %.3f, want about 0.02", got)
	}
}

func TestPickerTwoAccounts(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	seen := map[[2]int]bool{}
	for range 100 {
		from, to := picker{accounts: 2, hotAccounts: 1, hotRatio: 1}.pick(r)
		seen[[2]int{from, to}] = true
	}
	if len(seen) != 2 || !seen[[2]int{0, 1}] || !seen[[2]int{1, 0}] {
		t.Errorf("pairs seen = %v, want both directions between accounts 0 and 1", seen)
	}
}

func TestValidate(t *testing.T) {
	good := config{accounts: 10, workers: 2, duration: time.Second, maxAmount: 10, timeout: time.Second}
	if err := good.validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	cases := map[string]func(c *config){
		"one account":           func(c *config) { c.accounts = 1 },
		"no workers":            func(c *config) { c.workers = 0 },
		"never ends":            func(c *config) { c.duration = 0 },
		"all accounts hot":      func(c *config) { c.hotAccounts = 10 },
		"ratio above one":       func(c *config) { c.hotAccounts = 1; c.hotRatio = 1.5 },
		"ratio without hot set": func(c *config) { c.hotRatio = 0.5 },
		"zero max amount":       func(c *config) { c.maxAmount = 0 },
		"negative balance":      func(c *config) { c.openingBalance = -1 },
		"zero timeout":          func(c *config) { c.timeout = 0 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := good
			mutate(&c)
			if err := c.validate(); err == nil {
				t.Errorf("config %+v accepted", c)
			}
		})
	}
}

// fakeServer imitates the simulator's API: every payment whose amount is a
// multiple of 7 is rejected and every 50th request fails with 503.
type fakeServer struct {
	mu       sync.Mutex
	accounts int
	keys     map[string]bool
	payments int
}

func (f *fakeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/v1/accounts":
		f.accounts++
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, `{"id":"acc-%d"}`, f.accounts)
	case "/v1/payments":
		var req struct {
			From   string `json:"from_account"`
			To     string `json:"to_account"`
			Amount int64  `json:"amount"`
		}
		key := r.Header.Get("Idempotency-Key")
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || key == "" || f.keys[key] || req.From == req.To || req.Amount < 1 {
			w.WriteHeader(http.StatusBadRequest) // the test asserts there are none
			return
		}
		f.keys[key] = true
		f.payments++
		switch {
		case f.payments%50 == 0:
			w.WriteHeader(http.StatusServiceUnavailable)
		case req.Amount%7 == 0:
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, `{"status":"rejected"}`)
		default:
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, `{"status":"completed"}`)
		}
	default:
		http.NotFound(w, r)
	}
}

func TestRunStopsAtRequestBudget(t *testing.T) {
	fake := &fakeServer{keys: map[string]bool{}}
	srv := httptest.NewServer(fake)
	defer srv.Close()

	cfg := config{
		baseURL: srv.URL + "/", accounts: 5, workers: 8, requests: 500,
		hotAccounts: 1, hotRatio: 0.5, openingBalance: 100, maxAmount: 100,
		currency: "AED", timeout: 5 * time.Second, seed: 42,
	}
	res, err := run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if fake.accounts != 5 {
		t.Errorf("opened %d accounts, want 5", fake.accounts)
	}
	if got := res.requests(); got != 500 {
		t.Errorf("requests = %d, want 500 (outcomes %v)", got, res.outcomes)
	}
	if len(res.latencies) != 500 {
		t.Errorf("latency samples = %d, want 500", len(res.latencies))
	}
	if res.outcomes["http_400"] != 0 {
		t.Errorf("%d malformed or duplicate requests", res.outcomes["http_400"])
	}
	if res.outcomes["http_503"] != 10 {
		t.Errorf("http_503 = %d, want 10", res.outcomes["http_503"])
	}
	if res.outcomes["completed"] == 0 || res.outcomes["rejected"] == 0 {
		t.Errorf("outcomes = %v, want both completed and rejected", res.outcomes)
	}

	var out bytes.Buffer
	res.write(&out, cfg)
	for _, want := range []string{"requests    500\n", "throughput  ", "p50=", "p99=", "http_503=10"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("report is missing %q:\n%s", want, out.String())
		}
	}
}

func TestRunStopsAfterDuration(t *testing.T) {
	srv := httptest.NewServer(&fakeServer{keys: map[string]bool{}})
	defer srv.Close()

	cfg := config{
		baseURL: srv.URL, accounts: 3, workers: 4, duration: 200 * time.Millisecond,
		openingBalance: 100, maxAmount: 10, currency: "AED", timeout: 5 * time.Second, seed: 7,
	}
	res, err := run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if res.elapsed > 5*time.Second {
		t.Errorf("run took %s, want it to stop shortly after 200ms", res.elapsed)
	}
	if res.requests() == 0 {
		t.Error("no requests were sent")
	}
	if n := res.outcomes["transport_error"]; n != 0 {
		t.Errorf("%d transport errors; requests cut off by the deadline should not be counted", n)
	}
}

func TestRunFailsWhenAccountsCannotBeOpened(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := config{baseURL: srv.URL, accounts: 2, workers: 1, requests: 1, maxAmount: 1, timeout: time.Second}
	if _, err := run(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "status 500") {
		t.Errorf("err = %v, want an account-opening failure with status 500", err)
	}
}
