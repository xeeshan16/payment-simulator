package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	baseURL        string
	accounts       int
	workers        int
	duration       time.Duration
	requests       int
	hotAccounts    int
	hotRatio       float64
	openingBalance int64
	maxAmount      int64
	currency       string
	timeout        time.Duration
	seed           uint64
}

func (c config) validate() error {
	switch {
	case c.accounts < 2:
		return errors.New("-accounts must be at least 2")
	case c.workers < 1:
		return errors.New("-workers must be at least 1")
	case c.duration <= 0 && c.requests <= 0:
		return errors.New("set a positive -duration or -requests, otherwise the run never ends")
	case c.duration < 0 || c.requests < 0:
		return errors.New("-duration and -requests cannot be negative")
	case c.hotAccounts < 0 || c.hotAccounts >= c.accounts:
		return errors.New("-hot-accounts must be between 0 and -accounts minus 1")
	case c.hotRatio < 0 || c.hotRatio > 1:
		return errors.New("-hot-ratio must be between 0 and 1")
	case c.hotRatio > 0 && c.hotAccounts == 0:
		return errors.New("-hot-ratio needs -hot-accounts greater than 0")
	case c.maxAmount < 1:
		return errors.New("-max-amount must be at least 1")
	case c.openingBalance < 0:
		return errors.New("-opening-balance cannot be negative")
	case c.timeout <= 0:
		return errors.New("-timeout must be positive")
	}
	return nil
}

// result is what a run measured.
type result struct {
	elapsed   time.Duration
	latencies []time.Duration // one per request that got an HTTP response
	outcomes  map[string]int  // "completed", "rejected", "http_<status>", "transport_error"
}

func (r result) requests() int {
	n := 0
	for _, c := range r.outcomes {
		n += c
	}
	return n
}

// picker chooses the two accounts for a transfer. With probability hotRatio
// one side (payer or payee, at random) is drawn from the first hotAccounts
// accounts; the other side is drawn uniformly from all accounts.
type picker struct {
	accounts    int
	hotAccounts int
	hotRatio    float64
}

func (p picker) pick(r *rand.Rand) (from, to int) {
	a := r.IntN(p.accounts)
	if p.hotAccounts > 0 && r.Float64() < p.hotRatio {
		a = r.IntN(p.hotAccounts)
	}
	b := r.IntN(p.accounts - 1)
	if b >= a {
		b++ // uniform over every account except a
	}
	if r.IntN(2) == 0 {
		return a, b
	}
	return b, a
}

func run(ctx context.Context, cfg config) (result, error) {
	if err := cfg.validate(); err != nil {
		return result{}, err
	}
	cfg.baseURL = strings.TrimRight(cfg.baseURL, "/")
	client := &http.Client{
		Timeout: cfg.timeout,
		Transport: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			MaxIdleConns:        cfg.workers,
			MaxIdleConnsPerHost: cfg.workers,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	defer client.CloseIdleConnections()

	ids, err := openAccounts(ctx, client, cfg)
	if err != nil {
		return result{}, err
	}

	runCtx := ctx
	if cfg.duration > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, cfg.duration)
		defer cancel()
	}

	var (
		issued  atomic.Int64
		mu      sync.Mutex
		total   = result{outcomes: map[string]int{}}
		wg      sync.WaitGroup
		pick    = picker{accounts: cfg.accounts, hotAccounts: cfg.hotAccounts, hotRatio: cfg.hotRatio}
		started = time.Now()
	)
	for w := range cfg.workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(cfg.seed, uint64(w)))
			local := result{outcomes: map[string]int{}}
			for seq := 0; runCtx.Err() == nil; seq++ {
				if cfg.requests > 0 && issued.Add(1) > int64(cfg.requests) {
					break
				}
				from, to := pick.pick(r)
				body := map[string]any{
					"from_account": ids[from],
					"to_account":   ids[to],
					"amount":       1 + r.Int64N(cfg.maxAmount),
					"currency":     cfg.currency,
					"reference":    "loadgen",
				}
				key := fmt.Sprintf("loadgen-%x-%d-%d", cfg.seed, w, seq)
				t0 := time.Now()
				outcome, responded := transfer(runCtx, client, cfg.baseURL, key, body)
				if runCtx.Err() != nil && !responded {
					break // cut off by the end of the run, not a server failure
				}
				if responded {
					local.latencies = append(local.latencies, time.Since(t0))
				}
				local.outcomes[outcome]++
			}
			mu.Lock()
			total.latencies = append(total.latencies, local.latencies...)
			for k, v := range local.outcomes {
				total.outcomes[k] += v
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	total.elapsed = time.Since(started)
	return total, nil
}

func openAccounts(ctx context.Context, client *http.Client, cfg config) ([]string, error) {
	ids := make([]string, cfg.accounts)
	for i := range ids {
		var acc struct {
			ID string `json:"id"`
		}
		body := map[string]any{
			"owner":           fmt.Sprintf("loadgen-%x-%d", cfg.seed, i),
			"currency":        cfg.currency,
			"opening_balance": cfg.openingBalance,
		}
		status, err := postJSON(ctx, client, cfg.baseURL+"/v1/accounts", "", body, &acc)
		if err != nil {
			return nil, fmt.Errorf("open account %d: %w", i, err)
		}
		if status != http.StatusCreated || acc.ID == "" {
			return nil, fmt.Errorf("open account %d: unexpected status %d", i, status)
		}
		ids[i] = acc.ID
	}
	return ids, nil
}

// transfer sends one payment and classifies the outcome. responded is false
// when no HTTP response was received.
func transfer(ctx context.Context, client *http.Client, baseURL, key string, body any) (outcome string, responded bool) {
	var p struct {
		Status string `json:"status"`
	}
	status, err := postJSON(ctx, client, baseURL+"/v1/payments", key, body, &p)
	switch {
	case status == 0:
		return "transport_error", false
	case status == http.StatusCreated && err == nil && (p.Status == "completed" || p.Status == "rejected"):
		return p.Status, true
	default:
		return fmt.Sprintf("http_%d", status), true
	}
}

// postJSON returns the HTTP status (0 if no response arrived) and decodes a
// 2xx body into out.
func postJSON(ctx context.Context, client *http.Client, url, key string, body, out any) (int, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		_, _ = io.Copy(io.Discard, resp.Body) // drain so the connection is reused
		return resp.StatusCode, nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return resp.StatusCode, fmt.Errorf("decode response: %w", err)
	}
	return resp.StatusCode, nil
}

// percentile returns the nearest-rank p-th percentile (0 < p <= 100) of
// sorted, or 0 when it is empty.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	return sorted[min(max(rank, 1), len(sorted))-1]
}

func (r result) write(w io.Writer, cfg config) {
	lat := slices.Clone(r.latencies)
	slices.Sort(lat)
	n := r.requests()
	throughput := 0.0
	if r.elapsed > 0 {
		throughput = float64(n) / r.elapsed.Seconds()
	}

	keys := make([]string, 0, len(r.outcomes))
	for k := range r.outcomes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	outcomes := make([]string, 0, len(keys))
	for _, k := range keys {
		outcomes = append(outcomes, fmt.Sprintf("%s=%d", k, r.outcomes[k]))
	}

	fmt.Fprintf(w, "accounts=%d workers=%d hot_accounts=%d hot_ratio=%.2f seed=%d\n",
		cfg.accounts, cfg.workers, cfg.hotAccounts, cfg.hotRatio, cfg.seed)
	fmt.Fprintf(w, "elapsed     %s\n", r.elapsed.Round(time.Millisecond))
	fmt.Fprintf(w, "requests    %d\n", n)
	fmt.Fprintf(w, "throughput  %.1f req/s\n", throughput)
	fmt.Fprintf(w, "latency     p50=%s p95=%s p99=%s max=%s\n",
		round(percentile(lat, 50)), round(percentile(lat, 95)), round(percentile(lat, 99)), round(percentile(lat, 100)))
	fmt.Fprintf(w, "outcomes    %s\n", strings.Join(outcomes, " "))
}

func round(d time.Duration) time.Duration {
	if d >= time.Millisecond {
		return d.Round(10 * time.Microsecond)
	}
	return d.Round(time.Microsecond)
}
