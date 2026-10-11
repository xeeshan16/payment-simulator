// Command loadgen drives concurrent transfers against a running payment
// simulator over HTTP and reports throughput and latency percentiles.
//
// It opens a fresh set of synthetic accounts, then runs a fixed number of
// workers that each send POST /v1/payments in a loop with a unique
// Idempotency-Key, until the duration elapses or the request budget is spent.
// A configurable share of transfers involves a small set of "hot" accounts to
// reproduce lock contention on popular accounts.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := config{}
	flag.StringVar(&cfg.baseURL, "addr", "http://localhost:8080", "base URL of the simulator")
	flag.IntVar(&cfg.accounts, "accounts", 100, "number of accounts to open before the run")
	flag.IntVar(&cfg.workers, "workers", 16, "concurrent workers, each with one request in flight")
	flag.DurationVar(&cfg.duration, "duration", 30*time.Second, "how long to send transfers (0 = until -requests is reached)")
	flag.IntVar(&cfg.requests, "requests", 0, "stop after this many transfers (0 = no limit)")
	flag.IntVar(&cfg.hotAccounts, "hot-accounts", 0, "number of accounts treated as hot")
	flag.Float64Var(&cfg.hotRatio, "hot-ratio", 0, "share of transfers (0..1) that involve a hot account")
	flag.Int64Var(&cfg.openingBalance, "opening-balance", 1_000_000, "opening balance of each account, in minor units")
	flag.Int64Var(&cfg.maxAmount, "max-amount", 1_000, "transfer amounts are uniform in [1, max-amount] minor units")
	flag.StringVar(&cfg.currency, "currency", "AED", "currency of all accounts and transfers")
	flag.DurationVar(&cfg.timeout, "timeout", 10*time.Second, "per-request timeout")
	flag.Uint64Var(&cfg.seed, "seed", 0, "random seed (0 = derive from the clock)")
	flag.Parse()

	if cfg.seed == 0 {
		cfg.seed = uint64(time.Now().UnixNano())
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	res, err := run(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
	res.write(os.Stdout, cfg)
}
