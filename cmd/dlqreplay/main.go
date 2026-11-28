// Command dlqreplay re-publishes dead-lettered records to the main topic once
// the cause of their failure has been fixed.
//
// It reads the dead-letter topic up to its current end, selects records by
// event id (or all of them), and publishes each with its original key, value
// and event id, so it lands on the same partition and the consumer applies it
// at most once. Run with -dry-run first to see what would be replayed.
//
//	dlqreplay -event-id 0193...,0194...
//	dlqreplay -all -dry-run
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xeeshan16/payment-simulator/internal/dlq"
)

func main() {
	topic := env("PAYSIM_TOPIC", "payments.events")
	var (
		brokers  = flag.String("brokers", env("PAYSIM_KAFKA_BROKERS", "localhost:9092"), "comma-separated Kafka brokers")
		dlqTopic = flag.String("dlq-topic", env("PAYSIM_DLQ_TOPIC", topic+".dlq"), "dead-letter topic to read")
		target   = flag.String("topic", topic, "topic to re-publish to")
		eventIDs = flag.String("event-id", "", "comma-separated event ids to replay")
		all      = flag.Bool("all", false, "replay every record in the dead-letter topic")
		dryRun   = flag.Bool("dry-run", false, "list the matching records without publishing them")
		timeout  = flag.Duration("timeout", time.Minute, "give up after this long")
	)
	flag.Parse()

	opts := dlq.Options{DLQTopic: *dlqTopic, Topic: *target, All: *all, DryRun: *dryRun}
	if *eventIDs != "" {
		opts.EventIDs = strings.Split(*eventIDs, ",")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	ok, err := run(ctx, strings.Split(*brokers, ","), opts, os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dlqreplay:", err)
		os.Exit(1)
	}
	if !ok {
		os.Exit(1)
	}
}

// run replays and writes a report to w. It returns false when some requested
// event ids were not found, so scripts notice a typo.
func run(ctx context.Context, brokers []string, opts dlq.Options, w io.Writer) (bool, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return false, fmt.Errorf("kafka client: %w", err)
	}
	defer client.Close()

	res, err := dlq.Replay(ctx, client, opts)
	if err != nil {
		return false, err
	}
	for _, e := range res.Matched {
		fmt.Fprintf(w, "partition=%d offset=%d event_id=%q error=%q\n", e.Partition, e.Offset, e.EventID, e.Error)
	}
	for _, id := range res.Missing {
		fmt.Fprintf(w, "not found: event_id=%q\n", id)
	}
	verb := "replayed"
	if opts.DryRun {
		verb = "would replay"
	}
	fmt.Fprintf(w, "scanned %d dead-letter records from %s; %s %d to %s\n", res.Scanned, opts.DLQTopic, verb, len(res.Matched), opts.Topic)
	return len(res.Missing) == 0, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
