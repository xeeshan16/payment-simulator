package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xeeshan16/payment-simulator/internal/dlq"
	"github.com/xeeshan16/payment-simulator/internal/kafka"
)

func TestRunReportsMatchesAndMissingIDs(t *testing.T) {
	t.Parallel()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "payments.events", "payments.events.dlq"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	rec := &kgo.Record{
		Topic: "payments.events.dlq",
		Value: []byte(`{"event_id":"ev-a"}`),
		Headers: []kgo.RecordHeader{
			{Key: kafka.HeaderEventID, Value: []byte("ev-a")},
			{Key: dlq.HeaderError, Value: []byte("permanent failure")},
		},
	}
	if err := cl.ProduceSync(context.Background(), rec).FirstErr(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out bytes.Buffer
	opts := dlq.Options{DLQTopic: "payments.events.dlq", Topic: "payments.events", EventIDs: []string{"ev-a", "ev-x"}, DryRun: true}
	ok, err := run(ctx, cluster.ListenAddrs(), opts, &out)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("run reported success although ev-x is missing")
	}
	for _, want := range []string{
		`partition=0 offset=0 event_id="ev-a" error="permanent failure"`,
		`not found: event_id="ev-x"`,
		"scanned 1 dead-letter records from payments.events.dlq; would replay 1 to payments.events",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output missing %q:\n%s", want, out.String())
		}
	}
}
