package dlq

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xeeshan16/payment-simulator/internal/kafka"
)

const (
	topic    = "payments.events"
	dlqTopic = "payments.events.dlq"
)

func newCluster(t *testing.T) (*kfake.Cluster, *kgo.Client) {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, topic, dlqTopic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	return cluster, newClient(t, cluster)
}

func newClient(t *testing.T, cluster *kfake.Cluster, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(cluster.ListenAddrs()...)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// deadLetter writes a record shaped like the consumer's dead-letter output to
// the given partition of the dead-letter topic.
func deadLetter(t *testing.T, cluster *kfake.Cluster, partition int32, key, eventID, value string) {
	t.Helper()
	cl := newClient(t, cluster, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	rec := &kgo.Record{
		Topic:     dlqTopic,
		Partition: partition,
		Key:       []byte(key),
		Value:     []byte(value),
		Headers: []kgo.RecordHeader{
			{Key: HeaderError, Value: []byte("after 3 attempts: permanent failure")},
			{Key: HeaderOriginalTopic, Value: []byte(topic)},
			{Key: HeaderOriginalPartition, Value: []byte("1")},
			{Key: HeaderOriginalOffset, Value: []byte("7")},
		},
	}
	if eventID != "" {
		rec.Headers = append([]kgo.RecordHeader{{Key: kafka.HeaderEventID, Value: []byte(eventID)}}, rec.Headers...)
	}
	if err := cl.ProduceSync(context.Background(), rec).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// readTopic returns every record in topic, waiting until there are want.
func readTopic(t *testing.T, cluster *kfake.Cluster, want int) []*kgo.Record {
	t.Helper()
	cl := newClient(t, cluster, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var recs []*kgo.Record
	for len(recs) < want && ctx.Err() == nil {
		recs = append(recs, cl.PollFetches(ctx).Records()...)
	}
	return recs
}

// topicSize is the number of records in topic, summed over its partitions.
func topicSize(t *testing.T, cl *kgo.Client, name string) int64 {
	t.Helper()
	b, err := partitionBounds(context.Background(), cl, name)
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	for _, pb := range b {
		n += pb.end - pb.start
	}
	return n
}

func mustReplay(t *testing.T, cl *kgo.Client, opts Options) Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := Replay(ctx, cl, opts)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	return res
}

func headerValue(t *testing.T, r *kgo.Record, key string) (string, bool) {
	t.Helper()
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

// Only the selected record is re-published, with its original key, value and
// event id, without the dead-letter diagnostics, and pointing back at the
// dead-letter record it came from. The dead-letter topic is left untouched.
func TestReplaySelectedEvents(t *testing.T) {
	t.Parallel()
	cluster, cl := newCluster(t)
	deadLetter(t, cluster, 0, "acct-1", "ev-a", `{"event_id":"ev-a"}`)
	deadLetter(t, cluster, 1, "acct-2", "ev-b", `{"event_id":"ev-b"}`)
	deadLetter(t, cluster, 1, "acct-3", "ev-c", `{"event_id":"ev-c"}`)

	res := mustReplay(t, cl, Options{DLQTopic: dlqTopic, Topic: topic, EventIDs: []string{"ev-c", "ev-missing"}})

	if res.Scanned != 3 || res.Published != 1 {
		t.Errorf("scanned %d, published %d; want 3 and 1", res.Scanned, res.Published)
	}
	want := []Entry{{Partition: 1, Offset: 1, EventID: "ev-c", Error: "after 3 attempts: permanent failure"}}
	if !slices.Equal(res.Matched, want) {
		t.Errorf("matched %+v, want %+v", res.Matched, want)
	}
	if !slices.Equal(res.Missing, []string{"ev-missing"}) {
		t.Errorf("missing %v, want [ev-missing]", res.Missing)
	}

	got := readTopic(t, cluster, 1)
	if len(got) != 1 {
		t.Fatalf("main topic has %d records, want 1", len(got))
	}
	r := got[0]
	if string(r.Key) != "acct-3" || string(r.Value) != `{"event_id":"ev-c"}` {
		t.Errorf("replayed key %q value %q, want the original", r.Key, r.Value)
	}
	if v, _ := headerValue(t, r, kafka.HeaderEventID); v != "ev-c" {
		t.Errorf("event-id header = %q, want ev-c", v)
	}
	if v, _ := headerValue(t, r, HeaderReplayedFrom); v != dlqTopic+"/1/1" {
		t.Errorf("%s = %q, want %s/1/1", HeaderReplayedFrom, v, dlqTopic)
	}
	for _, h := range r.Headers {
		if strings.HasPrefix(h.Key, HeaderPrefix) && h.Key != HeaderReplayedFrom {
			t.Errorf("replayed record kept dead-letter header %s", h.Key)
		}
	}
	if n := topicSize(t, cl, dlqTopic); n != 3 {
		t.Errorf("dead-letter topic has %d records after replay, want 3", n)
	}
}

// A record without an event-id header is matched by the event_id in its
// payload, and a payload that is not JSON never matches an id.
func TestReplayFallsBackToPayloadEventID(t *testing.T) {
	t.Parallel()
	cluster, cl := newCluster(t)
	deadLetter(t, cluster, 0, "", "", `{not json`)
	deadLetter(t, cluster, 0, "acct-1", "", `{"event_id":"ev-a","type":"payment.completed"}`)

	res := mustReplay(t, cl, Options{DLQTopic: dlqTopic, Topic: topic, EventIDs: []string{"ev-a"}})

	if len(res.Matched) != 1 || res.Matched[0].EventID != "ev-a" || res.Matched[0].Offset != 1 {
		t.Fatalf("matched %+v, want ev-a at offset 1", res.Matched)
	}
	if got := readTopic(t, cluster, 1); len(got) != 1 || string(got[0].Key) != "acct-1" {
		t.Fatalf("main topic = %d records, want the ev-a record", len(got))
	}
}

// A dry run reports every record but publishes nothing.
func TestReplayAllDryRun(t *testing.T) {
	t.Parallel()
	cluster, cl := newCluster(t)
	deadLetter(t, cluster, 0, "acct-1", "ev-a", `{"event_id":"ev-a"}`)
	deadLetter(t, cluster, 1, "", "", `{not json`)

	res := mustReplay(t, cl, Options{DLQTopic: dlqTopic, Topic: topic, All: true, DryRun: true})

	if res.Scanned != 2 || len(res.Matched) != 2 || res.Published != 0 {
		t.Errorf("scanned %d, matched %d, published %d; want 2, 2, 0", res.Scanned, len(res.Matched), res.Published)
	}
	if n := topicSize(t, cl, topic); n != 0 {
		t.Errorf("dry run published %d records", n)
	}
}

// Replaying everything re-publishes every record, poison ones included; an
// empty dead-letter topic returns at once instead of waiting for records.
func TestReplayAll(t *testing.T) {
	t.Parallel()
	cluster, cl := newCluster(t)

	if res := mustReplay(t, cl, Options{DLQTopic: dlqTopic, Topic: topic, All: true}); res.Scanned != 0 || res.Published != 0 {
		t.Errorf("empty dead-letter topic: %+v", res)
	}

	deadLetter(t, cluster, 0, "acct-1", "ev-a", `{"event_id":"ev-a"}`)
	deadLetter(t, cluster, 1, "", "", `{not json`)
	if res := mustReplay(t, cl, Options{DLQTopic: dlqTopic, Topic: topic, All: true}); res.Published != 2 {
		t.Errorf("published %d, want 2", res.Published)
	}
	if got := readTopic(t, cluster, 2); len(got) != 2 {
		t.Errorf("main topic has %d records, want 2", len(got))
	}
}

func TestReplayRejectsInvalidOptions(t *testing.T) {
	t.Parallel()
	_, cl := newCluster(t)
	for name, opts := range map[string]Options{
		"no dead-letter topic": {Topic: topic, All: true},
		"no target topic":      {DLQTopic: dlqTopic, All: true},
		"same topic":           {DLQTopic: dlqTopic, Topic: dlqTopic, All: true},
		"no selection":         {DLQTopic: dlqTopic, Topic: topic},
		"ids and all":          {DLQTopic: dlqTopic, Topic: topic, All: true, EventIDs: []string{"ev-a"}},
		"empty id":             {DLQTopic: dlqTopic, Topic: topic, EventIDs: []string{""}},
	} {
		if _, err := Replay(context.Background(), cl, opts); err == nil {
			t.Errorf("%s: Replay accepted %+v", name, opts)
		}
	}
}

func TestReplayUnknownTopic(t *testing.T) {
	t.Parallel()
	_, cl := newCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := Replay(ctx, cl, Options{DLQTopic: "no.such.topic", Topic: topic, All: true}); err == nil {
		t.Error("Replay of a missing dead-letter topic succeeded")
	}
}
