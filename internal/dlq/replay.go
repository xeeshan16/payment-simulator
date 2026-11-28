// Package dlq replays dead-lettered records back to the main topic once the
// cause of their failure has been fixed.
//
// Replaying is safe to repeat: the consumer deduplicates by event id, and a
// dead-lettered event was never recorded as processed, so each replayed event
// is applied at most once however many times it is re-published.
package dlq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/xeeshan16/payment-simulator/internal/kafka"
)

// Header names written by the consumer when it dead-letters a record. Every
// header starting with HeaderPrefix is dropped when the record is replayed.
const (
	HeaderPrefix            = "dlq-"
	HeaderError             = "dlq-error"
	HeaderOriginalTopic     = "dlq-original-topic"
	HeaderOriginalPartition = "dlq-original-partition"
	HeaderOriginalOffset    = "dlq-original-offset"
	// HeaderReplayedFrom is added to a replayed record and names the
	// dead-letter record it came from, as topic/partition/offset.
	HeaderReplayedFrom = "dlq-replayed-from"
)

// Options selects which dead-lettered records to replay and where to.
type Options struct {
	// DLQTopic is read from the start up to its end at the time Replay is
	// called; records dead-lettered while Replay runs are left for later.
	DLQTopic string
	// Topic receives the replayed records.
	Topic string
	// EventIDs selects records by event id, taken from the event-id header
	// or, when that is missing, from the payload. Exactly one of EventIDs and
	// All must be set.
	EventIDs []string
	// All replays every record in the dead-letter topic.
	All bool
	// DryRun reports the matching records without publishing them.
	DryRun bool
}

// Entry describes one dead-lettered record that matched the selection.
type Entry struct {
	Partition int32
	Offset    int64
	EventID   string
	// Error is the dlq-error header: why the record was dead-lettered.
	Error string
}

// Result reports what Replay found and did.
type Result struct {
	// Scanned is how many dead-letter records were read.
	Scanned int
	// Matched lists the selected records in the order they were read.
	Matched []Entry
	// Published is how many records were re-published; 0 on a dry run.
	Published int
	// Missing lists requested event ids that were not in the dead-letter
	// topic.
	Missing []string
}

// Replay reads the dead-letter topic through client and re-publishes the
// selected records to opts.Topic with their original key, value and headers,
// so they land on the same partition as before and keep their event id. The
// dead-letter records themselves are left in place.
//
// client must not be a group consumer and must not already consume topics;
// Replay assigns the dead-letter partitions to it directly.
func Replay(ctx context.Context, client *kgo.Client, opts Options) (Result, error) {
	var res Result
	if err := opts.validate(); err != nil {
		return res, err
	}
	want := make(map[string]bool, len(opts.EventIDs))
	for _, id := range opts.EventIDs {
		want[id] = true
	}

	offsets, err := partitionBounds(ctx, client, opts.DLQTopic)
	if err != nil {
		return res, err
	}
	// remaining maps each non-empty partition to the last offset to read.
	remaining := make(map[int32]int64)
	start := make(map[int32]kgo.Offset)
	for p, b := range offsets {
		if b.end > b.start {
			remaining[p] = b.end - 1
			start[p] = kgo.NewOffset().At(b.start)
		}
	}

	var replay []*kgo.Record
	found := make(map[string]bool)
	if len(remaining) > 0 {
		client.AddConsumePartitions(map[string]map[int32]kgo.Offset{opts.DLQTopic: start})
		defer client.RemoveConsumePartitions(map[string][]int32{opts.DLQTopic: partitions(start)})
	}
	for len(remaining) > 0 {
		fetches := client.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return res, fmt.Errorf("read dead-letter topic: %w", err)
		}
		if fetches.IsClientClosed() {
			return res, kgo.ErrClientClosed
		}
		var fetchErr error
		fetches.EachError(func(topic string, partition int32, err error) {
			fetchErr = errors.Join(fetchErr, fmt.Errorf("fetch %s[%d]: %w", topic, partition, err))
		})
		if fetchErr != nil {
			return res, fetchErr
		}
		for rec := range fetches.RecordsAll() {
			last, ok := remaining[rec.Partition]
			if !ok || rec.Offset > last {
				continue // dead-lettered after Replay started
			}
			if rec.Offset == last {
				delete(remaining, rec.Partition)
			}
			res.Scanned++
			evID := eventID(rec)
			if !opts.All && !want[evID] {
				continue
			}
			found[evID] = true
			res.Matched = append(res.Matched, Entry{
				Partition: rec.Partition,
				Offset:    rec.Offset,
				EventID:   evID,
				Error:     header(rec, HeaderError),
			})
			replay = append(replay, replayRecord(rec, opts.Topic))
		}
	}

	for _, id := range opts.EventIDs {
		if !found[id] {
			res.Missing = append(res.Missing, id)
		}
	}
	if opts.DryRun || len(replay) == 0 {
		return res, nil
	}
	if err := client.ProduceSync(ctx, replay...).FirstErr(); err != nil {
		return res, fmt.Errorf("publish to %s: %w", opts.Topic, err)
	}
	res.Published = len(replay)
	return res, nil
}

func (o Options) validate() error {
	switch {
	case o.DLQTopic == "":
		return errors.New("dead-letter topic is required")
	case o.Topic == "":
		return errors.New("target topic is required")
	case o.Topic == o.DLQTopic:
		return errors.New("target topic must differ from the dead-letter topic")
	case o.All == (len(o.EventIDs) > 0):
		return errors.New("select records with either event ids or all, not both or neither")
	}
	for _, id := range o.EventIDs {
		if id == "" {
			return errors.New("event ids must not be empty")
		}
	}
	return nil
}

// replayRecord copies a dead-letter record for the target topic: same key and
// value, the original headers without the dead-letter diagnostics, plus a
// header pointing back at the dead-letter record. Partition is left to the
// client's partitioner, which hashes the key as the outbox publisher does.
func replayRecord(rec *kgo.Record, topic string) *kgo.Record {
	headers := make([]kgo.RecordHeader, 0, len(rec.Headers)+1)
	for _, h := range rec.Headers {
		if !strings.HasPrefix(h.Key, HeaderPrefix) {
			headers = append(headers, h)
		}
	}
	from := rec.Topic + "/" + strconv.Itoa(int(rec.Partition)) + "/" + strconv.FormatInt(rec.Offset, 10)
	headers = append(headers, kgo.RecordHeader{Key: HeaderReplayedFrom, Value: []byte(from)})
	return &kgo.Record{Topic: topic, Key: rec.Key, Value: rec.Value, Headers: headers}
}

// eventID returns the record's event id from its header, falling back to the
// event_id field of the payload; "" when neither has one, as for a record
// whose payload is not JSON.
func eventID(rec *kgo.Record) string {
	if id := header(rec, kafka.HeaderEventID); id != "" {
		return id
	}
	var ev struct {
		EventID string `json:"event_id"`
	}
	if json.Unmarshal(rec.Value, &ev) != nil {
		return ""
	}
	return ev.EventID
}

func header(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func partitions(m map[int32]kgo.Offset) []int32 {
	ps := make([]int32, 0, len(m))
	for p := range m {
		ps = append(ps, p)
	}
	return ps
}

type bounds struct{ start, end int64 }

// partitionBounds returns the earliest and end offsets of every partition of
// topic.
func partitionBounds(ctx context.Context, client *kgo.Client, topic string) (map[int32]bounds, error) {
	meta := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr(topic)
	meta.Topics = append(meta.Topics, mt)
	metaResp, err := meta.RequestWith(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("metadata for %s: %w", topic, err)
	}
	if len(metaResp.Topics) != 1 {
		return nil, fmt.Errorf("metadata for %s: got %d topics", topic, len(metaResp.Topics))
	}
	if err := kerr.ErrorForCode(metaResp.Topics[0].ErrorCode); err != nil {
		return nil, fmt.Errorf("metadata for %s: %w", topic, err)
	}

	res := make(map[int32]bounds)
	for _, ts := range []int64{-2, -1} { // -2: earliest offset, -1: end offset
		req := kmsg.NewPtrListOffsetsRequest()
		rt := kmsg.NewListOffsetsRequestTopic()
		rt.Topic = topic
		for _, mp := range metaResp.Topics[0].Partitions {
			rp := kmsg.NewListOffsetsRequestTopicPartition()
			rp.Partition = mp.Partition
			rp.Timestamp = ts
			rt.Partitions = append(rt.Partitions, rp)
		}
		req.Topics = append(req.Topics, rt)
		resp, err := req.RequestWith(ctx, client)
		if err != nil {
			return nil, fmt.Errorf("list offsets for %s: %w", topic, err)
		}
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
					return nil, fmt.Errorf("list offsets for %s[%d]: %w", topic, p.Partition, err)
				}
				b := res[p.Partition]
				if ts == -2 {
					b.start = p.Offset
				} else {
					b.end = p.Offset
				}
				res[p.Partition] = b
			}
		}
	}
	return res, nil
}
