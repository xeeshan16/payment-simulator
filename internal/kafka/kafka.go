// Package kafka adapts franz-go to the outbox Publisher interface and
// provides the topic names used by the service.
package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xeeshan16/payment-simulator/internal/outbox"
)

// HeaderEventID carries the event id so consumers can deduplicate without
// decoding the payload.
const HeaderEventID = "event-id"

// Publisher publishes outbox messages with a franz-go client. The client
// should be created with the idempotent producer enabled (franz-go's default)
// and acks=all, so a broker-side retry cannot introduce duplicates or reorder
// records within a partition.
type Publisher struct {
	Client *kgo.Client
}

// Publish implements outbox.Publisher.
func (p Publisher) Publish(ctx context.Context, msgs []outbox.Message) error {
	recs := make([]*kgo.Record, len(msgs))
	for i, m := range msgs {
		recs[i] = &kgo.Record{
			Topic:   m.Topic,
			Key:     []byte(m.Key),
			Value:   m.Value,
			Headers: []kgo.RecordHeader{{Key: HeaderEventID, Value: []byte(m.EventID)}},
		}
	}
	return p.Client.ProduceSync(ctx, recs...).FirstErr()
}
