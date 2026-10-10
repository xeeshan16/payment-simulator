package consumer

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/xeeshan16/payment-simulator/internal/payment"
)

// Notifications is a sample Handler: it records one customer notification per
// account affected by a payment outcome. It stands in for any downstream side
// effect (email, push, statement line) that must happen once per event.
func Notifications(ctx context.Context, tx pgx.Tx, ev payment.Event) error {
	p := ev.Payment
	type note struct{ account, msg string }
	var notes []note
	switch ev.Type {
	case payment.EventPaymentCompleted:
		notes = []note{
			{p.FromAccount, fmt.Sprintf("You sent %s to %s", money(p.Amount, p.Currency), p.ToAccount)},
			{p.ToAccount, fmt.Sprintf("You received %s from %s", money(p.Amount, p.Currency), p.FromAccount)},
		}
	case payment.EventPaymentRejected:
		notes = []note{
			{p.FromAccount, fmt.Sprintf("Payment of %s was declined: %s", money(p.Amount, p.Currency), p.FailureReason)},
		}
	default:
		return nil // unknown event types are ignored, not dead-lettered
	}
	for _, n := range notes {
		if _, err := tx.Exec(ctx,
			`INSERT INTO notifications (event_id, payment_id, account_id, message) VALUES ($1, $2, $3, $4)`,
			ev.EventID, p.ID, n.account, n.msg,
		); err != nil {
			return fmt.Errorf("insert notification: %w", err)
		}
	}
	return nil
}

// money formats minor units assuming two decimal places, which holds for the
// currencies the simulator is seeded with.
func money(minor int64, currency string) string {
	return fmt.Sprintf("%s %d.%02d", currency, minor/100, minor%100)
}
