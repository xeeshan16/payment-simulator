// Package payment owns accounts, transfers between them, the double-entry
// ledger, idempotency keys and the outbox events that describe each outcome.
package payment

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xeeshan16/payment-simulator/internal/id"
)

// Status of a payment. A payment is decided synchronously, so it is only ever
// stored as completed or rejected.
type Status string

const (
	StatusCompleted Status = "completed"
	StatusRejected  Status = "rejected"
)

// Reasons a payment can be rejected after it has been accepted for processing.
const (
	ReasonInsufficientFunds = "insufficient_funds"
)

// Event types written to the outbox.
const (
	EventPaymentCompleted = "payment.completed"
	EventPaymentRejected  = "payment.rejected"
)

// Account is a customer account (or the per-currency funding account).
type Account struct {
	ID        string    `json:"id"`
	Owner     string    `json:"owner"`
	Currency  string    `json:"currency"`
	Balance   int64     `json:"balance"`
	CreatedAt time.Time `json:"created_at"`
}

// Payment is a transfer of Amount minor units between two accounts.
type Payment struct {
	ID            string    `json:"id"`
	FromAccount   string    `json:"from_account"`
	ToAccount     string    `json:"to_account"`
	Amount        int64     `json:"amount"`
	Currency      string    `json:"currency"`
	Reference     string    `json:"reference"`
	Status        Status    `json:"status"`
	FailureReason string    `json:"failure_reason,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

// Event is the payload published for every payment outcome.
type Event struct {
	EventID    string    `json:"event_id"`
	Type       string    `json:"type"`
	OccurredAt time.Time `json:"occurred_at"`
	Payment    Payment   `json:"payment"`
}

// OpenAccountRequest opens a customer account with an optional opening
// balance, funded from the system funding account for that currency.
type OpenAccountRequest struct {
	Owner          string `json:"owner"`
	Currency       string `json:"currency"`
	OpeningBalance int64  `json:"opening_balance"`
}

// CreatePaymentRequest moves money from one account to another.
type CreatePaymentRequest struct {
	FromAccount string `json:"from_account"`
	ToAccount   string `json:"to_account"`
	Amount      int64  `json:"amount"`
	Currency    string `json:"currency"`
	Reference   string `json:"reference"`
}

// Errors returned by the service. Handlers map them to HTTP status codes.
var (
	ErrNotFound             = errors.New("not found")
	ErrCurrencyMismatch     = errors.New("currency does not match account currency")
	ErrIdempotencyKeyReused = errors.New("idempotency key was already used with a different request")
)

// ValidationError is returned for requests that are malformed before any
// state is read.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

func invalid(format string, args ...any) error {
	return &ValidationError{Msg: fmt.Sprintf(format, args...)}
}

const maxReferenceLen = 140

func (r *OpenAccountRequest) normalize() error {
	r.Owner = strings.TrimSpace(r.Owner)
	r.Currency = strings.ToUpper(strings.TrimSpace(r.Currency))
	switch {
	case r.Owner == "":
		return invalid("owner is required")
	case !validCurrency(r.Currency):
		return invalid("currency must be a three-letter ISO 4217 code")
	case r.OpeningBalance < 0:
		return invalid("opening_balance cannot be negative")
	}
	return nil
}

func (r *CreatePaymentRequest) normalize() error {
	r.Currency = strings.ToUpper(strings.TrimSpace(r.Currency))
	r.FromAccount = strings.ToLower(r.FromAccount)
	r.ToAccount = strings.ToLower(r.ToAccount)
	switch {
	case !id.Valid(r.FromAccount):
		return invalid("from_account must be a UUID")
	case !id.Valid(r.ToAccount):
		return invalid("to_account must be a UUID")
	case r.FromAccount == r.ToAccount:
		return invalid("from_account and to_account must differ")
	case r.Amount <= 0:
		return invalid("amount must be a positive number of minor units")
	case !validCurrency(r.Currency):
		return invalid("currency must be a three-letter ISO 4217 code")
	case len(r.Reference) > maxReferenceLen:
		return invalid("reference must be at most %d characters", maxReferenceLen)
	}
	return nil
}

func validCurrency(c string) bool {
	if len(c) != 3 {
		return false
	}
	for i := 0; i < 3; i++ {
		if c[i] < 'A' || c[i] > 'Z' {
			return false
		}
	}
	return true
}
