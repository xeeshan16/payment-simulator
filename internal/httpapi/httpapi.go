// Package httpapi exposes the payment service over JSON/HTTP.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/xeeshan16/payment-simulator/internal/payment"
)

const maxBodyBytes = 1 << 16

// HeaderIdempotencyKey is required on POST /v1/payments.
const HeaderIdempotencyKey = "Idempotency-Key"

// HeaderReplayed is set to "true" when a response is replayed for a retried
// idempotency key.
const HeaderReplayed = "Idempotent-Replayed"

// Service is the subset of payment.Service the API needs.
type Service interface {
	OpenAccount(ctx context.Context, req payment.OpenAccountRequest) (payment.Account, error)
	GetAccount(ctx context.Context, id string) (payment.Account, error)
	CreatePayment(ctx context.Context, key string, req payment.CreatePaymentRequest) (payment.Payment, bool, error)
	GetPayment(ctx context.Context, id string) (payment.Payment, error)
}

// New returns the HTTP handler.
func New(svc Service, log *slog.Logger) http.Handler {
	h := &handler{svc: svc, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/accounts", h.openAccount)
	mux.HandleFunc("GET /v1/accounts/{id}", h.getAccount)
	mux.HandleFunc("POST /v1/payments", h.createPayment)
	mux.HandleFunc("GET /v1/payments/{id}", h.getPayment)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

type handler struct {
	svc Service
	log *slog.Logger
}

func (h *handler) openAccount(w http.ResponseWriter, r *http.Request) {
	var req payment.OpenAccountRequest
	if !decode(w, r, &req) {
		return
	}
	acc, err := h.svc.OpenAccount(r.Context(), req)
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, acc)
}

func (h *handler) getAccount(w http.ResponseWriter, r *http.Request) {
	acc, err := h.svc.GetAccount(r.Context(), r.PathValue("id"))
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, acc)
}

func (h *handler) createPayment(w http.ResponseWriter, r *http.Request) {
	var req payment.CreatePaymentRequest
	if !decode(w, r, &req) {
		return
	}
	p, replayed, err := h.svc.CreatePayment(r.Context(), r.Header.Get(HeaderIdempotencyKey), req)
	if err != nil {
		h.fail(w, err)
		return
	}
	status := http.StatusCreated
	if replayed {
		w.Header().Set(HeaderReplayed, "true")
		status = http.StatusOK
	}
	writeJSON(w, status, p)
}

func (h *handler) getPayment(w http.ResponseWriter, r *http.Request) {
	p, err := h.svc.GetPayment(r.Context(), r.PathValue("id"))
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (h *handler) fail(w http.ResponseWriter, err error) {
	var ve *payment.ValidationError
	switch {
	case errors.As(err, &ve):
		writeJSON(w, http.StatusBadRequest, apiError{"invalid_request", ve.Msg})
	case errors.Is(err, payment.ErrNotFound):
		writeJSON(w, http.StatusNotFound, apiError{"not_found", err.Error()})
	case errors.Is(err, payment.ErrCurrencyMismatch):
		writeJSON(w, http.StatusUnprocessableEntity, apiError{"currency_mismatch", err.Error()})
	case errors.Is(err, payment.ErrIdempotencyKeyReused):
		writeJSON(w, http.StatusUnprocessableEntity, apiError{"idempotency_key_reused", err.Error()})
	default:
		h.log.Error("request failed", "err", err)
		writeJSON(w, http.StatusInternalServerError, apiError{"internal", "internal error"})
	}
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, apiError{"invalid_json", err.Error()})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
