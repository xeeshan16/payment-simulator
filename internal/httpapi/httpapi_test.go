package httpapi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xeeshan16/payment-simulator/internal/httpapi"
	"github.com/xeeshan16/payment-simulator/internal/payment"
	"github.com/xeeshan16/payment-simulator/internal/testutil"
)

type client struct {
	t   *testing.T
	srv *httptest.Server
}

func newClient(t *testing.T) *client {
	t.Helper()
	pool := testutil.NewDB(t)
	srv := httptest.NewServer(httpapi.New(payment.NewService(pool, "payments.events"), testutil.Logger(t)))
	t.Cleanup(srv.Close)
	return &client{t: t, srv: srv}
}

func (c *client) do(method, path, key string, body any, out any) *http.Response {
	c.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if s, ok := body.(string); ok {
			buf.WriteString(s)
		} else if err := json.NewEncoder(&buf).Encode(body); err != nil {
			c.t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, c.srv.URL+path, &buf)
	if err != nil {
		c.t.Fatal(err)
	}
	if key != "" {
		req.Header.Set(httpapi.HeaderIdempotencyKey, key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			c.t.Fatalf("%s %s: decode response: %v", method, path, err)
		}
	}
	return resp
}

func (c *client) account(owner string, balance int64) payment.Account {
	c.t.Helper()
	var acc payment.Account
	resp := c.do("POST", "/v1/accounts", "", map[string]any{"owner": owner, "currency": "AED", "opening_balance": balance}, &acc)
	if resp.StatusCode != http.StatusCreated {
		c.t.Fatalf("open account: status %d", resp.StatusCode)
	}
	return acc
}

func TestPaymentLifecycleOverHTTP(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	alice, bob := c.account("alice", 5_000), c.account("bob", 0)
	body := map[string]any{"from_account": alice.ID, "to_account": bob.ID, "amount": 1_500, "currency": "AED", "reference": "invoice 42"}

	var created payment.Payment
	resp := c.do("POST", "/v1/payments", "order-42", body, &created)
	if resp.StatusCode != http.StatusCreated || resp.Header.Get(httpapi.HeaderReplayed) != "" {
		t.Fatalf("first request: status %d, replayed header %q", resp.StatusCode, resp.Header.Get(httpapi.HeaderReplayed))
	}

	var replay payment.Payment
	resp = c.do("POST", "/v1/payments", "order-42", body, &replay)
	if resp.StatusCode != http.StatusOK || resp.Header.Get(httpapi.HeaderReplayed) != "true" {
		t.Fatalf("retry: status %d, replayed header %q", resp.StatusCode, resp.Header.Get(httpapi.HeaderReplayed))
	}
	if replay.ID != created.ID {
		t.Fatalf("retry returned payment %s, want %s", replay.ID, created.ID)
	}

	var fetched payment.Payment
	if resp := c.do("GET", "/v1/payments/"+created.ID, "", nil, &fetched); resp.StatusCode != http.StatusOK || fetched.ID != created.ID {
		t.Fatalf("get payment: status %d, body %+v", resp.StatusCode, fetched)
	}
	var acc payment.Account
	if c.do("GET", "/v1/accounts/"+alice.ID, "", nil, &acc); acc.Balance != 3_500 {
		t.Errorf("alice balance = %d, want 3500", acc.Balance)
	}
}

func TestErrorResponses(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	alice, bob := c.account("alice", 100), c.account("bob", 0)
	ok := map[string]any{"from_account": alice.ID, "to_account": bob.ID, "amount": 10, "currency": "AED"}
	c.do("POST", "/v1/payments", "used", ok, nil)

	cases := []struct {
		name   string
		method string
		path   string
		key    string
		body   any
		status int
		code   string
	}{
		{"missing idempotency key", "POST", "/v1/payments", "", ok, 400, "invalid_request"},
		{"malformed json", "POST", "/v1/payments", "k1", `{"amount":`, 400, "invalid_json"},
		{"unknown field", "POST", "/v1/payments", "k2", map[string]any{"amount": 1, "surprise": true}, 400, "invalid_json"},
		{"key reused with different body", "POST", "/v1/payments", "used",
			map[string]any{"from_account": alice.ID, "to_account": bob.ID, "amount": 99, "currency": "AED"}, 422, "idempotency_key_reused"},
		{"currency mismatch", "POST", "/v1/payments", "k3",
			map[string]any{"from_account": alice.ID, "to_account": bob.ID, "amount": 1, "currency": "USD"}, 422, "currency_mismatch"},
		{"unknown payment", "GET", "/v1/payments/00000000-0000-7000-8000-000000000000", "", nil, 404, "not_found"},
		{"non-uuid account", "GET", "/v1/accounts/abc", "", nil, 404, "not_found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var e struct{ Code string }
			resp := c.do(tc.method, tc.path, tc.key, tc.body, &e)
			if resp.StatusCode != tc.status || e.Code != tc.code {
				t.Errorf("got %d %q, want %d %q", resp.StatusCode, e.Code, tc.status, tc.code)
			}
		})
	}
}
