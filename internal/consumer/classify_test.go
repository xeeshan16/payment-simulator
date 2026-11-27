package consumer

import (
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsInfrastructure(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"marked by handler", fmt.Errorf("send email: %w", ErrUnavailable), true},
		{"connection lost", &pgconn.PgError{Code: "08006"}, true},
		{"too many connections", &pgconn.PgError{Code: "53300"}, true},
		{"server shutting down", fmt.Errorf("insert: %w", &pgconn.PgError{Code: "57P01"}), true},
		{"network error", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, true},
		{"unique violation", &pgconn.PgError{Code: "23505"}, false},
		{"check violation", fmt.Errorf("insert: %w", &pgconn.PgError{Code: "23514"}), false},
		{"plain handler error", errors.New("permanent failure"), false},
	}
	for _, tc := range cases {
		if got := isInfrastructure(tc.err); got != tc.want {
			t.Errorf("%s: isInfrastructure(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}
