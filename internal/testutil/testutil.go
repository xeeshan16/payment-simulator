// Package testutil gives each integration test its own freshly migrated
// Postgres database, so tests can run in parallel without sharing state.
package testutil

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xeeshan16/payment-simulator/internal/db"
	"github.com/xeeshan16/payment-simulator/internal/id"
)

// EnvDatabaseURL points at a Postgres server where tests may create and drop
// databases, e.g. postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable
const EnvDatabaseURL = "PAYSIM_TEST_DATABASE_URL"

// NewDB creates an empty database, migrates it and drops it when the test
// ends. Tests are skipped when EnvDatabaseURL is unset, so `go test ./...`
// still works without Postgres; CI always sets it.
func NewDB(t testing.TB) *pgxpool.Pool {
	t.Helper()
	adminURL := os.Getenv(EnvDatabaseURL)
	if adminURL == "" {
		t.Skipf("%s not set; skipping integration test", EnvDatabaseURL)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer admin.Close(ctx)

	name := "paysim_test_" + strings.ReplaceAll(id.New(), "-", "")[20:]
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}

	u, err := url.Parse(adminURL)
	if err != nil {
		t.Fatalf("parse %s: %v", EnvDatabaseURL, err)
	}
	u.Path = "/" + name
	q := u.Query()
	q.Set("pool_max_conns", "10") // parallel tests stay under max_connections
	u.RawQuery = q.Encode()
	pool, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		admin, err := pgx.Connect(ctx, adminURL)
		if err != nil {
			t.Logf("cleanup connect: %v", err)
			return
		}
		defer admin.Close(ctx)
		if _, err := admin.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", name)); err != nil {
			t.Logf("drop database %s: %v", name, err)
		}
	})
	return pool
}

// Logger discards output unless the test runs with -v.
func Logger(t testing.TB) *slog.Logger {
	if testing.Verbose() {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
