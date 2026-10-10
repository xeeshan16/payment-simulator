// Command paysim runs the payment simulator: HTTP API, outbox relay and the
// sample notifications consumer, in one process.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xeeshan16/payment-simulator/internal/consumer"
	"github.com/xeeshan16/payment-simulator/internal/db"
	"github.com/xeeshan16/payment-simulator/internal/httpapi"
	"github.com/xeeshan16/payment-simulator/internal/kafka"
	"github.com/xeeshan16/payment-simulator/internal/outbox"
	"github.com/xeeshan16/payment-simulator/internal/payment"
)

type config struct {
	httpAddr    string
	databaseURL string
	brokers     []string
	topic       string
	dlqTopic    string
	group       string
}

func loadConfig() (config, error) {
	c := config{
		httpAddr:    env("PAYSIM_HTTP_ADDR", ":8080"),
		databaseURL: os.Getenv("PAYSIM_DATABASE_URL"),
		brokers:     strings.Split(env("PAYSIM_KAFKA_BROKERS", "localhost:9092"), ","),
		topic:       env("PAYSIM_TOPIC", "payments.events"),
		group:       env("PAYSIM_CONSUMER_GROUP", "paysim-notifications"),
	}
	c.dlqTopic = env("PAYSIM_DLQ_TOPIC", c.topic+".dlq")
	if c.databaseURL == "" {
		return c, errors.New("PAYSIM_DATABASE_URL is required")
	}
	return c, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx, cfg.databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer producer.Close()

	consumerClient, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.brokers...),
		kgo.ConsumerGroup(cfg.group),
		kgo.ConsumeTopics(cfg.topic),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	defer consumerClient.Close()

	svc := payment.NewService(pool, cfg.topic)
	relay := outbox.NewRelay(pool, kafka.Publisher{Client: producer}, 100, 200*time.Millisecond, log)
	notifications := consumer.New(consumerClient, pool, consumer.Notifications, consumer.Config{
		Name:     "notifications",
		DLQTopic: cfg.dlqTopic,
	}, log)

	srv := &http.Server{
		Addr:              cfg.httpAddr,
		Handler:           httpapi.New(svc, log),
		ReadHeaderTimeout: 5 * time.Second,
	}

	var wg sync.WaitGroup
	errs := make(chan error, 3)
	wg.Add(3)
	go func() {
		defer wg.Done()
		log.Info("http listening", "addr", cfg.httpAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- fmt.Errorf("http server: %w", err)
		}
	}()
	go func() { defer wg.Done(); errs <- relay.Run(ctx) }()
	go func() { defer wg.Done(); errs <- notifications.Run(ctx) }()

	select {
	case <-ctx.Done():
	case err := <-errs:
		if err != nil {
			log.Error("component stopped", "err", err)
		}
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	wg.Wait()
	log.Info("stopped")
	return nil
}
