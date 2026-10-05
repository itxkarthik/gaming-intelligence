// Package main is the Gaming Intelligence Platform alert engine.
//
// It consumes the Kafka `alerts` topic written by the Spark streaming jobs
// and applies a second, engine-side processing layer (roadmap Phase 4):
//
//	dedup + rate limiting (Redis, max 1 alert / entity / 5 min by default)
//	severity classification / normalization
//	PostgreSQL alert history (idempotent on alert_id)
//	Redis publish on alerts:stream → FastAPI WebSocket fan-out
//	outbound webhook (Discord/Slack-style JSON POST, optional)
//
// Processing is at-least-once: a Kafka offset is committed only after its
// alert is stored in PostgreSQL, suppressed by the gate, or rejected as a
// poison pill. The gate and the insert are both idempotent per alert_id, so
// redelivery cannot duplicate history. If PostgreSQL stays unavailable past
// the retry budget the engine exits non-zero with the offset uncommitted,
// and its supervisor restarts it.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// Operational tuning.
const (
	// Startup wait for Redis/PostgreSQL, which may start alongside the
	// engine: 30 attempts, 2 s apart (~1 min).
	dependencyAttempts = 30
	dependencyInterval = 2 * time.Second

	// Alert inserts: 8 attempts, 0.5 s doubling to a 30 s cap (~1 min in
	// total) before the engine gives up and exits for a restart.
	storeAttempts     = 8
	storeInitialDelay = 500 * time.Millisecond
	storeMaxDelay     = 30 * time.Second

	webhookTimeout = 5 * time.Second
	kafkaMaxBytes  = 10e6 // largest fetch batch, bytes
)

func main() {
	if err := run(); err != nil {
		log.Printf("[FATAL] %v", err)
		os.Exit(1)
	}
}

// run loads the configuration and serves until SIGINT/SIGTERM. A signal is
// a clean shutdown even when it interrupts startup.
func run() error {
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	err = serve(ctx, cfg)
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return nil
	}
	return err
}

// serve owns every resource, so deferred cleanup always runs before main
// sets the exit code (log.Fatal would skip it).
func serve(ctx context.Context, cfg Config) error {
	startupRetry := retryPolicy{
		maxAttempts:  dependencyAttempts,
		initialDelay: dependencyInterval,
		maxDelay:     dependencyInterval,
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer closeLogged("redis client", rdb.Close)
	if err := startupRetry.do(ctx, "connect redis at "+cfg.RedisAddr, always,
		func(ctx context.Context) error { return rdb.Ping(ctx).Err() }); err != nil {
		return err
	}

	pool, err := connectPostgres(ctx, cfg.PostgresURL, startupRetry)
	if err != nil {
		return err
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("schema migration: %w", err)
	}

	// CommitInterval stays zero so CommitMessages is synchronous, which the
	// at-least-once guarantee relies on.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		GroupID:     cfg.GroupID,
		Topic:       cfg.Topic,
		MinBytes:    1,
		MaxBytes:    kafkaMaxBytes,
		StartOffset: kafka.FirstOffset,
	})
	// Deferred last so it runs first: leave the consumer group before the
	// sinks are closed.
	defer closeLogged("kafka reader", reader.Close)

	h := &handler{
		gate:         redisGate{rdb: rdb},
		store:        postgresStore{pool: pool},
		publisher:    redisPublisher{rdb: rdb},
		dedupTTL:     cfg.DedupTTL,
		rateLimitTTL: cfg.RateLimitTTL,
		storeRetry: retryPolicy{
			maxAttempts:  storeAttempts,
			initialDelay: storeInitialDelay,
			maxDelay:     storeMaxDelay,
		},
		now: time.Now,
	}
	if cfg.WebhookURL != "" {
		h.notifier = webhookNotifier{client: &http.Client{Timeout: webhookTimeout}, url: cfg.WebhookURL}
	}

	log.Printf("alert-engine up: brokers=%v group=%s topic=%s redis=%s postgres=%s webhook=%s dedup_ttl=%s rate_limit_ttl=%s",
		cfg.Brokers, cfg.GroupID, cfg.Topic, cfg.RedisAddr, redactDSN(cfg.PostgresURL),
		webhookDescription(cfg.WebhookURL), cfg.DedupTTL, cfg.RateLimitTTL)

	stats, err := consume(ctx, reader, h)
	log.Printf("shutdown: %s", stats)
	return err
}

// connectPostgres opens a pool and waits until the server answers. Messages
// only ever show the redacted DSN.
func connectPostgres(ctx context.Context, dsn string, policy retryPolicy) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn) // lazy: parses only, does not dial
	if err != nil {
		// The parse error may quote the DSN; config validation already
		// rejects unparseable settings, so this is not expected.
		return nil, errors.New("postgres: invalid connection settings")
	}
	if err := policy.do(ctx, "connect postgres at "+redactDSN(dsn), always, pool.Ping); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func closeLogged(name string, closeFn func() error) {
	if err := closeFn(); err != nil {
		log.Printf("[WARN] close %s: %v", name, err)
	}
}
