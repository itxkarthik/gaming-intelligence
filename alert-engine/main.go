// Package main is the Gaming Intelligence Platform alert engine.
//
// It consumes the Kafka `alerts` topic written by the Spark streaming jobs
// and applies a second, engine-side processing layer (roadmap Phase 4):
//
//	dedup + rate limiting (Redis SET NX, max 1 alert / entity / 5 min)
//	severity classification / normalization
//	PostgreSQL alert history (idempotent on alert_id)
//	outbound webhook (Discord/Slack-style JSON POST, optional)
//	Redis publish on alerts:stream → FastAPI WebSocket fan-out
//
// Failures are logged and never terminate the consume loop; PostgreSQL
// inserts are idempotent so at-least-once Kafka delivery cannot duplicate
// history rows.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// ─── Config ────────────────────────────────────────────────────────────────

type Config struct {
	Brokers      []string
	GroupID      string
	Topic        string
	RedisAddr    string
	PGURL        string
	WebhookURL   string
	DedupTTL     time.Duration
	RateLimitTTL time.Duration
}

func configFromEnv() Config {
	brokers := getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	return Config{
		Brokers:      strings.Split(brokers, ","),
		GroupID:      getenv("ALERT_ENGINE_GROUP", "alert-engine"),
		Topic:        getenv("ALERTS_TOPIC", "alerts"),
		RedisAddr:    getenv("REDIS_HOST", "localhost") + ":" + getenv("REDIS_PORT", "6379"),
		PGURL:        pgURLOnfig(),
		WebhookURL:   os.Getenv("ALERT_WEBHOOK_URL"),
		DedupTTL:     5 * time.Minute,
		RateLimitTTL: 5 * time.Minute,
	}
}

func pgURLOnfig() string {
	if u := os.Getenv("POSTGRES_URL"); u != "" {
		return u
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		getenv("POSTGRES_USER", "gaming"),
		getenv("POSTGRES_PASSWORD", "gaming_dev"),
		getenv("POSTGRES_HOST", "localhost"),
		getenv("POSTGRES_PORT", "5432"),
		getenv("POSTGRES_DB", "gaming_platform"))
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ─── Alert payload (mirrors schemas/alert.avsc, transported as JSON) ───────

type Alert struct {
	AlertID    string         `json:"alert_id"`
	AlertType  string         `json:"alert_type"`
	Severity   string         `json:"severity"`
	EntityType string         `json:"entity_type"`
	EntityID   string         `json:"entity_id"`
	Message    string         `json:"message"`
	Details    map[string]any `json:"details"`
	Timestamp  int64          `json:"timestamp"`
}

// validSeverities mirrors the Severity enum in schemas/alert.avsc.
var validSeverities = map[string]bool{"INFO": true, "WARNING": true, "CRITICAL": true}

// classify normalizes severity to the INFO/WARNING/CRITICAL enum. A payload
// carrying a valid severity passes through; anything else is classified from
// the alert type (cheat detections are critical, everything else a warning).
func classify(a Alert) string {
	if validSeverities[a.Severity] {
		return a.Severity
	}
	if a.AlertType == "CHEAT_DETECTED" {
		return "CRITICAL"
	}
	return "WARNING"
}

func parseAlert(data []byte) (Alert, error) {
	var a Alert
	if err := json.Unmarshal(data, &a); err != nil {
		return Alert{}, fmt.Errorf("malformed alert payload: %w", err)
	}
	if a.AlertID == "" || a.EntityID == "" || a.AlertType == "" {
		return Alert{}, fmt.Errorf("alert missing required fields (alert_id/entity_id/alert_type)")
	}
	return a, nil
}

// ─── Dedup + rate limiting gate ────────────────────────────────────────────

// Gate reports whether a key is seen for the first time within ttl.
// Production uses Redis SET NX EX; tests use an in-memory fake.
type Gate interface {
	First(key string, ttl time.Duration) bool
}

// dedupKey: one alert per (type, entity) per window — the engine's own
// replay/duplicate filter, independent of the producer-side dedup.
func dedupKey(a Alert) string {
	return "engine:dedup:" + a.AlertType + ":" + a.EntityID
}

// rateKey: at most ONE alert per entity per window regardless of type —
// the "max 1 alert per player per 5 min" roadmap requirement.
func rateKey(a Alert) string {
	return "engine:ratelimit:" + a.EntityType + ":" + a.EntityID
}

// shouldForward applies the dedup gate first (cheap key), then the
// entity-wide rate limit. Both must be first-seen.
func shouldForward(g Gate, a Alert, dedupTTL, rateTTL time.Duration) bool {
	if !g.First(dedupKey(a), dedupTTL) {
		return false
	}
	return g.First(rateKey(a), rateTTL)
}

type redisGate struct {
	rdb *redis.Client
	ctx context.Context
}

func (g redisGate) First(key string, ttl time.Duration) bool {
	ok, err := g.rdb.SetNX(g.ctx, key, "1", ttl).Result()
	if err != nil {
		// Fail open: a Redis blip should not silently drop alert delivery —
		// the engine-side layer is a nicety, the producers already dedup.
		log.Printf("[WARN] redis gate %s: %v", key, err)
		return true
	}
	return ok
}

// ─── Sinks ─────────────────────────────────────────────────────────────────

const schemaSQL = `
CREATE TABLE IF NOT EXISTS alert_history (
	id         BIGSERIAL PRIMARY KEY,
	alert_id   TEXT UNIQUE NOT NULL,
	alert_type TEXT NOT NULL,
	severity   TEXT NOT NULL,
	entity_type TEXT NOT NULL,
	entity_id  TEXT NOT NULL,
	message    TEXT NOT NULL,
	details    JSONB NOT NULL DEFAULT '{}'::jsonb,
	ts         TIMESTAMPTZ NOT NULL,
	received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);`

// storeAlert inserts history idempotently — ON CONFLICT keeps at-least-once
// Kafka delivery from duplicating rows.
func storeAlert(ctx context.Context, pool *pgxpool.Pool, a Alert, severity string) error {
	details, err := json.Marshal(a.Details)
	if err != nil {
		details = []byte("{}")
	}
	if a.Details == nil {
		details = []byte("{}")
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO alert_history
			(alert_id, alert_type, severity, entity_type, entity_id, message, details, ts)
		VALUES ($1, $2, $3, $4, $5, $6, $7, to_timestamp($8::double precision / 1000))
		ON CONFLICT (alert_id) DO NOTHING`,
		a.AlertID, a.AlertType, severity, a.EntityType, a.EntityID,
		a.Message, details, a.Timestamp)
	return err
}

func publishToWebSocketChannel(ctx context.Context, rdb *redis.Client, raw []byte) error {
	return rdb.Publish(ctx, "alerts:stream", raw).Err()
}

func sendWebhook(ctx context.Context, client *http.Client, url string, a Alert, severity string) error {
	payload, _ := json.Marshal(map[string]any{
		"alert_id":  a.AlertID,
		"type":      a.AlertType,
		"severity":  severity,
		"entity":    a.EntityType + "/" + a.EntityID,
		"text":      a.Message,
		"timestamp": a.Timestamp,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook status %d", resp.StatusCode)
	}
	return nil
}

// ─── Main ──────────────────────────────────────────────────────────────────

func main() {
	cfg := configFromEnv()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Redis
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("[FATAL] redis at %s: %v", cfg.RedisAddr, err)
	}
	gate := redisGate{rdb: rdb, ctx: ctx}

	// PostgreSQL (retry: engine may start alongside its dependencies)
	var pool *pgxpool.Pool
	var err error
	for i := 0; i < 30; i++ {
		pool, err = pgxpool.New(ctx, cfg.PGURL)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				break
			}
			pool.Close()
		}
		log.Printf("[WAIT] postgres not ready (%d/30): %v", i+1, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
	if pool == nil || err != nil {
		log.Fatalf("[FATAL] postgres at %s: %v", cfg.PGURL, err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		log.Fatalf("[FATAL] schema migration: %v", err)
	}

	// Kafka consumer
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		GroupID:     cfg.GroupID,
		Topic:       cfg.Topic,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	httpClient := &http.Client{Timeout: 5 * time.Second}
	log.Printf("alert-engine up: brokers=%v group=%s topic=%s redis=%s webhook=%s",
		cfg.Brokers, cfg.GroupID, cfg.Topic, cfg.RedisAddr,
		map[bool]string{true: cfg.WebhookURL, false: "(disabled)"}[cfg.WebhookURL != ""])

	var processed, forwarded int
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("shutdown: processed=%d forwarded=%d", processed, forwarded)
				return
			}
			log.Printf("[WARN] kafka read: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		processed++

		alert, err := parseAlert(msg.Value)
		if err != nil {
			// Malformed messages are counted and skipped — never crash the loop.
			log.Printf("[WARN] skip: %v", err)
			continue
		}
		severity := classify(alert)

		if !shouldForward(gate, alert, cfg.DedupTTL, cfg.RateLimitTTL) {
			log.Printf("suppressed type=%s entity=%s (dedup/rate limit)", alert.AlertType, alert.EntityID)
			continue
		}
		forwarded++

		if err := storeAlert(ctx, pool, alert, severity); err != nil {
			log.Printf("[WARN] postgres insert %s: %v", alert.AlertID, err)
		}
		if err := publishToWebSocketChannel(ctx, rdb, msg.Value); err != nil {
			log.Printf("[WARN] redis publish %s: %v", alert.AlertID, err)
		}
		if cfg.WebhookURL != "" {
			if err := sendWebhook(ctx, httpClient, cfg.WebhookURL, alert, severity); err != nil {
				log.Printf("[WARN] webhook %s: %v", alert.AlertID, err)
			}
		}
		log.Printf("forwarded type=%s severity=%s entity=%s/%s id=%s",
			alert.AlertType, severity, alert.EntityType, alert.EntityID, alert.AlertID)
	}
}
