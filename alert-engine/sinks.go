package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// alertsStreamChannel is the Redis pub/sub channel the FastAPI service
// forwards to /ws/alerts and the dashboard.
const alertsStreamChannel = "alerts:stream"

// maxWebhookResponseDrain bounds how much of a webhook response body is read
// so the keep-alive connection can be reused.
const maxWebhookResponseDrain = 64 << 10

// AlertStore persists alert history.
type AlertStore interface {
	Store(ctx context.Context, a Alert) error
}

// Publisher fans an alert out to live subscribers.
type Publisher interface {
	Publish(ctx context.Context, a Alert) error
}

// Notifier delivers an alert to an external system.
type Notifier interface {
	Notify(ctx context.Context, a Alert) error
}

// errUnstorable marks a store failure caused by the alert itself; retrying
// the same alert can never succeed.
var errUnstorable = errors.New("alert cannot be stored")

// isPermanentStoreError reports whether err is caused by the alert's content
// rather than by the database being unavailable: SQLSTATE class 22 (data
// exception, e.g. a NUL byte in text or jsonb) or 23 (integrity constraint
// violation), or an alert that could not be encoded. Everything else
// (network errors, timeouts, a restarting server) is treated as transient.
func isPermanentStoreError(err error) bool {
	if errors.Is(err, errUnstorable) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) >= 2 {
		switch pgErr.Code[:2] {
		case "22", "23":
			return true
		}
	}
	return false
}

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

const insertAlertSQL = `
INSERT INTO alert_history
	(alert_id, alert_type, severity, entity_type, entity_id, message, details, ts)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (alert_id) DO NOTHING`

// postgresStore writes alert_history. Inserts are idempotent on alert_id,
// so at-least-once Kafka delivery cannot duplicate rows.
type postgresStore struct {
	pool *pgxpool.Pool
}

func (s postgresStore) Store(ctx context.Context, a Alert) error {
	detailsJSON, err := json.Marshal(a.Details)
	if err != nil {
		return fmt.Errorf("%w: encode details: %w", errUnstorable, err)
	}
	if _, err := s.pool.Exec(ctx, insertAlertSQL,
		a.AlertID, a.AlertType, a.Severity, a.EntityType, a.EntityID,
		a.Message, detailsJSON, time.UnixMilli(a.Timestamp).UTC()); err != nil {
		return fmt.Errorf("insert alert_history: %w", err)
	}
	return nil
}

// redisPublisher publishes the stored alert as JSON (same fields as the
// producer's payload), so WebSocket clients see exactly what history holds.
type redisPublisher struct {
	rdb *redis.Client
}

func (p redisPublisher) Publish(ctx context.Context, a Alert) error {
	payload, err := json.Marshal(a)
	if err != nil {
		return fmt.Errorf("encode alert: %w", err)
	}
	if err := p.rdb.Publish(ctx, alertsStreamChannel, payload).Err(); err != nil {
		return fmt.Errorf("publish %s: %w", alertsStreamChannel, err)
	}
	return nil
}

// webhookNotifier POSTs a Discord/Slack-style JSON summary.
type webhookNotifier struct {
	client *http.Client
	url    string // embeds a token: never include it in errors or logs
}

type webhookPayload struct {
	AlertID   string `json:"alert_id"`
	Type      string `json:"type"`
	Severity  string `json:"severity"`
	Entity    string `json:"entity"`
	Text      string `json:"text"`
	Timestamp int64  `json:"timestamp"`
}

func (n webhookNotifier) Notify(ctx context.Context, a Alert) error {
	body, err := json.Marshal(webhookPayload{
		AlertID:   a.AlertID,
		Type:      a.AlertType,
		Severity:  a.Severity,
		Entity:    a.EntityType + "/" + a.EntityID,
		Text:      a.Message,
		Timestamp: a.Timestamp,
	})
	if err != nil {
		return fmt.Errorf("encode webhook payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build webhook request: %w", stripURL(err))
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook POST: %w", stripURL(err))
	}
	defer resp.Body.Close()
	// Drain (bounded) so the connection returns to the keep-alive pool.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxWebhookResponseDrain))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook responded %s", resp.Status)
	}
	return nil
}

// stripURL removes the request URL that net/http embeds in *url.Error, so a
// webhook token cannot leak into logs.
func stripURL(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return fmt.Errorf("%s: %w", urlErr.Op, urlErr.Err)
	}
	return err
}
