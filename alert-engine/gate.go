package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// Key prefixes of the engine-side gate. Distinct from the producer-side
// alert:dedup:* keys written by the Spark jobs.
const (
	dedupKeyPrefix     = "engine:dedup:"
	rateLimitKeyPrefix = "engine:ratelimit:"
)

// Gate is an idempotent, TTL-bounded claim on a key.
//
// Admit claims key for alertID for ttl and reports whether alertID may pass:
// true when the key was free (and is now held by alertID) or is already held
// by the same alertID. The latter makes the gate safe under at-least-once
// delivery: a message redelivered after a crash between the gate and the
// Kafka commit is let through instead of being suppressed as its own
// duplicate. Production uses Redis; tests use an in-memory fake.
type Gate interface {
	Admit(ctx context.Context, key, alertID string, ttl time.Duration) (bool, error)
}

// dedupKey: one alert per (type, entity) per window — the engine's own
// replay/duplicate filter, independent of the producer-side dedup.
func dedupKey(a Alert) string {
	return dedupKeyPrefix + a.AlertType + ":" + a.EntityID
}

// rateKey: at most ONE alert per entity per window regardless of type —
// the "max 1 alert per player per 5 min" roadmap requirement.
func rateKey(a Alert) string {
	return rateLimitKeyPrefix + a.EntityType + ":" + a.EntityID
}

// shouldForward applies the dedup gate first, then the entity-wide rate
// limit; both must admit the alert.
//
// The gate fails open: when Redis errors, the alert is forwarded (and the
// error logged once) because the producers already dedup and dropping an
// alert is worse than delivering a duplicate.
func shouldForward(ctx context.Context, g Gate, a Alert, dedupTTL, rateTTL time.Duration) bool {
	for _, check := range []struct {
		key string
		ttl time.Duration
	}{
		{dedupKey(a), dedupTTL},
		{rateKey(a), rateTTL},
	} {
		ok, err := g.Admit(ctx, check.key, a.AlertID, check.ttl)
		if err != nil {
			log.Printf("[WARN] gate %s unavailable, failing open for alert %s: %v", check.key, a.AlertID, err)
			return true
		}
		if !ok {
			return false
		}
	}
	return true
}

// redisGate implements Gate with one atomic `SET key alertID NX GET` plus
// expiry (Redis >= 7.0): the reply is nil when the key was claimed,
// otherwise the current holder's alert ID.
type redisGate struct {
	rdb *redis.Client
}

func (g redisGate) Admit(ctx context.Context, key, alertID string, ttl time.Duration) (bool, error) {
	holder, err := g.rdb.SetArgs(ctx, key, alertID, redis.SetArgs{Mode: "NX", TTL: ttl, Get: true}).Result()
	switch {
	case errors.Is(err, redis.Nil):
		return true, nil // key was free and is now ours
	case err != nil:
		return false, err
	default:
		return holder == alertID, nil
	}
}
