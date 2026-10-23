package main

import (
	"context"
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

// gateClaim is one key + TTL pair checked and claimed by a Gate.
type gateClaim struct {
	key string
	ttl time.Duration
}

// Gate is an atomic, idempotent, TTL-bounded claim across one or more keys.
//
// Admit checks every claim and, only when every key is either free or already
// held by alertID, claims all of them for alertID with their respective TTLs.
// Checking all keys before writing any prevents a rejected rate-limit check
// from leaving a phantom dedup claim (or vice versa), while allowing redelivery
// of the same alertID after a crash before Kafka offset commit.
type Gate interface {
	Admit(ctx context.Context, alertID string, claims ...gateClaim) (bool, error)
	Release(ctx context.Context, alertID string, claims ...gateClaim) error
}

// dedupKey: one alert per (type, entity, severity) per window. Keeping
// severity in the key lets a later escalation through both dedup layers.
// This is the only gate in front of alert history: distinct detections of
// one entity (a smurf, a behavior shift, a cheat) are all stored.
func dedupKey(a Alert) string {
	return dedupKeyPrefix + a.AlertType + ":" + a.EntityID + ":" + a.Severity
}

// rateKey: at most ONE webhook notification per entity per window per
// severity, regardless of type — the roadmap's "max 1 alert per player per
// 5 min", applied to paging only so it can never drop a different detection
// from history, and per severity so a routine WARNING can never mask the
// page for a CRITICAL that follows it.
func rateKey(a Alert) string {
	return rateLimitKeyPrefix + a.EntityType + ":" + a.EntityID + ":" + a.Severity
}

// shouldForward atomically applies the dedup gate: whether the alert is
// stored and published at all.
//
// The gate fails open: when Redis errors, the alert is forwarded (and the
// error logged) because the producers already dedup and dropping an alert
// is worse than delivering a duplicate.
func shouldForward(ctx context.Context, g Gate, a Alert, dedupTTL time.Duration) bool {
	return admit(ctx, g, a, "gate", gateClaim{key: dedupKey(a), ttl: dedupTTL})
}

// shouldNotify applies the per-entity rate limit to the webhook. It fails
// open like shouldForward: an extra page beats a missed one.
func shouldNotify(ctx context.Context, g Gate, a Alert, rateTTL time.Duration) bool {
	return admit(ctx, g, a, "webhook rate limit", gateClaim{key: rateKey(a), ttl: rateTTL})
}

func admit(ctx context.Context, g Gate, a Alert, name string, claims ...gateClaim) bool {
	ok, err := g.Admit(ctx, a.AlertID, claims...)
	if err != nil {
		log.Printf("[WARN] %s unavailable, failing open for alert %s: %v", name, a.AlertID, err)
		return true
	}
	return ok
}

// admitScript checks that every key in KEYS is either unset or already held
// by ARGV[1] (alertID). Only when all keys pass does it set each KEYS[i] to
// ARGV[1] with millisecond expiry ARGV[i+1].
var admitScript = redis.NewScript(`
local alert_id = ARGV[1]
for i = 1, #KEYS do
	local cur = redis.call('GET', KEYS[i])
	if cur ~= false and cur ~= alert_id then
		return 0
	end
end
for i = 1, #KEYS do
	local ttl_ms = tonumber(ARGV[i + 1])
	local cur = redis.call('GET', KEYS[i])
	if cur == false then
		redis.call('PSETEX', KEYS[i], ttl_ms, alert_id)
	end
end
return 1
`)

var releaseScript = redis.NewScript(`
for i = 1, #KEYS do
  if redis.call('GET', KEYS[i]) == ARGV[1] then redis.call('DEL', KEYS[i]) end
end
return 1
`)

// redisGate implements Gate atomically via a server-side Lua script.
type redisGate struct {
	rdb *redis.Client
}

func (g redisGate) Admit(ctx context.Context, alertID string, claims ...gateClaim) (bool, error) {
	if len(claims) == 0 {
		return true, nil
	}
	keys := make([]string, len(claims))
	args := make([]any, 1+len(claims))
	args[0] = alertID
	for i, c := range claims {
		keys[i] = c.key
		args[i+1] = max(int64(1), c.ttl.Milliseconds())
	}
	res, err := admitScript.Run(ctx, g.rdb, keys, args...).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

func (g redisGate) Release(ctx context.Context, alertID string, claims ...gateClaim) error {
	keys := make([]string, len(claims))
	for i, c := range claims {
		keys[i] = c.key
	}
	return releaseScript.Run(ctx, g.rdb, keys, alertID).Err()
}
