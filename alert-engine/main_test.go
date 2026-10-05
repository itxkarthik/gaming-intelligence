package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// ─── fakeGate ──────────────────────────────────────────────────────────────

// fakeGate is an in-memory Gate with the same idempotent semantics as
// redisGate (TTL is not modelled). Setting err makes every call fail.
type fakeGate struct {
	holders map[string]string
	err     error
}

func newFakeGate() *fakeGate {
	return &fakeGate{holders: map[string]string{}}
}

func (g *fakeGate) Admit(_ context.Context, key, alertID string, _ time.Duration) (bool, error) {
	if g.err != nil {
		return false, g.err
	}
	holder, held := g.holders[key]
	if !held {
		g.holders[key] = alertID
		return true, nil
	}
	return holder == alertID, nil
}

// ─── classify ──────────────────────────────────────────────────────────────

func TestClassifyPassesValidSeverity(t *testing.T) {
	for _, sev := range []string{"INFO", "WARNING", "CRITICAL"} {
		got := classify(Alert{Severity: sev, AlertType: "BEHAVIOR_ANOMALY"})
		if got != sev {
			t.Errorf("classify(%s) = %q, want passthrough", sev, got)
		}
	}
}

func TestClassifyDefaultsInvalidSeverity(t *testing.T) {
	tests := []struct {
		alertType, in, want string
	}{
		{"CHEAT_DETECTED", "", "CRITICAL"},
		{"CHEAT_DETECTED", "banana", "CRITICAL"},
		{"SMURF_DETECTED", "", "WARNING"},
		{"MATCH_QUALITY_LOW", "high", "WARNING"},
	}
	for _, tc := range tests {
		got := classify(Alert{AlertType: tc.alertType, Severity: tc.in})
		if got != tc.want {
			t.Errorf("classify(type=%s, severity=%q) = %q, want %q",
				tc.alertType, tc.in, got, tc.want)
		}
	}
}

// ─── parseAlert ────────────────────────────────────────────────────────────

func TestParseAlertValid(t *testing.T) {
	raw, err := json.Marshal(Alert{
		AlertID: "cheat_player_0001_1", AlertType: "CHEAT_DETECTED",
		Severity: "CRITICAL", EntityType: "PLAYER", EntityID: "player_0001",
		Message: "m", Timestamp: 1759000000000,
		Details: map[string]any{"suspicion": "0.91"},
	})
	if err != nil {
		t.Fatal(err)
	}
	a, err := parseAlert(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a.EntityID != "player_0001" || a.AlertType != "CHEAT_DETECTED" || a.Timestamp != 1759000000000 {
		t.Errorf("parsed wrong fields: %+v", a)
	}
}

func TestParseAlertRejectsMalformed(t *testing.T) {
	if _, err := parseAlert([]byte("{not json")); err == nil {
		t.Error("want error for malformed JSON, got nil")
	}
}

func TestParseAlertRejectsMissingRequiredFields(t *testing.T) {
	cases := map[string]string{
		"no alert_type":  `{"alert_id":"a","entity_type":"PLAYER","entity_id":"e"}`,
		"no alert_id":    `{"alert_type":"T","entity_type":"PLAYER","entity_id":"e"}`,
		"no entity_id":   `{"alert_id":"a","alert_type":"T","entity_type":"PLAYER"}`,
		"no entity_type": `{"alert_id":"a","alert_type":"T","entity_id":"e"}`,
		"empty object":   `{}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parseAlert([]byte(raw)); err == nil {
				t.Errorf("want error for %s, got nil", raw)
			}
		})
	}
}

// ─── keys ──────────────────────────────────────────────────────────────────

func TestKeyFormats(t *testing.T) {
	a := Alert{AlertType: "SMURF_DETECTED", EntityType: "PLAYER", EntityID: "player_0007"}
	if got, want := dedupKey(a), "engine:dedup:SMURF_DETECTED:player_0007"; got != want {
		t.Errorf("dedupKey = %q, want %q", got, want)
	}
	if got, want := rateKey(a), "engine:ratelimit:PLAYER:player_0007"; got != want {
		t.Errorf("rateKey = %q, want %q", got, want)
	}
}

// ─── shouldForward: dedup + rate limiting ──────────────────────────────────

func TestShouldForwardFirstTime(t *testing.T) {
	g := newFakeGate()
	a := Alert{AlertID: "a1", AlertType: "CHEAT_DETECTED", EntityType: "PLAYER", EntityID: "p1"}
	if !shouldForward(context.Background(), g, a, 5*time.Minute, 5*time.Minute) {
		t.Error("first alert must forward")
	}
}

func TestShouldForwardDeduplicatesSameTypeAndEntity(t *testing.T) {
	g := newFakeGate()
	first := Alert{AlertID: "a1", AlertType: "CHEAT_DETECTED", EntityType: "PLAYER", EntityID: "p1"}
	repeat := first
	repeat.AlertID = "a2"
	shouldForward(context.Background(), g, first, time.Minute, time.Minute)
	if shouldForward(context.Background(), g, repeat, time.Minute, time.Minute) {
		t.Error("new alert with duplicate (type+entity) must be suppressed")
	}
}

func TestShouldForwardRateLimitsAcrossTypes(t *testing.T) {
	g := newFakeGate()
	first := Alert{AlertID: "a1", AlertType: "CHEAT_DETECTED", EntityType: "PLAYER", EntityID: "p1"}
	second := Alert{AlertID: "a2", AlertType: "BEHAVIOR_ANOMALY", EntityType: "PLAYER", EntityID: "p1"}
	shouldForward(context.Background(), g, first, time.Minute, time.Minute)
	// Same entity, different type: dedup key passes but rate limit blocks —
	// "max 1 alert per player per 5 min".
	if shouldForward(context.Background(), g, second, time.Minute, time.Minute) {
		t.Error("second alert for same entity within window must be rate limited")
	}
}

func TestShouldForwardIndependentEntities(t *testing.T) {
	g := newFakeGate()
	a1 := Alert{AlertID: "a1", AlertType: "CHEAT_DETECTED", EntityType: "PLAYER", EntityID: "p1"}
	a2 := Alert{AlertID: "a2", AlertType: "CHEAT_DETECTED", EntityType: "PLAYER", EntityID: "p2"}
	if !shouldForward(context.Background(), g, a1, time.Minute, time.Minute) {
		t.Error("p1 must forward")
	}
	if !shouldForward(context.Background(), g, a2, time.Minute, time.Minute) {
		t.Error("p2 must forward independently of p1")
	}
}

func TestShouldForwardServerEntity(t *testing.T) {
	g := newFakeGate()
	a := Alert{AlertID: "s1", AlertType: "SERVER_DEGRADED", EntityType: "SERVER", EntityID: "eu-central-1"}
	b := a
	b.AlertID = "s2"
	if !shouldForward(context.Background(), g, a, time.Minute, time.Minute) {
		t.Error("server alert must forward")
	}
	if shouldForward(context.Background(), g, b, time.Minute, time.Minute) {
		t.Error("server alert must dedup within window")
	}
}

// TestShouldForwardIdempotentGate covers at-least-once redelivery: the same
// alert_id passing the gate again is let through, a different alert for the
// same keys is not.
func TestShouldForwardIdempotentGate(t *testing.T) {
	original := Alert{AlertID: "a1", AlertType: "CHEAT_DETECTED", EntityType: "PLAYER", EntityID: "p1"}
	otherSameKeys := original
	otherSameKeys.AlertID = "a2"
	otherTypeSameEntity := Alert{AlertID: "a3", AlertType: "SMURF_DETECTED", EntityType: "PLAYER", EntityID: "p1"}

	steps := []struct {
		name  string
		alert Alert
		want  bool
	}{
		{"first sighting forwards", original, true},
		{"different alert, same type+entity, suppressed", otherSameKeys, false},
		{"redelivery of same alert_id forwards", original, true},
		{"different type, same entity, rate limited", otherTypeSameEntity, false},
		{"second redelivery still forwards", original, true},
	}
	g := newFakeGate()
	for _, s := range steps {
		if got := shouldForward(context.Background(), g, s.alert, time.Minute, time.Minute); got != s.want {
			t.Errorf("%s: shouldForward(%s) = %v, want %v", s.name, s.alert.AlertID, got, s.want)
		}
	}
}

func TestShouldForwardFailsOpenOnGateError(t *testing.T) {
	g := newFakeGate()
	g.err = errors.New("redis: connection refused")
	a := Alert{AlertID: "a1", AlertType: "CHEAT_DETECTED", EntityType: "PLAYER", EntityID: "p1"}
	for i := range 2 {
		if !shouldForward(context.Background(), g, a, time.Minute, time.Minute) {
			t.Errorf("call %d: gate error must fail open (forward)", i+1)
		}
	}
}
