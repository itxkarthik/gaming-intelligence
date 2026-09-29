package main

import (
	"encoding/json"
	"testing"
	"time"
)

// ─── fakeGate ──────────────────────────────────────────────────────────────

type fakeGate struct {
	seen map[string]bool
}

func newFakeGate() *fakeGate {
	return &fakeGate{seen: map[string]bool{}}
}

func (g *fakeGate) First(key string, _ time.Duration) bool {
	if g.seen[key] {
		return false
	}
	g.seen[key] = true
	return true
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
	raw, _ := json.Marshal(Alert{
		AlertID: "cheat_player_0001_1", AlertType: "CHEAT_DETECTED",
		Severity: "CRITICAL", EntityType: "PLAYER", EntityID: "player_0001",
		Message: "m", Timestamp: 1759000000000,
		Details: map[string]any{"suspicion": "0.91"},
	})
	a, err := parseAlert(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a.EntityID != "player_0001" || a.AlertType != "CHEAT_DETECTED" {
		t.Errorf("parsed wrong fields: %+v", a)
	}
}

func TestParseAlertRejectsMalformed(t *testing.T) {
	if _, err := parseAlert([]byte("{not json")); err == nil {
		t.Error("want error for malformed JSON, got nil")
	}
}

func TestParseAlertRejectsMissingRequiredFields(t *testing.T) {
	cases := []string{
		`{"alert_id":"a","entity_id":"e"}`,   // no alert_type
		`{"alert_type":"T","entity_id":"e"}`, // no alert_id
		`{"alert_id":"a","alert_type":"T"}`,  // no entity_id
	}
	for _, raw := range cases {
		if _, err := parseAlert([]byte(raw)); err == nil {
			t.Errorf("want error for %s, got nil", raw)
		}
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
	a := Alert{AlertType: "CHEAT_DETECTED", EntityType: "PLAYER", EntityID: "p1"}
	if !shouldForward(g, a, 5*time.Minute, 5*time.Minute) {
		t.Error("first alert must forward")
	}
}

func TestShouldForwardDeduplicatesSameAlert(t *testing.T) {
	g := newFakeGate()
	a := Alert{AlertType: "CHEAT_DETECTED", EntityType: "PLAYER", EntityID: "p1"}
	shouldForward(g, a, time.Minute, time.Minute)
	if shouldForward(g, a, time.Minute, time.Minute) {
		t.Error("duplicate (type+entity) must be suppressed")
	}
}

func TestShouldForwardRateLimitsAcrossTypes(t *testing.T) {
	g := newFakeGate()
	first := Alert{AlertType: "CHEAT_DETECTED", EntityType: "PLAYER", EntityID: "p1"}
	second := Alert{AlertType: "BEHAVIOR_ANOMALY", EntityType: "PLAYER", EntityID: "p1"}
	shouldForward(g, first, time.Minute, time.Minute)
	// Same entity, different type: dedup key passes but rate limit blocks —
	// "max 1 alert per player per 5 min".
	if shouldForward(g, second, time.Minute, time.Minute) {
		t.Error("second alert for same entity within window must be rate limited")
	}
}

func TestShouldForwardIndependentEntities(t *testing.T) {
	g := newFakeGate()
	a1 := Alert{AlertType: "CHEAT_DETECTED", EntityType: "PLAYER", EntityID: "p1"}
	a2 := Alert{AlertType: "CHEAT_DETECTED", EntityType: "PLAYER", EntityID: "p2"}
	if !shouldForward(g, a1, time.Minute, time.Minute) {
		t.Error("p1 must forward")
	}
	if !shouldForward(g, a2, time.Minute, time.Minute) {
		t.Error("p2 must forward independently of p1")
	}
}

func TestShouldForwardServerEntity(t *testing.T) {
	g := newFakeGate()
	a := Alert{AlertType: "SERVER_DEGRADED", EntityType: "SERVER", EntityID: "eu-central-1"}
	if !shouldForward(g, a, time.Minute, time.Minute) {
		t.Error("server alert must forward")
	}
	if shouldForward(g, a, time.Minute, time.Minute) {
		t.Error("server alert must dedup within window")
	}
}
