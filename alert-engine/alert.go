package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Alert mirrors schemas/alert.avsc; it is transported as JSON on Kafka.
type Alert struct {
	AlertID    string         `json:"alert_id"`
	AlertType  string         `json:"alert_type"`
	Severity   string         `json:"severity"`
	EntityType string         `json:"entity_type"`
	EntityID   string         `json:"entity_id"`
	Message    string         `json:"message"`
	Details    map[string]any `json:"details"`
	Timestamp  int64          `json:"timestamp"` // Unix epoch milliseconds
}

// Severity enum (schemas/alert.avsc).
const (
	severityInfo     = "INFO"
	severityWarning  = "WARNING"
	severityCritical = "CRITICAL"
)

// alertTypeCheat is the alert type classified CRITICAL when the producer
// did not supply a valid severity.
const alertTypeCheat = "CHEAT_DETECTED"

var validSeverities = map[string]bool{
	severityInfo:     true,
	severityWarning:  true,
	severityCritical: true,
}

// classify normalizes severity to the INFO/WARNING/CRITICAL enum. A payload
// carrying a valid severity passes through; anything else is classified from
// the alert type (cheat detections are critical, everything else a warning).
func classify(a Alert) string {
	if validSeverities[a.Severity] {
		return a.Severity
	}
	if a.AlertType == alertTypeCheat {
		return severityCritical
	}
	return severityWarning
}

// parseAlert decodes and validates a Kafka message value. Any error means
// the message can never be processed (a poison pill), not a transient
// failure.
func parseAlert(data []byte) (Alert, error) {
	var a Alert
	if err := json.Unmarshal(data, &a); err != nil {
		return Alert{}, fmt.Errorf("malformed alert payload: %w", err)
	}
	var missing []string
	for _, f := range []struct{ name, value string }{
		{"alert_id", a.AlertID},
		{"alert_type", a.AlertType},
		{"entity_type", a.EntityType}, // part of the rate-limit key
		{"entity_id", a.EntityID},
	} {
		if f.value == "" {
			missing = append(missing, f.name)
		}
	}
	if len(missing) > 0 {
		return Alert{}, fmt.Errorf("alert missing required fields: %s", strings.Join(missing, ", "))
	}
	return a, nil
}

// normalize fills in everything a producer may omit, so Postgres, the live
// stream and the webhook all see one identical alert: a valid severity, a
// real timestamp, and details as an object rather than null.
func normalize(a Alert, messageTime, now time.Time) Alert {
	a.Severity = classify(a)
	a.Timestamp = resolveTimestamp(a.Timestamp, messageTime, now)
	if a.Details == nil {
		a.Details = map[string]any{}
	}
	return a
}

// resolveTimestamp returns ts (epoch ms) when the producer set a positive
// value, otherwise the Kafka message time, otherwise now — so a missing
// timestamp is never stored as 1970.
func resolveTimestamp(ts int64, messageTime, now time.Time) int64 {
	switch {
	case ts > 0:
		return ts
	case !messageTime.IsZero():
		return messageTime.UnixMilli()
	default:
		return now.UnixMilli()
	}
}
