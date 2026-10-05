package main

import (
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// envMap adapts a map to the lookup signature loadConfig expects.
func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig(envMap(nil))
	if err != nil {
		t.Fatalf("defaults must be valid: %v", err)
	}
	if got := strings.Join(cfg.Brokers, ","); got != defaultKafkaBrokers {
		t.Errorf("Brokers = %q, want %q", got, defaultKafkaBrokers)
	}
	if cfg.GroupID != "alert-engine" || cfg.Topic != "alerts" {
		t.Errorf("group/topic = %q/%q, want alert-engine/alerts", cfg.GroupID, cfg.Topic)
	}
	if cfg.RedisAddr != "localhost:6379" {
		t.Errorf("RedisAddr = %q", cfg.RedisAddr)
	}
	if cfg.DedupTTL != 5*time.Minute || cfg.RateLimitTTL != 5*time.Minute {
		t.Errorf("TTLs = %s/%s, want 5m/5m", cfg.DedupTTL, cfg.RateLimitTTL)
	}
	if cfg.WebhookURL != "" {
		t.Errorf("webhook must default to disabled")
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	cfg, err := loadConfig(envMap(map[string]string{
		envKafkaBrokers: " kafka-1:9092 , kafka-2:9092",
		envRedisHost:    "redis",
		envRedisPort:    "6380",
		envPostgresURL:  "postgres://u:p@db:5432/x",
		envWebhookURL:   "https://hooks.example.com/services/T0/B0/token",
		envDedupTTL:     "90s",
		envRateLimitTTL: "10m",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := strings.Join(cfg.Brokers, "|"); got != "kafka-1:9092|kafka-2:9092" {
		t.Errorf("Brokers = %q, want trimmed entries", got)
	}
	if cfg.RedisAddr != "redis:6380" {
		t.Errorf("RedisAddr = %q", cfg.RedisAddr)
	}
	if cfg.PostgresURL != "postgres://u:p@db:5432/x" {
		t.Errorf("POSTGRES_URL must be used verbatim")
	}
	if cfg.DedupTTL != 90*time.Second || cfg.RateLimitTTL != 10*time.Minute {
		t.Errorf("TTLs = %s/%s", cfg.DedupTTL, cfg.RateLimitTTL)
	}
}

func TestLoadConfigRejectsInvalid(t *testing.T) {
	const webhookToken = "s3cr3t-webhook-token"
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"trailing comma in brokers", map[string]string{envKafkaBrokers: "k1:9092,"}, envKafkaBrokers},
		{"blank broker entry", map[string]string{envKafkaBrokers: "k1:9092, ,k2:9092"}, envKafkaBrokers},
		{"unparseable dedup TTL", map[string]string{envDedupTTL: "five minutes"}, envDedupTTL},
		{"zero dedup TTL", map[string]string{envDedupTTL: "0s"}, envDedupTTL},
		{"negative rate-limit TTL", map[string]string{envRateLimitTTL: "-1m"}, envRateLimitTTL},
		{"unitless rate-limit TTL", map[string]string{envRateLimitTTL: "300"}, envRateLimitTTL},
		{"webhook without scheme", map[string]string{envWebhookURL: "hooks.example.com/" + webhookToken}, envWebhookURL},
		{"webhook ftp scheme", map[string]string{envWebhookURL: "ftp://hooks.example.com/" + webhookToken}, envWebhookURL},
		{"webhook without host", map[string]string{envWebhookURL: "https:///" + webhookToken}, envWebhookURL},
		{"webhook unparseable", map[string]string{envWebhookURL: "https://hooks.example.com/%zz" + webhookToken}, envWebhookURL},
		{"redis port not numeric", map[string]string{envRedisPort: "redis"}, envRedisPort},
		{"redis port out of range", map[string]string{envRedisPort: "70000"}, envRedisPort},
		{"postgres port not numeric", map[string]string{envPostgresPort: "pg"}, envPostgresPort},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadConfig(envMap(tc.env))
			if err == nil {
				t.Fatal("want error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not name %s", err, tc.wantErr)
			}
			if strings.Contains(err.Error(), webhookToken) {
				t.Errorf("error leaks the webhook token: %q", err)
			}
		})
	}
}

func TestLoadConfigReportsAllProblems(t *testing.T) {
	_, err := loadConfig(envMap(map[string]string{
		envKafkaBrokers: ",",
		envDedupTTL:     "nope",
		envRateLimitTTL: "0s",
		envWebhookURL:   "mailto:ops@example.com",
	}))
	if err == nil {
		t.Fatal("want error, got nil")
	}
	for _, key := range []string{envKafkaBrokers, envDedupTTL, envRateLimitTTL, envWebhookURL} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("joined error is missing %s: %q", key, err)
		}
	}
}

func TestLoadConfigInvalidPostgresURLWithholdsDetails(t *testing.T) {
	const password = "hunter2-not-logged"
	_, err := loadConfig(envMap(map[string]string{
		envPostgresURL: "postgres://user:" + password + "@db:notaport/x",
	}))
	if err == nil {
		t.Fatal("want error for invalid POSTGRES_URL")
	}
	if strings.Contains(err.Error(), password) {
		t.Errorf("error leaks the password: %q", err)
	}
}

// ─── DSN building + redaction ──────────────────────────────────────────────

func TestBuildPostgresURLEscapesCredentials(t *testing.T) {
	tests := []struct {
		name, user, password, db string
	}{
		{"plain", "gaming", "gaming_dev", "gaming_platform"},
		{"at and colon", "gaming", "p@ss:word", "gaming_platform"},
		{"slash question hash", "gaming", "a/b?c#d", "gaming_platform"},
		{"percent and space", "gam ing", "100% sure ", "gaming_platform"},
		{"user with at", "svc@tenant", "pw", "db"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dsn := buildPostgresURL(tc.user, tc.password, "db.internal", "5432", tc.db)
			// Round-trip through pgx's own parser: what the pool will see.
			pc, err := pgconn.ParseConfig(dsn)
			if err != nil {
				t.Fatalf("pgx cannot parse built DSN: %v", err)
			}
			if pc.User != tc.user || pc.Password != tc.password || pc.Database != tc.db {
				t.Errorf("round trip = user %q db %q (password match %v), want user %q db %q",
					pc.User, pc.Database, pc.Password == tc.password, tc.user, tc.db)
			}
			if pc.Host != "db.internal" || pc.Port != 5432 {
				t.Errorf("host/port = %s/%d", pc.Host, pc.Port)
			}

			redacted := redactDSN(dsn)
			if strings.Contains(redacted, tc.password) || strings.Contains(redacted, escapeForSearch(tc.password)) {
				t.Errorf("redactDSN leaks the password: %q", redacted)
			}
			if !strings.Contains(redacted, "db.internal") {
				t.Errorf("redactDSN should keep the host for diagnostics: %q", redacted)
			}
		})
	}
}

// escapeForSearch is the password as net/url encodes it in userinfo, so the
// leak check also catches a percent-encoded copy.
func escapeForSearch(password string) string {
	return strings.TrimPrefix(url.UserPassword("", password).String(), ":")
}

func TestRedactDSN(t *testing.T) {
	const secret = "TopSecret123"
	tests := []struct {
		name, dsn string
	}{
		{"userinfo password", "postgres://gaming:" + secret + "@db:5432/x?sslmode=disable"},
		{"postgresql scheme", "postgresql://gaming:" + secret + "@db/x"},
		{"query password", "postgres://gaming@db/x?password=" + secret},
		{"keyword DSN", "host=db user=gaming password=" + secret},
		{"garbage", "::" + secret},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := redactDSN(tc.dsn); strings.Contains(got, secret) {
				t.Errorf("redactDSN(%s) = %q leaks the password", tc.name, got)
			}
		})
	}
}

func TestWebhookDescriptionHidesToken(t *testing.T) {
	const token = "XyZ-token-123"
	if got := webhookDescription(""); got != "disabled" {
		t.Errorf("empty = %q, want disabled", got)
	}
	got := webhookDescription("https://hooks.example.com/services/" + token + "?k=" + token)
	if strings.Contains(got, token) {
		t.Errorf("description leaks token: %q", got)
	}
	if !strings.Contains(got, "hooks.example.com") {
		t.Errorf("description should name the host: %q", got)
	}
}

func TestParseTTL(t *testing.T) {
	if d, err := parseTTL("", time.Minute); err != nil || d != time.Minute {
		t.Errorf("empty = %s, %v; want fallback", d, err)
	}
	if d, err := parseTTL("250ms", time.Minute); err != nil || d != 250*time.Millisecond {
		t.Errorf("250ms = %s, %v", d, err)
	}
	if _, err := parseTTL("-5s", time.Minute); err == nil {
		t.Error("negative TTL must be rejected")
	}
}
