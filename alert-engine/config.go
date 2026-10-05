package main

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Environment variable names. They are part of the deployment contract
// (docker-compose.yml, README) and must not be renamed.
const (
	envKafkaBrokers     = "KAFKA_BOOTSTRAP_SERVERS"
	envGroupID          = "ALERT_ENGINE_GROUP"
	envTopic            = "ALERTS_TOPIC"
	envRedisHost        = "REDIS_HOST"
	envRedisPort        = "REDIS_PORT"
	envPostgresURL      = "POSTGRES_URL"
	envPostgresUser     = "POSTGRES_USER"
	envPostgresPassword = "POSTGRES_PASSWORD"
	envPostgresHost     = "POSTGRES_HOST"
	envPostgresPort     = "POSTGRES_PORT"
	envPostgresDB       = "POSTGRES_DB"
	envWebhookURL       = "ALERT_WEBHOOK_URL"
	envDedupTTL         = "ALERT_DEDUP_TTL"
	envRateLimitTTL     = "ALERT_RATE_LIMIT_TTL"
)

// Defaults used when the corresponding environment variable is unset.
const (
	defaultKafkaBrokers     = "localhost:9092"
	defaultGroupID          = "alert-engine"
	defaultTopic            = "alerts"
	defaultRedisHost        = "localhost"
	defaultRedisPort        = "6379"
	defaultPostgresUser     = "gaming"
	defaultPostgresPassword = "gaming_dev"
	defaultPostgresHost     = "localhost"
	defaultPostgresPort     = "5432"
	defaultPostgresDB       = "gaming_platform"

	// defaultDedupTTL and defaultRateLimitTTL implement the roadmap's
	// "max 1 alert per entity per 5 min" requirement.
	defaultDedupTTL     = 5 * time.Minute
	defaultRateLimitTTL = 5 * time.Minute
)

// redactedPlaceholder replaces secrets in anything that is logged.
const redactedPlaceholder = "xxxxx"

// Config is the validated runtime configuration of the engine.
type Config struct {
	Brokers   []string
	GroupID   string
	Topic     string
	RedisAddr string
	// PostgresURL carries credentials: log it only through redactDSN.
	PostgresURL string
	// WebhookURL usually embeds an access token: log it only through
	// webhookDescription. Empty disables the webhook.
	WebhookURL   string
	DedupTTL     time.Duration
	RateLimitTTL time.Duration
}

// configFromEnv loads and validates the configuration from the process
// environment.
func configFromEnv() (Config, error) {
	return loadConfig(os.Getenv)
}

// loadConfig builds a Config from lookup (os.Getenv in production) and
// validates it. Every problem found is reported in a single joined error so
// a misconfigured deployment can be fixed in one pass.
func loadConfig(lookup func(string) string) (Config, error) {
	// Values are used verbatim (not trimmed): a password may legitimately
	// contain leading or trailing spaces.
	env := func(key, fallback string) string {
		if v := lookup(key); v != "" {
			return v
		}
		return fallback
	}

	var problems []error

	brokers, err := parseBrokers(env(envKafkaBrokers, defaultKafkaBrokers))
	if err != nil {
		problems = append(problems, fmt.Errorf("%s: %w", envKafkaBrokers, err))
	}

	redisPort := env(envRedisPort, defaultRedisPort)
	if err := validatePort(redisPort); err != nil {
		problems = append(problems, fmt.Errorf("%s: %w", envRedisPort, err))
	}

	pgURL := env(envPostgresURL, "")
	if pgURL == "" {
		pgPort := env(envPostgresPort, defaultPostgresPort)
		if err := validatePort(pgPort); err != nil {
			problems = append(problems, fmt.Errorf("%s: %w", envPostgresPort, err))
		}
		pgURL = buildPostgresURL(
			env(envPostgresUser, defaultPostgresUser),
			env(envPostgresPassword, defaultPostgresPassword),
			env(envPostgresHost, defaultPostgresHost),
			pgPort,
			env(envPostgresDB, defaultPostgresDB),
		)
	}
	// pgx parse errors may echo parts of the connection string, so the
	// underlying error is deliberately not included.
	if _, err := pgxpool.ParseConfig(pgURL); err != nil {
		problems = append(problems, errors.New(
			"postgres connection settings (POSTGRES_URL or POSTGRES_*) are invalid; details withheld because they may contain credentials"))
	}

	webhookURL := env(envWebhookURL, "")
	if webhookURL != "" {
		if err := validateWebhookURL(webhookURL); err != nil {
			problems = append(problems, fmt.Errorf("%s: %w", envWebhookURL, err))
		}
	}

	dedupTTL, err := parseTTL(env(envDedupTTL, ""), defaultDedupTTL)
	if err != nil {
		problems = append(problems, fmt.Errorf("%s: %w", envDedupTTL, err))
	}
	rateLimitTTL, err := parseTTL(env(envRateLimitTTL, ""), defaultRateLimitTTL)
	if err != nil {
		problems = append(problems, fmt.Errorf("%s: %w", envRateLimitTTL, err))
	}

	if len(problems) > 0 {
		return Config{}, fmt.Errorf("invalid configuration: %w", errors.Join(problems...))
	}
	return Config{
		Brokers:      brokers,
		GroupID:      env(envGroupID, defaultGroupID),
		Topic:        env(envTopic, defaultTopic),
		RedisAddr:    net.JoinHostPort(env(envRedisHost, defaultRedisHost), redisPort),
		PostgresURL:  pgURL,
		WebhookURL:   webhookURL,
		DedupTTL:     dedupTTL,
		RateLimitTTL: rateLimitTTL,
	}, nil
}

// parseBrokers splits a comma-separated broker list, rejecting empty entries
// such as those produced by a trailing comma.
func parseBrokers(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			return nil, fmt.Errorf("broker entry %d is empty in %q", i+1, raw)
		}
		brokers = append(brokers, p)
	}
	return brokers, nil
}

func validatePort(port string) error {
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("port %q is not an integer in 1-65535", port)
	}
	return nil
}

// parseTTL parses a Go duration (e.g. "5m", "90s"); empty selects fallback.
func parseTTL(raw string, fallback time.Duration) (time.Duration, error) {
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, fmt.Errorf("duration %s must be positive", d)
	}
	return d, nil
}

// validateWebhookURL requires an absolute http(s) URL with a host. Error
// messages never echo the URL because webhook URLs embed access tokens.
func validateWebhookURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err // drop the echoed URL
		}
		return fmt.Errorf("not a valid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("URL has no host")
	}
	return nil
}

// buildPostgresURL assembles a postgres:// DSN. net/url escapes every
// component, so credentials containing @ : / ? # % stay intact.
func buildPostgresURL(user, password, host, port, db string) string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, password),
		Host:     net.JoinHostPort(host, port),
		Path:     "/" + db,
		RawQuery: url.Values{"sslmode": {"disable"}}.Encode(),
	}
	return u.String()
}

// redactDSN returns a loggable form of a Postgres connection string with the
// password removed from both the userinfo and a `password` query parameter.
// Strings that are not postgres URLs (e.g. keyword/value DSNs) cannot be
// redacted reliably and are replaced entirely.
func redactDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return "[redacted connection string]"
	}
	if q := u.Query(); q.Has("password") {
		q.Set("password", redactedPlaceholder)
		u.RawQuery = q.Encode()
	}
	return u.Redacted()
}

// webhookDescription is the loggable form of the webhook setting: the host
// only, never the path or query that carry the token.
func webhookDescription(raw string) string {
	if raw == "" {
		return "disabled"
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "enabled"
	}
	return "enabled (host " + u.Host + ")"
}
