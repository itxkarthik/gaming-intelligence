package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultProfilesDir = "./profiles"
	// repoProfilesDir is tried when the binary runs from the repository root
	// and --profiles-dir was left at its default.
	repoProfilesDir = "simulator/profiles"

	// maxEventsPerSec bounds the pacer interval (1s / rate) away from zero.
	maxEventsPerSec = 1_000_000
	// minMatchDuration keeps a match long enough to produce combat at all.
	minMatchDuration = 10 * time.Second

	// maxShotsPerMatchPerSec caps combat intensity. With persistent HP,
	// hotter matches wipe whole teams faster than they respawn and the
	// target rate can no longer be met; measured, idling starts above ~15.
	// More traffic therefore means more concurrent matches, as in a real
	// fleet, not hotter ones.
	maxShotsPerMatchPerSec = 10
	// queueShare is the extra player pool, beyond full lobbies, that waits
	// in matchmaking when --players is sized automatically.
	queueShare = 0.25
)

// Config is the fully validated simulator configuration.
type Config struct {
	Players            int
	MatchesConcurrent  int
	EventsPerSec       int
	CheaterRatio       float64
	SmurfRatio         float64
	ToxicRatio         float64
	BehaviorShiftRatio float64
	LateEventRatio     float64
	ServerCount        int
	DegradedServers    []string
	KafkaBrokers       []string
	Duration           time.Duration
	MatchDuration      time.Duration
	ProfilesDir        string
	Seed               uint64
	DryRun             bool
}

// parseConfig parses and validates command-line arguments. It returns
// flag.ErrHelp when -h/--help was requested (usage already printed).
func parseConfig(args []string, stderr io.Writer) (Config, error) {
	var (
		cfg      Config
		brokers  string
		degraded string
	)
	fs := flag.NewFlagSet("simulator", flag.ContinueOnError)
	fs.SetOutput(stderr)

	fs.IntVar(&cfg.Players, "players", 0, "player pool size; 0 sizes it from the match count (+25% queue)")
	fs.IntVar(&cfg.MatchesConcurrent, "matches-concurrent", 0, "matches running at the same time; 0 sizes it from --events-per-sec")
	fs.IntVar(&cfg.EventsPerSec, "events-per-sec", 1000, "target SHOT events published per second")
	fs.Float64Var(&cfg.CheaterRatio, "cheater-ratio", 0.05, "share of accounts that are cheaters [0,1]")
	fs.Float64Var(&cfg.SmurfRatio, "smurf-ratio", 0.05, "share of accounts that are smurfs [0,1]")
	fs.Float64Var(&cfg.ToxicRatio, "toxic-ratio", 0.05, "share of accounts that are toxic [0,1]")
	fs.Float64Var(&cfg.BehaviorShiftRatio, "behavior-shift", 0.05, "share of players whose accuracy jumps at the run midpoint [0,1]; 0 disables")
	fs.Float64Var(&cfg.LateEventRatio, "late-event-ratio", 0.05, "share of SHOT events emitted with a delayed event time [0,1]")
	fs.IntVar(&cfg.ServerCount, "server-count", 10, "game servers (server-01 … server-NN)")
	fs.StringVar(&degraded, "degraded-servers", "server-02", "comma-separated servers that report degraded health; empty disables")
	fs.StringVar(&brokers, "kafka-brokers", "localhost:9094", "comma-separated Kafka bootstrap brokers (host:port)")
	fs.DurationVar(&cfg.Duration, "duration", 5*time.Minute, "total run time (e.g. 90s, 5m, 1h)")
	fs.DurationVar(&cfg.MatchDuration, "match-duration", 3*time.Minute, "length of one match before the lobby rotates")
	fs.StringVar(&cfg.ProfilesDir, "profiles-dir", defaultProfilesDir, "directory with the archetype YAML profiles")
	fs.Uint64Var(&cfg.Seed, "seed", 0, "random seed for a reproducible run; 0 picks one at random")
	fs.BoolVar(&cfg.DryRun, "dry-run", false, "generate events without connecting to Kafka")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if fs.NArg() > 0 {
		return Config{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}

	cfg.KafkaBrokers = splitList(brokers)
	cfg.DegradedServers = splitList(degraded)
	cfg.autoSize()

	explicit := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
	if !explicit["profiles-dir"] && !dirExists(cfg.ProfilesDir) && dirExists(repoProfilesDir) {
		cfg.ProfilesDir = repoProfilesDir
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// autoSize fills in a zero (automatic) match count and player pool. Invalid
// rates are left for validate to report.
func (c *Config) autoSize() {
	if c.MatchesConcurrent == 0 && c.EventsPerSec > 0 {
		c.MatchesConcurrent = ceilDiv(c.EventsPerSec, maxShotsPerMatchPerSec)
	}
	if c.Players == 0 && c.MatchesConcurrent > 0 {
		seats := c.MatchesConcurrent * playersPerMatch
		c.Players = seats + int(math.Ceil(float64(seats)*queueShare))
	}
}

func ceilDiv(a, b int) int { return (a + b - 1) / b }

// validate reports every problem at once instead of failing on the first.
func (c Config) validate() error {
	var errs []error
	check := func(ok bool, format string, args ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf(format, args...))
		}
	}

	check(c.Players > 0, "--players must be > 0, or 0 to size automatically (got %d)", c.Players)
	check(c.MatchesConcurrent > 0, "--matches-concurrent must be > 0, or 0 to size automatically (got %d)", c.MatchesConcurrent)
	if c.Players > 0 && c.MatchesConcurrent > 0 {
		need := c.MatchesConcurrent * playersPerMatch
		check(c.Players >= need,
			"--players (%d) must be at least --matches-concurrent × %d = %d so every lobby is full",
			c.Players, playersPerMatch, need)
	}
	check(c.EventsPerSec > 0 && c.EventsPerSec <= maxEventsPerSec,
		"--events-per-sec must be in [1, %d] (got %d)", maxEventsPerSec, c.EventsPerSec)
	if c.EventsPerSec > 0 && c.MatchesConcurrent > 0 {
		need := ceilDiv(c.EventsPerSec, maxShotsPerMatchPerSec)
		check(c.MatchesConcurrent >= need,
			"--events-per-sec %d needs at least %d concurrent matches at <= %d shots/s each (got --matches-concurrent %d; omit it to size automatically)",
			c.EventsPerSec, need, maxShotsPerMatchPerSec, c.MatchesConcurrent)
	}
	check(c.ServerCount > 0, "--server-count must be > 0 (got %d)", c.ServerCount)

	for _, r := range []struct {
		flag  string
		value float64
	}{
		{"cheater-ratio", c.CheaterRatio},
		{"smurf-ratio", c.SmurfRatio},
		{"toxic-ratio", c.ToxicRatio},
		{"behavior-shift", c.BehaviorShiftRatio},
		{"late-event-ratio", c.LateEventRatio},
	} {
		check(r.value >= 0 && r.value <= 1, "--%s must be in [0,1] (got %v)", r.flag, r.value)
	}
	if sum := c.CheaterRatio + c.SmurfRatio + c.ToxicRatio; sum > 1 {
		errs = append(errs, fmt.Errorf("--cheater-ratio + --smurf-ratio + --toxic-ratio must be <= 1 (got %.2f)", sum))
	}

	check(c.Duration > 0, "--duration must be > 0 (got %v)", c.Duration)
	check(c.MatchDuration >= minMatchDuration, "--match-duration must be >= %v (got %v)", minMatchDuration, c.MatchDuration)

	if c.ServerCount > 0 {
		for _, id := range c.DegradedServers {
			n, ok := parseServerID(id)
			check(ok && n <= c.ServerCount,
				"--degraded-servers: %q is not one of server-01 … %s (pass --degraded-servers '' to disable)",
				id, serverID(c.ServerCount-1))
		}
	}

	if !c.DryRun {
		check(len(c.KafkaBrokers) > 0, "--kafka-brokers must list at least one host:port (or use --dry-run)")
		for _, b := range c.KafkaBrokers {
			check(validHostPort(b), "--kafka-brokers: %q is not a valid host:port", b)
		}
	}
	return errors.Join(errs...)
}

// serverID is the canonical name of the i-th (0-based) server.
func serverID(i int) string { return fmt.Sprintf("server-%02d", i+1) }

// parseServerID returns the 1-based number of a canonical server ID.
func parseServerID(id string) (int, bool) {
	num, ok := strings.CutPrefix(id, "server-")
	if !ok || len(num) < 2 {
		return 0, false
	}
	n, err := strconv.Atoi(num)
	if err != nil || n < 1 || serverID(n-1) != id {
		return 0, false
	}
	return n, true
}

func validHostPort(s string) bool {
	host, port, err := net.SplitHostPort(s)
	if err != nil || host == "" {
		return false
	}
	p, err := strconv.Atoi(port)
	return err == nil && p > 0 && p <= 65535
}

// splitList splits a comma-separated flag value, dropping blank entries.
func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
