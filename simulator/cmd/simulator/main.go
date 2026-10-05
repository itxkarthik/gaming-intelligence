// Command simulator generates realistic competitive-shooter telemetry and
// publishes it to Kafka: SHOT events (gameplay_events), player lifecycle
// events (player_events) and per-server health samples (server_metrics).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// Exit codes.
const (
	exitOK      = 0
	exitFailure = 1 // Kafka unreachable, or messages failed to deliver
	exitUsage   = 2 // invalid flags or profiles
)

// pcgStream is the fixed second PCG word; the seed varies the first.
const pcgStream = 0x9e3779b97f4a7c15

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	cfg, err := parseConfig(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return exitOK
	}
	if err != nil {
		fmt.Fprintf(stderr, "simulator: invalid configuration:\n%s\n", indent(err))
		return exitUsage
	}
	profiles, err := loadProfiles(cfg.ProfilesDir)
	if err != nil {
		fmt.Fprintf(stderr, "simulator: invalid profiles:\n%s\n", indent(err))
		return exitUsage
	}

	seed := cfg.Seed
	if seed == 0 {
		seed = rand.Uint64()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var sink Sink
	if cfg.DryRun {
		sink = newDryRunSink()
	} else {
		ks, err := newKafkaSink(ctx, cfg.KafkaBrokers, stderr)
		if err != nil {
			fmt.Fprintf(stderr, "simulator: %v\n", err)
			return exitFailure
		}
		sink = ks
	}

	start := time.Now()
	engine := newEngine(cfg, profiles, rand.New(rand.NewPCG(seed, pcgStream)), sink, stdout, stderr, start)
	printBanner(stdout, cfg, seed, engine.world)

	runCtx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()
	// Once the run ends (timeout or first Ctrl+C), restore default signal
	// handling so a second Ctrl+C exits at once even if the flush stalls.
	context.AfterFunc(runCtx, stop)

	engine.Run(runCtx)
	ran := time.Since(start) // the run itself, excluding the final flush
	interrupted := !errors.Is(runCtx.Err(), context.DeadlineExceeded)

	closeErr := sink.Close()
	stats := sink.Stats()
	printSummary(stdout, engine.stats, stats, ran, interrupted)

	if closeErr != nil {
		fmt.Fprintf(stderr, "simulator: flushing Kafka writers: %v\n", closeErr)
	}
	total := totalStats(stats)
	if closeErr != nil || total.Failed > 0 || total.Unconfirmed() > 0 || engine.stats.EncodeErrors > 0 {
		return exitFailure
	}
	return exitOK
}

func printBanner(w io.Writer, cfg Config, seed uint64, world *World) {
	counts := map[Archetype]int{}
	for _, p := range world.players {
		counts[p.Archetype]++
	}
	var mix []string
	for _, a := range archetypes {
		if counts[a] > 0 {
			mix = append(mix, fmt.Sprintf("%s=%d", a, counts[a]))
		}
	}
	output := "kafka " + strings.Join(cfg.KafkaBrokers, ",")
	if cfg.DryRun {
		output = "dry run (nothing is sent)"
	}
	degraded := "none"
	if len(cfg.DegradedServers) > 0 {
		degraded = strings.Join(cfg.DegradedServers, ",")
	}

	fmt.Fprintln(w, "Gaming Intelligence simulator")
	fmt.Fprintf(w, "  players   %d in %d concurrent 5v5 matches (%d queued), %v per match\n",
		cfg.Players, cfg.MatchesConcurrent, cfg.Players-cfg.MatchesConcurrent*playersPerMatch, cfg.MatchDuration)
	fmt.Fprintf(w, "  mix       %s\n", strings.Join(mix, " "))
	fmt.Fprintf(w, "  rate      %d shots/s for %v (late events %.0f%%, behaviour shift %.0f%%)\n",
		cfg.EventsPerSec, cfg.Duration, cfg.LateEventRatio*100, cfg.BehaviorShiftRatio*100)
	fmt.Fprintf(w, "  servers   %d (degraded: %s)\n", cfg.ServerCount, degraded)
	fmt.Fprintf(w, "  output    %s\n", output)
	fmt.Fprintf(w, "  seed      %d\n", seed)
}

func printSummary(w io.Writer, s runStats, byTopic map[Topic]TopicStats, elapsed time.Duration, interrupted bool) {
	state := "complete"
	if interrupted {
		state = "interrupted"
	}
	fmt.Fprintf(w, "\nRun %s after %v\n", state, elapsed.Truncate(time.Millisecond))
	fmt.Fprintf(w, "  shots          %d (%.0f/s), hit rate %.1f%%\n",
		s.Shots, float64(s.Shots)/elapsed.Seconds(), percent(s.Hits, s.Shots))
	fmt.Fprintf(w, "  kills          %d (headshot %.1f%%, assisted %.1f%%)\n",
		s.Kills, percent(s.HeadshotKills, s.Kills), percent(s.Assists, s.Kills))
	fmt.Fprintf(w, "  player events  %d\n", s.PlayerEvents)
	fmt.Fprintf(w, "  server metrics %d\n", s.ServerMetrics)
	if s.Idle > 0 || s.Dropped > 0 {
		fmt.Fprintf(w, "  pacer          %d idle slots, %d dropped (generator fell behind)\n", s.Idle, s.Dropped)
	}
	for _, t := range topics {
		ts := byTopic[t]
		line := fmt.Sprintf("  %-16s published %d, delivered %d, failed %d", t, ts.Published, ts.Delivered, ts.Failed)
		if n := ts.Unconfirmed(); n > 0 {
			line += fmt.Sprintf(", unconfirmed %d", n)
		}
		fmt.Fprintln(w, line)
	}
	if s.EncodeErrors > 0 {
		fmt.Fprintf(w, "  encode errors  %d\n", s.EncodeErrors)
	}
}

func percent(n, total int) float64 {
	if total == 0 {
		return 0
	}
	return 100 * float64(n) / float64(total)
}

// indent renders a (possibly joined) error one problem per line.
func indent(err error) string {
	lines := strings.Split(err.Error(), "\n")
	for i, l := range lines {
		lines[i] = "  - " + l
	}
	return strings.Join(lines, "\n")
}
