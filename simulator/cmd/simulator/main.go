package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

// Config holds command line simulation parameters.
type Config struct {
	Players           int
	MatchesConcurrent int
	EventsPerSec      int
	CheaterRatio      float64
	SmurfRatio        float64
	ToxicRatio        float64
	ServerCount       int
	KafkaBrokers      string
	Duration          time.Duration
	LateEventRatio    float64
	ProfilesDir       string
}

func main() {
	cfg := Config{}

	flag.IntVar(&cfg.Players, "players", 1000, "Total simulated player pool size")
	flag.IntVar(&cfg.MatchesConcurrent, "matches-concurrent", 50, "Number of concurrent matches to simulate")
	flag.IntVar(&cfg.EventsPerSec, "events-per-sec", 5000, "Target events published to Kafka per second")
	flag.Float64Var(&cfg.CheaterRatio, "cheater-ratio", 0.03, "Proportion of active players exhibiting cheater characteristics")
	flag.Float64Var(&cfg.SmurfRatio, "smurf-ratio", 0.05, "Proportion of active players exhibiting smurf characteristics")
	flag.Float64Var(&cfg.ToxicRatio, "toxic-ratio", 0.05, "Proportion of active players exhibiting toxic traits")
	flag.IntVar(&cfg.ServerCount, "server-count", 20, "Total number of virtual game server instances")
	flag.StringVar(&cfg.KafkaBrokers, "kafka-brokers", "localhost:9092,localhost:9094", "Comma-separated list of Kafka broker addresses")
	flag.DurationVar(&cfg.Duration, "duration", 10*time.Minute, "Total simulation run duration (e.g. 10m, 1h)")
	flag.Float64Var(&cfg.LateEventRatio, "late-event-ratio", 0.05, "Proportion of events emitted with simulated network delay (out-of-order)")
	flag.StringVar(&cfg.ProfilesDir, "profiles-dir", "./profiles", "Directory containing YAML player archetype profiles")

	flag.Parse()

	fmt.Println("=================================================================")
	fmt.Println(" Real-Time Competitive Gaming Intelligence Platform - Simulator")
	fmt.Println("=================================================================")
	fmt.Printf("• Player Pool Size       : %d\n", cfg.Players)
	fmt.Printf("• Concurrent Matches     : %d\n", cfg.MatchesConcurrent)
	fmt.Printf("• Target Throughput      : %d events/sec\n", cfg.EventsPerSec)
	fmt.Printf("• Cheater Ratio          : %.2f%%\n", cfg.CheaterRatio*100)
	fmt.Printf("• Smurf Ratio            : %.2f%%\n", cfg.SmurfRatio*100)
	fmt.Printf("• Toxic Ratio            : %.2f%%\n", cfg.ToxicRatio*100)
	fmt.Printf("• Virtual Game Servers   : %d\n", cfg.ServerCount)
	fmt.Printf("• Kafka Brokers          : %s\n", cfg.KafkaBrokers)
	fmt.Printf("• Simulation Duration    : %v\n", cfg.Duration)
	fmt.Printf("• Late Event Injection   : %.2f%%\n", cfg.LateEventRatio*100)
	fmt.Printf("• Profiles Directory     : %s\n", cfg.ProfilesDir)
	fmt.Println("=================================================================")
	fmt.Println("[Phase 0] Simulator CLI entrypoint verified.")
	fmt.Println("[Phase 1] Full event generator and multi-goroutine engine will be implemented next.")

	if _, err := os.Stat(cfg.ProfilesDir); os.IsNotExist(err) {
		fmt.Printf("Warning: Profiles directory '%s' does not exist in current working directory.\n", cfg.ProfilesDir)
	} else {
		fmt.Printf("Found player profiles directory at '%s'.\n", cfg.ProfilesDir)
	}
}
