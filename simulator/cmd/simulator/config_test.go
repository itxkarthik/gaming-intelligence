package main

import (
	"errors"
	"flag"
	"io"
	"strings"
	"testing"
	"time"
)

func TestParseConfigDefaultsSizeTheWorldFromTheRate(t *testing.T) {
	cfg, err := parseConfig([]string{"--events-per-sec", "20000"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MatchesConcurrent != 2000 {
		t.Fatalf("matches = %d; want 20000 / %d", cfg.MatchesConcurrent, maxShotsPerMatchPerSec)
	}
	if want := 2000*playersPerMatch + 5000; cfg.Players != want {
		t.Fatalf("players = %d; want seats + %.0f%% queue = %d", cfg.Players, queueShare*100, want)
	}
	if cfg.Duration != 5*time.Minute || cfg.ServerCount != 10 || len(cfg.KafkaBrokers) != 1 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestParseConfigAcceptsExplicitSizing(t *testing.T) {
	cfg, err := parseConfig([]string{
		"--events-per-sec", "100", "--matches-concurrent", "10", "--players", "100",
		"--kafka-brokers", " kafka:9092, localhost:9094 ,", "--degraded-servers", "",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Players != 100 || cfg.MatchesConcurrent != 10 {
		t.Fatalf("explicit sizing overridden: %+v", cfg)
	}
	if strings.Join(cfg.KafkaBrokers, "|") != "kafka:9092|localhost:9094" {
		t.Fatalf("brokers = %q", cfg.KafkaBrokers)
	}
	if len(cfg.DegradedServers) != 0 {
		t.Fatalf("degraded = %q; want none", cfg.DegradedServers)
	}
}

func TestParseConfigRejectsInvalidInput(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"negative players", []string{"--players", "-1"}, "--players must be > 0"},
		{"lobbies not full", []string{"--players", "50", "--matches-concurrent", "20", "--events-per-sec", "100"}, "every lobby is full"},
		{"matches too hot", []string{"--matches-concurrent", "5"}, "needs at least 100 concurrent matches"},
		{"zero rate", []string{"--events-per-sec", "0"}, "--events-per-sec must be in"},
		{"rate too high", []string{"--events-per-sec", "2000000"}, "--events-per-sec must be in"},
		{"ratio out of range", []string{"--late-event-ratio", "1.5"}, "--late-event-ratio must be in [0,1]"},
		{"NaN ratio", []string{"--cheater-ratio", "NaN"}, "--cheater-ratio must be in [0,1]"},
		{"ratios sum above one", []string{"--cheater-ratio", "0.6", "--smurf-ratio", "0.5"}, "must be <= 1"},
		{"zero servers", []string{"--server-count", "0"}, "--server-count must be > 0"},
		{"unknown degraded server", []string{"--server-count", "1"}, `"server-02" is not one of`},
		{"malformed degraded server", []string{"--degraded-servers", "server-2"}, `"server-2" is not one of`},
		{"zero duration", []string{"--duration", "0s"}, "--duration must be > 0"},
		{"short match", []string{"--match-duration", "1s"}, "--match-duration must be >="},
		{"broker without port", []string{"--kafka-brokers", "localhost"}, `"localhost" is not a valid host:port`},
		{"broker bad port", []string{"--kafka-brokers", "localhost:70000"}, "not a valid host:port"},
		{"no brokers", []string{"--kafka-brokers", " , "}, "at least one host:port"},
		{"stray argument", []string{"--players", "500", "1000"}, "unexpected arguments: 1000"},
		{"unknown flag", []string{"--nope"}, "flag provided but not defined"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseConfig(tc.args, io.Discard)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v; want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestParseConfigReportsEveryProblemAtOnce(t *testing.T) {
	_, err := parseConfig([]string{"--server-count", "0", "--duration", "0s", "--toxic-ratio", "-1"}, io.Discard)
	if err == nil {
		t.Fatal("want an error")
	}
	for _, want := range []string{"--server-count", "--duration", "--toxic-ratio"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("joined error lacks %s: %v", want, err)
		}
	}
}

func TestDryRunNeedsNoBrokers(t *testing.T) {
	if _, err := parseConfig([]string{"--dry-run", "--kafka-brokers", ""}, io.Discard); err != nil {
		t.Fatalf("dry run rejected: %v", err)
	}
}

func TestParseConfigHelp(t *testing.T) {
	if _, err := parseConfig([]string{"-h"}, io.Discard); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("err = %v; want flag.ErrHelp", err)
	}
}

func TestParseServerID(t *testing.T) {
	for id, want := range map[string]int{"server-01": 1, "server-10": 10, "server-120": 120} {
		if n, ok := parseServerID(id); !ok || n != want {
			t.Errorf("parseServerID(%q) = %d, %v; want %d", id, n, ok, want)
		}
	}
	for _, id := range []string{"server-1", "server-00", "server-x1", "srv-01", "server--1"} {
		if _, ok := parseServerID(id); ok {
			t.Errorf("parseServerID(%q) accepted", id)
		}
	}
}
