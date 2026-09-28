package main

import (
	"fmt"
	"testing"
	"time"
)

func TestSampleNormalClampsToRange(t *testing.T) {
	// Wide sigma relative to the clamp range: every sample must be bounded.
	dist := ProfileDistribution{Mean: 50, Std: 100}
	for i := 0; i < 1000; i++ {
		v := sampleNormal(dist, 0, 10)
		if v < 0 || v > 10 {
			t.Fatalf("sampleNormal returned %v outside [0,10]", v)
		}
	}

	if got := sampleNormal(ProfileDistribution{Mean: 3.5, Std: 0}, -100, 100); got != 3.5 {
		t.Fatalf("zero-std sample = %v, want 3.5", got)
	}
}

func TestChooseArchetype(t *testing.T) {
	const cheater, smurf, toxic = 0.05, 0.05, 0.05

	cases := []struct {
		name     string
		roll     float64
		rankRoll float64
		want     string
	}{
		{"aimbot", 0.01, 0.50, "cheater_aimbot"},
		{"wallhack", 0.01, 0.90, "cheater_wallhack"},
		{"cheater boundary is exclusive", 0.05, 0.50, "smurf"},
		{"smurf band", 0.07, 0.50, "smurf"},
		{"toxic band", 0.12, 0.50, "toxic"},
		{"bronze", 0.50, 0.39, "normal_bronze"},
		{"gold lower bound", 0.50, 0.40, "normal_gold"},
		{"diamond lower bound", 0.50, 0.80, "normal_diamond"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := chooseArchetype(tc.roll, tc.rankRoll, cheater, smurf, toxic)
			if got != tc.want {
				t.Errorf("chooseArchetype(%v, %v, ...) = %q, want %q",
					tc.roll, tc.rankRoll, got, tc.want)
			}
		})
	}
}

func TestLoadProfilesFromRepoProfilesDir(t *testing.T) {
	profiles, err := loadProfiles("../../profiles")
	if err != nil {
		t.Fatalf("loadProfiles: %v", err)
	}
	if len(profiles) != 7 {
		t.Fatalf("loaded %d profiles, want 7", len(profiles))
	}

	want := []string{
		"normal_bronze", "normal_gold", "normal_diamond",
		"cheater_aimbot", "cheater_wallhack", "smurf", "toxic",
	}
	for _, key := range want {
		p, ok := profiles[key]
		if !ok {
			t.Errorf("missing profile %q", key)
			continue
		}
		if p.Name == "" || p.SkillTier == "" {
			t.Errorf("profile %q missing name or skill_tier", key)
		}
		if p.Accuracy.Std <= 0 || p.ReactionTimeMs.Std <= 0 {
			t.Errorf("profile %q has non-positive distribution std", key)
		}
	}

	// Archetype separation the detection pipeline depends on.
	if p := profiles["cheater_aimbot"]; p.Accuracy.Mean < 0.9 {
		t.Errorf("aimbot accuracy mean = %v, want >= 0.9", p.Accuracy.Mean)
	}
	if p := profiles["normal_gold"]; p.Accuracy.Mean > 0.4 {
		t.Errorf("gold accuracy mean = %v, want <= 0.4", p.Accuracy.Mean)
	}
}

func TestInitialPlayerEvents(t *testing.T) {
	const playerCount = 40

	players := make([]*Player, playerCount)
	for i := range players {
		players[i] = &Player{
			ID:        fmt.Sprintf("player_%04d", i+1),
			Archetype: "normal_gold",
			ServerID:  fmt.Sprintf("server-%02d", i%2+1),
			Profile:   PlayerProfile{SkillTier: "gold"},
		}
	}

	matches := make([]*Match, 4)
	for m := range matches {
		matches[m] = &Match{
			ID:       fmt.Sprintf("match_%04d", m+1),
			ServerID: "server-01",
		}
		for p := 0; p < 10; p++ {
			pl := players[m*10+p]
			pl.MatchID = matches[m].ID
			if p < 5 {
				pl.TeamID = "team_a"
				matches[m].TeamA = append(matches[m].TeamA, pl)
			} else {
				pl.TeamID = "team_b"
				matches[m].TeamB = append(matches[m].TeamB, pl)
			}
		}
	}

	events := initialPlayerEvents(players, matches, time.Now())

	counts := map[string]int{}
	for _, ev := range events {
		counts[ev.EventType]++

		if ev.EventID == "" || ev.PlayerID == "" || ev.EventTime == 0 {
			t.Errorf("event missing required fields: %+v", ev)
		}
		switch ev.EventType {
		case "LOGIN":
			if ev.Metadata["archetype"] == "" {
				t.Errorf("LOGIN missing archetype ground truth: %+v", ev)
			}
		case "MATCH_JOIN":
			if ev.MatchID == nil || *ev.MatchID == "" {
				t.Errorf("MATCH_JOIN missing match_id: %+v", ev)
			}
			if ev.Metadata["team_id"] == "" {
				t.Errorf("MATCH_JOIN missing team_id metadata: %+v", ev)
			}
			if ev.Metadata["skill_tier"] == "" {
				t.Errorf("MATCH_JOIN missing skill_tier metadata: %+v", ev)
			}
		case "MATCHMAKING_START", "MATCHMAKING_FOUND":
			// Queue events precede match assignment; match_id stays unset.
		default:
			t.Errorf("unexpected event type %q", ev.EventType)
		}
	}

	if counts["LOGIN"] != playerCount {
		t.Errorf("LOGIN count = %d, want %d", counts["LOGIN"], playerCount)
	}
	if counts["MATCHMAKING_START"] != playerCount {
		t.Errorf("MATCHMAKING_START count = %d, want %d", counts["MATCHMAKING_START"], playerCount)
	}
	if counts["MATCHMAKING_FOUND"] != playerCount {
		t.Errorf("MATCHMAKING_FOUND count = %d, want %d", counts["MATCHMAKING_FOUND"], playerCount)
	}
	if counts["MATCH_JOIN"] != playerCount {
		t.Errorf("MATCH_JOIN count = %d, want %d", counts["MATCH_JOIN"], playerCount)
	}
}
