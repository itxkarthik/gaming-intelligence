package main

import (
	"fmt"
	"math"
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

func TestValidatePoolSize(t *testing.T) {
	t.Run("rejects pool smaller than matches need", func(t *testing.T) {
		if err := validatePoolSize(100, 20); err == nil {
			t.Fatal("expected error for 100 players with 20 matches (needs 200)")
		}
	})
	t.Run("accepts exact fit", func(t *testing.T) {
		if err := validatePoolSize(200, 20); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("accepts larger pool", func(t *testing.T) {
		if err := validatePoolSize(500, 20); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestSelectShiftedPlayers(t *testing.T) {
	pool := make([]*Player, 100)
	for i := range pool {
		pool[i] = &Player{ID: fmt.Sprintf("player_%04d", i+1)}
	}

	if got := selectShiftedPlayers(pool, 0); len(got) != 0 {
		t.Errorf("ratio 0 selected %d players, want 0", len(got))
	}
	all := selectShiftedPlayers(pool, 1.0)
	if len(all) != 100 {
		t.Errorf("ratio 1 selected %d players, want 100", len(all))
	}
}

func TestApplyBehaviorShift(t *testing.T) {
	defer func(shifted map[string]bool, at time.Time) {
		behaviorShifted, behaviorShiftAt = shifted, at
	}(behaviorShifted, behaviorShiftAt)

	behaviorShifted = map[string]bool{"player_0001": true}
	base := float32(0.30)

	// Before the midpoint: nothing shifts
	behaviorShiftAt = time.Now().Add(time.Minute)
	if got := applyBehaviorShift("player_0001", base); got != base {
		t.Errorf("before shift: got %v, want %v", got, base)
	}

	// After the midpoint: shifted players boosted, clamped at 0.95
	behaviorShiftAt = time.Now().Add(-time.Second)
	if got := applyBehaviorShift("player_0001", base); math.Abs(float64(got)-0.51) > 1e-6 {
		t.Errorf("shifted player: got %v, want 0.51", got)
	}
	if got := applyBehaviorShift("player_0001", 0.80); got != 0.95 {
		t.Errorf("shift clamping: got %v, want 0.95", got)
	}
	// Unshifted players untouched
	if got := applyBehaviorShift("player_0002", base); got != base {
		t.Errorf("unshifted player: got %v, want %v", got, base)
	}

	// Disabled (zero time): always unchanged
	behaviorShiftAt = time.Time{}
	if got := applyBehaviorShift("player_0001", base); got != base {
		t.Errorf("disabled shift: got %v, want %v", got, base)
	}
}

func TestArchetypeSeedsStableAcrossRuns(t *testing.T) {
	// Same ID must map to the same archetype on every run (identity stability).
	a1, a2 := archetypeSeeds("player_0042")
	b1, b2 := archetypeSeeds("player_0042")
	if a1 != b1 || a2 != b2 {
		t.Fatalf("seeds not deterministic: (%v,%v) vs (%v,%v)", a1, a2, b1, b2)
	}
	for i := 1; i <= 300; i++ {
		id := fmt.Sprintf("player_%04d", i)
		r1, r2 := archetypeSeeds(id)
		if got, want := chooseArchetype(r1, r2, 0.05, 0.05, 0.05),
			chooseArchetype(r1, r2, 0.05, 0.05, 0.05); got != want {
			t.Fatalf("archetype for %s changed within same inputs: %s != %s", id, got, want)
		}
	}

	// Across 300 stable IDs the configured ratios must still produce a mix.
	counts := map[string]int{}
	for i := 1; i <= 300; i++ {
		r1, r2 := archetypeSeeds(fmt.Sprintf("player_%04d", i))
		counts[chooseArchetype(r1, r2, 0.05, 0.05, 0.05)]++
	}
	if len(counts) < 4 {
		t.Errorf("expected a mix of archetypes across 300 IDs, got %v", counts)
	}
	if counts["cheater_aimbot"]+counts["cheater_wallhack"] == 0 {
		t.Errorf("expected some cheaters at 5%% ratio, got %v", counts)
	}
}

func TestLoginCarriesAccountFeatures(t *testing.T) {
	players := []*Player{{
		ID: "player_0001", Archetype: "smurf",
		Profile: PlayerProfile{SkillTier: "bronze", AccountAgeDays: 3, GamesPlayed: 12},
	}}
	matches := []*Match{{ID: "match_0001", ServerID: "server-01"}}

	var login *PlayerEvent
	for _, ev := range initialPlayerEvents(players, matches, time.Now()) {
		if ev.EventType == "LOGIN" {
			login = &ev
			break
		}
	}
	if login == nil {
		t.Fatal("no LOGIN event emitted")
	}
	if login.Metadata["account_age_days"] != "3" || login.Metadata["games_played"] != "12" {
		t.Errorf("LOGIN missing account features: %+v", login.Metadata)
	}
	if login.Metadata["rank"] != "bronze" {
		t.Errorf("LOGIN missing rank: %+v", login.Metadata)
	}
}

func TestMatchJoinEmitsItemPurchase(t *testing.T) {
	players := make([]*Player, 0, 20)
	for i := 0; i < 20; i++ {
		players = append(players, &Player{
			ID: fmt.Sprintf("player_%04d", i+1), Archetype: "normal_gold",
			Profile: PlayerProfile{SkillTier: "gold"},
		})
	}
	matches := []*Match{{ID: "match_0001", ServerID: "server-01"}}
	for i := 0; i < 10; i++ {
		matches[0].TeamA = append(matches[0].TeamA, players[i])
		matches[0].TeamB = append(matches[0].TeamB, players[10+i])
	}

	purchases := 0
	for _, ev := range initialPlayerEvents(players, matches, time.Now()) {
		if ev.EventType != "ITEM_PURCHASE" {
			continue
		}
		purchases++
		if ev.MatchID == nil || *ev.MatchID != "match_0001" {
			t.Errorf("purchase missing match_id: %+v", ev)
		}
		if ev.Metadata["weapon_id"] == "" || ev.Metadata["cost"] == "" {
			t.Errorf("purchase missing weapon/cost metadata: %+v", ev.Metadata)
		}
	}
	if purchases != 20 {
		t.Errorf("expected one purchase per match participant, got %d", purchases)
	}
}

func TestProfileAccountDefaults(t *testing.T) {
	// Test cwd is simulator/cmd/simulator; profiles live at simulator/profiles.
	profiles, err := loadProfiles("../../profiles")
	if err != nil {
		t.Fatalf("loadProfiles: %v", err)
	}
	if len(profiles) == 0 {
		t.Fatal("no profiles loaded")
	}
	for key, p := range profiles {
		if p.AccountAgeDays <= 0 || p.GamesPlayed <= 0 {
			t.Errorf("profile %q missing account defaults: age=%d games=%d",
				key, p.AccountAgeDays, p.GamesPlayed)
		}
	}
	if profiles["smurf"].AccountAgeDays >= 14 {
		t.Errorf("smurf profile must be a fresh account, age=%d", profiles["smurf"].AccountAgeDays)
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
		case "ITEM_PURCHASE":
			// Purchases ride along with match joins; validated in
			// TestMatchJoinEmitsItemPurchase.
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
