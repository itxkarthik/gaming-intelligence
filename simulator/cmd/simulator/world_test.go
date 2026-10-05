package main

import (
	"math"
	"slices"
	"testing"
	"time"
)

// recorder is an emitter that keeps everything the world generates.
type recorder struct {
	shots   []ShotEvent
	players []PlayerEvent
	metrics []ServerMetric
}

func (r *recorder) shot(e ShotEvent)      { r.shots = append(r.shots, e) }
func (r *recorder) player(e PlayerEvent)  { r.players = append(r.players, e) }
func (r *recorder) server(m ServerMetric) { r.metrics = append(r.metrics, m) }

func (r *recorder) playerEvents(eventType string) []PlayerEvent {
	var out []PlayerEvent
	for _, e := range r.players {
		if e.EventType == eventType {
			out = append(out, e)
		}
	}
	return out
}

func testProfiles(t *testing.T) map[Archetype]*Profile {
	t.Helper()
	profiles, err := loadProfiles("../../profiles")
	if err != nil {
		t.Fatalf("loading repo profiles: %v", err)
	}
	return profiles
}

func testConfig() Config {
	return Config{
		Players:           25,
		MatchesConcurrent: 2,
		EventsPerSec:      20,
		CheaterRatio:      0.05,
		SmurfRatio:        0.05,
		ToxicRatio:        0.05,
		ServerCount:       2,
		DegradedServers:   []string{"server-02"},
		Duration:          10 * time.Minute,
		MatchDuration:     3 * time.Minute,
	}
}

func newTestWorld(t *testing.T, cfg Config) (*World, *recorder) {
	t.Helper()
	rec := &recorder{}
	w := newWorld(cfg, testProfiles(t), newRand(), rec, t0)
	w.begin(t0)
	return w, rec
}

func TestBeginFormsFullLobbies(t *testing.T) {
	cfg := testConfig()
	w, rec := newTestWorld(t, cfg)

	if got := len(rec.playerEvents(EventLogin)); got != cfg.Players {
		t.Fatalf("LOGIN events = %d; want %d", got, cfg.Players)
	}
	seated := map[string]bool{}
	for _, m := range w.slots {
		if m == nil {
			t.Fatal("a slot was left empty although the queue had enough players")
		}
		for team, players := range m.teams {
			if len(players) != playersPerTeam {
				t.Fatalf("%s team %d has %d players; want %d", m.ID, team, len(players), playersPerTeam)
			}
			for _, p := range players {
				if seated[p.ID] {
					t.Fatalf("%s is seated in two matches", p.ID)
				}
				seated[p.ID] = true
			}
		}
	}
	if want := cfg.Players - cfg.MatchesConcurrent*playersPerMatch; len(w.queue) != want {
		t.Fatalf("queue = %d; want %d", len(w.queue), want)
	}
	if got := len(rec.playerEvents(EventMatchJoin)); got != len(seated) {
		t.Fatalf("MATCH_JOIN events = %d; want %d", got, len(seated))
	}
	// Every seated player buys the weapon they then fight with.
	if got := len(rec.playerEvents(EventItemPurchase)); got != len(seated) {
		t.Fatalf("ITEM_PURCHASE events = %d; want %d", got, len(seated))
	}
}

func TestLoginCarriesGroundTruth(t *testing.T) {
	w, rec := newTestWorld(t, testConfig())
	byID := map[string]*Player{}
	for _, p := range w.players {
		byID[p.ID] = p
	}
	for _, e := range rec.playerEvents(EventLogin) {
		p := byID[e.PlayerID]
		for _, key := range []string{"archetype", "account_age_days", "games_played", "rank"} {
			if e.Metadata[key] == "" {
				t.Fatalf("%s LOGIN lacks %q: %v", e.PlayerID, key, e.Metadata)
			}
		}
		if e.Metadata["archetype"] != string(p.Archetype) || e.Metadata["rank"] != p.Profile.SkillTier {
			t.Fatalf("%s LOGIN metadata %v does not match its profile", p.ID, e.Metadata)
		}
	}
}

func TestMatchRotation(t *testing.T) {
	cfg := testConfig()
	cfg.MatchesConcurrent = 1
	cfg.Players = playersPerMatch
	w, rec := newTestWorld(t, cfg)
	first := w.slots[0]

	w.tick(first.endsAt)
	second := w.slots[0]
	if second == nil || second == first || second.ID == first.ID {
		t.Fatalf("match did not rotate: first=%v second=%v", first.ID, second)
	}
	if got := len(rec.playerEvents(EventMatchEnd)); got != playersPerMatch {
		t.Fatalf("MATCH_END events = %d; want %d", got, playersPerMatch)
	}
	for _, e := range rec.playerEvents(EventMatchEnd) {
		if e.MatchID == nil || *e.MatchID != first.ID {
			t.Fatalf("MATCH_END carries match %v; want %s", e.MatchID, first.ID)
		}
	}
	if !second.endsAt.Equal(first.endsAt.Add(cfg.MatchDuration)) {
		t.Fatalf("second match ends at %v; want a full %v after rotation", second.endsAt, cfg.MatchDuration)
	}
}

func TestMatchIDsAreUniqueAcrossRuns(t *testing.T) {
	cfg := testConfig()
	a := newWorld(cfg, testProfiles(t), newRand(), &recorder{}, t0)
	b := newWorld(cfg, testProfiles(t), newRand(), &recorder{}, t0.Add(time.Minute))
	a.begin(t0)
	b.begin(t0.Add(time.Minute))
	if a.slots[0].ID == b.slots[0].ID {
		t.Fatalf("two runs produced the same match ID %s", a.slots[0].ID)
	}
}

func TestOfflinePlayersNeitherShootNorAreShot(t *testing.T) {
	cfg := testConfig()
	cfg.MatchesConcurrent = 1
	cfg.Players = playersPerMatch
	w, rec := newTestWorld(t, cfg)

	for _, p := range w.slots[0].teams[1] {
		w.disconnect(p, t0)
	}
	for range 100 {
		if w.shoot(t0) {
			t.Fatal("a shot was generated with a whole team offline")
		}
	}
	if len(rec.shots) != 0 {
		t.Fatalf("%d shots recorded", len(rec.shots))
	}
	for _, p := range w.slots[0].teams[1] {
		w.reconnect(p, t0)
	}
	if !w.shoot(t0) {
		t.Fatal("no shot after the team reconnected")
	}
}

func TestDisconnectWhileQueuedLeavesQueue(t *testing.T) {
	w, _ := newTestWorld(t, testConfig())
	queued := w.queue[0]
	w.disconnect(queued, t0)
	for _, p := range w.queue {
		if p == queued {
			t.Fatal("offline player is still queued")
		}
	}
	w.reconnect(queued, t0)
	if w.queue[len(w.queue)-1] != queued {
		t.Fatal("reconnected player was not re-queued")
	}
}

// Replays a long stream and checks the SHOT contract on every event, plus
// the HP rules end to end: nobody shoots or is shot while dead.
func TestShotStreamContract(t *testing.T) {
	cfg := testConfig()
	w, rec := newTestWorld(t, cfg)

	now := t0
	for range 20_000 {
		now = now.Add(10 * time.Millisecond)
		w.shoot(now)
	}

	deadUntil := map[string]time.Time{}
	team := map[string]Team{}
	var kills, hits int
	for i, s := range rec.shots {
		at := time.UnixMilli(s.EventTime)
		if s.EventType != EventShot {
			t.Fatalf("shot %d: event_type %q", i, s.EventType)
		}
		if s.PlayerID == s.TargetPlayerID {
			t.Fatalf("shot %d: %s shot themself", i, s.PlayerID)
		}
		if tm, ok := team[s.PlayerID]; ok && tm != s.TeamID {
			t.Fatalf("shot %d: %s switched team mid-match", i, s.PlayerID)
		}
		team[s.PlayerID] = s.TeamID
		for _, id := range []string{s.PlayerID, s.TargetPlayerID} {
			if at.Before(deadUntil[id]) {
				t.Fatalf("shot %d: %s takes part while dead", i, id)
			}
		}

		switch {
		case !s.Hit:
			if s.Damage != nil || s.VictimHPAfter != nil || s.IsHeadshot || s.IsKill || s.AssisterID != nil {
				t.Fatalf("shot %d: miss carries an outcome: %+v", i, s)
			}
		default:
			hits++
			if s.Damage == nil || *s.Damage <= 0 || s.VictimHPAfter == nil {
				t.Fatalf("shot %d: hit without damage/hp: %+v", i, s)
			}
			if s.IsKill != (*s.VictimHPAfter == 0) {
				t.Fatalf("shot %d: is_kill=%v but victim_hp_after=%d", i, s.IsKill, *s.VictimHPAfter)
			}
		}
		if s.IsKill {
			kills++
			deadUntil[s.TargetPlayerID] = at.Add(respawnDelay)
			if s.AssisterID != nil && (*s.AssisterID == s.PlayerID || *s.AssisterID == s.TargetPlayerID) {
				t.Fatalf("shot %d: invalid assister %s", i, *s.AssisterID)
			}
		} else if s.AssisterID != nil {
			t.Fatalf("shot %d: assist on a non-kill", i)
		}
		if s.Accuracy < minAccuracy || s.Accuracy > maxAccuracy ||
			s.ReactionTimeMs < minReactionMs || s.ReactionTimeMs > maxReactionMs {
			t.Fatalf("shot %d: aim outside bounds: acc=%v rxn=%d", i, s.Accuracy, s.ReactionTimeMs)
		}
		if s.Distance < 0 || s.Distance > float32(math.Sqrt(2*arenaSize*arenaSize+arenaHeight*arenaHeight)) {
			t.Fatalf("shot %d: distance %v outside the arena", i, s.Distance)
		}
	}
	if kills == 0 || hits == 0 {
		t.Fatalf("degenerate stream: %d shots, %d hits, %d kills", len(rec.shots), hits, kills)
	}
}

func TestServerMetricsReportRealOccupancy(t *testing.T) {
	cfg := testConfig()
	w, rec := newTestWorld(t, cfg)
	offline := w.slots[0].teams[0][0]
	w.disconnect(offline, t0)

	rec.metrics = nil
	w.reportServers(t0)
	if len(rec.metrics) != cfg.ServerCount {
		t.Fatalf("metrics = %d; want one per server", len(rec.metrics))
	}
	var players, matches int
	for _, m := range rec.metrics {
		players += m.ActivePlayers
		matches += m.ActiveMatches
		load := healthyLoad
		if m.ServerID == "server-02" {
			load = degradedLoad
		}
		if float64(m.CPUPercent) < load.cpu.lo || float64(m.CPUPercent) > load.cpu.hi {
			t.Fatalf("%s cpu %.1f outside %v", m.ServerID, m.CPUPercent, load.cpu)
		}
	}
	if want := cfg.MatchesConcurrent*playersPerMatch - 1; players != want {
		t.Fatalf("active players = %d; want %d (seated minus the offline one)", players, want)
	}
	if matches != cfg.MatchesConcurrent {
		t.Fatalf("active matches = %d; want %d", matches, cfg.MatchesConcurrent)
	}
}

func TestLateEventsFallInsideTheInjectionWindow(t *testing.T) {
	cfg := testConfig()
	cfg.LateEventRatio = 1
	w, rec := newTestWorld(t, cfg)
	now := t0.Add(time.Minute)
	for range 500 {
		w.shoot(now)
	}
	for _, s := range rec.shots {
		delay := now.Sub(time.UnixMilli(s.EventTime))
		if delay < lateMinDelay || delay > lateMaxDelay {
			t.Fatalf("late delay %v outside [%v, %v]", delay, lateMinDelay, lateMaxDelay)
		}
	}
}

func TestBehaviorShiftRaisesButNeverLowersAccuracy(t *testing.T) {
	cfg := testConfig()
	cfg.BehaviorShiftRatio = 1
	w, rec := newTestWorld(t, cfg)
	if w.shiftAt.IsZero() {
		t.Fatal("shift time not set although every player is selected")
	}

	mean := func(from time.Time) map[Archetype]float64 {
		rec.shots = nil
		for i := range 20_000 {
			w.shoot(from.Add(time.Duration(i) * 10 * time.Millisecond))
		}
		sum, n := map[Archetype]float64{}, map[Archetype]int{}
		byID := map[string]*Player{}
		for _, p := range w.players {
			byID[p.ID] = p
		}
		for _, s := range rec.shots {
			a := byID[s.PlayerID].Archetype
			sum[a] += float64(s.Accuracy)
			n[a]++
		}
		for a := range sum {
			sum[a] /= float64(n[a])
		}
		return sum
	}
	before, after := mean(t0), mean(w.shiftAt)
	for a, b := range before {
		if after[a] < b-0.01 {
			t.Errorf("%s accuracy fell from %.3f to %.3f after the shift", a, b, after[a])
		}
	}
	if after[ArchetypeNormalGold] < before[ArchetypeNormalGold]*1.4 {
		t.Errorf("gold accuracy %.3f -> %.3f: shift not applied", before[ArchetypeNormalGold], after[ArchetypeNormalGold])
	}
}

func TestArchetypeIsStablePerAccount(t *testing.T) {
	cfg := testConfig()
	a := newWorld(cfg, testProfiles(t), newRand(), &recorder{}, t0)
	b := newWorld(cfg, testProfiles(t), newRandSeed(99), &recorder{}, t0.Add(time.Hour))
	for i := range a.players {
		if a.players[i].ID != b.players[i].ID || a.players[i].Archetype != b.players[i].Archetype {
			t.Fatalf("%s: archetype %s vs %s across runs", a.players[i].ID, a.players[i].Archetype, b.players[i].Archetype)
		}
	}
}

func TestChooseArchetype(t *testing.T) {
	cases := []struct {
		roll, rankRoll float64
		want           Archetype
	}{
		{0.01, 0.10, ArchetypeCheaterAimbot},
		{0.01, 0.90, ArchetypeCheaterWallhack},
		{0.07, 0.50, ArchetypeSmurf},
		{0.12, 0.50, ArchetypeToxic},
		{0.50, 0.10, ArchetypeNormalBronze},
		{0.50, 0.50, ArchetypeNormalGold},
		{0.50, 0.95, ArchetypeNormalDiamond},
	}
	for _, tc := range cases {
		if got := chooseArchetype(tc.roll, tc.rankRoll, 0.05, 0.05, 0.05); got != tc.want {
			t.Errorf("chooseArchetype(%v, %v) = %s; want %s", tc.roll, tc.rankRoll, got, tc.want)
		}
	}
}

func TestReportsComeFromMatchmates(t *testing.T) {
	cfg := testConfig()
	w, rec := newTestWorld(t, cfg)
	target := w.slots[0].teams[0][0]
	for range 50 {
		w.reportedBy(target, t0)
	}
	inMatch := map[string]bool{}
	for _, team := range w.slots[0].teams {
		for _, p := range team {
			inMatch[p.ID] = true
		}
	}
	reports := rec.playerEvents(EventReportPlayer)
	if len(reports) != 50 {
		t.Fatalf("reports = %d; want 50", len(reports))
	}
	for _, r := range reports {
		if r.PlayerID == target.ID || !inMatch[r.PlayerID] || r.Metadata["reported_player"] != target.ID {
			t.Fatalf("bad report %+v", r)
		}
		if !slices.Contains(reportReasons, r.Metadata["reason"]) {
			t.Fatalf("unknown reason %q", r.Metadata["reason"])
		}
	}
}
