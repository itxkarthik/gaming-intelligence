package main

import (
	"math"
	"testing"
	"time"
)

// features are the per-player inputs the streaming cheat detector computes
// (cheat_detection.py), aggregated here per archetype.
type features struct {
	shots, kills, headshotKills int
	accuracySum, reactionSum    float64
}

func (f features) accuracy() float64      { return f.accuracySum / float64(f.shots) }
func (f features) reaction() float64      { return f.reactionSum / float64(f.shots) }
func (f features) headshotRatio() float64 { return float64(f.headshotKills) / float64(f.kills) }

// One row per shot: averaging accuracy over the stream must reproduce each
// profile's mean exactly. The old SHOT_FIRED/DAMAGE/KILL fan-out copied the
// per-shot accuracy onto hit and kill rows, so a plain average weighted
// accurate shots 2-3x and drifted upward.
func TestPerShotAveragesAreUnbiased(t *testing.T) {
	cfg := testConfig()
	cfg.Players = 600
	cfg.MatchesConcurrent = 50
	cfg.CheaterRatio, cfg.SmurfRatio, cfg.ToxicRatio = 0.15, 0.10, 0.10
	w, rec := newTestWorld(t, cfg)

	now := t0
	for range 300_000 {
		now = now.Add(time.Millisecond)
		w.shoot(now)
	}

	archetypeOf := map[string]Archetype{}
	for _, p := range w.players {
		archetypeOf[p.ID] = p.Archetype
	}
	by := map[Archetype]*features{}
	for _, s := range rec.shots {
		a := archetypeOf[s.PlayerID]
		f := by[a]
		if f == nil {
			f = &features{}
			by[a] = f
		}
		f.shots++
		f.accuracySum += float64(s.Accuracy)
		f.reactionSum += float64(s.ReactionTimeMs)
		if s.IsKill {
			f.kills++
			if s.IsHeadshot {
				f.headshotKills++
			}
		}
	}

	profiles := testProfiles(t)
	for _, a := range archetypes {
		f, p := by[a], profiles[a]
		if f == nil || f.kills == 0 {
			t.Fatalf("%s produced no kills", a)
		}
		t.Logf("%-17s shots %6d  accuracy %.3f (profile %.2f)  reaction %3.0f ms  headshot-kill ratio %.2f",
			a, f.shots, f.accuracy(), p.Accuracy.Mean, f.reaction(), f.headshotRatio())
		if math.Abs(f.accuracy()-p.Accuracy.Mean) > 0.01 {
			t.Errorf("%s mean accuracy %.3f; want profile mean %.2f", a, f.accuracy(), p.Accuracy.Mean)
		}
		if math.Abs(f.reaction()-p.ReactionTimeMs.Mean) > 3 {
			t.Errorf("%s mean reaction %.1f; want profile mean %.0f", a, f.reaction(), p.ReactionTimeMs.Mean)
		}
	}

	// The detector separates on these features, so the aimbot must lead
	// every legitimate class on headshot-kill ratio as well.
	aimbot := by[ArchetypeCheaterAimbot].headshotRatio()
	for _, a := range []Archetype{ArchetypeNormalBronze, ArchetypeNormalGold, ArchetypeNormalDiamond, ArchetypeToxic} {
		if by[a].headshotRatio() >= aimbot {
			t.Errorf("%s headshot-kill ratio %.2f >= aimbot %.2f", a, by[a].headshotRatio(), aimbot)
		}
	}
}
