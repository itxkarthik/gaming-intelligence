package main

import (
	"math/rand/v2"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

// fixed is a weapon with deterministic body damage.
func fixed(dmg int) Weapon { return Weapon{ID: "test", BodyDamageMin: dmg, BodyDamageMax: dmg} }

func combatant(id string, w Weapon) *Player {
	p := &Player{ID: id, weapon: w, connected: true, damageBy: map[string]int{}}
	p.spawn(Position{})
	return p
}

func newRand() *rand.Rand { return newRandSeed(1) }

func newRandSeed(seed uint64) *rand.Rand { return rand.New(rand.NewPCG(seed, pcgStream)) }

func TestMissDealsNoDamage(t *testing.T) {
	r := newRand()
	a, v := combatant("a", fixed(50)), combatant("v", fixed(50))
	res := resolveShot(r, a, v, 0, 1, t0) // accuracy 0: always misses
	if res.Hit || res.Kill || res.Headshot || res.Damage != 0 {
		t.Fatalf("miss produced an outcome: %+v", res)
	}
	if v.hp != maxHP || res.HPAfter != maxHP {
		t.Fatalf("victim hp = %d, HPAfter = %d; want %d", v.hp, res.HPAfter, maxHP)
	}
}

func TestHeadshotFromFullHealthKills(t *testing.T) {
	r := newRand()
	a, v := combatant("a", fixed(30)), combatant("v", fixed(30))
	res := resolveShot(r, a, v, 1, 1, t0) // 30 × 4 = 120 > 100
	if !res.Hit || !res.Headshot || !res.Kill {
		t.Fatalf("want headshot kill, got %+v", res)
	}
	if res.Damage != maxHP {
		t.Fatalf("damage = %d; want %d (overkill is not counted)", res.Damage, maxHP)
	}
	if v.alive || res.HPAfter != 0 {
		t.Fatalf("victim still alive: alive=%v hp=%d", v.alive, res.HPAfter)
	}
}

// Health belongs to the victim: 50 from X then 50 from Y kills, and the
// kill goes to the player who removed the last point.
func TestDamageAccumulatesAcrossAttackers(t *testing.T) {
	r := newRand()
	x, y, v := combatant("x", fixed(50)), combatant("y", fixed(50)), combatant("v", fixed(50))

	first := resolveShot(r, x, v, 1, 0, t0)
	if first.Kill || first.HPAfter != 50 {
		t.Fatalf("first hit: %+v; want HPAfter 50, no kill", first)
	}
	second := resolveShot(r, y, v, 1, 0, t0)
	if !second.Kill || second.HPAfter != 0 {
		t.Fatalf("second hit: %+v; want the kill", second)
	}
	if second.Assister != "x" {
		t.Fatalf("assister = %q; want x (dealt 50 >= %d)", second.Assister, assistMinDamage)
	}
}

func TestAssistNeedsMinimumDamage(t *testing.T) {
	r := newRand()
	x, y, v := combatant("x", fixed(assistMinDamage-1)), combatant("y", fixed(100)), combatant("v", fixed(1))
	resolveShot(r, x, v, 1, 0, t0)
	res := resolveShot(r, y, v, 1, 0, t0)
	if !res.Kill || res.Assister != "" {
		t.Fatalf("got %+v; want kill without assist", res)
	}
	if res.Damage != maxHP-(assistMinDamage-1) {
		t.Fatalf("damage = %d; want only the remaining health", res.Damage)
	}
}

func TestTopAssisterPicksLargestThenID(t *testing.T) {
	cases := []struct {
		name     string
		damageBy map[string]int
		killer   string
		want     string
	}{
		{"largest contributor", map[string]int{"k": 60, "b": 40, "c": 55}, "k", "c"},
		{"tie broken by id", map[string]int{"k": 10, "z": 45, "m": 45}, "k", "m"},
		{"killer excluded", map[string]int{"k": 100}, "k", ""},
		{"below threshold", map[string]int{"k": 70, "b": assistMinDamage - 1}, "k", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := topAssister(tc.damageBy, tc.killer); got != tc.want {
				t.Fatalf("topAssister = %q; want %q", got, tc.want)
			}
		})
	}
}

func TestRespawnAfterDelayResetsLife(t *testing.T) {
	r := newRand()
	x, v := combatant("x", fixed(100)), combatant("v", fixed(1))
	resolveShot(r, x, v, 1, 0, t0)
	if v.canRespawn(t0.Add(respawnDelay - time.Millisecond)) {
		t.Fatal("respawned before the delay elapsed")
	}
	if !v.canRespawn(t0.Add(respawnDelay)) {
		t.Fatal("cannot respawn after the delay")
	}
	v.spawn(Position{})
	if v.hp != maxHP || !v.alive || len(v.damageBy) != 0 {
		t.Fatalf("spawn did not reset life: hp=%d alive=%v damageBy=%v", v.hp, v.alive, v.damageBy)
	}
}

func TestWeaponCatalogIsValid(t *testing.T) {
	seen := map[string]bool{}
	for _, w := range weaponCatalog {
		if seen[w.ID] {
			t.Errorf("duplicate weapon %q", w.ID)
		}
		seen[w.ID] = true
		if w.BodyDamageMin <= 0 || w.BodyDamageMax < w.BodyDamageMin || w.Cost <= 0 || w.BuyWeight <= 0 {
			t.Errorf("invalid weapon %+v", w)
		}
	}
}

func TestPickWeaponFollowsBuyWeights(t *testing.T) {
	r := newRand()
	const n = 200_000
	counts := map[string]int{}
	for range n {
		counts[pickWeapon(r).ID]++
	}
	var total float64
	for _, w := range weaponCatalog {
		total += w.BuyWeight
	}
	for _, w := range weaponCatalog {
		got, want := float64(counts[w.ID])/n, w.BuyWeight/total
		if diff := got - want; diff > 0.01 || diff < -0.01 {
			t.Errorf("%s share = %.3f; want %.3f", w.ID, got, want)
		}
	}
}
