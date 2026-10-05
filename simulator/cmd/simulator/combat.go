package main

import (
	"math/rand/v2"
	"time"
)

// Combat rules, modelled on CS-style tactical shooters.
const (
	maxHP              = 100
	headshotMultiplier = 4
	// assistMinDamage is the health a non-killer must have removed from the
	// victim's current life to earn an assist (CS2 uses the same threshold).
	assistMinDamage = 40
	respawnDelay    = 3 * time.Second
)

// Weapon is a purchasable gun. Body damage is drawn uniformly from
// [BodyDamageMin, BodyDamageMax]; a headshot multiplies it.
type Weapon struct {
	ID            string
	Cost          int
	BodyDamageMin int
	BodyDamageMax int
	BuyWeight     float64 // relative purchase popularity
}

var weaponCatalog = []Weapon{
	{ID: "ak47", Cost: 2700, BodyDamageMin: 33, BodyDamageMax: 40, BuyWeight: 0.35},
	{ID: "m4a4", Cost: 3100, BodyDamageMin: 28, BodyDamageMax: 34, BuyWeight: 0.30},
	{ID: "awp", Cost: 4750, BodyDamageMin: 100, BodyDamageMax: 115, BuyWeight: 0.10},
	{ID: "deagle", Cost: 700, BodyDamageMin: 45, BodyDamageMax: 60, BuyWeight: 0.15},
	{ID: "usp", Cost: 200, BodyDamageMin: 20, BodyDamageMax: 30, BuyWeight: 0.10},
}

// pickWeapon draws a weapon weighted by purchase popularity.
func pickWeapon(r *rand.Rand) Weapon {
	var total float64
	for _, w := range weaponCatalog {
		total += w.BuyWeight
	}
	x := r.Float64() * total
	for _, w := range weaponCatalog {
		if x < w.BuyWeight {
			return w
		}
		x -= w.BuyWeight
	}
	return weaponCatalog[len(weaponCatalog)-1] // floating-point remainder
}

// shotResult is the outcome of one trigger pull.
type shotResult struct {
	Hit      bool
	Headshot bool
	Kill     bool
	Damage   int // health actually removed (overkill is not counted)
	HPAfter  int
	Assister string
}

// resolveShot applies one shot from attacker to victim. Health persists
// across attackers: damage from anyone accumulates, the player who removes
// the last point gets the kill, and the top other contributor with at least
// assistMinDamage gets the assist.
func resolveShot(r *rand.Rand, attacker, victim *Player, accuracy, headshotChance float64, now time.Time) shotResult {
	if r.Float64() >= accuracy {
		return shotResult{HPAfter: victim.hp}
	}
	res := shotResult{Hit: true, Headshot: r.Float64() < headshotChance}

	w := attacker.weapon
	raw := w.BodyDamageMin + r.IntN(w.BodyDamageMax-w.BodyDamageMin+1)
	if res.Headshot {
		raw *= headshotMultiplier
	}
	res.Damage = min(raw, victim.hp)
	victim.hp -= res.Damage
	victim.damageBy[attacker.ID] += res.Damage
	res.HPAfter = victim.hp

	if victim.hp == 0 {
		res.Kill = true
		res.Assister = topAssister(victim.damageBy, attacker.ID)
		victim.die(now)
	}
	return res
}

// topAssister returns the non-killer who removed the most health (ties
// broken by ID for determinism), or "" if nobody reached assistMinDamage.
func topAssister(damageBy map[string]int, killer string) string {
	best, bestDmg := "", 0
	for id, dmg := range damageBy {
		if id == killer || dmg < assistMinDamage {
			continue
		}
		if dmg > bestDmg || (dmg == bestDmg && id < best) {
			best, bestDmg = id, dmg
		}
	}
	return best
}

// spawn puts the player back into play at full health.
func (p *Player) spawn(pos Position) {
	p.hp = maxHP
	p.alive = true
	p.pos = pos
	clear(p.damageBy)
}

func (p *Player) die(now time.Time) {
	p.hp = 0
	p.alive = false
	p.respawnAt = now.Add(respawnDelay)
	clear(p.damageBy)
}

// canRespawn reports whether a dead player's respawn timer has elapsed.
func (p *Player) canRespawn(now time.Time) bool {
	return !p.alive && !now.Before(p.respawnAt)
}
