package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

// Archetype is a player behaviour class; each one is backed by a YAML
// profile named <archetype>.yaml in the profiles directory.
type Archetype string

const (
	ArchetypeNormalBronze    Archetype = "normal_bronze"
	ArchetypeNormalGold      Archetype = "normal_gold"
	ArchetypeNormalDiamond   Archetype = "normal_diamond"
	ArchetypeCheaterAimbot   Archetype = "cheater_aimbot"
	ArchetypeCheaterWallhack Archetype = "cheater_wallhack"
	ArchetypeSmurf           Archetype = "smurf"
	ArchetypeToxic           Archetype = "toxic"
)

// archetypes lists every class chooseArchetype can produce; each must have
// a profile.
var archetypes = []Archetype{
	ArchetypeNormalBronze, ArchetypeNormalGold, ArchetypeNormalDiamond,
	ArchetypeCheaterAimbot, ArchetypeCheaterWallhack, ArchetypeSmurf, ArchetypeToxic,
}

// skillTiers are the ranks match_quality maps to numeric skill.
var skillTiers = []string{"bronze", "silver", "gold", "diamond"}

const profileExt = ".yaml"

// Distribution is a normal distribution a per-shot value is drawn from.
type Distribution struct {
	Mean float64 `yaml:"mean"`
	Std  float64 `yaml:"std"`
}

// Activity holds per-player rates for non-combat player events.
type Activity struct {
	ChatPerMinute      float64 `yaml:"chat_per_minute"`
	ReportedPerMinute  float64 `yaml:"reported_per_minute"`
	DisconnectsPerHour float64 `yaml:"disconnects_per_hour"`
	PurchasesPerMinute float64 `yaml:"purchases_per_minute"`
}

// Profile is the statistical description of one archetype.
type Profile struct {
	Name           string       `yaml:"name"`
	SkillTier      string       `yaml:"skill_tier"`
	AccountAgeDays int          `yaml:"account_age_days"`
	GamesPlayed    int          `yaml:"games_played"`
	Accuracy       Distribution `yaml:"accuracy"`
	HeadshotRatio  Distribution `yaml:"headshot_ratio"`
	ReactionTimeMs Distribution `yaml:"reaction_time_ms"`
	Activity       Activity     `yaml:"activity"`
}

// loadProfiles loads and validates one profile per archetype. A missing,
// malformed or unexpected profile is an error: running with a substitute
// would publish archetype labels that do not match the generated behaviour.
func loadProfiles(dir string) (map[Archetype]*Profile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("profiles: %w", err)
	}

	var errs []error
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || filepath.Ext(name) != profileExt {
			continue
		}
		if !slices.Contains(archetypes, Archetype(strings.TrimSuffix(name, profileExt))) {
			errs = append(errs, fmt.Errorf("profiles: %s is not a known archetype", name))
		}
	}

	profiles := make(map[Archetype]*Profile, len(archetypes))
	for _, a := range archetypes {
		p, err := loadProfile(filepath.Join(dir, string(a)+profileExt))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		profiles[a] = p
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return profiles, nil
}

func loadProfile(path string) (*Profile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("profiles: %w", err)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true) // a typo'd key must fail, not silently become zero
	var p Profile
	if err := dec.Decode(&p); err != nil {
		if errors.Is(err, io.EOF) {
			err = errors.New("file is empty")
		}
		return nil, fmt.Errorf("profiles: %s: %w", path, err)
	}
	if err := p.validate(); err != nil {
		return nil, fmt.Errorf("profiles: %s: %w", path, err)
	}
	return &p, nil
}

func (p *Profile) validate() error {
	var errs []error
	check := func(ok bool, format string, args ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf(format, args...))
		}
	}
	check(p.Name != "", "name is required")
	check(slices.Contains(skillTiers, p.SkillTier), "skill_tier %q must be one of %v", p.SkillTier, skillTiers)
	check(p.AccountAgeDays > 0, "account_age_days must be > 0")
	check(p.GamesPlayed > 0, "games_played must be > 0")

	probability := func(field string, d Distribution) {
		check(d.Mean >= 0 && d.Mean <= 1, "%s.mean must be in [0,1] (got %v)", field, d.Mean)
		check(d.Std >= 0, "%s.std must be >= 0 (got %v)", field, d.Std)
	}
	probability("accuracy", p.Accuracy)
	probability("headshot_ratio", p.HeadshotRatio)
	check(p.ReactionTimeMs.Mean > 0, "reaction_time_ms.mean must be > 0")
	check(p.ReactionTimeMs.Std >= 0, "reaction_time_ms.std must be >= 0")

	// Each rate becomes a per-tick probability, so it must not exceed one
	// event per tick.
	rate := func(field string, v, unitsPerTick float64) {
		check(v >= 0 && v*unitsPerTick <= 1, "activity.%s must be in [0, %v] (got %v)", field, 1/unitsPerTick, v)
	}
	a := p.Activity
	rate("chat_per_minute", a.ChatPerMinute, tickInterval.Minutes())
	rate("reported_per_minute", a.ReportedPerMinute, tickInterval.Minutes())
	rate("purchases_per_minute", a.PurchasesPerMinute, tickInterval.Minutes())
	rate("disconnects_per_hour", a.DisconnectsPerHour, tickInterval.Hours())
	return errors.Join(errs...)
}
