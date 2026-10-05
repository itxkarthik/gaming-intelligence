package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const repoProfiles = "../../profiles"

func TestRepoProfilesAreCompleteAndValid(t *testing.T) {
	profiles, err := loadProfiles(repoProfiles)
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != len(archetypes) {
		t.Fatalf("loaded %d profiles; want %d", len(profiles), len(archetypes))
	}
	// smurf_detection keys on young accounts with few games.
	if s := profiles[ArchetypeSmurf]; s.AccountAgeDays >= 14 || s.GamesPlayed >= 50 {
		t.Fatalf("smurf profile looks established: %+v", s)
	}
	// The detector relies on cheaters out-aiming every legitimate class.
	for _, a := range []Archetype{ArchetypeNormalBronze, ArchetypeNormalGold, ArchetypeNormalDiamond, ArchetypeToxic} {
		if profiles[a].Accuracy.Mean >= profiles[ArchetypeCheaterAimbot].Accuracy.Mean {
			t.Fatalf("%s out-aims the aimbot", a)
		}
	}
}

// writeProfiles copies the repo profiles into a temp dir and applies edit.
func writeProfiles(t *testing.T, edit func(dir string)) string {
	t.Helper()
	dir := t.TempDir()
	for _, a := range archetypes {
		b, err := os.ReadFile(filepath.Join(repoProfiles, string(a)+profileExt))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, string(a)+profileExt), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	edit(dir)
	return dir
}

func TestLoadProfilesRejectsBadInput(t *testing.T) {
	gold := string(ArchetypeNormalGold) + profileExt
	rewrite := func(dir, file, old, new string) {
		path := filepath.Join(dir, file)
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(b), old) {
			t.Fatalf("%s lacks %q", file, old)
		}
		if err := os.WriteFile(path, []byte(strings.Replace(string(b), old, new, 1)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		name string
		edit func(dir string)
		want string
	}{
		{"missing archetype", func(d string) { os.Remove(filepath.Join(d, gold)) }, "normal_gold.yaml"},
		{"unknown archetype file", func(d string) { os.WriteFile(filepath.Join(d, "pro.yaml"), nil, 0o644) }, "pro.yaml is not a known archetype"},
		{"typo'd key", func(d string) { rewrite(d, gold, "games_played", "games_playd") }, "field games_playd not found"},
		{"empty file", func(d string) { os.WriteFile(filepath.Join(d, gold), nil, 0o644) }, "file is empty"},
		{"probability above one", func(d string) { rewrite(d, gold, "mean: 0.28", "mean: 1.28") }, "accuracy.mean must be in [0,1]"},
		{"negative std", func(d string) { rewrite(d, gold, "std: 0.08", "std: -0.08") }, "accuracy.std must be >= 0"},
		{"unknown tier", func(d string) { rewrite(d, gold, `skill_tier: "gold"`, `skill_tier: "platinum"`) }, "skill_tier"},
		{"impossible rate", func(d string) { rewrite(d, gold, "chat_per_minute: 0.4", "chat_per_minute: 120") }, "activity.chat_per_minute"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadProfiles(writeProfiles(t, tc.edit))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v; want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestLoadProfilesMissingDir(t *testing.T) {
	if _, err := loadProfiles(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatal("want an error for a missing directory")
	}
}
