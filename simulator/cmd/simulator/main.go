package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v3"
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
	DryRun            bool
}

// PlayerProfile represents statistical parameters for a player archetype.
type ProfileDistribution struct {
	Mean float64 `yaml:"mean"`
	Std  float64 `yaml:"std"`
}

type PlayerProfile struct {
	Name           string              `yaml:"name"`
	SkillTier      string              `yaml:"skill_tier"`
	Accuracy       ProfileDistribution `yaml:"accuracy"`
	HeadshotRatio  ProfileDistribution `yaml:"headshot_ratio"`
	ReactionTimeMs ProfileDistribution `yaml:"reaction_time_ms"`
	KillsPerMin    ProfileDistribution `yaml:"kills_per_minute"`
	DeathsPerMin   ProfileDistribution `yaml:"deaths_per_minute"`
}

type Player struct {
	ID        string
	Name      string
	Archetype string
	Profile   PlayerProfile
	TeamID    string
	MatchID   string
	ServerID  string
}

type Match struct {
	ID       string
	ServerID string
	TeamA    []*Player
	TeamB    []*Player
}

// Event structures matching the PySpark schemas
type GameplayEvent struct {
	EventID        string   `json:"event_id"`
	EventType      string   `json:"event_type"`
	MatchID        string   `json:"match_id"`
	PlayerID       string   `json:"player_id"`
	TeamID         string   `json:"team_id"`
	TargetPlayerID *string  `json:"target_player_id,omitempty"`
	WeaponID       *string  `json:"weapon_id,omitempty"`
	Damage         *float32 `json:"damage,omitempty"`
	PositionX      float32  `json:"position_x"`
	PositionY      float32  `json:"position_y"`
	PositionZ      float32  `json:"position_z"`
	Accuracy       *float32 `json:"accuracy,omitempty"`
	Distance       *float32 `json:"distance,omitempty"`
	ReactionTimeMs *int     `json:"reaction_time_ms,omitempty"`
	IsHeadshot     bool     `json:"is_headshot"`
	EventTime      int64    `json:"event_time"`
	ServerID       string   `json:"server_id"`
}

type ServerMetric struct {
	ServerID          string  `json:"server_id"`
	Region            string  `json:"region"`
	CPUPercent        float32 `json:"cpu_percent"`
	RAMPercent        float32 `json:"ram_percent"`
	TickRate          int     `json:"tick_rate"`
	PacketLossPercent float32 `json:"packet_loss_percent"`
	AvgLatencyMs      float32 `json:"avg_latency_ms"`
	ActivePlayers     int     `json:"active_players"`
	ActiveMatches     int     `json:"active_matches"`
	Timestamp         int64   `json:"timestamp"`
}

type PlayerEvent struct {
	EventID   string            `json:"event_id"`
	PlayerID  string            `json:"player_id"`
	EventType string            `json:"event_type"`
	MatchID   *string           `json:"match_id,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	EventTime int64             `json:"event_time"`
	ServerID  *string           `json:"server_id,omitempty"`
}

func sampleNormal(dist ProfileDistribution, minVal, maxVal float64) float64 {
	val := rand.NormFloat64()*dist.Std + dist.Mean
	if val < minVal {
		val = minVal
	}
	if val > maxVal {
		val = maxVal
	}
	return val
}

// chooseArchetype maps sampling rolls to a profile key.
// roll picks the archetype class (cheater / smurf / toxic / normal);
// rankRoll disambiguates within a class (aimbot vs wallhack, rank tier).
func chooseArchetype(roll, rankRoll, cheaterRatio, smurfRatio, toxicRatio float64) string {
	switch {
	case roll < cheaterRatio:
		if rankRoll < 0.7 {
			return "cheater_aimbot"
		}
		return "cheater_wallhack"
	case roll < cheaterRatio+smurfRatio:
		return "smurf"
	case roll < cheaterRatio+smurfRatio+toxicRatio:
		return "toxic"
	default:
		switch {
		case rankRoll < 0.4:
			return "normal_bronze"
		case rankRoll < 0.8:
			return "normal_gold"
		default:
			return "normal_diamond"
		}
	}
}

// validatePoolSize rejects configurations that would corrupt event semantics.
// Players are assigned to matches in place (player.MatchID / player.TeamID are
// overwritten per match), so a pool smaller than matches*10 would put the same
// player in two matches simultaneously and corrupt team attribution.
func validatePoolSize(players, matchesConcurrent int) error {
	if matchesConcurrent > 0 && players < matchesConcurrent*10 {
		return fmt.Errorf("--players (%d) must be at least --matches-concurrent*10 (%d); "+
			"the player pool is assigned to matches in place",
			players, matchesConcurrent*10)
	}
	return nil
}

func loadProfiles(dir string) (map[string]PlayerProfile, error) {
	profiles := make(map[string]PlayerProfile)
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".yaml") || strings.HasSuffix(f.Name(), ".yml") {
			data, err := os.ReadFile(filepath.Join(dir, f.Name()))
			if err != nil {
				continue
			}
			var p PlayerProfile
			if err := yaml.Unmarshal(data, &p); err == nil {
				key := strings.TrimSuffix(f.Name(), filepath.Ext(f.Name()))
				profiles[key] = p
			}
		}
	}
	return profiles, nil
}

// initialPlayerEvents emits the session lifecycle events produced once per
// simulation start: logins, queue entries, and match joins. LOGIN metadata
// carries the archetype label as ground truth for later ML jobs.
func initialPlayerEvents(players []*Player, matches []*Match, start time.Time) []PlayerEvent {
	var events []PlayerEvent
	base := start.UnixMilli()

	for _, p := range players {
		var serverID *string
		if p.ServerID != "" {
			serverID = &p.ServerID
		}
		events = append(events,
			PlayerEvent{
				EventID:   uuid.New().String(),
				PlayerID:  p.ID,
				EventType: "LOGIN",
				Metadata:  map[string]string{"archetype": p.Archetype},
				EventTime: base,
				ServerID:  serverID,
			},
			PlayerEvent{
				EventID:   uuid.New().String(),
				PlayerID:  p.ID,
				EventType: "MATCHMAKING_START",
				EventTime: base + 100,
				ServerID:  serverID,
			},
			PlayerEvent{
				EventID:   uuid.New().String(),
				PlayerID:  p.ID,
				EventType: "MATCHMAKING_FOUND",
				EventTime: base + 250,
				ServerID:  serverID,
			},
		)
	}

	for _, m := range matches {
		join := func(p *Player) {
			var serverID *string
			if m.ServerID != "" {
				serverID = &m.ServerID
			}
			events = append(events, PlayerEvent{
				EventID:   uuid.New().String(),
				PlayerID:  p.ID,
				EventType: "MATCH_JOIN",
				MatchID:   &m.ID,
				Metadata:  map[string]string{"team_id": p.TeamID, "skill_tier": p.Profile.SkillTier},
				EventTime: base + 400,
				ServerID:  serverID,
			})
		}
		for _, p := range m.TeamA {
			join(p)
		}
		for _, p := range m.TeamB {
			join(p)
		}
	}
	return events
}

func main() {
	cfg := Config{}

	flag.IntVar(&cfg.Players, "players", 500, "Total simulated player pool size")
	flag.IntVar(&cfg.MatchesConcurrent, "matches-concurrent", 20, "Number of concurrent matches to simulate")
	flag.IntVar(&cfg.EventsPerSec, "events-per-sec", 1000, "Target events published to Kafka per second")
	flag.Float64Var(&cfg.CheaterRatio, "cheater-ratio", 0.05, "Proportion of active players exhibiting cheater characteristics")
	flag.Float64Var(&cfg.SmurfRatio, "smurf-ratio", 0.05, "Proportion of active players exhibiting smurf characteristics")
	flag.Float64Var(&cfg.ToxicRatio, "toxic-ratio", 0.05, "Proportion of active players exhibiting toxic traits")
	flag.IntVar(&cfg.ServerCount, "server-count", 10, "Total number of virtual game server instances")
	// Use localhost:9094 as default for external host access
	flag.StringVar(&cfg.KafkaBrokers, "kafka-brokers", "localhost:9094", "Comma-separated list of Kafka broker addresses")
	flag.DurationVar(&cfg.Duration, "duration", 5*time.Minute, "Total simulation run duration (e.g. 5m, 1h)")
	flag.Float64Var(&cfg.LateEventRatio, "late-event-ratio", 0.05, "Proportion of events emitted with simulated network delay")
	flag.StringVar(&cfg.ProfilesDir, "profiles-dir", "./profiles", "Directory containing YAML player archetype profiles")
	flag.BoolVar(&cfg.DryRun, "dry-run", false, "Simulate without sending to Kafka (prints stats to console)")

	flag.Parse()

	if err := validatePoolSize(cfg.Players, cfg.MatchesConcurrent); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("=================================================================")
	fmt.Println("🎮 Gaming Intelligence Platform - Real-Time Event Simulator")
	fmt.Println("=================================================================")
	fmt.Printf("• Player Pool Size       : %d\n", cfg.Players)
	fmt.Printf("• Concurrent Matches     : %d\n", cfg.MatchesConcurrent)
	fmt.Printf("• Target Throughput      : %d events/sec\n", cfg.EventsPerSec)
	fmt.Printf("• Cheater Ratio          : %.2f%%\n", cfg.CheaterRatio*100)
	fmt.Printf("• Smurf Ratio            : %.2f%%\n", cfg.SmurfRatio*100)
	fmt.Printf("• Server Count           : %d\n", cfg.ServerCount)
	fmt.Printf("• Kafka Brokers          : %s\n", cfg.KafkaBrokers)
	fmt.Printf("• Duration               : %v\n", cfg.Duration)
	fmt.Printf("• Dry Run Mode           : %v\n", cfg.DryRun)
	fmt.Println("=================================================================")

	profilesDir := cfg.ProfilesDir
	if _, err := os.Stat(profilesDir); os.IsNotExist(err) {
		if _, err2 := os.Stat("simulator/profiles"); err2 == nil {
			profilesDir = "simulator/profiles"
		}
	}

	profiles, err := loadProfiles(profilesDir)
	if err != nil || len(profiles) == 0 {
		fmt.Printf("[WARN] Failed to load profiles from %s: %v. Using built-in fallbacks.\n", profilesDir, err)
		profiles = map[string]PlayerProfile{
			"normal_gold": {
				Name:           "Normal Gold Player",
				SkillTier:      "gold",
				Accuracy:       ProfileDistribution{Mean: 0.28, Std: 0.08},
				HeadshotRatio:  ProfileDistribution{Mean: 0.15, Std: 0.05},
				ReactionTimeMs: ProfileDistribution{Mean: 260, Std: 50},
			},
			"cheater_aimbot": {
				Name:           "Aimbot Cheater",
				SkillTier:      "silver",
				Accuracy:       ProfileDistribution{Mean: 0.93, Std: 0.03},
				HeadshotRatio:  ProfileDistribution{Mean: 0.88, Std: 0.05},
				ReactionTimeMs: ProfileDistribution{Mean: 85, Std: 10},
			},
		}
	} else {
		fmt.Printf("Loaded %d player profiles from %s.\n", len(profiles), profilesDir)
	}

	brokers := strings.Split(cfg.KafkaBrokers, ",")
	var gameplayWriter, playerWriter, serverWriter *kafka.Writer
	var kafkaWriteErrors atomic.Uint64
	writersDone := make(chan struct{})

	// Async writers swallow delivery failures (WriteMessages returns nil
	// once queued), but synchronous writes serialize the publisher behind
	// every 20ms batch flush (~50 msg/s). So: run async for throughput, a
	// monitor polls each writer's error counter so broker failures still
	// surface mid-run, and recordWriteError catches the errors kafka-go
	// does return synchronously (e.g. partition metadata lookup).
	recordWriteError := func(topic string, err error) {
		if err == nil {
			return
		}
		if n := kafkaWriteErrors.Add(1); n <= 5 {
			fmt.Fprintf(os.Stderr, "[KAFKA] %s write failed: %v\n", topic, err)
		}
	}

	if !cfg.DryRun {
		createWriter := func(topic string) *kafka.Writer {
			w := &kafka.Writer{
				Addr:         kafka.TCP(brokers...),
				Topic:        topic,
				Balancer:     &kafka.LeastBytes{},
				BatchSize:    200,
				BatchTimeout: 20 * time.Millisecond,
				Async:        true,
			}
			go func() {
				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()
				var last int64
				for {
					select {
					case <-writersDone:
						return
					case <-ticker.C:
						if n := w.Stats().Errors; n > last {
							fmt.Fprintf(os.Stderr, "[KAFKA] %s: %d async write failure(s)\n", topic, n-last)
							last = n
						}
					}
				}
			}()
			return w
		}
		gameplayWriter = createWriter("gameplay_events")
		playerWriter = createWriter("player_events")
		serverWriter = createWriter("server_metrics")
	}

	// Build servers
	regions := []string{"us-east", "eu-west", "ap-south"}
	servers := make([]string, cfg.ServerCount)
	for i := 0; i < cfg.ServerCount; i++ {
		servers[i] = fmt.Sprintf("server-%02d", i+1)
	}

	// Build players
	players := make([]*Player, cfg.Players)
	for i := 0; i < cfg.Players; i++ {
		archetype := chooseArchetype(rand.Float64(), rand.Float64(), cfg.CheaterRatio, cfg.SmurfRatio, cfg.ToxicRatio)

		pProf, ok := profiles[archetype]
		if !ok {
			pProf = profiles["normal_gold"]
		}

		players[i] = &Player{
			ID:        fmt.Sprintf("player_%04d", i+1),
			Name:      fmt.Sprintf("Player_%d", i+1),
			Archetype: archetype,
			Profile:   pProf,
		}
	}

	// Build matches
	matches := make([]*Match, cfg.MatchesConcurrent)
	playersPerMatch := 10
	for m := 0; m < cfg.MatchesConcurrent; m++ {
		matchID := fmt.Sprintf("match_%04d", m+1)
		srvID := servers[m%len(servers)]
		match := &Match{
			ID:       matchID,
			ServerID: srvID,
		}
		startIdx := (m * playersPerMatch) % len(players)
		for p := 0; p < playersPerMatch; p++ {
			player := players[(startIdx+p)%len(players)]
			player.MatchID = matchID
			player.ServerID = srvID
			if p < playersPerMatch/2 {
				player.TeamID = "team_a"
				match.TeamA = append(match.TeamA, player)
			} else {
				player.TeamID = "team_b"
				match.TeamB = append(match.TeamB, player)
			}
		}
		matches[m] = match
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nReceived termination signal. Shutting down simulator...")
		cancel()
	}()

	var totalGameplayEvents atomic.Uint64
	var totalServerEvents atomic.Uint64
	var totalPlayerEvents atomic.Uint64

	// Emit the player session lifecycle burst (login -> queue -> join).
	for _, ev := range initialPlayerEvents(players, matches, time.Now()) {
		if !cfg.DryRun && playerWriter != nil {
			payload, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			recordWriteError("player_events", playerWriter.WriteMessages(ctx, kafka.Message{
				Key:   []byte(ev.PlayerID),
				Value: payload,
			}))
		}
		totalPlayerEvents.Add(1)
	}

	limiter := rate.NewLimiter(rate.Limit(cfg.EventsPerSec), cfg.EventsPerSec*2)
	weapons := []string{"ak47", "m4a4", "awp", "usp", "deagle"}

	// publishGameplay rate-limits at the event level: --events-per-sec is
	// the number of messages actually written to Kafka, not shot attempts
	// (one attempt can emit SHOT_FIRED + DAMAGE + KILL).
	publishGameplay := func(ev GameplayEvent) {
		if err := limiter.Wait(ctx); err != nil {
			return
		}
		payload, err := json.Marshal(ev)
		if err != nil {
			return
		}
		if !cfg.DryRun && gameplayWriter != nil {
			recordWriteError("gameplay_events", gameplayWriter.WriteMessages(ctx, kafka.Message{
				Key:   []byte(ev.PlayerID),
				Value: payload,
			}))
		}
		totalGameplayEvents.Add(1)
	}

	var wg sync.WaitGroup

	// ─── 1. Server Metrics Routine (Emitted every 1s per server) ───
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				for idx, srv := range servers {
					region := regions[idx%len(regions)]
					// Introduce simulated degradation on server-02 for demonstration
					var cpu, ram, loss, latency float32
					tickRate := 128
					if srv == "server-02" {
						cpu = float32(85.0 + rand.Float64()*12.0)
						ram = float32(88.0 + rand.Float64()*10.0)
						loss = float32(6.0 + rand.Float64()*8.0)
						latency = float32(95.0 + rand.Float64()*40.0)
					} else {
						cpu = float32(20.0 + rand.Float64()*35.0)
						ram = float32(35.0 + rand.Float64()*25.0)
						loss = float32(rand.Float64() * 0.8)
						latency = float32(15.0 + rand.Float64()*25.0)
					}

					metric := ServerMetric{
						ServerID:          srv,
						Region:            region,
						CPUPercent:        cpu,
						RAMPercent:        ram,
						TickRate:          tickRate,
						PacketLossPercent: loss,
						AvgLatencyMs:      latency,
						ActivePlayers:     len(players) / len(servers),
						ActiveMatches:     cfg.MatchesConcurrent / len(servers),
						Timestamp:         now.UnixMilli(),
					}

					payload, _ := json.Marshal(metric)
					if !cfg.DryRun && serverWriter != nil {
						recordWriteError("server_metrics", serverWriter.WriteMessages(ctx, kafka.Message{
							Key:   []byte(srv),
							Value: payload,
						}))
					}
					totalServerEvents.Add(1)
				}
			}
		}
	}()

	// ─── 2. Gameplay Events Generator (Combat & Actions) ───
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				match := matches[rand.Intn(len(matches))]
				attackerTeam := match.TeamA
				defenderTeam := match.TeamB
				if rand.Float64() < 0.5 {
					attackerTeam = match.TeamB
					defenderTeam = match.TeamA
				}

				attacker := attackerTeam[rand.Intn(len(attackerTeam))]
				defender := defenderTeam[rand.Intn(len(defenderTeam))]

				accuracy := float32(sampleNormal(attacker.Profile.Accuracy, 0.05, 1.0))
				hsChance := sampleNormal(attacker.Profile.HeadshotRatio, 0.02, 1.0)
				rxnTime := int(sampleNormal(attacker.Profile.ReactionTimeMs, 50, 600))
				weapon := weapons[rand.Intn(len(weapons))]
				dist := float32(10.0 + rand.Float64()*40.0)

				now := time.Now()
				// Late event injection
				if rand.Float64() < cfg.LateEventRatio {
					now = now.Add(-time.Duration(5+rand.Intn(15)) * time.Second)
				}
				eventTime := now.UnixMilli()

				// 1. Emit Shot Fired
				shotEvent := GameplayEvent{
					EventID:        uuid.New().String(),
					EventType:      "SHOT_FIRED",
					MatchID:        match.ID,
					PlayerID:       attacker.ID,
					TeamID:         attacker.TeamID,
					TargetPlayerID: &defender.ID,
					WeaponID:       &weapon,
					Accuracy:       &accuracy,
					Distance:       &dist,
					ReactionTimeMs: &rxnTime,
					IsHeadshot:     false,
					EventTime:      eventTime,
					ServerID:       match.ServerID,
				}
				publishGameplay(shotEvent)

				// 2. Check if hit
				if rand.Float32() < accuracy {
					isHeadshot := rand.Float64() < hsChance
					var dmg float32 = 28.0 + rand.Float32()*35.0
					if isHeadshot {
						dmg = 120.0
					}

					hitEvent := GameplayEvent{
						EventID:        uuid.New().String(),
						EventType:      "DAMAGE",
						MatchID:        match.ID,
						PlayerID:       attacker.ID,
						TeamID:         attacker.TeamID,
						TargetPlayerID: &defender.ID,
						WeaponID:       &weapon,
						Damage:         &dmg,
						Accuracy:       &accuracy,
						Distance:       &dist,
						ReactionTimeMs: &rxnTime,
						IsHeadshot:     isHeadshot,
						EventTime:      eventTime + int64(rxnTime),
						ServerID:       match.ServerID,
					}
					publishGameplay(hitEvent)

					// 3. Check for kill
					if dmg >= 100.0 || isHeadshot || rand.Float64() < 0.25 {
						killEvent := GameplayEvent{
							EventID:        uuid.New().String(),
							EventType:      "KILL",
							MatchID:        match.ID,
							PlayerID:       attacker.ID,
							TeamID:         attacker.TeamID,
							TargetPlayerID: &defender.ID,
							WeaponID:       &weapon,
							Damage:         &dmg,
							Accuracy:       &accuracy,
							Distance:       &dist,
							ReactionTimeMs: &rxnTime,
							IsHeadshot:     isHeadshot,
							EventTime:      eventTime + int64(rxnTime) + 10,
							ServerID:       match.ServerID,
						}
						publishGameplay(killEvent)
					}
				}
			}
		}
	}()

	// ─── 3. Player Activity Events (chat, reports, disconnects) ───
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		disconnected := make(map[string]*Player)

		emit := func(p *Player, eventType string, meta map[string]string) {
			ev := PlayerEvent{
				EventID:   uuid.New().String(),
				PlayerID:  p.ID,
				EventType: eventType,
				EventTime: time.Now().UnixMilli(),
			}
			if p.MatchID != "" {
				ev.MatchID = &p.MatchID
			}
			if p.ServerID != "" {
				ev.ServerID = &p.ServerID
			}
			if meta != nil {
				ev.Metadata = meta
			}
			if !cfg.DryRun && playerWriter != nil {
				if payload, err := json.Marshal(ev); err == nil {
					recordWriteError("player_events", playerWriter.WriteMessages(ctx, kafka.Message{
						Key:   []byte(p.ID),
						Value: payload,
					}))
				}
			}
			totalPlayerEvents.Add(1)
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Dropped players attempt a reconnect (30% each tick).
				for id, p := range disconnected {
					if rand.Float64() < 0.30 {
						emit(p, "RECONNECT", nil)
						delete(disconnected, id)
					}
				}

				// Sparse per-player activity sampled each tick.
				for i := 0; i < 5; i++ {
					p := players[rand.Intn(len(players))]
					if _, down := disconnected[p.ID]; down {
						continue
					}
					switch {
					case p.Archetype == "toxic" && rand.Float64() < 0.30:
						emit(p, "CHAT_MESSAGE", map[string]string{
							"chars": fmt.Sprintf("%d", 10+rand.Intn(120)),
						})
					case p.Archetype == "toxic" && rand.Float64() < 0.20:
						target := players[rand.Intn(len(players))]
						if target.ID != p.ID {
							emit(p, "REPORT_PLAYER", map[string]string{
								"reported_player": target.ID,
								"reason":          []string{"gameplay", "chat", "griefing"}[rand.Intn(3)],
							})
						}
					case rand.Float64() < 0.01:
						emit(p, "DISCONNECT", map[string]string{
							"cause": []string{"network", "crash", "quit"}[rand.Intn(3)],
						})
						disconnected[p.ID] = p
					}
				}
			}
		}
	}()

	// ─── 4. Periodic Progress Reporter ───
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		startTime := time.Now()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				elapsed := time.Since(startTime).Seconds()
				gpTotal := totalGameplayEvents.Load()
				svTotal := totalServerEvents.Load()
				rate := float64(gpTotal+svTotal) / elapsed

				fmt.Printf("[SIMULATOR] Elapsed: %4.1fs | Gameplay Events: %7d | Server Metrics: %5d | Rate: %6.0f events/sec\n",
					elapsed, gpTotal, svTotal, rate)
			}
		}
	}()

	wg.Wait()

	// Stop monitors, let async batches flush (BatchTimeout is 20ms), tally
	// the writers' cumulative failure counters, then shut the writers down.
	totalWriteErrors := kafkaWriteErrors.Load()
	if !cfg.DryRun {
		close(writersDone)
		time.Sleep(300 * time.Millisecond)
		totalWriteErrors += uint64(gameplayWriter.Stats().Errors)
		totalWriteErrors += uint64(playerWriter.Stats().Errors)
		totalWriteErrors += uint64(serverWriter.Stats().Errors)
		_ = gameplayWriter.Close()
		_ = playerWriter.Close()
		_ = serverWriter.Close()
	}

	fmt.Println("\n=================================================================")
	fmt.Println("Simulation completed successfully.")
	fmt.Printf("Total Gameplay Events Produced : %d\n", totalGameplayEvents.Load())
	fmt.Printf("Total Server Metrics Produced  : %d\n", totalServerEvents.Load())
	fmt.Printf("Total Player Events Produced   : %d\n", totalPlayerEvents.Load())
	fmt.Printf("Kafka Write Errors             : %d\n", totalWriteErrors)
	fmt.Println("=================================================================")
}
