package main

import (
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// Lobby shape.
const (
	playersPerTeam  = 5
	playersPerMatch = 2 * playersPerTeam
)

// tickInterval drives match rotation, player activity and server metrics.
const tickInterval = time.Second

// Team is a side of a match as it appears on the wire.
type Team string

const (
	TeamA Team = "team_a"
	TeamB Team = "team_b"
)

var teamIDs = [2]Team{TeamA, TeamB}

// Arena geometry, in metres.
const (
	arenaSize   = 120.0
	arenaHeight = 8.0
	maxMoveStep = 2.0 // how far a shooter moves between shots
)

// Per-shot aim draws are clamped to what a human (or a cheat) can produce.
const (
	minAccuracy       = 0.05
	maxAccuracy       = 1.0
	minHeadshotChance = 0.02
	maxHeadshotChance = 1.0
	minReactionMs     = 50
	maxReactionMs     = 600
)

// Behaviour-shift injection (the CUSUM target): from the run midpoint on,
// selected players' accuracy is multiplied by shiftAccuracyGain, capped.
const (
	shiftAccuracyGain = 1.7
	shiftAccuracyCap  = 0.95
)

// Late-arrival injection. The streaming jobs use a 10 s watermark, so this
// range produces both late-but-accepted and too-late-to-count events.
const (
	lateMinDelay = 5 * time.Second
	lateMaxDelay = 19 * time.Second
)

// Archetype mix within a class (see chooseArchetype).
const (
	aimbotShareOfCheaters = 0.7
	bronzeShareOfNormals  = 0.4
	goldShareOfNormals    = 0.4 // the remainder are diamond
)

const (
	reconnectChancePerTick = 0.30
	chatMinChars           = 10
	chatMaxChars           = 130
	serverTickRate         = 128
)

var (
	regions          = []string{"us-east", "eu-west", "ap-south"}
	reportReasons    = []string{"gameplay", "chat", "griefing"}
	disconnectCauses = []string{"network", "crash", "quit"}
)

// span is a range a metric is sampled from uniformly.
type span struct{ lo, hi float64 }

func (s span) sample(r *rand.Rand) float32 { return float32(s.lo + r.Float64()*(s.hi-s.lo)) }

// serverLoad is the envelope a server's health metrics are drawn from.
type serverLoad struct{ cpu, ram, loss, latency span }

var (
	healthyLoad  = serverLoad{cpu: span{20, 55}, ram: span{35, 60}, loss: span{0, 0.8}, latency: span{15, 40}}
	degradedLoad = serverLoad{cpu: span{85, 97}, ram: span{88, 98}, loss: span{6, 14}, latency: span{95, 135}}
)

// Position is a point in the arena.
type Position struct{ X, Y, Z float64 }

func (a Position) distance(b Position) float64 {
	dx, dy, dz := a.X-b.X, a.Y-b.Y, a.Z-b.Z
	return math.Sqrt(dx*dx + dy*dy + dz*dz)
}

// Server is a game server instance.
type Server struct {
	ID       string
	Region   string
	Degraded bool
}

// Player is one account. Identity (ID, archetype) is stable across runs;
// everything below the blank line is per-run state.
type Player struct {
	ID        string
	Archetype Archetype
	Profile   *Profile
	shifted   bool // selected for the behaviour-shift injection

	connected bool
	match     *Match // nil while queued or offline between matches
	team      int    // index into match.teams
	weapon    Weapon
	pos       Position
	hp        int
	alive     bool
	respawnAt time.Time
	damageBy  map[string]int // health removed this life, keyed by attacker ID
}

// Match is one 5v5 lobby on a server.
type Match struct {
	ID     string
	server *Server
	teams  [2][]*Player
	endsAt time.Time
}

// World owns all simulation state. It is deliberately not safe for
// concurrent use: the engine drives it from one goroutine, which keeps the
// simulation free of locks and data races by construction.
type World struct {
	rng *rand.Rand
	out emitter

	players []*Player
	servers []*Server
	slots   []*Match // one per concurrent match; nil while its lobby refills
	queue   []*Player

	runTag        string // makes match IDs unique across runs
	matchSeq      int
	matchDuration time.Duration
	lateRatio     float64
	shiftAt       time.Time // zero when the behaviour shift is disabled
}

func newWorld(cfg Config, profiles map[Archetype]*Profile, rng *rand.Rand, out emitter, start time.Time) *World {
	w := &World{
		rng:           rng,
		out:           out,
		slots:         make([]*Match, cfg.MatchesConcurrent),
		runTag:        strconv.FormatInt(start.Unix(), 36),
		matchDuration: cfg.MatchDuration,
		lateRatio:     cfg.LateEventRatio,
	}
	for i := range cfg.ServerCount {
		id := serverID(i)
		w.servers = append(w.servers, &Server{
			ID:       id,
			Region:   regions[i%len(regions)],
			Degraded: slices.Contains(cfg.DegradedServers, id),
		})
	}
	anyShifted := false
	for i := range cfg.Players {
		id := fmt.Sprintf("player_%04d", i+1)
		roll, rankRoll := archetypeSeeds(id)
		a := chooseArchetype(roll, rankRoll, cfg.CheaterRatio, cfg.SmurfRatio, cfg.ToxicRatio)
		p := &Player{
			ID:        id,
			Archetype: a,
			Profile:   profiles[a],
			shifted:   rng.Float64() < cfg.BehaviorShiftRatio,
			damageBy:  map[string]int{},
		}
		anyShifted = anyShifted || p.shifted
		w.players = append(w.players, p)
	}
	if anyShifted {
		w.shiftAt = start.Add(cfg.Duration / 2)
	}
	return w
}

// archetypeSeeds derives two deterministic draws in [0,1) from a player ID,
// so an account keeps its archetype across runs. The CUSUM baselines and
// smurf ground truth persist in Redis between runs; a re-rolled archetype
// would look like a behaviour change.
func archetypeSeeds(playerID string) (roll, rankRoll float64) {
	h1 := fnv.New64a()
	io.WriteString(h1, playerID+"#archetype")
	h2 := fnv.New64a()
	io.WriteString(h2, playerID+"#rank")
	return float64(h1.Sum64()%1_000_000_000) / 1e9, float64(h2.Sum64()%1_000_000_000) / 1e9
}

// chooseArchetype maps the two draws to a class: roll picks cheater / smurf /
// toxic / normal by the configured ratios, rankRoll picks within the class.
func chooseArchetype(roll, rankRoll, cheaterRatio, smurfRatio, toxicRatio float64) Archetype {
	switch {
	case roll < cheaterRatio:
		if rankRoll < aimbotShareOfCheaters {
			return ArchetypeCheaterAimbot
		}
		return ArchetypeCheaterWallhack
	case roll < cheaterRatio+smurfRatio:
		return ArchetypeSmurf
	case roll < cheaterRatio+smurfRatio+toxicRatio:
		return ArchetypeToxic
	case rankRoll < bronzeShareOfNormals:
		return ArchetypeNormalBronze
	case rankRoll < bronzeShareOfNormals+goldShareOfNormals:
		return ArchetypeNormalGold
	default:
		return ArchetypeNormalDiamond
	}
}

// begin logs every player in, queues them and opens the first lobbies.
func (w *World) begin(now time.Time) {
	for _, p := range w.players {
		p.connected = true
		w.emitPlayer(p, EventLogin, map[string]string{
			"archetype":        string(p.Archetype),
			"account_age_days": strconv.Itoa(p.Profile.AccountAgeDays),
			"games_played":     strconv.Itoa(p.Profile.GamesPlayed),
			"rank":             p.Profile.SkillTier,
		}, now)
		w.enqueue(p, now)
	}
	w.rng.Shuffle(len(w.queue), func(i, j int) { w.queue[i], w.queue[j] = w.queue[j], w.queue[i] })

	// Stagger the first round across [d/2, d] so lobbies do not all rotate
	// in the same second.
	half := w.matchDuration / 2
	for i := range w.slots {
		w.startMatch(i, now, half+half*time.Duration(i+1)/time.Duration(len(w.slots)))
	}
}

// tick advances everything that runs on the 1 s clock.
func (w *World) tick(now time.Time) {
	for i, m := range w.slots {
		if m != nil && !now.Before(m.endsAt) {
			w.endMatch(i, now)
		}
	}
	for i, m := range w.slots {
		if m == nil {
			w.startMatch(i, now, w.matchDuration)
		}
	}
	w.activity(now)
	w.reportServers(now)
}

// startMatch fills slot from the head of the queue; it is a no-op while
// fewer than playersPerMatch players are waiting.
func (w *World) startMatch(slot int, now time.Time, d time.Duration) {
	if len(w.queue) < playersPerMatch {
		return
	}
	lobby := slices.Clone(w.queue[:playersPerMatch])
	w.queue = slices.Delete(w.queue, 0, playersPerMatch)
	w.rng.Shuffle(len(lobby), func(i, j int) { lobby[i], lobby[j] = lobby[j], lobby[i] })

	w.matchSeq++
	m := &Match{
		ID:     fmt.Sprintf("match_%s_%04d", w.runTag, w.matchSeq),
		server: w.servers[slot%len(w.servers)],
		endsAt: now.Add(d),
	}
	for i, p := range lobby {
		team := i / playersPerTeam
		p.match, p.team = m, team
		m.teams[team] = append(m.teams[team], p)
		p.spawn(w.spawnPoint())
		w.emitPlayer(p, EventMatchmakingFound, nil, now)
		w.emitPlayer(p, EventMatchJoin, map[string]string{
			"team_id":    string(teamIDs[team]),
			"skill_tier": p.Profile.SkillTier,
		}, now)
		w.buy(p, now)
	}
	w.slots[slot] = m
}

// endMatch closes a lobby; connected players go back into matchmaking.
func (w *World) endMatch(slot int, now time.Time) {
	m := w.slots[slot]
	w.slots[slot] = nil
	for _, team := range m.teams {
		for _, p := range team {
			if p.connected {
				w.emitPlayer(p, EventMatchEnd, nil, now)
			}
			p.match = nil
			if p.connected {
				w.enqueue(p, now)
			}
		}
	}
}

func (w *World) enqueue(p *Player, now time.Time) {
	w.emitPlayer(p, EventMatchmakingStart, nil, now)
	w.queue = append(w.queue, p)
}

// shoot generates one SHOT in a random live match. It returns false when no
// match has an eligible shooter and target (everyone dead or offline).
func (w *World) shoot(now time.Time) bool {
	for range len(w.slots) {
		if m := w.slots[w.rng.IntN(len(w.slots))]; m != nil && w.engage(m, now) {
			return true
		}
	}
	return false
}

func (w *World) engage(m *Match, now time.Time) bool {
	side := w.rng.IntN(2)
	attacker := w.pickReady(m.teams[side], now)
	victim := w.pickReady(m.teams[1-side], now)
	if attacker == nil || victim == nil {
		return false
	}

	prof := attacker.Profile
	accuracy := clamp(w.draw(prof.Accuracy), minAccuracy, maxAccuracy)
	if attacker.shifted && !w.shiftAt.IsZero() && !now.Before(w.shiftAt) {
		accuracy = max(accuracy, min(accuracy*shiftAccuracyGain, shiftAccuracyCap))
	}
	headshotChance := clamp(w.draw(prof.HeadshotRatio), minHeadshotChance, maxHeadshotChance)
	reactionMs := int(clamp(w.draw(prof.ReactionTimeMs), minReactionMs, maxReactionMs))

	attacker.pos = w.step(attacker.pos)
	res := resolveShot(w.rng, attacker, victim, accuracy, headshotChance, now)

	ev := ShotEvent{
		EventID:        uuid.NewString(),
		EventType:      EventShot,
		MatchID:        m.ID,
		PlayerID:       attacker.ID,
		TeamID:         teamIDs[attacker.team],
		TargetPlayerID: victim.ID,
		WeaponID:       attacker.weapon.ID,
		PositionX:      float32(attacker.pos.X),
		PositionY:      float32(attacker.pos.Y),
		PositionZ:      float32(attacker.pos.Z),
		Distance:       float32(attacker.pos.distance(victim.pos)),
		Accuracy:       float32(accuracy),
		ReactionTimeMs: reactionMs,
		Hit:            res.Hit,
		IsHeadshot:     res.Headshot,
		IsKill:         res.Kill,
		EventTime:      w.eventTime(now).UnixMilli(),
		ServerID:       m.server.ID,
	}
	if res.Hit {
		damage, hpAfter := float32(res.Damage), res.HPAfter
		ev.Damage, ev.VictimHPAfter = &damage, &hpAfter
	}
	if res.Assister != "" {
		assister := res.Assister
		ev.AssisterID = &assister
	}
	w.out.shot(ev)
	return true
}

// pickReady returns a random connected, living player of team, respawning
// anyone whose timer has elapsed; nil if nobody can take part.
func (w *World) pickReady(team []*Player, now time.Time) *Player {
	var buf [playersPerTeam]*Player
	ready := buf[:0]
	for _, p := range team {
		if !p.connected {
			continue
		}
		if p.canRespawn(now) {
			p.spawn(w.spawnPoint())
		}
		if p.alive {
			ready = append(ready, p)
		}
	}
	if len(ready) == 0 {
		return nil
	}
	return ready[w.rng.IntN(len(ready))]
}

// activity draws each player's non-combat events for one tick.
func (w *World) activity(now time.Time) {
	minutes, hours := tickInterval.Minutes(), tickInterval.Hours()
	for _, p := range w.players {
		if !p.connected {
			if w.chance(reconnectChancePerTick) {
				w.reconnect(p, now)
			}
			continue
		}
		a := p.Profile.Activity
		if w.chance(a.ChatPerMinute * minutes) {
			chars := chatMinChars + w.rng.IntN(chatMaxChars-chatMinChars+1)
			w.emitPlayer(p, EventChatMessage, map[string]string{"chars": strconv.Itoa(chars)}, now)
		}
		if p.match != nil {
			if w.chance(a.ReportedPerMinute * minutes) {
				w.reportedBy(p, now)
			}
			if w.chance(a.PurchasesPerMinute * minutes) {
				w.buy(p, now)
			}
		}
		if w.chance(a.DisconnectsPerHour * hours) {
			w.disconnect(p, now)
		}
	}
}

// reportedBy emits a REPORT_PLAYER against p from another connected player
// in the same match.
func (w *World) reportedBy(p *Player, now time.Time) {
	var buf [playersPerMatch]*Player
	reporters := buf[:0]
	for _, team := range p.match.teams {
		for _, q := range team {
			if q != p && q.connected {
				reporters = append(reporters, q)
			}
		}
	}
	if len(reporters) == 0 {
		return
	}
	reporter := reporters[w.rng.IntN(len(reporters))]
	w.emitPlayer(reporter, EventReportPlayer, map[string]string{
		"reported_player": p.ID,
		"reason":          reportReasons[w.rng.IntN(len(reportReasons))],
	}, now)
}

// buy equips a new weapon; the purchase is what the player then fights with.
func (w *World) buy(p *Player, now time.Time) {
	p.weapon = pickWeapon(w.rng)
	w.emitPlayer(p, EventItemPurchase, map[string]string{
		"weapon_id": p.weapon.ID,
		"cost":      strconv.Itoa(p.weapon.Cost),
	}, now)
}

// disconnect takes p offline. A player in a match keeps their slot and can
// reconnect into it; a queued player leaves the queue.
func (w *World) disconnect(p *Player, now time.Time) {
	w.emitPlayer(p, EventDisconnect, map[string]string{
		"cause": disconnectCauses[w.rng.IntN(len(disconnectCauses))],
	}, now)
	p.connected = false
	if p.match == nil {
		w.queue = slices.DeleteFunc(w.queue, func(q *Player) bool { return q == p })
	}
}

func (w *World) reconnect(p *Player, now time.Time) {
	p.connected = true
	w.emitPlayer(p, EventReconnect, nil, now)
	if p.match == nil { // their match ended while they were away
		w.enqueue(p, now)
	}
}

// reportServers emits one health sample per server, with occupancy counted
// from the matches it actually hosts.
func (w *World) reportServers(now time.Time) {
	for _, s := range w.servers {
		var matches, players int
		for _, m := range w.slots {
			if m == nil || m.server != s {
				continue
			}
			matches++
			for _, team := range m.teams {
				for _, p := range team {
					if p.connected {
						players++
					}
				}
			}
		}
		load := healthyLoad
		if s.Degraded {
			load = degradedLoad
		}
		w.out.server(ServerMetric{
			ServerID:          s.ID,
			Region:            s.Region,
			CPUPercent:        load.cpu.sample(w.rng),
			RAMPercent:        load.ram.sample(w.rng),
			TickRate:          serverTickRate,
			PacketLossPercent: load.loss.sample(w.rng),
			AvgLatencyMs:      load.latency.sample(w.rng),
			ActivePlayers:     players,
			ActiveMatches:     matches,
			Timestamp:         now.UnixMilli(),
		})
	}
}

func (w *World) emitPlayer(p *Player, eventType string, metadata map[string]string, now time.Time) {
	ev := PlayerEvent{
		EventID:   uuid.NewString(),
		PlayerID:  p.ID,
		EventType: eventType,
		Metadata:  metadata,
		EventTime: now.UnixMilli(),
	}
	if p.match != nil {
		matchID, server := p.match.ID, p.match.server.ID
		ev.MatchID, ev.ServerID = &matchID, &server
	}
	w.out.player(ev)
}

// eventTime is now, except for the injected share of late events.
func (w *World) eventTime(now time.Time) time.Time {
	if !w.chance(w.lateRatio) {
		return now
	}
	delay := lateMinDelay + time.Duration(w.rng.Int64N(int64(lateMaxDelay-lateMinDelay)+1))
	return now.Add(-delay)
}

func (w *World) spawnPoint() Position {
	return Position{X: w.rng.Float64() * arenaSize, Y: w.rng.Float64() * arenaSize, Z: w.rng.Float64() * arenaHeight}
}

func (w *World) step(p Position) Position {
	p.X = clamp(p.X+(2*w.rng.Float64()-1)*maxMoveStep, 0, arenaSize)
	p.Y = clamp(p.Y+(2*w.rng.Float64()-1)*maxMoveStep, 0, arenaSize)
	return p
}

func (w *World) draw(d Distribution) float64 { return w.rng.NormFloat64()*d.Std + d.Mean }

func (w *World) chance(p float64) bool { return w.rng.Float64() < p }

func clamp(v, lo, hi float64) float64 { return min(max(v, lo), hi) }
