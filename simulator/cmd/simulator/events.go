package main

// Kafka topics, one per event family.
type Topic string

const (
	TopicGameplay Topic = "gameplay_events"
	TopicPlayer   Topic = "player_events"
	TopicServer   Topic = "server_metrics"
)

var topics = []Topic{TopicGameplay, TopicPlayer, TopicServer}

// EventShot is the only gameplay event type: one event per trigger pull,
// carrying its own outcome (hit, damage, headshot, kill). Keeping the outcome
// on the shot means per-shot statistics (accuracy, reaction time) are never
// repeated across rows, so downstream averages need no de-duplication.
const EventShot = "SHOT"

// Player lifecycle and activity event types (schemas/player_event.avsc).
const (
	EventLogin            = "LOGIN"
	EventMatchmakingStart = "MATCHMAKING_START"
	EventMatchmakingFound = "MATCHMAKING_FOUND"
	EventMatchJoin        = "MATCH_JOIN"
	EventMatchEnd         = "MATCH_END"
	EventItemPurchase     = "ITEM_PURCHASE"
	EventChatMessage      = "CHAT_MESSAGE"
	EventReportPlayer     = "REPORT_PLAYER"
	EventDisconnect       = "DISCONNECT"
	EventReconnect        = "RECONNECT"
)

// ShotEvent mirrors schemas/gameplay_event.avsc. Outcome fields that only
// exist for a hit (damage, victim_hp_after) are null on a miss; assister_id
// is set only on a kill that earned an assist.
type ShotEvent struct {
	EventID        string   `json:"event_id"`
	EventType      string   `json:"event_type"`
	MatchID        string   `json:"match_id"`
	PlayerID       string   `json:"player_id"`
	TeamID         Team     `json:"team_id"`
	TargetPlayerID string   `json:"target_player_id"`
	WeaponID       string   `json:"weapon_id"`
	PositionX      float32  `json:"position_x"`
	PositionY      float32  `json:"position_y"`
	PositionZ      float32  `json:"position_z"`
	Distance       float32  `json:"distance"`
	Accuracy       float32  `json:"accuracy"`
	ReactionTimeMs int      `json:"reaction_time_ms"`
	Hit            bool     `json:"hit"`
	Damage         *float32 `json:"damage,omitempty"`
	IsHeadshot     bool     `json:"is_headshot"`
	IsKill         bool     `json:"is_kill"`
	VictimHPAfter  *int     `json:"victim_hp_after,omitempty"`
	AssisterID     *string  `json:"assister_id,omitempty"`
	EventTime      int64    `json:"event_time"`
	ServerID       string   `json:"server_id"`
}

// PlayerEvent mirrors schemas/player_event.avsc.
type PlayerEvent struct {
	EventID   string            `json:"event_id"`
	PlayerID  string            `json:"player_id"`
	EventType string            `json:"event_type"`
	MatchID   *string           `json:"match_id,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	EventTime int64             `json:"event_time"`
	ServerID  *string           `json:"server_id,omitempty"`
}

// ServerMetric mirrors schemas/server_metric.avsc.
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

// emitter receives generated events. The engine publishes them; tests record them.
type emitter interface {
	shot(ShotEvent)
	player(PlayerEvent)
	server(ServerMetric)
}
