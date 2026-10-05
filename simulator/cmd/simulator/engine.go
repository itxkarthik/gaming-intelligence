package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"time"
)

const (
	// progressEvery is the number of ticks between progress lines.
	progressEvery = 5
	// pacerMaxLag bounds catch-up: a generator that falls further behind
	// drops the backlog (counted) instead of replaying it as a burst.
	pacerMaxLag = 100 * time.Millisecond
)

// runStats counts generated events; owned by the engine goroutine.
type runStats struct {
	Shots         int
	Hits          int
	Kills         int
	HeadshotKills int
	Assists       int
	PlayerEvents  int
	ServerMetrics int
	Idle          int // shot slots with no eligible shooter (all dead or offline)
	Dropped       int // shot slots skipped because the generator fell behind
	EncodeErrors  int
}

// Engine drives the World on a single goroutine (a pacer for shots, a ticker
// for everything else) and publishes what it emits.
type Engine struct {
	world *World
	sink  Sink
	rate  int
	start time.Time
	stats runStats
	out   io.Writer // progress
	log   io.Writer // errors
}

func newEngine(cfg Config, profiles map[Archetype]*Profile, rng *rand.Rand, sink Sink, out, log io.Writer, start time.Time) *Engine {
	e := &Engine{sink: sink, rate: cfg.EventsPerSec, start: start, out: out, log: log}
	e.world = newWorld(cfg, profiles, rng, e, start)
	return e
}

// Run generates events until ctx is done.
func (e *Engine) Run(ctx context.Context) {
	e.world.begin(e.start)
	pace := newPacer(e.rate, e.start)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	timer := time.NewTimer(0)
	defer timer.Stop()

	ticks := 0
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			e.world.tick(now)
			ticks++
			if ticks%progressEvery == 0 {
				e.progress(now)
			}
		case <-timer.C:
			now := time.Now()
			due, dropped := pace.take(now)
			e.stats.Dropped += dropped
			for range due {
				if !e.world.shoot(now) {
					e.stats.Idle++
				}
			}
			timer.Reset(time.Until(pace.next))
		}
	}
}

func (e *Engine) shot(ev ShotEvent) {
	e.stats.Shots++
	if ev.Hit {
		e.stats.Hits++
	}
	if ev.IsKill {
		e.stats.Kills++
		if ev.IsHeadshot {
			e.stats.HeadshotKills++
		}
		if ev.AssisterID != nil {
			e.stats.Assists++
		}
	}
	e.publish(TopicGameplay, ev.PlayerID, ev)
}

func (e *Engine) player(ev PlayerEvent) {
	e.stats.PlayerEvents++
	e.publish(TopicPlayer, ev.PlayerID, ev)
}

func (e *Engine) server(m ServerMetric) {
	e.stats.ServerMetrics++
	e.publish(TopicServer, m.ServerID, m)
}

func (e *Engine) publish(t Topic, key string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		if e.stats.EncodeErrors == 0 {
			fmt.Fprintf(e.log, "[encode] %s: %v\n", t, err)
		}
		e.stats.EncodeErrors++
		return
	}
	e.sink.Publish(t, key, b)
}

func (e *Engine) progress(now time.Time) {
	elapsed := now.Sub(e.start)
	total := totalStats(e.sink.Stats())
	fmt.Fprintf(e.out, "[sim] %6s  shots %9d (%6.0f/s)  kills %7d  player %7d  server %6d  delivered %9d  failed %d\n",
		elapsed.Truncate(time.Second), e.stats.Shots, float64(e.stats.Shots)/elapsed.Seconds(),
		e.stats.Kills, e.stats.PlayerEvents, e.stats.ServerMetrics, total.Delivered, total.Failed)
}

func totalStats(byTopic map[Topic]TopicStats) TopicStats {
	var t TopicStats
	for _, s := range byTopic {
		t.Published += s.Published
		t.Delivered += s.Delivered
		t.Failed += s.Failed
	}
	return t
}

// pacer schedules events at a fixed rate: one becomes due every interval.
// Unlike a token bucket it has no burst allowance, so the produced rate
// matches the target; catch-up after a stall is bounded by pacerMaxLag.
type pacer struct {
	interval time.Duration
	next     time.Time
}

func newPacer(rate int, start time.Time) *pacer {
	return &pacer{interval: time.Second / time.Duration(rate), next: start}
}

// take returns how many events are due at now, and how many were dropped
// because the caller fell more than pacerMaxLag behind.
func (p *pacer) take(now time.Time) (due, dropped int) {
	if now.Before(p.next) {
		return 0, 0
	}
	if behind := now.Sub(p.next); behind > pacerMaxLag {
		dropped = int((behind - pacerMaxLag) / p.interval)
		p.next = p.next.Add(time.Duration(dropped) * p.interval)
	}
	due = int(now.Sub(p.next)/p.interval) + 1
	p.next = p.next.Add(time.Duration(due) * p.interval)
	return due, dropped
}
