package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestPacerProducesTheExactRate(t *testing.T) {
	const rate = 20_000
	p := newPacer(rate, t0)
	total := 0
	end := t0.Add(10 * time.Second)
	for now := t0; !now.After(end); now = now.Add(time.Millisecond) {
		due, dropped := p.take(now)
		if dropped != 0 {
			t.Fatalf("dropped %d while keeping up", dropped)
		}
		total += due
	}
	// Slots fall at t0, t0+1/rate, …, t0+10 s inclusive: 10·rate + 1.
	if want := rate*10 + 1; total != want {
		t.Fatalf("produced %d over [0, 10 s]; want exactly %d", total, want)
	}
}

func TestPacerHasNoStartupBurst(t *testing.T) {
	p := newPacer(1000, t0)
	if due, _ := p.take(t0); due != 1 {
		t.Fatalf("due at start = %d; want 1 (a token bucket would allow a burst)", due)
	}
	if due, _ := p.take(t0); due != 0 {
		t.Fatalf("due again at the same instant = %d; want 0", due)
	}
}

func TestPacerDropsBacklogInsteadOfBursting(t *testing.T) {
	const rate = 1000
	p := newPacer(rate, t0)
	due, dropped := p.take(t0.Add(5 * time.Second)) // a 5 s stall
	maxCatchUp := int(pacerMaxLag/(time.Second/rate)) + 1
	if due > maxCatchUp {
		t.Fatalf("caught up %d at once; want <= %d", due, maxCatchUp)
	}
	if due+dropped != 5*rate+1 {
		t.Fatalf("due %d + dropped %d != %d slots", due, dropped, 5*rate+1)
	}
}

// memorySink keeps every payload so tests can inspect the wire format.
type memorySink struct {
	*ledger
	mu       sync.Mutex
	payloads map[Topic][][]byte
}

func newMemorySink() *memorySink {
	return &memorySink{ledger: newLedger(&bytes.Buffer{}), payloads: map[Topic][][]byte{}}
}

func (s *memorySink) Publish(t Topic, _ string, v []byte) {
	s.published(t)
	s.delivered(t, 1)
	s.mu.Lock()
	s.payloads[t] = append(s.payloads[t], v)
	s.mu.Unlock()
}

func (s *memorySink) Close() error { return nil }

func TestEngineRunPublishesTheWireContract(t *testing.T) {
	cfg := testConfig()
	sink := newMemorySink()
	var out bytes.Buffer
	e := newEngine(cfg, testProfiles(t), newRand(), sink, &out, &out, time.Now())

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	e.Run(ctx)

	stats := sink.Stats()
	if got := stats[TopicGameplay].Published; got < 25 || got > 35 {
		t.Fatalf("published %d shots in 1.5 s at %d/s", got, cfg.EventsPerSec)
	}
	if stats[TopicServer].Published != int64(cfg.ServerCount) {
		t.Fatalf("server metrics = %d; want one tick's worth", stats[TopicServer].Published)
	}

	sawMiss, sawHit := false, false
	for _, raw := range sink.payloads[TopicGameplay] {
		var fields map[string]any
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatal(err)
		}
		if fields["event_type"] != EventShot {
			t.Fatalf("event_type = %v", fields["event_type"])
		}
		_, hasDamage := fields["damage"]
		_, hasHP := fields["victim_hp_after"]
		if fields["hit"] == true {
			sawHit = true
			if !hasDamage || !hasHP {
				t.Fatalf("hit without damage/victim_hp_after: %s", raw)
			}
		} else {
			sawMiss = true
			if hasDamage || hasHP {
				t.Fatalf("miss carries damage/victim_hp_after: %s", raw)
			}
		}
	}
	if !sawHit || !sawMiss {
		t.Fatalf("expected both hits and misses (hit=%v miss=%v)", sawHit, sawMiss)
	}
}

func TestLedgerLogsFirstFailurePerTopicOnly(t *testing.T) {
	var log bytes.Buffer
	l := newLedger(&log)
	for range 3 {
		l.published(TopicGameplay)
	}
	l.delivered(TopicGameplay, 1)
	l.failed(TopicGameplay, 2, errors.New("broker down"))
	l.failed(TopicGameplay, 1, errors.New("broker still down"))

	got := l.Stats()[TopicGameplay]
	if got != (TopicStats{Published: 3, Delivered: 1, Failed: 3}) {
		t.Fatalf("stats = %+v", got)
	}
	if n := strings.Count(log.String(), "delivery failed"); n != 1 {
		t.Fatalf("logged %d failures; want the first only:\n%s", n, log.String())
	}
}

func TestKafkaWritersAreConfiguredForVisibleKeyedDelivery(t *testing.T) {
	s := newKafkaWriters([]string{"localhost:9094"}, &bytes.Buffer{}, 16, time.Second, time.Second)
	defer s.Close()
	for _, topic := range topics {
		w := s.senders[topic].writer
		if w.Topic != string(topic) {
			t.Fatalf("writer topic = %s; want %s", w.Topic, topic)
		}
		if _, ok := w.Balancer.(*kafka.Hash); !ok {
			t.Fatalf("%s balancer = %T; keys must pick the partition", topic, w.Balancer)
		}
		if w.RequiredAcks != kafka.RequireOne {
			t.Fatalf("%s acks = %v; RequireNone hides broker errors", topic, w.RequiredAcks)
		}
		// Completion is the only path delivery outcomes take.
		w.Completion(make([]kafka.Message, 4), nil)
		w.Completion(make([]kafka.Message, 2), errors.New("leader not available"))
	}
	for _, topic := range topics {
		if got := s.Stats()[topic]; got.Delivered != 4 || got.Failed != 2 {
			t.Fatalf("%s stats = %+v; want 4 delivered, 2 failed", topic, got)
		}
	}
}

// blackhole accepts TCP connections and never answers: a paused or
// partitioned broker, where every request hangs instead of failing.
func blackhole(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	return ln.Addr().String()
}

// Regression: kafka-go's async WriteMessages does a blocking metadata lookup,
// which froze the whole simulation once the broker stopped answering.
func TestPublishNeverBlocksOnAHungBroker(t *testing.T) {
	const queue, n = 100, 5000
	s := newKafkaWriters([]string{blackhole(t)}, io.Discard, queue, 200*time.Millisecond, 500*time.Millisecond)

	start := time.Now()
	for range n {
		s.Publish(TopicGameplay, "player_0001", []byte(`{}`))
	}
	if d := time.Since(start); d > 250*time.Millisecond {
		t.Fatalf("publishing %d messages took %v; Publish must never wait on the broker", n, d)
	}

	start = time.Now()
	_ = s.Close() // may time out; the accounting below is what matters
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Close took %v; it must be bounded by closeTimeout", d)
	}

	st := s.Stats()[TopicGameplay]
	if st.Published != n || st.Delivered != 0 {
		t.Fatalf("stats = %+v; want %d published, none delivered", st, n)
	}
	if st.Failed+st.Unconfirmed() != n {
		t.Fatalf("stats = %+v; every message must be failed or unconfirmed", st)
	}
	if st.Failed < n-queue-writerBatchSize {
		t.Fatalf("failed = %d; messages beyond the queue must fail fast as queue-full", st.Failed)
	}
}

func TestPreflightFailsFastWithoutABroker(t *testing.T) {
	start := time.Now()
	err := preflight(context.Background(), []string{"127.0.0.1:1"})
	if err == nil || !strings.Contains(err.Error(), "kafka preflight failed") {
		t.Fatalf("err = %v", err)
	}
	if elapsed := time.Since(start); elapsed > preflightTimeout+time.Second {
		t.Fatalf("preflight took %v", elapsed)
	}
}
