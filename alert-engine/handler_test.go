package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/segmentio/kafka-go"
)

// ─── fakes ─────────────────────────────────────────────────────────────────

// fakeStore fails the first len(errs) calls with errs[i], then succeeds.
type fakeStore struct {
	errs   []error
	calls  int
	stored []Alert
}

func (s *fakeStore) Store(_ context.Context, a Alert) error {
	s.calls++
	if s.calls <= len(s.errs) {
		return s.errs[s.calls-1]
	}
	s.stored = append(s.stored, a)
	return nil
}

type fakePublisher struct {
	err       error
	published []Alert
}

func (p *fakePublisher) Publish(_ context.Context, a Alert) error {
	p.published = append(p.published, a)
	return p.err
}

type fakeNotifier struct {
	err      error
	notified []Alert
}

func (n *fakeNotifier) Notify(_ context.Context, a Alert) error {
	n.notified = append(n.notified, a)
	return n.err
}

// fastRetry keeps retry tests quick while exercising real backoff.
var fastRetry = retryPolicy{maxAttempts: 3, initialDelay: time.Millisecond, maxDelay: 2 * time.Millisecond}

var fixedNow = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

func newTestHandler(store AlertStore, pub Publisher, notifier Notifier) *handler {
	return &handler{
		gate:         newFakeGate(),
		store:        store,
		publisher:    pub,
		notifier:     notifier,
		dedupTTL:     time.Minute,
		rateLimitTTL: time.Minute,
		storeRetry:   fastRetry,
		now:          func() time.Time { return fixedNow },
	}
}

func alertMessage(t *testing.T, a Alert) kafka.Message {
	t.Helper()
	raw, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Value: raw, Time: fixedNow.Add(-time.Minute)}
}

func sampleAlert(id string) Alert {
	return Alert{AlertID: id, AlertType: "CHEAT_DETECTED", Severity: "CRITICAL",
		EntityType: "PLAYER", EntityID: "player_" + id, Message: "m", Timestamp: 1759000000000}
}

var (
	errTransient    = errors.New("dial tcp: connection refused")
	errDataPg       = &pgconn.PgError{Code: "22P05", Message: "unsupported Unicode escape sequence"}
	errConstraintPg = &pgconn.PgError{Code: "23502", Message: "null value violates not-null constraint"}
	errSchemaPg     = &pgconn.PgError{Code: "42P01", Message: "relation does not exist"}
)

// ─── resolveTimestamp ──────────────────────────────────────────────────────

func TestResolveTimestamp(t *testing.T) {
	msgTime := time.UnixMilli(1759000001234)
	tests := []struct {
		name    string
		ts      int64
		msgTime time.Time
		want    int64
	}{
		{"producer timestamp kept", 1759000000000, msgTime, 1759000000000},
		{"zero falls back to kafka time", 0, msgTime, msgTime.UnixMilli()},
		{"negative falls back to kafka time", -5, msgTime, msgTime.UnixMilli()},
		{"no kafka time falls back to now", 0, time.Time{}, fixedNow.UnixMilli()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveTimestamp(tc.ts, tc.msgTime, fixedNow); got != tc.want {
				t.Errorf("resolveTimestamp = %d, want %d", got, tc.want)
			}
		})
	}
}

// The producer may omit severity, timestamp and details; every sink must
// still receive the same complete alert (the live stream used to get the raw
// payload, so a missing timestamp was broadcast as 1970).
func TestHandleGivesEverySinkTheNormalizedAlert(t *testing.T) {
	store, pub, notifier := &fakeStore{}, &fakePublisher{}, &fakeNotifier{}
	h := newTestHandler(store, pub, notifier)
	a := sampleAlert("a1")
	a.Severity, a.Timestamp, a.Details = "bogus", 0, nil
	msg := alertMessage(t, a)

	if _, err := h.handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if len(store.stored) != 1 || len(pub.published) != 1 || len(notifier.notified) != 1 {
		t.Fatalf("stored %d, published %d, notified %d; want 1 each",
			len(store.stored), len(pub.published), len(notifier.notified))
	}
	got := store.stored[0]
	if got.Severity != severityCritical || got.Timestamp != msg.Time.UnixMilli() || got.Details == nil {
		t.Errorf("stored %+v; want CRITICAL, timestamp %d from the kafka message, non-nil details",
			got, msg.Time.UnixMilli())
	}
	for sink, other := range map[string]Alert{"published": pub.published[0], "notified": notifier.notified[0]} {
		if fmt.Sprint(other) != fmt.Sprint(got) {
			t.Errorf("%s %+v differs from stored %+v", sink, other, got)
		}
	}
}

// ─── store error classification ────────────────────────────────────────────

func TestIsPermanentStoreError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"network error", errTransient, false},
		{"deadline", context.DeadlineExceeded, false},
		{"undefined table (deployment, not data)", errSchemaPg, false},
		{"data exception", errDataPg, true},
		{"integrity violation", errConstraintPg, true},
		{"wrapped data exception", fmt.Errorf("insert alert_history: %w", errDataPg), true},
		{"unencodable alert", fmt.Errorf("%w: encode details", errUnstorable), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isPermanentStoreError(tc.err); got != tc.want {
				t.Errorf("isPermanentStoreError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// ─── handle: poison pill vs retry ──────────────────────────────────────────

func TestHandleOutcomes(t *testing.T) {
	tests := []struct {
		name          string
		value         []byte // overrides the sample alert when set
		storeErrs     []error
		publishErr    error
		notifyErr     error
		wantOutcome   outcome
		wantErr       bool
		wantStores    int // Store calls
		wantPublished int
	}{
		{name: "happy path", wantOutcome: outcomeForwarded, wantStores: 1, wantPublished: 1},
		{name: "malformed JSON is a poison pill", value: []byte("{not json"), wantOutcome: outcomeRejected},
		{name: "missing fields is a poison pill", value: []byte(`{"alert_id":"x"}`), wantOutcome: outcomeRejected},
		{name: "transient store error is retried", storeErrs: []error{errTransient, errTransient},
			wantOutcome: outcomeForwarded, wantStores: 3, wantPublished: 1},
		{name: "store retries exhausted blocks commit", storeErrs: []error{errTransient, errTransient, errTransient},
			wantErr: true, wantStores: 3},
		{name: "permanent store error is a poison pill", storeErrs: []error{errDataPg},
			wantOutcome: outcomeRejected, wantStores: 1},
		{name: "publish failure is best-effort", publishErr: errTransient,
			wantOutcome: outcomeForwarded, wantStores: 1, wantPublished: 1},
		{name: "webhook failure is best-effort", notifyErr: errTransient,
			wantOutcome: outcomeForwarded, wantStores: 1, wantPublished: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{errs: tc.storeErrs}
			pub := &fakePublisher{err: tc.publishErr}
			notifier := &fakeNotifier{err: tc.notifyErr}
			h := newTestHandler(store, pub, notifier)

			msg := alertMessage(t, sampleAlert("a1"))
			if tc.value != nil {
				msg.Value = tc.value
			}
			got, err := h.handle(context.Background(), msg)

			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.wantOutcome {
				t.Errorf("outcome = %d, want %d", got, tc.wantOutcome)
			}
			if store.calls != tc.wantStores {
				t.Errorf("Store calls = %d, want %d", store.calls, tc.wantStores)
			}
			if len(pub.published) != tc.wantPublished {
				t.Errorf("published %d, want %d", len(pub.published), tc.wantPublished)
			}
			if len(notifier.notified) != tc.wantPublished {
				t.Errorf("notified %d, want %d", len(notifier.notified), tc.wantPublished)
			}
		})
	}
}

func TestHandleSuppressesDuplicateButAllowsRedelivery(t *testing.T) {
	store := &fakeStore{}
	h := newTestHandler(store, &fakePublisher{}, nil)
	first := sampleAlert("a1")
	duplicate := first
	duplicate.AlertID = "a2"

	for _, step := range []struct {
		alert Alert
		want  outcome
	}{
		{first, outcomeForwarded},
		{duplicate, outcomeSuppressed},
		{first, outcomeForwarded}, // redelivery after a crash before commit
	} {
		got, err := h.handle(context.Background(), alertMessage(t, step.alert))
		if err != nil {
			t.Fatal(err)
		}
		if got != step.want {
			t.Errorf("alert %s: outcome %d, want %d", step.alert.AlertID, got, step.want)
		}
	}
}

func TestHandleStopsRetryingOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &fakeStore{errs: []error{errTransient, errTransient, errTransient}}
	h := newTestHandler(store, &fakePublisher{}, nil)
	h.storeRetry = retryPolicy{maxAttempts: 3, initialDelay: time.Hour, maxDelay: time.Hour}

	cancel()
	_, err := h.handle(ctx, alertMessage(t, sampleAlert("a1")))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if store.calls != 1 {
		t.Errorf("Store calls = %d, want 1 (no retry after cancel)", store.calls)
	}
}

// ─── retryPolicy ───────────────────────────────────────────────────────────

func TestRetryPolicyStopsOnNonRetryable(t *testing.T) {
	calls := 0
	err := fastRetry.do(context.Background(), "op",
		func(err error) bool { return !errors.Is(err, errUnstorable) },
		func(context.Context) error { calls++; return errUnstorable })
	if !errors.Is(err, errUnstorable) || calls != 1 {
		t.Errorf("err = %v after %d calls, want errUnstorable after 1", err, calls)
	}
}

func TestRetryPolicyCapsDelay(t *testing.T) {
	p := retryPolicy{maxAttempts: 4, initialDelay: time.Millisecond, maxDelay: 2 * time.Millisecond}
	start := time.Now()
	calls := 0
	err := p.do(context.Background(), "op", always,
		func(context.Context) error { calls++; return errTransient })
	if !errors.Is(err, errTransient) || calls != 4 {
		t.Errorf("err = %v after %d calls, want errTransient after 4", err, calls)
	}
	// 1 + 2 + 2 ms of backoff; generous bound for slow CI.
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("backoff took %s, delay cap not applied", elapsed)
	}
}

// ─── consume: commit only after handling ───────────────────────────────────

// fakeReader serves msgs in order, then blocks until ctx is cancelled.
type fakeReader struct {
	msgs      []kafka.Message
	next      int
	committed []int64
	cancel    context.CancelFunc
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if r.next < len(r.msgs) {
		m := r.msgs[r.next]
		r.next++
		return m, nil
	}
	r.cancel() // drained: simulate SIGTERM
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	for _, m := range msgs {
		r.committed = append(r.committed, m.Offset)
	}
	return nil
}

func TestConsumeCommitsAfterHandling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	good := alertMessage(t, sampleAlert("a1"))
	good.Offset = 0
	poison := kafka.Message{Offset: 1, Value: []byte("garbage")}
	dup := alertMessage(t, Alert{AlertID: "a2", AlertType: "CHEAT_DETECTED",
		EntityType: "PLAYER", EntityID: "player_a1"})
	dup.Offset = 2

	r := &fakeReader{msgs: []kafka.Message{good, poison, dup}, cancel: cancel}
	h := newTestHandler(&fakeStore{}, &fakePublisher{}, nil)

	stats, err := consume(ctx, r, h)
	if err != nil {
		t.Fatalf("clean shutdown must return nil, got %v", err)
	}
	if fmt.Sprint(r.committed) != "[0 1 2]" {
		t.Errorf("committed offsets %v, want [0 1 2]", r.committed)
	}
	if stats.forwarded != 1 || stats.rejected != 1 || stats.suppressed != 1 {
		t.Errorf("stats = %s", stats)
	}
}

func TestConsumeDoesNotCommitUnstoredAlert(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stored := alertMessage(t, sampleAlert("a1"))
	stored.Offset = 7
	failing := alertMessage(t, sampleAlert("b2"))
	failing.Offset = 8
	never := alertMessage(t, sampleAlert("c3"))
	never.Offset = 9

	r := &fakeReader{msgs: []kafka.Message{stored, failing, never}, cancel: cancel}
	// First Store succeeds, then every attempt for the second alert fails.
	store := &fakeStore{}
	h := newTestHandler(storeFunc(func(ctx context.Context, a Alert) error {
		if a.AlertID == "b2" {
			return errTransient
		}
		return store.Store(ctx, a)
	}), &fakePublisher{}, nil)

	_, err := consume(ctx, r, h)
	if !errors.Is(err, errTransient) {
		t.Fatalf("err = %v, want the store failure", err)
	}
	if fmt.Sprint(r.committed) != "[7]" {
		t.Errorf("committed %v, want only [7]: offset 8 must stay uncommitted", r.committed)
	}
	if r.next != 2 {
		t.Errorf("fetched %d messages, want consumption to stop at the failure", r.next)
	}
}

// storeFunc adapts a function to AlertStore.
type storeFunc func(ctx context.Context, a Alert) error

func (f storeFunc) Store(ctx context.Context, a Alert) error {
	return f(ctx, a)
}
