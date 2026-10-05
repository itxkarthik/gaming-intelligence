package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// ─── Retry ─────────────────────────────────────────────────────────────────

// retryPolicy is a bounded exponential backoff: up to maxAttempts tries,
// the delay doubling from initialDelay and capped at maxDelay.
type retryPolicy struct {
	maxAttempts  int
	initialDelay time.Duration
	maxDelay     time.Duration
}

// do runs op until it succeeds, fails with an error retryable rejects, the
// attempts are exhausted, or ctx is done. Waiting between attempts aborts
// promptly on ctx cancellation.
func (p retryPolicy) do(ctx context.Context, name string, retryable func(error) bool, op func(context.Context) error) error {
	delay := p.initialDelay
	for attempt := 1; ; attempt++ {
		err := op(ctx)
		switch {
		case err == nil:
			return nil
		case ctx.Err() != nil:
			return fmt.Errorf("%s: %w", name, errors.Join(ctx.Err(), err))
		case !retryable(err):
			return fmt.Errorf("%s: %w", name, err)
		case attempt >= p.maxAttempts:
			return fmt.Errorf("%s: giving up after %d attempts: %w", name, attempt, err)
		}
		log.Printf("[WARN] %s failed (attempt %d/%d), retrying in %s: %v", name, attempt, p.maxAttempts, delay, err)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("%s: %w", name, errors.Join(ctx.Err(), err))
		case <-timer.C:
		}
		delay = min(2*delay, p.maxDelay)
	}
}

func always(error) bool { return true }

// ─── Message handling ──────────────────────────────────────────────────────

// outcome is how a message was disposed of. Every outcome is final: the
// message's offset may be committed.
type outcome int

const (
	outcomeForwarded  outcome = iota // stored and fanned out
	outcomeSuppressed                // dedup / rate limit
	outcomeRejected                  // poison pill: can never be processed
)

// handler applies the engine's processing to one Kafka message. It has no
// Kafka, Postgres, or Redis dependency of its own, only the small sink
// interfaces, so it is tested with in-memory fakes.
type handler struct {
	gate         Gate
	store        AlertStore
	publisher    Publisher
	notifier     Notifier // nil when the webhook is disabled
	dedupTTL     time.Duration
	rateLimitTTL time.Duration
	storeRetry   retryPolicy
	now          func() time.Time
}

// handle processes msg. A nil error means the message is finished (see
// outcome) and its offset may be committed; a non-nil error means the alert
// could not be stored and the offset must NOT be committed, so it is
// redelivered after a restart.
//
// Delivery guarantees per sink:
//   - Postgres history is the system of record: transient failures are
//     retried with backoff and block the commit (at-least-once; the insert
//     is idempotent on alert_id).
//   - Redis publish and the webhook are best-effort live notifications:
//     a failure is logged and the message is still committed. Holding the
//     partition (or redelivering, which would re-notify) for a missed
//     real-time push is worse than the miss; history is already durable.
func (h *handler) handle(ctx context.Context, msg kafka.Message) (outcome, error) {
	alert, err := parseAlert(msg.Value)
	if err != nil {
		log.Printf("[WARN] reject partition=%d offset=%d: %v", msg.Partition, msg.Offset, err)
		return outcomeRejected, nil
	}
	alert = normalize(alert, msg.Time, h.now())

	if !shouldForward(ctx, h.gate, alert, h.dedupTTL, h.rateLimitTTL) {
		log.Printf("suppressed type=%s entity=%s/%s id=%s (dedup/rate limit)",
			alert.AlertType, alert.EntityType, alert.EntityID, alert.AlertID)
		return outcomeSuppressed, nil
	}

	err = h.storeRetry.do(ctx, "store alert "+alert.AlertID,
		func(err error) bool { return !isPermanentStoreError(err) },
		func(ctx context.Context) error { return h.store.Store(ctx, alert) })
	if err != nil {
		if isPermanentStoreError(err) {
			log.Printf("[ERROR] reject partition=%d offset=%d: %v", msg.Partition, msg.Offset, err)
			return outcomeRejected, nil
		}
		return 0, err
	}

	if err := h.publisher.Publish(ctx, alert); err != nil {
		log.Printf("[WARN] live publish %s: %v", alert.AlertID, err)
	}
	if h.notifier != nil {
		if err := h.notifier.Notify(ctx, alert); err != nil {
			log.Printf("[WARN] webhook %s: %v", alert.AlertID, err)
		}
	}
	log.Printf("forwarded type=%s severity=%s entity=%s/%s id=%s",
		alert.AlertType, alert.Severity, alert.EntityType, alert.EntityID, alert.AlertID)
	return outcomeForwarded, nil
}

// ─── Consume loop ──────────────────────────────────────────────────────────

// messageReader is the subset of *kafka.Reader the consume loop needs.
type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

// fetchErrorBackoff is the pause after a failed fetch before trying again.
const fetchErrorBackoff = time.Second

// consumeStats counts messages by outcome.
type consumeStats struct {
	forwarded, suppressed, rejected int
}

func (s consumeStats) String() string {
	return fmt.Sprintf("processed=%d forwarded=%d suppressed=%d rejected=%d",
		s.forwarded+s.suppressed+s.rejected, s.forwarded, s.suppressed, s.rejected)
}

// consume fetches, handles, and only then commits each message, giving
// at-least-once processing. It returns nil on ctx cancellation and an error
// when a message cannot be stored; that message stays uncommitted so the
// restarted engine reprocesses it.
func consume(ctx context.Context, r messageReader, h *handler) (consumeStats, error) {
	var stats consumeStats
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return stats, nil
			}
			log.Printf("[WARN] kafka fetch: %v", err)
			select {
			case <-ctx.Done():
				return stats, nil
			case <-time.After(fetchErrorBackoff):
			}
			continue
		}

		result, err := h.handle(ctx, msg)
		if err != nil {
			if ctx.Err() != nil {
				return stats, nil // shutdown mid-message: redelivered on restart
			}
			return stats, fmt.Errorf("partition=%d offset=%d left uncommitted: %w", msg.Partition, msg.Offset, err)
		}
		switch result {
		case outcomeForwarded:
			stats.forwarded++
		case outcomeSuppressed:
			stats.suppressed++
		case outcomeRejected:
			stats.rejected++
		}

		if err := r.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return stats, nil
			}
			// Not fatal: the next successful commit covers this offset. A
			// redelivery cannot duplicate history (idempotent gate and
			// insert) but may repeat the best-effort live notifications.
			log.Printf("[WARN] kafka commit partition=%d offset=%d: %v", msg.Partition, msg.Offset, err)
		}
	}
}
