package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	writerBatchSize    = 500
	writerBatchTimeout = 20 * time.Millisecond
	preflightTimeout   = 5 * time.Second

	// sendQueueSize bounds each topic's buffer between the engine and its
	// sender: a few seconds of headroom at benchmark rates, ~20 MB worst case.
	sendQueueSize = 50_000
	// enqueueTimeout caps one WriteMessages call (its metadata lookup).
	enqueueTimeout = 5 * time.Second
	// closeTimeout caps the final flush, so a dead broker cannot hang exit.
	closeTimeout = 15 * time.Second
)

// TopicStats counts one topic's messages. Every published message ends up
// delivered (broker-acknowledged), failed, or, if the final flush ran out of
// time, unconfirmed.
type TopicStats struct {
	Published int64
	Delivered int64
	Failed    int64
}

// Unconfirmed is the number of messages with no broker verdict yet: in
// flight during a run, lost to a timed-out flush after Close.
func (s TopicStats) Unconfirmed() int64 { return s.Published - s.Delivered - s.Failed }

// Sink publishes encoded events and accounts for every message.
type Sink interface {
	Publish(topic Topic, key string, value []byte)
	// Stats returns a snapshot of per-topic delivery counts.
	Stats() map[Topic]TopicStats
	// Close flushes pending messages; Stats is final once it returns.
	Close() error
}

// ledger records delivery outcomes per topic. Kafka reports outcomes on its
// own goroutines, so all access is mutex-guarded.
type ledger struct {
	mu     sync.Mutex
	counts map[Topic]TopicStats
	logged map[Topic]bool
	log    io.Writer
}

func newLedger(log io.Writer) *ledger {
	return &ledger{counts: map[Topic]TopicStats{}, logged: map[Topic]bool{}, log: log}
}

func (l *ledger) update(t Topic, f func(*TopicStats)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.counts[t]
	f(&s)
	l.counts[t] = s
}

func (l *ledger) published(t Topic) { l.update(t, func(s *TopicStats) { s.Published++ }) }

func (l *ledger) delivered(t Topic, n int) {
	l.update(t, func(s *TopicStats) { s.Delivered += int64(n) })
}

// failed records n undeliverable messages. The first failure per topic is
// logged at once so a broken broker is visible mid-run; the totals end up in
// the summary and the exit code.
func (l *ledger) failed(t Topic, n int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.counts[t]
	s.Failed += int64(n)
	l.counts[t] = s
	if !l.logged[t] {
		l.logged[t] = true
		fmt.Fprintf(l.log, "[kafka] %s: delivery failed: %v (further failures are counted in the summary)\n", t, err)
	}
}

func (l *ledger) Stats() map[Topic]TopicStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return maps.Clone(l.counts)
}

// kafkaSink writes each topic through an async kafka-go writer fed by its own
// sender goroutine. Delivery results come back through the writer's
// Completion callback: no polling, no sleeps.
//
// The engine must never block on Kafka. kafka-go's async WriteMessages still
// performs a synchronous metadata lookup before queueing, which blocks for
// as long as its context allows once the broker is gone. So Publish only
// enqueues onto a bounded per-topic queue (counting a full queue as a
// failure), and each sender calls WriteMessages with a deadline.
type kafkaSink struct {
	*ledger
	senders map[Topic]*topicSender
	ctx     context.Context // cancelled when Close runs out of time
	cancel  context.CancelFunc

	enqueueTimeout time.Duration
	closeTimeout   time.Duration
}

type topicSender struct {
	topic  Topic
	writer *kafka.Writer
	queue  chan kafka.Message
	done   chan struct{}
}

var errQueueFull = errors.New("send queue full (broker too slow or unreachable)")

func newKafkaSink(ctx context.Context, brokers []string, log io.Writer) (*kafkaSink, error) {
	if err := preflight(ctx, brokers); err != nil {
		return nil, err
	}
	return newKafkaWriters(brokers, log, sendQueueSize, enqueueTimeout, closeTimeout), nil
}

// newKafkaWriters builds the per-topic writers and starts their senders
// without contacting a broker.
func newKafkaWriters(brokers []string, log io.Writer, queueSize int, enqueue, closeAfter time.Duration) *kafkaSink {
	ctx, cancel := context.WithCancel(context.Background())
	s := &kafkaSink{
		ledger:         newLedger(log),
		senders:        map[Topic]*topicSender{},
		ctx:            ctx,
		cancel:         cancel,
		enqueueTimeout: enqueue,
		closeTimeout:   closeAfter,
	}
	for _, t := range topics {
		p := &topicSender{
			topic: t,
			writer: &kafka.Writer{
				Addr:  kafka.TCP(brokers...),
				Topic: string(t),
				// Hash keeps each key (player or server ID) on one partition, so
				// per-entity ordering survives. LeastBytes would ignore the key.
				Balancer: &kafka.Hash{},
				// kafka-go defaults to RequireNone: fire-and-forget, the broker
				// never reports a failure, so errors would be invisible.
				RequiredAcks: kafka.RequireOne,
				Async:        true,
				BatchSize:    writerBatchSize,
				BatchTimeout: writerBatchTimeout,
				Completion: func(msgs []kafka.Message, err error) {
					if err != nil {
						s.failed(t, len(msgs), err)
						return
					}
					s.delivered(t, len(msgs))
				},
			},
			queue: make(chan kafka.Message, queueSize),
			done:  make(chan struct{}),
		}
		s.senders[t] = p
		go s.send(p)
	}
	return s
}

// Publish never blocks: a message either enters the topic's send queue or
// is counted as failed.
func (s *kafkaSink) Publish(t Topic, key string, value []byte) {
	s.published(t)
	select {
	case s.senders[t].queue <- kafka.Message{Key: []byte(key), Value: value}:
	default:
		s.failed(t, 1, errQueueFull)
	}
}

// send hands queued messages to the writer in batches until the queue is
// closed. A batch that cannot even be queued in time is counted as failed.
func (s *kafkaSink) send(p *topicSender) {
	defer close(p.done)
	for msg := range p.queue {
		batch := []kafka.Message{msg}
	drain:
		for len(batch) < writerBatchSize {
			select {
			case m, ok := <-p.queue:
				if !ok {
					break drain
				}
				batch = append(batch, m)
			default:
				break drain
			}
		}
		ctx, cancel := context.WithTimeout(s.ctx, s.enqueueTimeout)
		err := p.writer.WriteMessages(ctx, batch...)
		cancel()
		if err != nil {
			s.failed(p.topic, len(batch), err)
		}
	}
}

// Close flushes everything still queued and waits for the broker's verdict
// on it, for at most closeTimeout. Messages still pending past that are
// reported by Stats as unconfirmed, never as delivered.
func (s *kafkaSink) Close() error {
	for _, p := range s.senders {
		close(p.queue)
	}
	done := make(chan error, 1)
	go func() {
		var errs []error
		for _, t := range topics {
			p := s.senders[t]
			<-p.done
			// Blocks until every in-flight batch has run its Completion.
			if err := p.writer.Close(); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", t, err))
			}
		}
		done <- errors.Join(errs...)
	}()

	timeout := time.NewTimer(s.closeTimeout)
	defer timeout.Stop()
	select {
	case err := <-done:
		s.cancel()
		return err
	case <-timeout.C:
		// Fail the remaining queue fast, then stop waiting on the writers.
		s.cancel()
		return fmt.Errorf("gave up flushing after %s: %d messages unconfirmed",
			s.closeTimeout, totalStats(s.Stats()).Unconfirmed())
	}
}

// preflight fails fast when no broker is reachable or a topic is missing,
// rather than generating a whole run into writers that cannot deliver.
func preflight(ctx context.Context, brokers []string) error {
	ctx, cancel := context.WithTimeout(ctx, preflightTimeout)
	defer cancel()
	var errs []error
	for _, b := range brokers {
		err := checkBroker(ctx, b)
		if err == nil {
			return nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", b, err))
	}
	return fmt.Errorf("kafka preflight failed: %w", errors.Join(errs...))
}

func checkBroker(ctx context.Context, addr string) error {
	conn, err := kafka.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return err
		}
	}

	// One metadata request per topic: the broker fails a multi-topic request
	// as a whole on any unknown topic, which would hide which one is missing.
	var missing []string
	for _, t := range topics {
		_, err := conn.ReadPartitions(string(t))
		switch {
		case errors.Is(err, kafka.UnknownTopicOrPartition):
			missing = append(missing, string(t))
		case err != nil:
			return fmt.Errorf("reading metadata for %s: %w", t, err)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing topics %s (start the stack with `make up`)", strings.Join(missing, ", "))
	}
	return nil
}

// dryRunSink counts messages without sending them.
type dryRunSink struct{ *ledger }

func newDryRunSink() *dryRunSink { return &dryRunSink{ledger: newLedger(io.Discard)} }

func (s *dryRunSink) Publish(t Topic, _ string, _ []byte) {
	s.published(t)
	s.delivered(t, 1)
}

func (s *dryRunSink) Close() error { return nil }
