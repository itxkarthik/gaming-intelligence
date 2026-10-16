package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ─── Metrics ────────────────────────────────────────────────────────────────

type alertCounterKey struct {
	alertType string
	severity  string
	outcome   string
}

type Metrics struct {
	mu           sync.Mutex
	alertCounts  map[alertCounterKey]*atomic.Uint64
	suppressed   map[string]*atomic.Uint64 // key: reason
	rejected     atomic.Uint64
	storeTotalMs atomic.Uint64
	storeCount   atomic.Uint64
}

var globalMetrics = &Metrics{
	alertCounts: make(map[alertCounterKey]*atomic.Uint64),
	suppressed:  make(map[string]*atomic.Uint64),
}

func (m *Metrics) getAlertCounter(alertType, severity, outcome string) *atomic.Uint64 {
	key := alertCounterKey{alertType: alertType, severity: severity, outcome: outcome}
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.alertCounts[key]
	if !ok {
		c = &atomic.Uint64{}
		m.alertCounts[key] = c
	}
	return c
}

func (m *Metrics) getSuppressedCounter(reason string) *atomic.Uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.suppressed[reason]
	if !ok {
		c = &atomic.Uint64{}
		m.suppressed[reason] = c
	}
	return c
}

func recordAlertReceived(alertType, severity string) {
	globalMetrics.getAlertCounter(alertType, severity, "received").Add(1)
}

func recordAlertForwarded(alertType, severity string) {
	globalMetrics.getAlertCounter(alertType, severity, "forwarded").Add(1)
}

func recordAlertSuppressed(reason string) {
	globalMetrics.getSuppressedCounter(reason).Add(1)
}

func recordAlertRejected() {
	globalMetrics.rejected.Add(1)
}

func recordStoreDuration(dur time.Duration) {
	globalMetrics.storeTotalMs.Add(uint64(dur.Milliseconds()))
	globalMetrics.storeCount.Add(1)
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	globalMetrics.mu.Lock()
	defer globalMetrics.mu.Unlock()

	// 1. Alerts total
	io.WriteString(w, "# HELP gaming_alerts_total Total alerts processed by alert-engine\n")
	io.WriteString(w, "# TYPE gaming_alerts_total counter\n")
	for k, c := range globalMetrics.alertCounts {
		fmt.Fprintf(w, "gaming_alerts_total{type=\"%s\",severity=\"%s\",outcome=\"%s\"} %d\n",
			k.alertType, k.severity, k.outcome, c.Load())
	}

	// 2. Suppressed
	io.WriteString(w, "\n# HELP gaming_alerts_suppressed_total Alerts suppressed by dedup or rate limiting\n")
	io.WriteString(w, "# TYPE gaming_alerts_suppressed_total counter\n")
	for reason, c := range globalMetrics.suppressed {
		fmt.Fprintf(w, "gaming_alerts_suppressed_total{reason=\"%s\"} %d\n", reason, c.Load())
	}

	// 3. Rejected
	io.WriteString(w, "\n# HELP gaming_alerts_rejected_total Malformed alerts rejected as poison pills\n")
	io.WriteString(w, "# TYPE gaming_alerts_rejected_total counter\n")
	fmt.Fprintf(w, "gaming_alerts_rejected_total %d\n", globalMetrics.rejected.Load())

	// 4. Store latency
	count := globalMetrics.storeCount.Load()
	var avgStoreSec float64
	if count > 0 {
		avgStoreSec = float64(globalMetrics.storeTotalMs.Load()) / float64(count) / 1000.0
	}
	io.WriteString(w, "\n# HELP gaming_alerts_store_latency_seconds Average PostgreSQL store latency\n")
	io.WriteString(w, "# TYPE gaming_alerts_store_latency_seconds gauge\n")
	fmt.Fprintf(w, "gaming_alerts_store_latency_seconds %.5f\n", avgStoreSec)
}

func startMetricsServer(ctx context.Context, port string) {
	if port == "" {
		port = "9095"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", metricsHandler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
	})

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		log.Printf("alert-engine telemetry metrics listening on :%s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[WARN] metrics server: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
}

// ─── Distributed Tracing (OTLP over HTTP to Jaeger :4318) ───────────────────

type otlpAttribute struct {
	Key   string          `json:"key"`
	Value otlpStringValue `json:"value"`
}

type otlpStringValue struct {
	StringValue string `json:"stringValue"`
}

type otlpSpan struct {
	TraceID           string          `json:"traceId"`
	SpanID            string          `json:"spanId"`
	ParentSpanID      string          `json:"parentSpanId,omitempty"`
	Name              string          `json:"name"`
	Kind              int             `json:"kind"`
	StartTimeUnixNano string          `json:"startTimeUnixNano"`
	EndTimeUnixNano   string          `json:"endTimeUnixNano"`
	Attributes        []otlpAttribute `json:"attributes,omitempty"`
	Status            otlpStatus      `json:"status"`
}

type otlpStatus struct {
	Code int `json:"code"` // 1 = OK, 2 = ERROR
}

type otlpResourceSpan struct {
	Resource struct {
		Attributes []otlpAttribute `json:"attributes"`
	} `json:"resource"`
	ScopeSpans []struct {
		Scope struct {
			Name string `json:"name"`
		} `json:"scope"`
		Spans []otlpSpan `json:"spans"`
	} `json:"scopeSpans"`
}

type otlpPayload struct {
	ResourceSpans []otlpResourceSpan `json:"resourceSpans"`
}

var (
	traceCh       = make(chan otlpSpan, 2048)
	jaegerURLOnce sync.Once
	jaegerURL     string
)

func getJaegerEndpoint() string {
	jaegerURLOnce.Do(func() {
		jaegerURL = os.Getenv("JAEGER_ENDPOINT")
		if jaegerURL == "" {
			jaegerURL = "http://jaeger:4318/v1/traces"
		}
	})
	return jaegerURL
}

func randomHex(bytesLen int) string {
	b := make([]byte, bytesLen)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func startTraceExporter(ctx context.Context) {
	client := &http.Client{Timeout: 3 * time.Second}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case firstSpan := <-traceCh:
				spans := []otlpSpan{firstSpan}
				// Drain any pending up to 50
				for len(spans) < 50 {
					select {
					case s := <-traceCh:
						spans = append(spans, s)
					default:
						goto send
					}
				}
			send:
				exportSpans(ctx, client, spans)
			}
		}
	}()
}

func exportSpans(ctx context.Context, client *http.Client, spans []otlpSpan) {
	payload := otlpPayload{
		ResourceSpans: []otlpResourceSpan{
			{
				Resource: struct {
					Attributes []otlpAttribute `json:"attributes"`
				}{
					Attributes: []otlpAttribute{
						{Key: "service.name", Value: otlpStringValue{StringValue: "alert-engine"}},
					},
				},
				ScopeSpans: []struct {
					Scope struct {
						Name string `json:"name"`
					} `json:"scope"`
					Spans []otlpSpan `json:"spans"`
				}{
					{
						Scope: struct {
							Name string `json:"name"`
						}{Name: "alert-engine"},
						Spans: spans,
					},
				},
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, getJaegerEndpoint(), bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// SpanTracer records a span and enqueues it to Jaeger.
type SpanTracer struct {
	traceID      string
	spanID       string
	parentSpanID string
	name         string
	start        time.Time
	attrs        []otlpAttribute
}

func StartTrace(name string) *SpanTracer {
	return &SpanTracer{
		traceID: randomHex(16), // 32 hex chars
		spanID:  randomHex(8),  // 16 hex chars
		name:    name,
		start:   time.Now(),
	}
}

func (s *SpanTracer) StartChild(name string) *SpanTracer {
	return &SpanTracer{
		traceID:      s.traceID,
		spanID:       randomHex(8),
		parentSpanID: s.spanID,
		name:         name,
		start:        time.Now(),
	}
}

func (s *SpanTracer) SetAttr(key, val string) {
	s.attrs = append(s.attrs, otlpAttribute{Key: key, Value: otlpStringValue{StringValue: val}})
}

func (s *SpanTracer) Finish(err error) {
	end := time.Now()
	code := 1
	if err != nil {
		code = 2
		s.SetAttr("error", "true")
		s.SetAttr("error.message", err.Error())
	}
	span := otlpSpan{
		TraceID:           s.traceID,
		SpanID:            s.spanID,
		ParentSpanID:      s.parentSpanID,
		Name:              s.name,
		Kind:              1,
		StartTimeUnixNano: fmt.Sprintf("%d", s.start.UnixNano()),
		EndTimeUnixNano:   fmt.Sprintf("%d", end.UnixNano()),
		Attributes:        s.attrs,
		Status:            otlpStatus{Code: code},
	}
	select {
	case traceCh <- span:
	default:
	}
}
