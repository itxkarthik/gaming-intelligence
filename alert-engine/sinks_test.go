package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testWebhookToken = "T000/B000/secret-token"

func TestWebhookNotifierPostsPayload(t *testing.T) {
	var got webhookPayload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected request %s %s", r.Method, r.Header.Get("Content-Type"))
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	n := webhookNotifier{client: srv.Client(), url: srv.URL + "/" + testWebhookToken}
	a := sampleAlert("a1")
	if err := n.Notify(context.Background(), a); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	want := webhookPayload{AlertID: "a1", Type: a.AlertType, Severity: a.Severity,
		Entity: "PLAYER/player_a1", Text: a.Message, Timestamp: a.Timestamp}
	if got != want {
		t.Errorf("payload = %+v, want %+v", got, want)
	}
}

func TestWebhookNotifierErrorsDoNotLeakURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	closed := httptest.NewServer(http.NotFoundHandler())
	closedURL := closed.URL
	closed.Close() // connection refused from now on

	tests := []struct {
		name, url string
	}{
		{"non-2xx status", srv.URL + "/" + testWebhookToken},
		{"connection refused", closedURL + "/" + testWebhookToken},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n := webhookNotifier{client: &http.Client{Timeout: time.Second}, url: tc.url}
			err := n.Notify(context.Background(), sampleAlert("a1"))
			if err == nil {
				t.Fatal("want error, got nil")
			}
			if strings.Contains(err.Error(), "secret-token") {
				t.Errorf("error leaks the webhook token: %q", err)
			}
		})
	}
}
