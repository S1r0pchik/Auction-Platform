package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestStageMeasuresFullInterval(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	r := &runner{client: server.Client(), base: server.URL, lots: []string{"test-auction"}}
	result := r.stage("reads", 1, time.Second)
	if result.Success != 1 || result.Dropped != 0 {
		t.Fatalf("expected one successful request: %+v", result)
	}
	if result.Seconds < 1 || result.SuccessRPS > 1 {
		t.Fatalf("one request over one second must not exceed 1 RPS: seconds=%f, RPS=%f", result.Seconds, result.SuccessRPS)
	}
}
