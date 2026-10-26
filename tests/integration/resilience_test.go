package integration

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"auction-platform/internal/auction"
	"auction-platform/internal/auth"
	"auction-platform/internal/events"
	"auction-platform/internal/httpapi"
	"auction-platform/internal/monitor"
	"auction-platform/internal/realtime"
)

func TestHotAuctionDoesNotStarveReads(t *testing.T) {
	db := database(t)
	store := &auction.Store{DB: db}
	hot, cold := lot(t, store), lot(t, store)
	ctx := context.Background()
	bidder := user(t, db)
	token := strings.Repeat("x", 43)
	sum := sha256.Sum256([]byte(token))
	if _, err := db.Exec(ctx, `INSERT INTO sessions(token_hash,user_id,expires_at) VALUES($1,$2,clock_timestamp()+interval '1 hour')`, sum[:], bidder); err != nil {
		t.Fatal(err)
	}
	api := &httpapi.API{Auctions: store, Auth: &auth.Store{DB: db}, Hub: realtime.NewHub()}
	server := httptest.NewServer(api.Router())
	defer server.Close()
	client := server.Client()
	client.Timeout = 3 * time.Second
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT id FROM auctions WHERE id=$1 FOR UPDATE`, hot.ID); err != nil {
		t.Fatal(err)
	}
	var peak atomic.Int32
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				current := db.Stat().AcquiredConns()
				for old := peak.Load(); current > old; old = peak.Load() {
					if peak.CompareAndSwap(old, current) {
						break
					}
				}
			}
		}
	}()
	defer func() { close(stop); <-sampled }()
	start := make(chan struct{})
	results := make(chan int, 80)
	var wg sync.WaitGroup
	for i := 0; i < 80; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			req, _ := http.NewRequest("POST", server.URL+"/api/v1/auctions/"+hot.ID.String()+"/bids", strings.NewReader(`{"amount":1000}`))
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Idempotency-Key", fmt.Sprint(i))
			req.Header.Set("Content-Type", "application/json")
			response, e := client.Do(req)
			if e != nil {
				t.Error(e)
				results <- 0
				return
			}
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
			results <- response.StatusCode
		}(i)
	}
	close(start)
	deadline := time.Now().Add(2 * time.Second)
	for db.Stat().AcquiredConns() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	readStart := time.Now()
	response, err := client.Get(server.URL + "/api/v1/auctions/" + cold.ID.String())
	readLatency := time.Since(readStart)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("unrelated read failed", response.StatusCode)
	}
	wg.Wait()
	close(results)
	counts := map[int]int{}
	for status := range results {
		counts[status]++
		if status != 429 && status != 503 {
			t.Errorf("blocked auction returned %d", status)
		}
	}
	if counts[429] == 0 || counts[503] == 0 {
		t.Fatal("expected bounded admission and lock timeout", counts)
	}
	if peak.Load() > 4 {
		t.Fatalf("hot lot exhausted connections: peak=%d", peak.Load())
	}
	t.Logf("80 concurrent requests, statuses=%v, peak acquired connections=%d (includes held lock and unrelated read), unrelated GET=%s", counts, peak.Load(), readLatency)
}

func TestConsumerFailureDoesNotChangeRESTReadiness(t *testing.T) {
	db := database(t)
	store := &auction.Store{DB: db}
	lot(t, store)
	state := monitor.NewConsumer()
	hub := realtime.NewHub()
	api := &httpapi.API{Auctions: store, Auth: &auth.Store{DB: db}, Hub: hub, Consumer: state}
	server := httptest.NewServer(api.Router())
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		events.Supervise(ctx, func(context.Context) error { return &events.PoisonError{Err: fmt.Errorf("invalid event offset=42")} }, state)
	}()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(time.Second)
	for state.Snapshot().State != "blocked" {
		if time.Now().After(deadline) {
			t.Fatal("consumer did not block")
		}
		time.Sleep(time.Millisecond)
	}
	for _, path := range []string{"/health/live", "/health/ready", "/api/v1/auctions"} {
		res, err := server.Client().Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatal(path, res.StatusCode)
		}
	}
	res, err := server.Client().Get(server.URL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var snapshot struct {
		Status   string                   `json:"status"`
		Consumer monitor.ConsumerSnapshot `json:"consumer"`
		Outbox   struct {
			Pending int `json:"pending"`
		} `json:"outbox"`
	}
	if err = json.NewDecoder(res.Body).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 200 || snapshot.Status != "degraded" || snapshot.Consumer.State != "blocked" || snapshot.Outbox.Pending != 1 {
		t.Fatalf("unexpected status: %+v", snapshot)
	}
}
