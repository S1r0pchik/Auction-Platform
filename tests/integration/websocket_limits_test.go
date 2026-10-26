package integration

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"auction-platform/internal/auction"
	"auction-platform/internal/auth"
	"auction-platform/internal/events"
	"auction-platform/internal/httpapi"
	"auction-platform/internal/realtime"
)

func TestWebSocketLimitsAndFailedHandshakeRelease(t *testing.T) {
	db := database(t)
	store := &auction.Store{DB: db}
	first, second, third := lot(t, store), lot(t, store), lot(t, store)
	hub := realtime.NewHubWithLimits(2, 1)
	api := &httpapi.API{Auctions: store, Auth: &auth.Store{DB: db}, Hub: hub}
	server := httptest.NewServer(api.Router())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	url := func(id uuid.UUID) string {
		return "ws" + strings.TrimPrefix(server.URL, "http") + "/api/v1/auctions/" + id.String() + "/ws"
	}
	connect := func(id uuid.UUID) *websocket.Conn {
		t.Helper()
		conn, _, err := websocket.Dial(ctx, url(id), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.CloseNow() })
		return conn
	}
	reject := func(id uuid.UUID) {
		t.Helper()
		conn, response, err := websocket.Dial(ctx, url(id), nil)
		if conn != nil {
			conn.CloseNow()
		}
		if response != nil {
			defer response.Body.Close()
		}
		if err == nil || response == nil || response.StatusCode != 429 || response.Header.Get("Retry-After") != "1" {
			t.Fatal("capacity not rejected", err, response)
		}
	}
	one := connect(first.ID)
	reject(first.ID)
	connect(second.ID)
	reject(third.ID)
	one.CloseNow()
	deadline := time.Now().Add(2 * time.Second)
	for hub.Stats().Connections != 1 {
		if time.Now().After(deadline) {
			t.Fatal("closed socket retained capacity")
		}
		time.Sleep(time.Millisecond)
	}
	// A regular HTTP request fails upgrade validation after reserving a slot.
	response, err := server.Client().Get(server.URL + "/api/v1/auctions/" + first.ID.String() + "/ws")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != 426 || hub.Stats().Connections != 1 {
		t.Fatal("failed handshake leaked reservation", response.StatusCode, hub.Stats())
	}
	response, err = server.Client().Get(server.URL + "/api/v1/auctions/" + uuid.NewString() + "/ws")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != 404 || hub.Stats().Connections != 1 {
		t.Fatal("failed lookup leaked reservation", response.StatusCode, hub.Stats())
	}
	connect(first.ID)
	if stats := hub.Stats(); stats.Connections != 2 || stats.Rejected != 2 {
		t.Fatal(stats)
	}
}

func TestDatabaseOutboxLifecyclePassesValidation(t *testing.T) {
	db := database(t)
	store := &auction.Store{DB: db}
	a := lot(t, store)
	ctx := context.Background()
	if _, _, err := store.PlaceBid(ctx, a.ID, user(t, db), 1000, "validated-bid"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `UPDATE auctions SET ends_at=clock_timestamp() WHERE id=$1`, a.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(ctx, `SELECT payload FROM outbox_events WHERE auction_id=$1 ORDER BY aggregate_version`, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	hub := realtime.NewHub()
	sub, release, err := hub.Subscribe(a.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	for rows.Next() {
		var payload []byte
		if err = rows.Scan(&payload); err != nil {
			t.Fatal(err)
		}
		if err = events.Handle(hub, []byte(a.ID.String()), payload); err != nil {
			t.Fatal(err)
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(sub.Events) != 3 {
		t.Fatalf("expected created, bid, finished; got %d", len(sub.Events))
	}
}
