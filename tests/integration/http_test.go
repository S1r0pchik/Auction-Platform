package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"auction-platform/internal/auction"
	"auction-platform/internal/auth"
	"auction-platform/internal/events"
	"auction-platform/internal/httpapi"
	"auction-platform/internal/outbox"
	"auction-platform/internal/realtime"
	"auction-platform/internal/testsupport"
)

func TestHTTPAuthBidsAndWebSocket(t *testing.T) {
	db := database(t)
	store := &auction.Store{DB: db}
	hub := realtime.NewHub()
	api := &httpapi.API{Auctions: store, Auth: &auth.Store{DB: db}, Hub: hub}
	server := httptest.NewServer(api.Router())
	defer server.Close()
	request := func(method, path, token, key string, body any, status int) []byte {
		t.Helper()
		data, _ := json.Marshal(body)
		req, err := http.NewRequest(method, server.URL+path, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		res, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		result, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != status {
			t.Fatalf("%s %s: got %d want %d: %s", method, path, res.StatusCode, status, result)
		}
		return result
	}
	credentials := auth.Credentials{Email: "bidder@example.com", Password: "strong-password-123"}
	request("POST", "/api/v1/auth/register", "", "", credentials, 201)
	request("POST", "/api/v1/auth/register", "", "", credentials, 409)
	request("POST", "/api/v1/auth/login", "", "", auth.Credentials{Email: credentials.Email, Password: "wrong-password-123"}, 401)
	var session auth.Session
	if err := json.Unmarshal(request("POST", "/api/v1/auth/login", "", "", credentials, 200), &session); err != nil {
		t.Fatal(err)
	}
	input := auction.CreateInput{Title: "HTTP lot", StartingPrice: 1000, MinIncrement: 100, StartsAt: time.Now().Add(-time.Minute), EndsAt: time.Now().Add(time.Hour)}
	request("POST", "/api/v1/auctions", "", "", input, 401)
	var owned auction.Auction
	if err := json.Unmarshal(request("POST", "/api/v1/auctions", session.Token, "", input, 201), &owned); err != nil {
		t.Fatal(err)
	}
	request("POST", "/api/v1/auctions/"+owned.ID.String()+"/bids", session.Token, "owner", map[string]int{"amount": 1000}, 403)
	a := lot(t, store)
	path := "/api/v1/auctions/" + a.ID.String()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+path+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	var bid auction.Bid
	if err = json.Unmarshal(request("POST", path+"/bids", session.Token, "one", map[string]int{"amount": 1000}, 201), &bid); err != nil {
		t.Fatal(err)
	}
	request("POST", path+"/bids", session.Token, "one", map[string]int{"amount": 1000}, 200)
	request("POST", path+"/bids", session.Token, "one", map[string]int{"amount": 1100}, 409)
	var low struct {
		Details struct {
			Minimum int64 `json:"minimum_amount"`
		} `json:"details"`
	}
	if err = json.Unmarshal(request("POST", path+"/bids", session.Token, "two", map[string]int{"amount": 1000}, 409), &low); err != nil || low.Details.Minimum != 1100 {
		t.Fatal("incorrect minimum", low, err)
	}
	var missingKey struct {
		Fields map[string]string `json:"fields"`
	}
	if err = json.Unmarshal(request("POST", path+"/bids", session.Token, "", map[string]int{"amount": 1100}, 400), &missingKey); err != nil || missingKey.Fields["Idempotency-Key"] == "" {
		t.Fatal("missing key explanation", missingKey, err)
	}
	request("GET", path, "", "", nil, 200)
	request("GET", path+"/bids?limit=1", "", "", nil, 200)
	request("GET", "/api/v1/auctions?limit=1", "", "", nil, 200)
	request("GET", "/api/v1/auctions?limit=101", "", "", nil, 400)
	var payload []byte
	if err = db.QueryRow(ctx, `SELECT payload FROM outbox_events WHERE auction_id=$1 AND event_type='bid.placed'`, a.ID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if err = events.Handle(hub, []byte(a.ID.String()), payload); err != nil {
		t.Fatal(err)
	}
	var notification auction.Event
	if err = wsjson.Read(ctx, conn, &notification); err != nil {
		t.Fatal(err)
	}
	if notification.Bid == nil || notification.Bid.ID != bid.ID {
		t.Fatal("wrong notification")
	}
}

func TestKafkaDuplicateDelivery(t *testing.T) {
	raw := os.Getenv("KAFKA_TEST_BROKERS")
	if raw == "" {
		t.Skip("KAFKA_TEST_BROKERS not set: real Kafka test skipped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	brokers := strings.Split(raw, ",")
	topic := testsupport.KafkaTopic(t, brokers)
	consumer, err := events.Consumer(brokers, topic, "test-"+uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	producer, err := events.Producer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	id := uuid.New()
	now := time.Now().UTC()
	e := auction.Event{ID: uuid.New(), Type: "auction.created", SchemaVersion: 1, AuctionID: id, Version: 1, Auction: auction.Auction{ID: id, OwnerID: uuid.New(), Title: "Kafka test", Currency: "RUB", StartingPrice: 100, CurrentPrice: 100, MinIncrement: 10, StartsAt: now, EndsAt: now.Add(time.Hour), CreatedAt: now, Status: "scheduled", Version: 1}}
	payload, _ := json.Marshal(e)
	for i := 0; i < 2; i++ {
		if err = producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(id.String()), Value: payload}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	hub := realtime.NewHub()
	sub, unsub, err := hub.Subscribe(id)
	if err != nil {
		t.Fatal(err)
	}
	defer unsub()
	received := 0
	for received < 2 && ctx.Err() == nil {
		fetched := consumer.PollRecords(ctx, 100)
		if errs := fetched.Errors(); len(errs) > 0 {
			consumer.AllowRebalance()
			t.Fatal(errs)
		}
		for _, r := range fetched.Records() {
			if string(r.Key) == id.String() {
				if err = events.Handle(hub, r.Key, r.Value); err != nil {
					t.Fatal(err)
				}
				received++
			}
		}
		consumer.AllowRebalance()
	}
	if received != 2 || len(sub.Events) != 1 {
		t.Fatalf("Kafka delivered=%d, hub notifications=%d", received, len(sub.Events))
	}
}

func TestOutboxToKafkaConsumer(t *testing.T) {
	raw := os.Getenv("KAFKA_TEST_BROKERS")
	if raw == "" {
		t.Skip("KAFKA_TEST_BROKERS not set")
	}
	db := database(t)
	store := &auction.Store{DB: db}
	a := lot(t, store)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bid, _, err := store.PlaceBid(ctx, a.ID, user(t, db), 1000, "end-to-end")
	if err != nil {
		t.Fatal(err)
	}
	hub := realtime.NewHub()
	sub, unsub, err := hub.Subscribe(a.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer unsub()
	topic := testsupport.KafkaTopic(t, strings.Split(raw, ","))
	consumer, err := events.Consumer(strings.Split(raw, ","), topic, "test-"+uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	producer, err := events.Producer(strings.Split(raw, ","))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	stopped := make(chan error, 1)
	go func() { stopped <- events.RunConsumer(ctx, consumer, hub) }()
	defer func() {
		cancel()
		if err := <-stopped; err != nil {
			t.Error(err)
		}
	}()
	publisher := &outbox.Publisher{DB: db, Sender: outbox.KafkaSender{Client: producer, Topic: topic}}
	for i := 0; i < 2; i++ {
		worked, err := publisher.PublishOne(ctx)
		if err != nil || !worked {
			t.Fatal("publish", worked, err)
		}
	}
	for {
		select {
		case <-ctx.Done():
			t.Fatal("consumer notification timeout")
		case e := <-sub.Events:
			if e.Type == "bid.placed" {
				if e.Bid == nil || e.Bid.ID != bid.ID || e.Auction.CurrentPrice != 1000 {
					t.Fatal("invalid bid event")
				}
				return
			}
		}
	}
}
