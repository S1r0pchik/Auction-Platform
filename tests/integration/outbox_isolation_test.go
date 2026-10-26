package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"auction-platform/internal/auction"
	"auction-platform/internal/outbox"
)

func TestOutboxFailureOnlyDelaysItsAuction(t *testing.T) {
	db := database(t)
	store := &auction.Store{DB: db}
	a, b := lot(t, store), lot(t, store)
	ctx := context.Background()
	if _, _, err := store.PlaceBid(ctx, a.ID, user(t, db), 1000, "next"); err != nil {
		t.Fatal(err)
	}
	fail := true
	var sent []auction.Event
	publisher := &outbox.Publisher{DB: db, Sender: senderFunc(func(_ context.Context, key, value []byte) error {
		var e auction.Event
		if err := json.Unmarshal(value, &e); err != nil {
			return err
		}
		if e.AuctionID == a.ID && fail {
			return errors.New("isolated send failure")
		}
		sent = append(sent, e)
		return nil
	})}
	if attempted, err := publisher.PublishOne(ctx); err == nil || !attempted {
		t.Fatal("expected recorded attempt", attempted, err)
	}
	if _, err := db.Exec(ctx, `UPDATE outbox_events SET next_attempt_at=clock_timestamp()+interval '1 hour' WHERE auction_id=$1 AND aggregate_version=1`, a.ID); err != nil {
		t.Fatal(err)
	}
	if worked, err := publisher.PublishOne(ctx); err != nil || !worked {
		t.Fatal("unrelated auction blocked", worked, err)
	}
	if len(sent) != 1 || sent[0].AuctionID != b.ID {
		t.Fatal("wrong auction sent", sent)
	}
	if worked, err := publisher.PublishOne(ctx); err != nil || worked {
		t.Fatal("successor overtook failed head", worked, err)
	}
	var lastError *string
	var attempts int
	if err := db.QueryRow(ctx, `SELECT last_error,attempts FROM outbox_events WHERE auction_id=$1 AND aggregate_version=1`, a.ID).Scan(&lastError, &attempts); err != nil {
		t.Fatal(err)
	}
	if lastError == nil || attempts != 1 {
		t.Fatal("failure not observable", lastError, attempts)
	}
	if _, err := db.Exec(ctx, `UPDATE outbox_events SET next_attempt_at=clock_timestamp() WHERE auction_id=$1`, a.ID); err != nil {
		t.Fatal(err)
	}
	fail = false
	for i := 0; i < 2; i++ {
		if worked, err := publisher.PublishOne(ctx); err != nil || !worked {
			t.Fatal(worked, err)
		}
	}
	if len(sent) != 3 || sent[1].AuctionID != a.ID || sent[1].Version != 1 || sent[2].AuctionID != a.ID || sent[2].Version != 2 {
		t.Fatal("auction order violated", sent)
	}
	if err := db.QueryRow(ctx, `SELECT last_error FROM outbox_events WHERE auction_id=$1 AND aggregate_version=1`, a.ID).Scan(&lastError); err != nil || lastError != nil {
		t.Fatal("stale send error", lastError, err)
	}
}

func TestOutboxConcurrentWorkersCannotOvertakeLockedHead(t *testing.T) {
	db := database(t)
	store := &auction.Store{DB: db}
	a, b := lot(t, store), lot(t, store)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, _, err := store.PlaceBid(ctx, a.ID, user(t, db), 1000, "next"); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	publisher := &outbox.Publisher{DB: db, Sender: senderFunc(func(ctx context.Context, key, value []byte) error {
		var e auction.Event
		if err := json.Unmarshal(value, &e); err != nil {
			return err
		}
		if e.AuctionID == a.ID && e.Version == 1 {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})}
	first := make(chan error, 1)
	go func() { _, err := publisher.PublishOne(ctx); first <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("head not locked")
	}
	if worked, err := publisher.PublishOne(ctx); err != nil || !worked {
		t.Fatal("other auction blocked by sender", worked, err)
	}
	var published bool
	if err := db.QueryRow(ctx, `SELECT published_at IS NOT NULL FROM outbox_events WHERE auction_id=$1`, b.ID).Scan(&published); err != nil || !published {
		t.Fatal(err)
	}
	if worked, err := publisher.PublishOne(ctx); err != nil || worked {
		t.Fatal("successor bypassed in-flight head", worked, err)
	}
	once.Do(func() { close(release) })
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if worked, err := publisher.PublishOne(ctx); err != nil || !worked {
		t.Fatal("successor not released", worked, err)
	}
}

func TestOutboxWorkerPoolPreservesOrderAcrossAuctions(t *testing.T) {
	db := database(t)
	store := &auction.Store{DB: db}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	bidder := user(t, db)
	for i := 0; i < 8; i++ {
		a := lot(t, store)
		for n := 0; n < 4; n++ {
			if _, _, err := store.PlaceBid(ctx, a.ID, bidder, int64(1000+n*100), fmt.Sprint(n)); err != nil {
				t.Fatal(err)
			}
		}
	}
	var mu sync.Mutex
	last := map[uuid.UUID]int64{}
	active := map[uuid.UUID]bool{}
	inFlight, peak := 0, 0
	publisher := &outbox.Publisher{DB: db, Sender: senderFunc(func(ctx context.Context, _, value []byte) error {
		var e auction.Event
		if err := json.Unmarshal(value, &e); err != nil {
			return err
		}
		mu.Lock()
		if active[e.AuctionID] {
			t.Error("concurrent sends for same auction")
		}
		active[e.AuctionID] = true
		inFlight++
		if inFlight > peak {
			peak = inFlight
		}
		mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
		mu.Lock()
		defer mu.Unlock()
		if e.Version != last[e.AuctionID]+1 {
			t.Errorf("version out of order: previous=%d next=%d", last[e.AuctionID], e.Version)
		}
		last[e.AuctionID] = e.Version
		delete(active, e.AuctionID)
		inFlight--
		return nil
	})}
	done := make(chan struct{})
	go func() { defer close(done); publisher.Run(ctx, 4) }()
	defer func() { cancel(); <-done }()
	for {
		var pending int
		if err := db.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("publisher did not drain outbox")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if len(last) != 8 || peak < 2 {
		t.Fatal("worker pool did not run concurrently", len(last), peak)
	}
	for _, version := range last {
		if version != 5 {
			t.Fatal("missing events", version)
		}
	}
}
