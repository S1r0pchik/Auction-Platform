package realtime

import (
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"auction-platform/internal/auction"
)

func TestHubDeduplicatesAndDisconnectsSlowSubscriber(t *testing.T) {
	h := NewHub()
	id := uuid.New()
	s, unsub, err := h.Subscribe(id)
	if err != nil {
		t.Fatal(err)
	}
	defer unsub()
	e := auction.Event{AuctionID: id, Version: 2}
	h.Publish(e)
	h.Publish(e)
	e.Version = 1
	h.Publish(e)
	if len(s.Events) != 1 {
		t.Fatalf("duplicate delivered: %d", len(s.Events))
	}
	<-s.Events
	for i := int64(3); i < 40; i++ {
		h.Publish(auction.Event{AuctionID: id, Version: i})
	}
	select {
	case <-s.Done:
	default:
		t.Fatal("slow subscriber not disconnected")
	}
}

func TestHubLimitsAndRelease(t *testing.T) {
	h := NewHubWithLimits(2, 1)
	first, second := uuid.New(), uuid.New()
	_, release, err := h.Subscribe(first)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = h.Subscribe(first); !errors.Is(err, ErrCapacity) {
		t.Fatal("per-auction limit", err)
	}
	_, releaseSecond, err := h.Subscribe(second)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = h.Subscribe(uuid.New()); !errors.Is(err, ErrCapacity) {
		t.Fatal("total limit", err)
	}
	release()
	release()
	_, replacement, err := h.Subscribe(first)
	if err != nil {
		t.Fatal("capacity not released", err)
	}
	replacement()
	releaseSecond()
	if stats := h.Stats(); stats.Connections != 0 || stats.Rejected != 2 || len(h.counts) != 0 {
		t.Fatal(stats)
	}
}

func TestSlowClientReservesCapacityUntilHandlerExits(t *testing.T) {
	h := NewHubWithLimits(1, 1)
	id := uuid.New()
	sub, release, err := h.Subscribe(id)
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 33; i++ {
		h.Publish(auction.Event{AuctionID: id, Version: i})
	}
	select {
	case <-sub.Done:
	default:
		t.Fatal("slow client not disconnected")
	}
	if _, _, err = h.Subscribe(id); !errors.Is(err, ErrCapacity) {
		t.Fatal("closing socket released capacity too early", err)
	}
	release()
	_, next, err := h.Subscribe(id)
	if err != nil {
		t.Fatal(err)
	}
	next()
}

func TestHubConcurrentSubscriptions(t *testing.T) {
	h := NewHub()
	id := uuid.New()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, unsub, err := h.Subscribe(id)
			if err != nil {
				t.Error(err)
				return
			}
			h.Publish(auction.Event{AuctionID: id, Version: 1})
			unsub()
			unsub()
		}()
	}
	wg.Wait()
	if len(h.subscribers) != 0 {
		t.Fatal("subscriptions leaked")
	}
}
