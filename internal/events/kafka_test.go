package events

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"auction-platform/internal/auction"
	"auction-platform/internal/realtime"
)

func TestRepeatedDelivery(t *testing.T) {
	h := realtime.NewHub()
	id := uuid.New()
	s, unsubscribe, err := h.Subscribe(id)
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribe()
	e := validBidEvent()
	e.AuctionID = id
	e.Auction.ID = id
	e.Bid.AuctionID = id
	data, _ := json.Marshal(e)
	for i := 0; i < 10; i++ {
		if err := Handle(h, []byte(id.String()), data); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.Events) != 1 {
		t.Fatalf("got %d notifications", len(s.Events))
	}
	if err := Handle(h, []byte(uuid.NewString()), data); err == nil {
		t.Fatal("wrong key accepted")
	}
	if err := Handle(h, nil, []byte(`broken`)); err == nil {
		t.Fatal("malformed event accepted")
	}
}

func validBidEvent() auction.Event {
	id, owner, bidder, bidID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	now := time.Now().UTC()
	a := auction.Auction{ID: id, OwnerID: owner, Title: "Test", Currency: "RUB", StartingPrice: 100, CurrentPrice: 110, MinIncrement: 10, StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), CreatedAt: now.Add(-time.Minute), Status: "active", Version: 2, LastBidID: &bidID, LeaderID: &bidder}
	return auction.Event{ID: uuid.New(), Type: "bid.placed", SchemaVersion: 1, AuctionID: id, Version: 2, Auction: a, Bid: &auction.Bid{ID: bidID, AuctionID: id, BidderID: bidder, Amount: 110, AcceptedAt: now, AuctionVersion: 2}}
}
