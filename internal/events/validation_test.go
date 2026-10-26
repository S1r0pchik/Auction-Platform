package events

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"auction-platform/internal/auction"
	"auction-platform/internal/realtime"
)

func TestInvalidEventCannotReachHubOrAdvanceVersion(t *testing.T) {
	cases := []struct {
		name   string
		change func(*auction.Event)
	}{
		{"missing event ID", func(e *auction.Event) { e.ID = uuid.Nil }},
		{"missing auction ID", func(e *auction.Event) { e.AuctionID = uuid.Nil }},
		{"wrong schema", func(e *auction.Event) { e.SchemaVersion = 2 }},
		{"wrong type", func(e *auction.Event) { e.Type = "other" }},
		{"missing bid with future version", func(e *auction.Event) { e.Bid = nil; e.Version = 999; e.Auction.Version = 999 }},
		{"negative price", func(e *auction.Event) { e.Auction.CurrentPrice = -1 }},
		{"negative starting price", func(e *auction.Event) { e.Auction.StartingPrice = -1 }},
		{"overflow price", func(e *auction.Event) { e.Auction.CurrentPrice = auction.MaxMoney + 1 }},
		{"zero increment", func(e *auction.Event) { e.Auction.MinIncrement = 0 }},
		{"wrong currency", func(e *auction.Event) { e.Auction.Currency = "USD" }},
		{"missing owner", func(e *auction.Event) { e.Auction.OwnerID = uuid.Nil }},
		{"missing title", func(e *auction.Event) { e.Auction.Title = " " }},
		{"long description", func(e *auction.Event) { e.Auction.Description = strings.Repeat("x", 10001) }},
		{"missing created time", func(e *auction.Event) { e.Auction.CreatedAt = time.Time{} }},
		{"bad deadline", func(e *auction.Event) { e.Auction.EndsAt = e.Auction.StartsAt }},
		{"missing leader", func(e *auction.Event) { e.Auction.LeaderID = nil }},
		{"owner is leader", func(e *auction.Event) { e.Auction.LeaderID = &e.Auction.OwnerID }},
		{"zero last bid", func(e *auction.Event) { id := uuid.Nil; e.Auction.LastBidID = &id }},
		{"wrong bidder", func(e *auction.Event) { e.Bid.BidderID = uuid.New() }},
		{"wrong bid id", func(e *auction.Event) { e.Bid.ID = uuid.New() }},
		{"wrong bid auction", func(e *auction.Event) { e.Bid.AuctionID = uuid.New() }},
		{"wrong bid version", func(e *auction.Event) { e.Bid.AuctionVersion++ }},
		{"wrong amount", func(e *auction.Event) { e.Bid.Amount++ }},
		{"accepted before creation", func(e *auction.Event) { e.Bid.AcceptedAt = e.Auction.CreatedAt.Add(-time.Second) }},
		{"accepted at deadline", func(e *auction.Event) { e.Bid.AcceptedAt = e.Auction.EndsAt }},
		{"finished snapshot for bid", func(e *auction.Event) { e.Auction.Status = "finished" }},
		{"created has bids", func(e *auction.Event) { e.Type = "auction.created"; e.Version = 1; e.Auction.Version = 1 }},
		{"finished too early", func(e *auction.Event) {
			e.Type = "auction.finished"
			e.Bid = nil
			e.Auction.Status = "finished"
			at := e.Auction.EndsAt.Add(-time.Second)
			e.Auction.FinishedAt = &at
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			valid := validBidEvent()
			data, _ := json.Marshal(valid)
			var invalid auction.Event
			json.Unmarshal(data, &invalid)
			tc.change(&invalid)
			bad, _ := json.Marshal(invalid)
			hub := realtime.NewHub()
			sub, release, err := hub.Subscribe(valid.AuctionID)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			if err = Handle(hub, []byte(valid.AuctionID.String()), bad); err == nil {
				t.Fatal("invalid event accepted")
			}
			if len(sub.Events) != 0 {
				t.Fatal("invalid event broadcast")
			}
			if err = Handle(hub, []byte(valid.AuctionID.String()), data); err != nil {
				t.Fatal(err)
			}
			if len(sub.Events) != 1 {
				t.Fatal("invalid event advanced deduplication version")
			}
		})
	}
}

func TestEventLifecycleSnapshots(t *testing.T) {
	bid := validBidEvent()
	created := bid
	created.Type = "auction.created"
	created.Version = 1
	created.Bid = nil
	created.Auction.Version = 1
	created.Auction.LastBidID = nil
	created.Auction.LeaderID = nil
	created.Auction.CurrentPrice = created.Auction.StartingPrice
	finished := bid
	finished.Type = "auction.finished"
	finished.Version = 3
	finished.Bid = nil
	finished.Auction.Version = 3
	finished.Auction.Status = "finished"
	at := finished.Auction.EndsAt
	finished.Auction.FinishedAt = &at
	noWinner := created
	noWinner.Type = "auction.finished"
	noWinner.Version = 2
	noWinner.Auction.Version = 2
	noWinner.Auction.Status = "finished"
	noWinner.Auction.FinishedAt = &at
	for _, e := range []auction.Event{created, bid, finished, noWinner} {
		if err := validateEvent(e, []byte(e.AuctionID.String())); err != nil {
			t.Fatal(e.Type, err)
		}
	}
	if err := Handle(realtime.NewHub(), nil, []byte(strings.Repeat("x", MaxEventBytes+1))); err == nil {
		t.Fatal("oversized event accepted")
	}
}
