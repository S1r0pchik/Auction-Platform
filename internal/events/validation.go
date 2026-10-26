package events

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"

	"auction-platform/internal/auction"
)

const MaxEventBytes = 64 << 10

func validateEvent(e auction.Event, key []byte) error {
	bad := func(field string) error { return fmt.Errorf("invalid event field: %s", field) }
	a := e.Auction
	if e.SchemaVersion != 1 {
		return bad("schema_version")
	}
	if e.ID == uuid.Nil {
		return bad("event_id")
	}
	if e.AuctionID == uuid.Nil || e.AuctionID.String() != string(key) || a.ID != e.AuctionID {
		return bad("auction_id")
	}
	if e.Version < 1 || a.Version != e.Version {
		return bad("version")
	}
	if a.OwnerID == uuid.Nil {
		return bad("auction.owner_id")
	}
	if strings.TrimSpace(a.Title) == "" || utf8.RuneCountInString(a.Title) > 200 || len(a.Description) > 10000 {
		return bad("auction.title/description")
	}
	if a.Currency != "RUB" {
		return bad("auction.currency")
	}
	if a.StartingPrice < 0 || a.StartingPrice > auction.MaxMoney || a.CurrentPrice < a.StartingPrice || a.CurrentPrice > auction.MaxMoney || a.MinIncrement < 1 || a.MinIncrement > auction.MaxMoney {
		return bad("auction.prices")
	}
	if a.CreatedAt.IsZero() || a.StartsAt.IsZero() || !a.EndsAt.After(a.StartsAt) {
		return bad("auction.times")
	}
	if (a.LastBidID == nil) != (a.LeaderID == nil) {
		return bad("auction.leader")
	}
	if a.LastBidID == nil {
		if a.CurrentPrice != a.StartingPrice {
			return bad("auction.current_price")
		}
	} else if *a.LastBidID == uuid.Nil || *a.LeaderID == uuid.Nil || *a.LeaderID == a.OwnerID || a.CurrentPrice < 1 {
		return bad("auction.leader")
	}
	switch e.Type {
	case "auction.created":
		if e.Version != 1 || e.Bid != nil || a.LastBidID != nil || a.FinishedAt != nil || (a.Status != "scheduled" && a.Status != "active") {
			return bad("auction.created snapshot")
		}
	case "bid.placed":
		b := e.Bid
		if e.Version < 2 || a.Status != "active" || a.FinishedAt != nil || b == nil || a.LastBidID == nil {
			return bad("bid.placed snapshot")
		}
		if b.ID == uuid.Nil || b.ID != *a.LastBidID || b.AuctionID != a.ID || b.BidderID != *a.LeaderID || b.AuctionVersion != e.Version {
			return bad("bid.identifiers")
		}
		if b.Amount < 1 || b.Amount > auction.MaxMoney || b.Amount != a.CurrentPrice {
			return bad("bid.amount")
		}
		if b.AcceptedAt.IsZero() || b.AcceptedAt.Before(a.StartsAt) || !b.AcceptedAt.Before(a.EndsAt) || b.AcceptedAt.Before(a.CreatedAt) {
			return bad("bid.accepted_at")
		}
	case "auction.finished":
		if e.Version < 2 || e.Bid != nil || a.Status != "finished" || a.FinishedAt == nil || a.FinishedAt.Before(a.EndsAt) || a.FinishedAt.Before(a.CreatedAt) {
			return bad("auction.finished snapshot")
		}
	default:
		return bad("type")
	}
	return nil
}
