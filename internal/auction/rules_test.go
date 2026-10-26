package auction

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"auction-platform/internal/validation"
)

func TestValidateBid(t *testing.T) {
	now := time.Now()
	owner, bidder, last := uuid.New(), uuid.New(), uuid.New()
	base := Auction{OwnerID: owner, StartingPrice: 100, CurrentPrice: 100, MinIncrement: 10, StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour), Status: "active"}
	cases := []struct {
		name   string
		modify func(*Auction)
		bidder uuid.UUID
		amount int64
		at     time.Time
		want   error
	}{
		{"first at starting price", nil, bidder, 100, now, nil},
		{"below starting price", nil, bidder, 99, now, ErrLowBid},
		{"owner", nil, owner, 100, now, ErrForbidden},
		{"zero", nil, bidder, 0, now, ErrInvalid},
		{"too large", nil, bidder, MaxMoney + 1, now, ErrInvalid},
		{"at start", nil, bidder, 100, base.StartsAt, nil},
		{"before start", nil, bidder, 100, base.StartsAt.Add(-time.Nanosecond), ErrClosed},
		{"at deadline", nil, bidder, 100, base.EndsAt, ErrClosed},
		{"finished", func(a *Auction) { a.Status = "finished" }, bidder, 100, now, ErrClosed},
		{"cancelled", func(a *Auction) { a.Status = "cancelled" }, bidder, 100, now, ErrClosed},
		{"scheduled eligible", func(a *Auction) { a.Status = "scheduled" }, bidder, 100, now, nil},
		{"increment", func(a *Auction) { a.LastBidID = &last }, bidder, 110, now, nil},
		{"increment missing", func(a *Auction) { a.LastBidID = &last }, bidder, 109, now, ErrLowBid},
		{"overflow guard", func(a *Auction) { a.LastBidID = &last; a.CurrentPrice = MaxMoney }, bidder, MaxMoney, now, ErrLowBid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := base
			if tc.modify != nil {
				tc.modify(&a)
			}
			if err := ValidateBid(a, tc.bidder, tc.amount, tc.at); !errors.Is(err, tc.want) {
				t.Fatalf("got %v want %v", err, tc.want)
			}
		})
	}
}

func TestCreateValidationExplainsFields(t *testing.T) {
	err := ValidateCreate(CreateInput{Title: "\x00", Description: "\x00", StartingPrice: -1, MinIncrement: 0})
	var invalid *validation.Error
	if !errors.As(err, &invalid) || !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, field := range []string{"title", "description", "starting_price", "min_increment", "starts_at", "ends_at"} {
		if invalid.Fields[field] == "" {
			t.Fatal("missing explanation", field)
		}
	}
}
