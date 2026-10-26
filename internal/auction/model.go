package auction

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"auction-platform/internal/validation"
)

const MaxMoney int64 = 9_000_000_000_000_000

var (
	ErrInvalid     = errors.New("invalid_request")
	ErrNotFound    = errors.New("not_found")
	ErrForbidden   = errors.New("owner_cannot_bid")
	ErrClosed      = errors.New("auction_unavailable")
	ErrLowBid      = errors.New("bid_too_low")
	ErrIdempotency = errors.New("idempotency_conflict")
)

type Auction struct {
	ID            uuid.UUID  `json:"id"`
	OwnerID       uuid.UUID  `json:"owner_id"`
	Title         string     `json:"title"`
	Description   string     `json:"description"`
	Currency      string     `json:"currency"`
	StartingPrice int64      `json:"starting_price"`
	CurrentPrice  int64      `json:"current_price"`
	MinIncrement  int64      `json:"min_increment"`
	StartsAt      time.Time  `json:"starts_at"`
	EndsAt        time.Time  `json:"ends_at"`
	Status        string     `json:"status"`
	LastBidID     *uuid.UUID `json:"last_bid_id"`
	LeaderID      *uuid.UUID `json:"leader_id"`
	Version       int64      `json:"version"`
	FinishedAt    *time.Time `json:"finished_at"`
	CreatedAt     time.Time  `json:"created_at"`
}

type CreateInput struct {
	Title         string    `json:"title"`
	Description   string    `json:"description"`
	StartingPrice int64     `json:"starting_price"`
	MinIncrement  int64     `json:"min_increment"`
	StartsAt      time.Time `json:"starts_at"`
	EndsAt        time.Time `json:"ends_at"`
}

type Bid struct {
	ID             uuid.UUID `json:"id"`
	AuctionID      uuid.UUID `json:"auction_id"`
	BidderID       uuid.UUID `json:"bidder_id"`
	Amount         int64     `json:"amount"`
	AcceptedAt     time.Time `json:"accepted_at"`
	AuctionVersion int64     `json:"auction_version"`
}

type BidTooLowError struct{ Minimum int64 }

func (e *BidTooLowError) Error() string { return ErrLowBid.Error() }

func (e *BidTooLowError) Unwrap() error { return ErrLowBid }

func ValidateCreate(in CreateInput) error {
	fields := map[string]string{}
	if strings.TrimSpace(in.Title) == "" || utf8.RuneCountInString(in.Title) > 200 {
		fields["title"] = "must contain 1 to 200 characters"
	}
	if strings.ContainsRune(in.Title, 0) {
		fields["title"] = "must not contain a NUL character"
	}
	if len(in.Description) > 10000 {
		fields["description"] = "must not exceed 10000 bytes"
	}
	if strings.ContainsRune(in.Description, 0) {
		fields["description"] = "must not contain a NUL character"
	}
	if in.StartingPrice < 0 || in.StartingPrice > MaxMoney {
		fields["starting_price"] = "must be between 0 and 9000000000000000 kopecks"
	}
	if in.MinIncrement < 1 || in.MinIncrement > MaxMoney {
		fields["min_increment"] = "must be between 1 and 9000000000000000 kopecks"
	}
	if in.StartsAt.IsZero() {
		fields["starts_at"] = "is required"
	}
	if in.EndsAt.IsZero() {
		fields["ends_at"] = "is required"
	} else if !in.EndsAt.Truncate(time.Microsecond).After(in.StartsAt.Truncate(time.Microsecond)) {
		fields["ends_at"] = "must be later than starts_at at microsecond precision"
	}
	if len(fields) > 0 {
		return &validation.Error{Cause: ErrInvalid, Fields: fields}
	}
	return nil
}

func ValidateBid(a Auction, bidder uuid.UUID, amount int64, now time.Time) error {
	if amount < 1 || amount > MaxMoney {
		return validation.Field(ErrInvalid, "amount", "must be between 1 and 9000000000000000 kopecks")
	}
	if a.OwnerID == bidder {
		return ErrForbidden
	}
	if (a.Status != "active" && a.Status != "scheduled") || now.Before(a.StartsAt) || !now.Before(a.EndsAt) {
		return ErrClosed
	}
	minimum := a.StartingPrice
	if a.LastBidID != nil {
		if a.CurrentPrice > MaxMoney-a.MinIncrement {
			return ErrLowBid
		}
		minimum = a.CurrentPrice + a.MinIncrement
	}
	if amount < minimum {
		return &BidTooLowError{Minimum: minimum}
	}
	return nil
}

type Event struct {
	ID            uuid.UUID `json:"event_id"`
	Type          string    `json:"type"`
	SchemaVersion int       `json:"schema_version"`
	AuctionID     uuid.UUID `json:"auction_id"`
	Version       int64     `json:"version"`
	Auction       Auction   `json:"auction"`
	Bid           *Bid      `json:"bid,omitempty"`
}
