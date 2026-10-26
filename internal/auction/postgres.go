package auction

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"auction-platform/internal/validation"
)

type Store struct{ DB *pgxpool.Pool }

const columns = `a.id, a.owner_id, a.title, a.description, a.currency,
	a.starting_price, a.current_price, a.min_increment,
	a.starts_at, a.ends_at, a.status, a.last_bid_id,
	(SELECT bidder_id FROM bids WHERE id = a.last_bid_id),
	a.version, a.finished_at, a.created_at`

func scanAuction(row pgx.Row) (Auction, error) {
	var a Auction
	err := row.Scan(
		&a.ID, &a.OwnerID, &a.Title, &a.Description, &a.Currency,
		&a.StartingPrice, &a.CurrentPrice, &a.MinIncrement,
		&a.StartsAt, &a.EndsAt, &a.Status, &a.LastBidID, &a.LeaderID,
		&a.Version, &a.FinishedAt, &a.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	return a, err
}

func (s *Store) Get(ctx context.Context, id uuid.UUID) (Auction, error) {
	return scanAuction(s.DB.QueryRow(ctx, `SELECT `+columns+` FROM auctions a WHERE a.id=$1`, id))
}

func (s *Store) List(ctx context.Context, before time.Time, id uuid.UUID, limit int) ([]Auction, error) {
	rows, err := s.DB.Query(ctx, `SELECT `+columns+`
		FROM auctions a
		WHERE (a.created_at, a.id) < ($1, $2)
		ORDER BY a.created_at DESC, a.id DESC
		LIMIT $3`, before, id, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Auction{}
	for rows.Next() {
		a, err := scanAuction(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, a)
	}
	return result, rows.Err()
}

func recordEvent(ctx context.Context, tx pgx.Tx, typ string, a Auction, bid *Bid) error {
	e := Event{ID: uuid.New(), Type: typ, SchemaVersion: 1, AuctionID: a.ID, Version: a.Version, Auction: a, Bid: bid}
	payload, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events(id,auction_id,aggregate_version,event_type,payload) VALUES($1,$2,$3,$4,$5)`, e.ID, a.ID, a.Version, typ, payload)
	return err
}

func (s *Store) Create(ctx context.Context, owner uuid.UUID, in CreateInput) (Auction, error) {
	in.Title = strings.TrimSpace(in.Title)
	if err := ValidateCreate(in); err != nil {
		return Auction{}, err
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return Auction{}, err
	}
	defer tx.Rollback(ctx)
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return Auction{}, err
	}
	if !in.EndsAt.After(now) {
		return Auction{}, validation.Field(ErrInvalid, "ends_at", "must be in the future")
	}
	status := "scheduled"
	if !now.Before(in.StartsAt) {
		status = "active"
	}
	id := uuid.New()
	_, err = tx.Exec(ctx, `INSERT INTO auctions(id, owner_id, title, description, starting_price, current_price, min_increment, starts_at, ends_at, status)
	VALUES($1, $2, $3, $4, $5, $5, $6, $7, $8, $9)`, id, owner, in.Title, in.Description, in.StartingPrice, in.MinIncrement, in.StartsAt, in.EndsAt, status)
	if err != nil {
		return Auction{}, err
	}
	a, err := scanAuction(tx.QueryRow(ctx, `SELECT `+columns+` FROM auctions a WHERE a.id=$1`, id))
	if err != nil {
		return Auction{}, err
	}
	if err = recordEvent(ctx, tx, "auction.created", a, nil); err != nil {
		return Auction{}, err
	}
	err = tx.Commit(ctx)
	return a, err
}

func scanBid(row pgx.Row) (Bid, error) {
	var b Bid
	err := row.Scan(&b.ID, &b.AuctionID, &b.BidderID, &b.Amount, &b.AcceptedAt, &b.AuctionVersion)
	return b, err
}

const bidColumns = `id,auction_id,bidder_id,amount,accepted_at,auction_version`

// Lock the auction before checking retries or accepting a new bid.
func (s *Store) PlaceBid(ctx context.Context, id, bidder uuid.UUID, amount int64, key string) (Bid, bool, error) {
	if len(key) < 1 || len(key) > 128 || strings.TrimSpace(key) != key || strings.IndexFunc(key, func(r rune) bool { return r < 32 || r > 126 }) >= 0 {
		return Bid{}, false, validation.Field(ErrInvalid, "Idempotency-Key", "must contain 1 to 128 ASCII characters without leading or trailing whitespace")
	}
	tx, err := s.DB.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Bid{}, false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SET LOCAL lock_timeout = '500ms'`); err != nil {
		return Bid{}, false, err
	}
	a, err := scanAuction(tx.QueryRow(ctx, `SELECT `+columns+` FROM auctions a WHERE a.id=$1 FOR UPDATE OF a`, id))
	if err != nil {
		return Bid{}, false, err
	}
	old, err := scanBid(tx.QueryRow(ctx, `SELECT `+bidColumns+` FROM bids WHERE auction_id=$1 AND bidder_id=$2 AND idempotency_key=$3`, id, bidder, key))
	if err == nil {
		if old.Amount != amount {
			return Bid{}, false, ErrIdempotency
		}
		return old, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Bid{}, false, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return Bid{}, false, err
	}
	if err = ValidateBid(a, bidder, amount, now); err != nil {
		return Bid{}, false, err
	}
	b := Bid{ID: uuid.New(), AuctionID: id, BidderID: bidder, Amount: amount, AcceptedAt: now, AuctionVersion: a.Version + 1}
	_, err = tx.Exec(ctx, `INSERT INTO bids(id, auction_id, bidder_id, amount, accepted_at, auction_version, idempotency_key)
	VALUES($1, $2, $3, $4, $5, $6, $7)`, b.ID, id, bidder, amount, now, b.AuctionVersion, key)
	if err != nil {
		return Bid{}, false, err
	}
	_, err = tx.Exec(ctx, `UPDATE auctions SET current_price=$2,last_bid_id=$3,version=$4,status='active' WHERE id=$1`, id, amount, b.ID, b.AuctionVersion)
	if err != nil {
		return Bid{}, false, err
	}
	a.CurrentPrice = amount
	a.LastBidID = &b.ID
	a.LeaderID = &bidder
	a.Version = b.AuctionVersion
	a.Status = "active"
	if err = recordEvent(ctx, tx, "bid.placed", a, &b); err != nil {
		return Bid{}, false, err
	}
	err = tx.Commit(ctx)
	return b, false, err
}

func (s *Store) Bids(ctx context.Context, id uuid.UUID, after int64, limit int) ([]Bid, error) {
	if _, err := s.Get(ctx, id); err != nil {
		return nil, err
	}
	rows, err := s.DB.Query(ctx, `SELECT `+bidColumns+` FROM bids WHERE auction_id=$1 AND auction_version>$2 ORDER BY auction_version LIMIT $3`, id, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Bid{}
	for rows.Next() {
		b, err := scanBid(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, b)
	}
	return result, rows.Err()
}

func (s *Store) Tick(ctx context.Context) (bool, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	a, err := scanAuction(tx.QueryRow(ctx, `SELECT `+columns+`
	FROM auctions a
	WHERE (status='scheduled' AND starts_at<=clock_timestamp()) OR (status='active' AND ends_at<=clock_timestamp())
	ORDER BY ends_at, id
	LIMIT 1
	FOR UPDATE OF a SKIP LOCKED`))
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return false, err
	}
	if !now.Before(a.EndsAt) {
		a.Status = "finished"
		a.FinishedAt = &now
		a.Version++
		_, err = tx.Exec(ctx, `UPDATE auctions SET status='finished',finished_at=$2,version=$3 WHERE id=$1`, a.ID, now, a.Version)
		if err == nil {
			err = recordEvent(ctx, tx, "auction.finished", a, nil)
		}
	} else {
		_, err = tx.Exec(ctx, `UPDATE auctions SET status='active' WHERE id=$1`, a.ID)
	}
	if err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}
