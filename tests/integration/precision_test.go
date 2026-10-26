package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"auction-platform/internal/auction"
)

func TestCreateDatePrecision(t *testing.T) {
	db := database(t)
	store := &auction.Store{DB: db}
	owner := user(t, db)
	start := time.Now().Add(time.Hour).Truncate(time.Second).Add(100 * time.Nanosecond)
	in := auction.CreateInput{Title: "Short auction", MinIncrement: 1, StartsAt: start, EndsAt: start.Add(100 * time.Nanosecond)}
	ctx := context.Background()
	if _, err := store.Create(ctx, owner, in); !errors.Is(err, auction.ErrInvalid) {
		t.Fatalf("collapsed timestamps: got %v, want validation error", err)
	}
	var count int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM auctions`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rejected auction was saved: count=%d err=%v", count, err)
	}
	in.EndsAt = start.Add(time.Microsecond)
	a, err := store.Create(ctx, owner, in)
	if err != nil {
		t.Fatal(err)
	}
	if a.EndsAt.Sub(a.StartsAt) != time.Microsecond {
		t.Fatalf("stored interval: %s", a.EndsAt.Sub(a.StartsAt))
	}
}
