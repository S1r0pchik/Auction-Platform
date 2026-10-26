package integration

import (
	"context"
	"testing"
	"time"

	"auction-platform/internal/auction"
	"auction-platform/internal/outbox"
)

func TestOutboxCleanupPreservesPendingAndRecent(t *testing.T) {
	db := database(t)
	ctx := context.Background()
	a := lot(t, &auction.Store{DB: db})
	// Synthetic published history tests retention independently of bid rules.
	_, err := db.Exec(ctx, `INSERT INTO outbox_events(id,auction_id,aggregate_version,event_type,payload,published_at)
 SELECT md5(i::text)::uuid,$1,i,'test','{}',CASE WHEN i<=1003 THEN clock_timestamp()-interval '8 days' ELSE clock_timestamp() END
 FROM generate_series(2,1005) i`, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	p := &outbox.Publisher{DB: db}
	if n, err := p.CleanupPublished(ctx, 7*24*time.Hour); err != nil || n != 1000 {
		t.Fatal(n, err)
	}
	if n, err := p.CleanupPublished(ctx, 7*24*time.Hour); err != nil || n != 2 {
		t.Fatal(n, err)
	}
	var pending, recent int
	if err := db.QueryRow(ctx, `SELECT count(*) FILTER(WHERE published_at IS NULL),count(*) FILTER(WHERE published_at IS NOT NULL) FROM outbox_events`).Scan(&pending, &recent); err != nil || pending != 1 || recent != 2 {
		t.Fatal(pending, recent, err)
	}
	if n, err := p.CleanupPublished(ctx, 0); err == nil || n != 0 {
		t.Fatal("invalid retention accepted")
	}
}
