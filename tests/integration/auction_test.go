package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"auction-platform/internal/auction"
	"auction-platform/internal/outbox"
	"auction-platform/migrations"
)

func database(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set: PostgreSQL integration tests skipped")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	schema := "test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	// schema consists only of an ASCII prefix and hex digits generated here.
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.ConnConfig.RuntimeParams["application_name"] = schema
	cfg.MaxConns = 30
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		_, err := admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		if err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	if err = migrations.Up(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err = migrations.Up(ctx, db); err != nil {
		t.Fatal("migration is not idempotent:", err)
	}
	return db
}

func user(t *testing.T, db *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(context.Background(), `INSERT INTO users(id,email,password_hash) VALUES($1,$2,'test-only')`, id, id.String()+"@example.com")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func lot(t *testing.T, s *auction.Store) auction.Auction {
	t.Helper()
	a, err := s.Create(context.Background(), user(t, s.DB), auction.CreateInput{Title: "Test lot", StartingPrice: 1000, MinIncrement: 100, StartsAt: time.Now().Add(-time.Hour), EndsAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func invariants(t *testing.T, db *pgxpool.Pool, id uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	var bad int
	err := db.QueryRow(ctx, `SELECT count(*) FROM (SELECT amount,lag(amount) OVER(ORDER BY auction_version) AS previous FROM bids WHERE auction_id=$1) b WHERE previous IS NOT NULL AND amount<previous+100`, id).Scan(&bad)
	if err != nil {
		t.Fatal(err)
	}
	if bad != 0 {
		t.Fatal("prices did not increase")
	}
	err = db.QueryRow(ctx, `SELECT count(*) FROM auctions a WHERE id=$1 AND (current_price<>COALESCE((SELECT amount FROM bids WHERE id=a.last_bid_id),starting_price) OR EXISTS(SELECT 1 FROM bids b WHERE b.auction_id=a.id AND (b.bidder_id=a.owner_id OR b.accepted_at<a.starts_at OR b.accepted_at>=a.ends_at)) OR (SELECT count(*) FROM outbox_events o WHERE o.auction_id=a.id)<>a.version)`, id).Scan(&bad)
	if err != nil {
		t.Fatal(err)
	}
	if bad != 0 {
		t.Fatal("auction invariant broken")
	}
	err = db.QueryRow(ctx, `SELECT count(*) FROM bids b WHERE auction_id=$1 AND NOT EXISTS(SELECT 1 FROM outbox_events o WHERE o.auction_id=b.auction_id AND o.aggregate_version=b.auction_version AND o.event_type='bid.placed')`, id).Scan(&bad)
	if err != nil {
		t.Fatal(err)
	}
	if bad != 0 {
		t.Fatal("bid has no event")
	}
}

func TestConcurrentBids(t *testing.T) {
	db := database(t)
	s := &auction.Store{DB: db}
	ctx := context.Background()
	t.Run("same amount has one winner", func(t *testing.T) {
		a := lot(t, s)
		bidder := user(t, db)
		var wins atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				_, _, err := s.PlaceBid(ctx, a.ID, bidder, 1100, fmt.Sprint(i))
				if err == nil {
					wins.Add(1)
				} else if !errors.Is(err, auction.ErrLowBid) {
					t.Errorf("unexpected: %v", err)
				}
			}(i)
		}
		close(start)
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("accepted %d", wins.Load())
		}
		invariants(t, db, a.ID)
	})
	t.Run("different amounts preserve history", func(t *testing.T) {
		a := lot(t, s)
		bidder := user(t, db)
		var wins atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				_, _, err := s.PlaceBid(ctx, a.ID, bidder, int64(1100+i*100), fmt.Sprint(i))
				if err == nil {
					wins.Add(1)
				} else if !errors.Is(err, auction.ErrLowBid) {
					t.Errorf("unexpected: %v", err)
				}
			}(i)
		}
		close(start)
		wg.Wait()
		if wins.Load() < 1 {
			t.Fatal("no bids accepted")
		}
		invariants(t, db, a.ID)
		a, err := s.Get(ctx, a.ID)
		if err != nil {
			t.Fatal(err)
		}
		if a.CurrentPrice != 11000 {
			t.Fatalf("maximum bid missing: %d", a.CurrentPrice)
		}
	})
}

func TestIdempotencyAndLifecycle(t *testing.T) {
	db := database(t)
	s := &auction.Store{DB: db}
	a := lot(t, s)
	bidder := user(t, db)
	ctx := context.Background()
	var wg sync.WaitGroup
	var fresh atomic.Int32
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, replay, err := s.PlaceBid(ctx, a.ID, bidder, 1000, "same-key")
			if err != nil {
				t.Error(err)
			}
			if !replay {
				fresh.Add(1)
			}
		}()
	}
	wg.Wait()
	if fresh.Load() != 1 {
		t.Fatalf("created %d bids", fresh.Load())
	}
	if _, _, err := s.PlaceBid(ctx, a.ID, bidder, 1100, "same-key"); !errors.Is(err, auction.ErrIdempotency) {
		t.Fatal(err)
	}
	if _, _, err := s.PlaceBid(ctx, a.ID, a.OwnerID, 1100, "owner"); !errors.Is(err, auction.ErrForbidden) {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `UPDATE auctions SET ends_at=clock_timestamp() WHERE id=$1`, a.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	finished, err := s.Get(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finished.Status != "finished" || finished.LeaderID == nil || *finished.LeaderID != bidder {
		t.Fatal("wrong winner", finished)
	}
	if _, replay, err := s.PlaceBid(ctx, a.ID, bidder, 1000, "same-key"); err != nil || !replay {
		t.Fatal("replay after finish", err)
	}
	if _, _, err := s.PlaceBid(ctx, a.ID, bidder, 1200, "new-key"); !errors.Is(err, auction.ErrClosed) {
		t.Fatal(err)
	}
	if worked, err := s.Tick(ctx); err != nil || worked {
		t.Fatal("double finish", err)
	}
	invariants(t, db, a.ID)
}

func TestDeadlineCheckedAfterLock(t *testing.T) {
	db := database(t)
	s := &auction.Store{DB: db}
	a := lot(t, s)
	bidder := user(t, db)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT id FROM auctions WHERE id=$1 FOR UPDATE`, a.ID); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	var lockerPID int32
	if err = tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&lockerPID); err != nil {
		t.Fatal(err)
	}
	go func() { _, _, e := s.PlaceBid(ctx, a.ID, bidder, 1000, "waiting"); result <- e }()
	// Ensure the bid transaction actually started and is waiting on our lock.
	// A mutation to now() instead of clock_timestamp() would then admit a stale bid.
	deadline := time.Now().Add(3 * time.Second)
	for {
		var waiting bool
		if err = db.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE application_name = current_setting('application_name')
			AND $1 = ANY(pg_blocking_pids(pid))
		)`, lockerPID).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("bid did not wait for row lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err = tx.Exec(ctx, `UPDATE auctions SET ends_at=clock_timestamp() WHERE id=$1`, a.ID); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { _, e := s.Tick(ctx); closed <- e }()
	if err = <-result; !errors.Is(err, auction.ErrClosed) {
		t.Fatal("accepted after deadline", err)
	}
	if err = <-closed; err != nil {
		t.Fatal(err)
	}
	// A skipped locked row may need the next worker tick.
	if _, err = s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	invariants(t, db, a.ID)
}

func TestLifecycleScheduledAndNoWinner(t *testing.T) {
	db := database(t)
	s := &auction.Store{DB: db}
	ctx := context.Background()
	a, err := s.Create(ctx, user(t, db), auction.CreateInput{Title: "Future", StartingPrice: 1000, MinIncrement: 100, StartsAt: time.Now().Add(time.Hour), EndsAt: time.Now().Add(2 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if a.Status != "scheduled" {
		t.Fatal(a.Status)
	}
	if _, _, err = s.PlaceBid(ctx, a.ID, user(t, db), 1000, "early"); !errors.Is(err, auction.ErrClosed) {
		t.Fatal(err)
	}
	if _, err = db.Exec(ctx, `UPDATE auctions SET starts_at=clock_timestamp()-interval '1 hour' WHERE id=$1`, a.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	a, err = s.Get(ctx, a.ID)
	if err != nil || a.Status != "active" {
		t.Fatal(a, err)
	}
	if _, err = db.Exec(ctx, `UPDATE auctions SET ends_at=clock_timestamp() WHERE id=$1`, a.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	a, err = s.Get(ctx, a.ID)
	if err != nil || a.Status != "finished" || a.LeaderID != nil {
		t.Fatal(a, err)
	}
	invariants(t, db, a.ID)
}

func TestOutboxFailureRollsBackBid(t *testing.T) {
	db := database(t)
	s := &auction.Store{DB: db}
	a := lot(t, s)
	ctx := context.Background()
	_, err := db.Exec(ctx, `CREATE FUNCTION reject_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected outbox failure'; END $$; CREATE TRIGGER reject_event BEFORE INSERT ON outbox_events FOR EACH ROW EXECUTE FUNCTION reject_event()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.PlaceBid(ctx, a.ID, user(t, db), 1000, "rollback"); err == nil {
		t.Fatal("expected failure")
	}
	var count int
	if err = db.QueryRow(ctx, `SELECT count(*) FROM bids WHERE auction_id=$1`, a.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("bid survived rollback")
	}
	invariants(t, db, a.ID)
}

type senderFunc func(context.Context, []byte, []byte) error

func (f senderFunc) Send(ctx context.Context, k, v []byte) error { return f(ctx, k, v) }

func TestOutboxRetryAndCrashWindow(t *testing.T) {
	db := database(t)
	s := &auction.Store{DB: db}
	a := lot(t, s)
	ctx := context.Background()
	var sends int
	p := &outbox.Publisher{DB: db, Sender: senderFunc(func(context.Context, []byte, []byte) error { sends++; return errors.New("broker unavailable") })}
	if _, err := p.PublishOne(ctx); err == nil {
		t.Fatal("expected send failure")
	}
	var attempts int
	var published bool
	if err := db.QueryRow(ctx, `SELECT attempts,published_at IS NOT NULL FROM outbox_events WHERE auction_id=$1`, a.ID).Scan(&attempts, &published); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || published {
		t.Fatal("wrong retry metadata")
	}
	if _, err := db.Exec(ctx, `UPDATE outbox_events SET next_attempt_at=clock_timestamp()`); err != nil {
		t.Fatal(err)
	}
	crashCtx, cancel := context.WithCancel(ctx)
	p.Sender = senderFunc(func(context.Context, []byte, []byte) error { sends++; cancel(); return nil })
	if _, err := p.PublishOne(crashCtx); err == nil {
		t.Fatal("expected DB failure after Kafka ack")
	}
	p.Sender = senderFunc(func(context.Context, []byte, []byte) error { sends++; return nil })
	// The failed transaction's connection is discarded asynchronously by pgx.
	if worked, err := p.PublishOne(ctx); err != nil || !worked {
		t.Fatal("retry", worked, err)
	}
	if worked, err := p.PublishOne(ctx); err != nil || worked {
		t.Fatal("already published", err)
	}
	if sends != 3 {
		t.Fatalf("expected duplicate send across crash window, got %d", sends)
	}
}
