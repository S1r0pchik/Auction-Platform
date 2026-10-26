package outbox

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Sender interface {
	Send(context.Context, []byte, []byte) error
}

type KafkaSender struct {
	Client *kgo.Client
	Topic  string
}

func (s KafkaSender) Send(ctx context.Context, key, value []byte) error {
	return s.Client.ProduceSync(ctx, &kgo.Record{Topic: s.Topic, Key: key, Value: value}).FirstErr()
}

type Publisher struct {
	DB     *pgxpool.Pool
	Sender Sender
}

// Only the first pending event of each auction is eligible.
func (p *Publisher) PublishOne(ctx context.Context) (bool, error) {
	tx, err := p.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var seq int64
	var key string
	var value []byte
	err = tx.QueryRow(ctx, `SELECT e.seq,e.auction_id::text,e.payload
 FROM outbox_events e
 WHERE e.published_at IS NULL AND e.next_attempt_at<=statement_timestamp()
 AND NOT EXISTS(SELECT 1 FROM outbox_events predecessor
  WHERE predecessor.auction_id=e.auction_id AND predecessor.seq<e.seq AND predecessor.published_at IS NULL)
 ORDER BY e.seq LIMIT 1 FOR UPDATE OF e SKIP LOCKED`).Scan(&seq, &key, &value)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err = p.Sender.Send(ctx, []byte(key), value); err != nil {
		message := []rune(err.Error())
		if len(message) > 1024 {
			message = message[:1024]
		}
		_, e := tx.Exec(ctx, `UPDATE outbox_events
	SET attempts=attempts+1, last_attempt_at=clock_timestamp(), last_error=$2, next_attempt_at=clock_timestamp()+make_interval(secs=>LEAST(30, power(2, LEAST(attempts, 5))::integer))
	WHERE seq=$1`, seq, string(message))
		if e != nil {
			return false, e
		}
		if e = tx.Commit(ctx); e != nil {
			return false, e
		}
		return true, err
	}
	_, err = tx.Exec(ctx, `UPDATE outbox_events
	SET published_at=clock_timestamp(), last_attempt_at=clock_timestamp(), last_error=NULL, attempts=attempts+1
	WHERE seq=$1`, seq)
	if err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func (p *Publisher) Run(ctx context.Context, workers int) {
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					for range 100 {
						op, cancel := context.WithTimeout(ctx, 15*time.Second)
						attempted, err := p.PublishOne(op)
						cancel()
						if err != nil {
							slog.Error("publish event", "error", err)
						}
						if !attempted {
							break
						}
						if ctx.Err() != nil {
							return
						}
					}
				}
			}
		})
	}
	wg.Wait()
}
