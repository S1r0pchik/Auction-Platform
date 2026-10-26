package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

func (p *Publisher) CleanupPublished(ctx context.Context, retention time.Duration) (int64, error) {
	if retention < time.Hour {
		return 0, fmt.Errorf("outbox retention must be at least one hour")
	}
	result, err := p.DB.Exec(ctx, `WITH expired AS (
	 SELECT seq FROM outbox_events
	 WHERE published_at < statement_timestamp()-make_interval(secs=>$1)
	 ORDER BY published_at,seq LIMIT 1000 FOR UPDATE SKIP LOCKED
	) DELETE FROM outbox_events e USING expired x WHERE e.seq=x.seq`, retention.Seconds())
	return result.RowsAffected(), err
}

func (p *Publisher) RunCleanup(ctx context.Context, retention time.Duration) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		op, cancel := context.WithTimeout(ctx, 5*time.Second)
		var n int64
		var err error
		for range 100 {
			var batch int64
			batch, err = p.CleanupPublished(op, retention)
			n += batch
			if err != nil || batch < 1000 {
				break
			}
		}
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			slog.Error("outbox cleanup", "error", err)
		} else if n > 0 {
			slog.Info("published outbox events removed", "count", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
