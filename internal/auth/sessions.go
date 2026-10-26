package auth

import (
	"context"
	"crypto/sha256"
	"log/slog"
	"time"
)

func (s *Store) Logout(ctx context.Context, token string) error {
	if len(token) != 43 {
		return ErrCredentials
	}
	sum := sha256.Sum256([]byte(token))
	_, err := s.DB.Exec(ctx, `DELETE FROM sessions WHERE token_hash=$1`, sum[:])
	return err
}

func (s *Store) CleanupExpired(ctx context.Context) (int64, error) {
	result, err := s.DB.Exec(ctx, `WITH expired AS (
		SELECT token_hash FROM sessions WHERE expires_at<=statement_timestamp()
		ORDER BY expires_at,token_hash LIMIT 1000 FOR UPDATE SKIP LOCKED
	) DELETE FROM sessions s USING expired e WHERE s.token_hash=e.token_hash`)
	return result.RowsAffected(), err
}

func (s *Store) RunSessionCleanup(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		op, cancel := context.WithTimeout(ctx, 5*time.Second)
		n, err := s.CleanupExpired(op)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			slog.Error("session cleanup", "error", err)
		} else if n > 0 {
			slog.Info("expired sessions removed", "count", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
