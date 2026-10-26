package events

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"auction-platform/internal/monitor"
)

type PoisonError struct{ Err error }

func (e *PoisonError) Error() string { return e.Err.Error() }

func (e *PoisonError) Unwrap() error { return e.Err }

func Supervise(ctx context.Context, run func(context.Context) error, state *monitor.Consumer) {
	supervise(ctx, run, state, time.Second, 30*time.Second)
}

func supervise(ctx context.Context, run func(context.Context) error, state *monitor.Consumer, initial, maxDelay time.Duration) {
	defer state.Stopped()
	delay := initial
	for ctx.Err() == nil {
		start := time.Now()
		err := run(ctx)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			err = errors.New("consumer session stopped unexpectedly")
		}
		var poison *PoisonError
		blocked := errors.As(err, &poison)
		state.Failed(err, blocked)
		slog.Error("consumer session failed", "error", err, "blocked", blocked, "retry_delay", delay)
		if blocked {
			<-ctx.Done()
			return
		}
		if time.Since(start) > time.Minute {
			delay = initial
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		state.Restart()
		delay = min(delay*2, maxDelay)
	}
}
