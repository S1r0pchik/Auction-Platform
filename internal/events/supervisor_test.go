package events

import (
	"context"
	"errors"
	"testing"
	"time"

	"auction-platform/internal/monitor"
)

func TestSupervisorRecoversTransientFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := monitor.NewConsumer()
	done := make(chan struct{})
	recovered := make(chan struct{})
	go func() {
		defer close(done)
		calls := 0
		supervise(ctx, func(ctx context.Context) error {
			calls++
			if calls == 1 {
				return errors.New("broker unavailable")
			}
			state.Session()
			state.Assigned([]int32{0})
			state.ObserveLag(0)
			state.BeginCommit(1)
			state.EndCommit(1, false, true)
			close(recovered)
			<-ctx.Done()
			return nil
		}, state, time.Millisecond, 2*time.Millisecond)
	}()
	select {
	case <-recovered:
	case <-time.After(time.Second):
		t.Fatal("consumer did not recover")
	}
	snapshot := state.Snapshot()
	if snapshot.State != "running" || snapshot.Failures != 1 || snapshot.Restarts != 1 || snapshot.Records != 1 {
		t.Fatal(snapshot)
	}
	cancel()
	<-done
}

func TestSupervisorBlocksPoisonWithoutHotRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := monitor.NewConsumer()
	done := make(chan struct{})
	go func() {
		defer close(done)
		supervise(ctx, func(context.Context) error { return &PoisonError{Err: errors.New("bad event offset=7")} }, state, time.Millisecond, time.Millisecond)
	}()
	deadline := time.After(time.Second)
	for state.Snapshot().State != "blocked" {
		select {
		case <-deadline:
			t.Fatal("poison not reported")
		case <-time.After(time.Millisecond):
		}
	}
	if state.Snapshot().Restarts != 0 {
		t.Fatal("poison event retried")
	}
	cancel()
	<-done
}
