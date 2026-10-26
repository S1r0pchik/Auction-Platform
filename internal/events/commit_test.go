package events

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"auction-platform/internal/monitor"
)

func TestCommitDeadlineDoesNotReportProgress(t *testing.T) {
	state := monitor.NewConsumer()
	state.Session()
	state.Assigned([]int32{0})
	state.ObserveLag(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := commitBatch(ctx, func(ctx context.Context, _ ...*kgo.Record) error { <-ctx.Done(); return ctx.Err() }, []*kgo.Record{{Offset: 4}}, state, 20*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	snapshot := state.Snapshot()
	if snapshot.CommitTimeouts != 1 || snapshot.Records != 0 || snapshot.LastCommit != nil || snapshot.InFlight != 0 {
		t.Fatal(snapshot)
	}
	if ctx.Err() != nil {
		t.Fatal("commit deadline canceled parent")
	}
	// A subsequent session can confirm the replayed record.
	if err = commitBatch(ctx, func(context.Context, ...*kgo.Record) error { return nil }, []*kgo.Record{{Offset: 4}}, state, time.Second); err != nil {
		t.Fatal(err)
	}
	if s := state.Snapshot(); s.LastCommit == nil || s.Records != 1 {
		t.Fatal(s)
	}
}

func TestCommitHonorsShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	state := monitor.NewConsumer()
	err := commitBatch(ctx, func(ctx context.Context, _ ...*kgo.Record) error { <-ctx.Done(); return ctx.Err() }, nil, state, time.Minute)
	if !errors.Is(err, context.Canceled) || state.Snapshot().CommitTimeouts != 0 {
		t.Fatal(err)
	}
}
