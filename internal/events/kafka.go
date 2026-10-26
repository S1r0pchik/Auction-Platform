package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"auction-platform/internal/auction"
	"auction-platform/internal/monitor"
	"auction-platform/internal/realtime"
)

func Producer(brokers []string) (*kgo.Client, error) {
	return kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordDeliveryTimeout(10*time.Second))
}

func Consumer(brokers []string, topic, group string) (*kgo.Client, error) {
	return ConsumerObserved(brokers, topic, group, nil)
}

func ConsumerObserved(brokers []string, topic, group string, state *monitor.Consumer) (*kgo.Client, error) {
	return kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic), kgo.ConsumerGroup(group), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			state.ChangeAssignment(assigned[topic], true)
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
			state.ChangeAssignment(revoked[topic], false)
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
			state.ChangeAssignment(lost[topic], false)
		}))
}

func CloseConsumer(c *kgo.Client) {
	c.AllowRebalance()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.LeaveGroupContext(ctx); err != nil {
		slog.Warn("Kafka group leave timed out or failed; closing client", "error", err)
	}
	c.CloseAllowingRebalance()
}

func Handle(h *realtime.Hub, key, value []byte) error {
	if len(value) > MaxEventBytes {
		return fmt.Errorf("event exceeds %d bytes", MaxEventBytes)
	}
	var e auction.Event
	if err := json.Unmarshal(value, &e); err != nil {
		return err
	}
	if err := validateEvent(e, key); err != nil {
		return err
	}
	h.Publish(e)
	return nil
}

func RunConsumer(ctx context.Context, c *kgo.Client, h *realtime.Hub) error {
	return RunConsumerObserved(ctx, c, h, nil)
}

func RunConsumerObserved(ctx context.Context, c *kgo.Client, h *realtime.Hub, state *monitor.Consumer) error {
	// A canceled poll can still hold the rebalance gate.
	defer c.AllowRebalance()
	state.Session()
	ctx, cancelSession := context.WithCancel(ctx)
	defer cancelSession()
	if state != nil {
		done := make(chan struct{})
		topics := c.GetConsumeTopics()
		if len(topics) != 1 {
			return errors.New("progress monitoring requires exactly one topic")
		}
		go func() { defer close(done); watchProgress(ctx, c, topics[0], state) }()
		defer func() { cancelSession(); <-done }()
	}
	if err := probe(ctx, c); err != nil {
		return err
	}
	state.BrokerSuccess()
	for ctx.Err() == nil {
		pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := c.PollRecords(pollCtx, 100)
		timedOut := errors.Is(pollCtx.Err(), context.DeadlineExceeded)
		cancel()
		if fetches.IsClientClosed() {
			return errors.New("kafka client closed")
		}
		if ctx.Err() != nil {
			return nil
		}
		if timedOut && len(fetches.Records()) == 0 {
			c.AllowRebalance()
			if err := probe(ctx, c); err != nil {
				return err
			}
			state.BrokerSuccess()
			continue
		}
		if errs := fetches.Errors(); len(errs) > 0 && !timedOut {
			c.AllowRebalance()
			return fmt.Errorf("kafka fetch: %v", errs)
		}
		records := fetches.Records()
		for _, r := range records {
			if err := Handle(h, r.Key, r.Value); err != nil {
				c.AllowRebalance()
				return &PoisonError{Err: fmt.Errorf("poison event topic=%s partition=%d offset=%d: %w", r.Topic, r.Partition, r.Offset, err)}
			}
		}
		if len(records) > 0 {
			if err := commitBatch(ctx, c.CommitRecords, records, state, 3*time.Second); err != nil {
				c.AllowRebalance()
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
		c.AllowRebalance()
	}
	return nil
}

func commitBatch(ctx context.Context, commit func(context.Context, ...*kgo.Record) error, records []*kgo.Record, state *monitor.Consumer, budget time.Duration) error {
	state.BeginCommit(len(records))
	op, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	err := commit(op, records...)
	timedOut := err != nil && errors.Is(op.Err(), context.DeadlineExceeded)
	state.EndCommit(len(records), timedOut, err == nil)
	if err != nil {
		return fmt.Errorf("commit Kafka offsets: %w", err)
	}
	return nil
}

func probe(ctx context.Context, c *kgo.Client) error {
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := c.Ping(probeCtx); err != nil {
		return fmt.Errorf("kafka probe: %w", err)
	}
	return nil
}
