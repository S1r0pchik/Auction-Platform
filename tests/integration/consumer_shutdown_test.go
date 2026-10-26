package integration

import (
	"context"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"auction-platform/internal/events"
	"auction-platform/internal/realtime"
	"auction-platform/internal/testsupport"
)

type cancelAssignedPoll struct {
	armed  atomic.Bool
	once   sync.Once
	cancel context.CancelFunc
	fired  chan struct{}
}

func (h *cancelAssignedPoll) OnPollStart(context.Context) {
	if h.armed.Load() {
		h.once.Do(func() { h.cancel(); close(h.fired) })
	}
}

// Cancel after joining a real group, precisely as the next PollRecords begins.
// A canceled poll still holds the rebalance gate. Plain Close below deliberately
// verifies that RunConsumer releases it, rather than masking the bug in cleanup.
func TestConsumerShutdownAfterCanceledPoll(t *testing.T) {
	raw := os.Getenv("KAFKA_TEST_BROKERS")
	if raw == "" {
		t.Skip("KAFKA_TEST_BROKERS not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	hook := &cancelAssignedPoll{cancel: cancel, fired: make(chan struct{})}
	topic := testsupport.KafkaTopic(t, strings.Split(raw, ","))
	consumer, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(raw, ",")...), kgo.ConsumeTopics(topic), kgo.ConsumerGroup("shutdown-"+uuid.NewString()), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.WithHooks(hook), kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) { hook.armed.Store(true) }))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { err := events.RunConsumer(ctx, consumer, realtime.NewHub()); consumer.Close(); done <- err }()
	select {
	case <-hook.fired:
	case <-time.After(15 * time.Second):
		consumer.AllowRebalance()
		t.Fatal("consumer never reached cancellation after assignment")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		// Unstick the broken implementation so a failing regression test does not
		// leave a goroutine or Kafka group member behind.
		consumer.AllowRebalance()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
		t.Fatal("consumer Close hung after canceled PollRecords")
	}
}

func TestConsumerShutdownBeforeAssignment(t *testing.T) {
	raw := os.Getenv("KAFKA_TEST_BROKERS")
	if raw == "" {
		t.Skip("KAFKA_TEST_BROKERS not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hook := &cancelAssignedPoll{cancel: cancel, fired: make(chan struct{})}
	hook.armed.Store(true)
	topic := testsupport.KafkaTopic(t, strings.Split(raw, ","))
	consumer, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(raw, ",")...), kgo.ConsumeTopics(topic), kgo.ConsumerGroup("joining-"+uuid.NewString()), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.WithHooks(hook))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		err := events.RunConsumer(ctx, consumer, realtime.NewHub())
		events.CloseConsumer(consumer)
		done <- err
	}()
	select {
	case <-hook.fired:
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not reach first poll")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("consumer close waited for group assignment")
	}
}
