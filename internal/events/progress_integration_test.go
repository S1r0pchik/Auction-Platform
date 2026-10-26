package events

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"auction-platform/internal/monitor"
	"auction-platform/internal/realtime"
	"auction-platform/internal/testsupport"
)

func TestRealKafkaLagDistinguishesPingFromCommit(t *testing.T) {
	raw := os.Getenv("KAFKA_TEST_BROKERS")
	if raw == "" {
		t.Skip("KAFKA_TEST_BROKERS not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	brokers := strings.Split(raw, ",")
	producer, err := Producer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	topic := "progress-" + uuid.NewString()
	create := kmsg.NewPtrCreateTopicsRequest()
	create.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}
	created, err := create.RequestWith(ctx, producer)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range created.Topics {
		if r.ErrorCode != 0 {
			t.Fatal("create topic error", r.ErrorCode)
		}
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := kmsg.NewPtrDeleteTopicsRequest()
		req.TopicNames = []string{topic}
		req.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: &topic}}
		if _, err := req.RequestWith(cleanup, producer); err != nil {
			t.Error(err)
		}
	}()
	if err = producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("key"), Value: []byte("record")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	state := monitor.NewConsumer()
	state.Session()
	consumer, err := ConsumerObserved(brokers, topic, "progress-"+uuid.NewString(), state)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseConsumer(consumer)
	var records []*kgo.Record
	for len(records) == 0 && ctx.Err() == nil {
		fetched := consumer.PollRecords(ctx, 10)
		if errs := fetched.Errors(); len(errs) > 0 {
			consumer.AllowRebalance()
			t.Fatal(errs)
		}
		records = append(records, fetched.Records()...)
		consumer.AllowRebalance()
	}
	if len(records) != 1 {
		t.Fatal("missing test record")
	}
	state.BrokerSuccess()
	partitions, generation := state.Assignment()
	lag, err := sampleLag(ctx, consumer, topic, partitions)
	if err != nil {
		t.Fatal(err)
	}
	state.LagSample(generation, lag, nil)
	if snapshot := state.Snapshot(); lag != 1 || snapshot.State != "catching_up" || snapshot.LastCommit != nil {
		t.Fatalf("Ping incorrectly reported progress: %+v lag=%d", snapshot, lag)
	}
	if err = commitBatch(ctx, consumer.CommitRecords, records, state, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	lag, err = sampleLag(ctx, consumer, topic, partitions)
	if err != nil {
		t.Fatal(err)
	}
	state.LagSample(generation, lag, nil)
	if snapshot := state.Snapshot(); lag != 0 || snapshot.State != "running" || snapshot.LastCommit == nil || snapshot.Records != 1 {
		t.Fatalf("commit not observed: %+v lag=%d", snapshot, lag)
	}
}

func TestObservedConsumerStartsProgressSampler(t *testing.T) {
	raw := os.Getenv("KAFKA_TEST_BROKERS")
	if raw == "" {
		t.Skip("KAFKA_TEST_BROKERS not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	state := monitor.NewConsumer()
	topic := testsupport.KafkaTopic(t, strings.Split(raw, ","))
	consumer, err := ConsumerObserved(strings.Split(raw, ","), topic, "sampler-"+uuid.NewString(), state)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- RunConsumerObserved(ctx, consumer, realtime.NewHub(), state) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("observed consumer failed to stop")
		}
		CloseConsumer(consumer)
	}()
	for {
		snapshot := state.Snapshot()
		if snapshot.State == "running" {
			if snapshot.AssignedPartitions < 1 || snapshot.Lag == nil || *snapshot.Lag != 0 || snapshot.LagSampleAt == nil {
				t.Fatal(snapshot)
			}
			return
		}
		select {
		case err := <-done:
			done <- err
			t.Fatalf("observed consumer exited: %v", err)
		case <-ctx.Done():
			t.Fatal("sampler did not report running", snapshot)
		case <-time.After(20 * time.Millisecond):
		}
	}
}
