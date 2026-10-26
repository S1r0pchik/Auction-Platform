// Package testsupport contains helpers used only by tests.
package testsupport

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// KafkaTopic isolates test records from the application and other tests.
func KafkaTopic(t *testing.T, brokers []string) string {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	topic := "test-" + uuid.NewString()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req := kmsg.NewPtrCreateTopicsRequest()
	req.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}
	response, err := req.RequestWith(ctx, client)
	if err != nil {
		client.Close()
		t.Fatal(err)
	}
	for _, r := range response.Topics {
		if r.ErrorCode != 0 {
			client.Close()
			t.Fatal("create test topic", r.ErrorCode)
		}
	}
	t.Cleanup(func() {
		defer client.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := kmsg.NewPtrDeleteTopicsRequest()
		req.TopicNames = []string{topic}
		req.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: &topic}}
		response, err := req.RequestWith(ctx, client)
		if err != nil {
			t.Error(err)
			return
		}
		for _, r := range response.Topics {
			if r.ErrorCode != 0 {
				t.Error("delete test topic", r.ErrorCode)
			}
		}
	})
	return topic
}
