package events

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"auction-platform/internal/monitor"
)

func partitionOffsets(ctx context.Context, c *kgo.Client, topic string, partitions []int32, timestamp int64) (map[int32]int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	t := kmsg.NewListOffsetsRequestTopic()
	t.Topic = topic
	for _, id := range partitions {
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition = id
		p.Timestamp = timestamp
		t.Partitions = append(t.Partitions, p)
	}
	req.Topics = []kmsg.ListOffsetsRequestTopic{t}
	response, err := req.RequestWith(ctx, c)
	if err != nil {
		return nil, err
	}
	result := map[int32]int64{}
	for _, t := range response.Topics {
		if t.Topic != topic {
			continue
		}
		for _, p := range t.Partitions {
			if p.ErrorCode != 0 {
				return nil, fmt.Errorf("ListOffsets partition=%d Kafka error=%d", p.Partition, p.ErrorCode)
			}
			if p.Offset < 0 {
				return nil, fmt.Errorf("unknown offset partition=%d", p.Partition)
			}
			result[p.Partition] = p.Offset
		}
	}
	if len(result) != len(partitions) {
		return nil, fmt.Errorf("incomplete ListOffsets response")
	}
	return result, nil
}

func sampleLag(ctx context.Context, c *kgo.Client, topic string, partitions []int32) (int64, error) {
	end, err := partitionOffsets(ctx, c, topic, partitions, -1)
	if err != nil {
		return 0, err
	}
	committed := c.CommittedOffsets()[topic]
	var missing []int32
	for _, id := range partitions {
		if offset, ok := committed[id]; !ok || offset.Offset < 0 {
			missing = append(missing, id)
		}
	}
	start := map[int32]int64{}
	if len(missing) > 0 {
		start, err = partitionOffsets(ctx, c, topic, missing, -2)
		if err != nil {
			return 0, err
		}
	}
	var lag int64
	for _, id := range partitions {
		offset, ok := committed[id]
		next := offset.Offset
		if !ok || next < 0 {
			next = start[id]
		}
		lag += max(0, end[id]-next)
	}
	return lag, nil
}

func watchProgress(ctx context.Context, c *kgo.Client, topic string, state *monitor.Consumer) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			partitions, generation := state.Assignment()
			if len(partitions) == 0 {
				continue
			}
			op, cancel := context.WithTimeout(ctx, 3*time.Second)
			lag, err := sampleLag(op, c, topic, partitions)
			cancel()
			if ctx.Err() != nil {
				return
			}
			state.LagSample(generation, lag, err)
		}
	}
}
