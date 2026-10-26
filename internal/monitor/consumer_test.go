package monitor

import (
	"errors"
	"testing"
	"time"
)

func TestBrokerPingDoesNotMeanProgress(t *testing.T) {
	c := NewConsumer()
	c.Session()
	c.BrokerSuccess()
	if s := c.Snapshot(); s.State != "joining" || s.LastCommit != nil {
		t.Fatal(s)
	}
	c.Assigned([]int32{0})
	c.ObserveLag(5)
	before := c.snapshotAt(time.Now().UTC())
	if before.State != "catching_up" {
		t.Fatal(before)
	}
	c.BrokerSuccess()
	stalled := c.snapshotAt(time.Now().UTC().Add(StaleAfter + time.Second))
	// Keep the lag sample fresh while commits do not advance.
	sampleTime := time.Now().UTC().Add(StaleAfter + time.Second)
	c.snapshot.LagSampleAt = &sampleTime
	stalled = c.snapshotAt(sampleTime)
	if stalled.State != "stalled" || stalled.LastCommit != nil {
		t.Fatal(stalled)
	}
	c.BeginCommit(5)
	c.EndCommit(5, false, true)
	c.ObserveLag(0)
	if s := c.Snapshot(); s.State != "running" || s.Records != 5 || s.LastCommit == nil {
		t.Fatal(s)
	}
}

func TestIdleConsumerAndUnknownLag(t *testing.T) {
	c := NewConsumer()
	c.Session()
	c.Assigned([]int32{0})
	c.ObserveLag(0)
	// No traffic is not a stalled consumer, even without a prior commit.
	future := time.Now().UTC().Add(time.Hour)
	c.snapshot.LagSampleAt = &future
	if s := c.snapshotAt(future); s.State != "running" || s.LastCommit != nil {
		t.Fatal(s)
	}
	if s := c.snapshotAt(future.Add(StaleAfter + time.Second)); s.State != "unknown" {
		t.Fatal(s)
	}
	_, generation := c.Assignment()
	c.LagSample(generation, 0, errors.New("broker unavailable"))
	if s := c.Snapshot(); s.Lag != nil || s.LagError == "" || s.State != "unknown" {
		t.Fatal(s)
	}
}

func TestLagSampleDoesNotSurviveRebalance(t *testing.T) {
	c := NewConsumer()
	c.Session()
	c.ChangeAssignment([]int32{0}, true)
	c.ChangeAssignment([]int32{1}, true)
	if c.Snapshot().AssignedPartitions != 2 {
		t.Fatal("incremental assignment lost partition")
	}
	_, old := c.Assignment()
	c.ChangeAssignment([]int32{0}, false)
	c.LagSample(old, 0, nil)
	if s := c.Snapshot(); s.Lag != nil || s.AssignedPartitions != 1 {
		t.Fatal("stale sample accepted", s)
	}
}
