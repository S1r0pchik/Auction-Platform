package monitor

import (
	"sync"
	"time"
)

type ConsumerSnapshot struct {
	State              string     `json:"state"`
	LastError          string     `json:"last_error,omitempty"`
	LastBrokerSuccess  *time.Time `json:"last_broker_success,omitempty"`
	LastCommit         *time.Time `json:"last_commit,omitempty"`
	Lag                *int64     `json:"lag"`
	LagSampleAt        *time.Time `json:"lag_sample_at,omitempty"`
	LagError           string     `json:"lag_error,omitempty"`
	AssignedPartitions int        `json:"assigned_partitions"`
	Phase              string     `json:"phase"`
	InFlight           int        `json:"in_flight"`
	CommitTimeouts     uint64     `json:"commit_timeouts"`
	Failures           uint64     `json:"failures"`
	Restarts           uint64     `json:"restarts"`
	Records            uint64     `json:"records"`
}

type Consumer struct {
	mu            sync.Mutex
	snapshot      ConsumerSnapshot
	partitions    []int32
	commitStarted *time.Time
	backlogSince  *time.Time
	generation    uint64
}

func NewConsumer() *Consumer { return &Consumer{snapshot: ConsumerSnapshot{State: "starting"}} }

func (c *Consumer) Snapshot() ConsumerSnapshot {
	if c == nil {
		return ConsumerSnapshot{State: "disabled"}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshotAt(time.Now().UTC())
}

const StaleAfter = 15 * time.Second

func (c *Consumer) snapshotAt(now time.Time) ConsumerSnapshot {
	s := c.snapshot
	if s.State != "active" {
		return s
	}
	s.State = "running"
	switch {
	case c.commitStarted != nil && now.Sub(*c.commitStarted) > 3*time.Second:
		s.State = "stalled"
	case s.AssignedPartitions == 0:
		s.State = "joining"
	case s.Lag == nil || s.LagSampleAt == nil || now.Sub(*s.LagSampleAt) > StaleAfter:
		s.State = "unknown"
	case *s.Lag > 0:
		s.State = "catching_up"
		since := c.backlogSince
		if s.LastCommit != nil && (since == nil || s.LastCommit.After(*since)) {
			since = s.LastCommit
		}
		if since != nil && now.Sub(*since) > StaleAfter {
			s.State = "stalled"
		}
	}
	return s
}

func (c *Consumer) Session() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot.State = "active"
	c.snapshot.Phase = "polling"
	c.snapshot.AssignedPartitions = 0
	c.snapshot.Lag = nil
	c.snapshot.LagSampleAt = nil
	c.snapshot.LagError = ""
	c.snapshot.InFlight = 0
	c.partitions = nil
	c.generation++
	c.commitStarted = nil
	c.backlogSince = nil
}

func (c *Consumer) Assigned(partitions []int32) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.partitions = append([]int32(nil), partitions...)
	c.generation++
	c.snapshot.AssignedPartitions = len(partitions)
	c.snapshot.Lag = nil
	c.snapshot.LagSampleAt = nil
	c.backlogSince = nil
}

func (c *Consumer) Assignment() ([]int32, uint64) {
	if c == nil {
		return nil, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int32(nil), c.partitions...), c.generation
}

func (c *Consumer) ChangeAssignment(partitions []int32, add bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	set := map[int32]bool{}
	for _, id := range c.partitions {
		set[id] = true
	}
	for _, id := range partitions {
		if add {
			set[id] = true
		} else {
			delete(set, id)
		}
	}
	c.partitions = nil
	for id := range set {
		c.partitions = append(c.partitions, id)
	}
	c.snapshot.AssignedPartitions = len(c.partitions)
	c.generation++
	c.snapshot.Lag = nil
	c.snapshot.LagSampleAt = nil
	c.backlogSince = nil
}

func (c *Consumer) BrokerSuccess() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().UTC()
	c.snapshot.LastBrokerSuccess = &now
}

func (c *Consumer) BeginCommit(records int) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().UTC()
	c.commitStarted = &now
	c.snapshot.Phase = "committing"
	c.snapshot.InFlight = records
}

func (c *Consumer) EndCommit(records int, timedOut bool, success bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commitStarted = nil
	c.snapshot.Phase = "polling"
	c.snapshot.InFlight = 0
	if timedOut {
		c.snapshot.CommitTimeouts++
	}
	if success {
		now := time.Now().UTC()
		c.snapshot.LastCommit = &now
		c.snapshot.Records += uint64(records)
	}
}

func (c *Consumer) ObserveLag(lag int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.observeLag(lag)
}

func (c *Consumer) observeLag(lag int64) {
	now := time.Now().UTC()
	c.snapshot.Lag = &lag
	c.snapshot.LagSampleAt = &now
	c.snapshot.LagError = ""
	if lag == 0 {
		c.backlogSince = nil
	} else if c.backlogSince == nil {
		c.backlogSince = &now
	}
}

func (c *Consumer) LagSample(generation uint64, lag int64, err error) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		return
	}
	if err != nil {
		c.snapshot.Lag = nil
		c.snapshot.LagError = err.Error()
		return
	}
	c.observeLag(lag)
}

func (c *Consumer) Failed(err error, blocked bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot.State = "retrying"
	if blocked {
		c.snapshot.State = "blocked"
	}
	c.snapshot.LastError = err.Error()
	c.snapshot.Failures++
}

func (c *Consumer) Restart() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot.Restarts++
}

func (c *Consumer) Stopped() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot.State = "stopped"
}
