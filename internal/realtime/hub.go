package realtime

import (
	"errors"
	"sync"

	"github.com/google/uuid"

	"auction-platform/internal/auction"
)

type Subscription struct {
	Events  chan auction.Event
	Done    chan struct{}
	version int64
	closed  bool
}

type Hub struct {
	mu                                         sync.Mutex
	subscribers                                map[uuid.UUID]map[*Subscription]struct{}
	counts                                     map[uuid.UUID]int
	connections, maxConnections, maxPerAuction int
	rejected                                   uint64
}

var ErrCapacity = errors.New("websocket_capacity_exceeded")

type Stats struct {
	Connections    int    `json:"connections"`
	MaxConnections int    `json:"max_connections"`
	MaxPerAuction  int    `json:"max_per_auction"`
	Rejected       uint64 `json:"rejected_total"`
}

func NewHub() *Hub { return NewHubWithLimits(1000, 100) }

func NewHubWithLimits(total, perAuction int) *Hub {
	if total < 1 || perAuction < 1 {
		panic("positive WebSocket limits required")
	}
	return &Hub{subscribers: make(map[uuid.UUID]map[*Subscription]struct{}), counts: map[uuid.UUID]int{}, maxConnections: total, maxPerAuction: perAuction}
}

func (h *Hub) Stats() Stats {
	h.mu.Lock()
	defer h.mu.Unlock()
	return Stats{Connections: h.connections, MaxConnections: h.maxConnections, MaxPerAuction: h.maxPerAuction, Rejected: h.rejected}
}

func (h *Hub) Subscribe(id uuid.UUID) (*Subscription, func(), error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.connections >= h.maxConnections || h.counts[id] >= h.maxPerAuction {
		h.rejected++
		return nil, nil, ErrCapacity
	}
	h.connections++
	h.counts[id]++
	s := &Subscription{Events: make(chan auction.Event, 32), Done: make(chan struct{})}
	if h.subscribers[id] == nil {
		h.subscribers[id] = make(map[*Subscription]struct{})
	}
	h.subscribers[id][s] = struct{}{}
	var once sync.Once
	return s, func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			delete(h.subscribers[id], s)
			if !s.closed {
				close(s.Done)
				s.closed = true
			}
			h.connections--
			h.counts[id]--
			if h.counts[id] == 0 {
				delete(h.counts, id)
			}
			if len(h.subscribers[id]) == 0 {
				delete(h.subscribers, id)
			}
		})
	}, nil
}

func (h *Hub) Publish(e auction.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for s := range h.subscribers[e.AuctionID] {
		if e.Version <= s.version {
			continue
		}
		select {
		case s.Events <- e:
			s.version = e.Version
		default:
			close(s.Done)
			s.closed = true
			delete(h.subscribers[e.AuctionID], s)
		}
	}
	if len(h.subscribers[e.AuctionID]) == 0 {
		delete(h.subscribers, e.AuctionID)
	}
}
