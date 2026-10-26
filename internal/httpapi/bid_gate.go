package httpapi

import (
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type BidGate struct {
	mu                        sync.Mutex
	active, total, perAuction int
	byAuction                 map[uuid.UUID]int
	rejected                  uint64
}

func NewBidGate(total, perAuction int) *BidGate {
	if total < 1 || perAuction < 1 {
		panic("positive bid limits required")
	}
	return &BidGate{total: total, perAuction: perAuction, byAuction: map[uuid.UUID]int{}}
}

func (g *BidGate) acquire(id uuid.UUID) (func(), bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active >= g.total || g.byAuction[id] >= g.perAuction {
		g.rejected++
		return nil, false
	}
	g.active++
	g.byAuction[id]++
	return func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.active--
		g.byAuction[id]--
		if g.byAuction[id] == 0 {
			delete(g.byAuction, id)
		}
	}, true
}

func (g *BidGate) snapshot() map[string]any {
	g.mu.Lock()
	defer g.mu.Unlock()
	return map[string]any{"in_flight": g.active, "limit": g.total, "per_auction_limit": g.perAuction, "rejected_total": g.rejected}
}

func (g *BidGate) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			writeProblem(w, r, 400, "invalid_id", map[string]string{"id": "must be a UUID"}, nil)
			return
		}
		release, ok := g.acquire(id)
		if !ok {
			w.Header().Set("Retry-After", "1")
			writeError(w, r, 429, "bid_capacity_exceeded")
			return
		}
		defer release()
		next.ServeHTTP(w, r)
	})
}
