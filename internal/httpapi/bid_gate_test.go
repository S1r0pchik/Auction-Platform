package httpapi

import (
	"testing"

	"github.com/google/uuid"
)

func TestBidGateIsolatesHotAuction(t *testing.T) {
	gate := NewBidGate(3, 2)
	hot, cold := uuid.New(), uuid.New()
	first, ok := gate.acquire(hot)
	if !ok {
		t.Fatal("first rejected")
	}
	second, ok := gate.acquire(hot)
	if !ok {
		t.Fatal("second rejected")
	}
	if _, ok = gate.acquire(hot); ok {
		t.Fatal("hot limit ignored")
	}
	third, ok := gate.acquire(cold)
	if !ok {
		t.Fatal("hot auction starved another")
	}
	if _, ok = gate.acquire(uuid.New()); ok {
		t.Fatal("global limit ignored")
	}
	first()
	second()
	third()
	if len(gate.byAuction) != 0 || gate.active != 0 {
		t.Fatal("admission state leaked")
	}
}
