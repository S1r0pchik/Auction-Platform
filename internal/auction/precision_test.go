package auction

import (
	"errors"
	"testing"
	"time"
)

func TestCreateRejectsDatesThatCollapseInPostgres(t *testing.T) {
	start := time.Date(2030, 1, 1, 12, 0, 0, 100, time.UTC)
	in := CreateInput{Title: "Test", MinIncrement: 1, StartsAt: start, EndsAt: start.Add(100 * time.Nanosecond)}
	if err := ValidateCreate(in); !errors.Is(err, ErrInvalid) {
		t.Fatalf("got %v, want invalid dates at PostgreSQL precision", err)
	}
	in.EndsAt = start.Add(time.Microsecond)
	if err := ValidateCreate(in); err != nil {
		t.Fatalf("valid microsecond interval: %v", err)
	}
}
