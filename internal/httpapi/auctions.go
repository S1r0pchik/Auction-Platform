package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"auction-platform/internal/auction"
)

func (a *API) create(w http.ResponseWriter, r *http.Request) {
	r, cancel := timeout(r)
	defer cancel()
	user, ok := a.user(w, r)
	if !ok {
		return
	}
	var in auction.CreateInput
	if !decode(w, r, &in) {
		return
	}
	v, err := a.Auctions.Create(r.Context(), user, in)
	if err != nil {
		fail(w, r, err)
		return
	}
	w.Header().Set("Location", "/api/v1/auctions/"+v.ID.String())
	respond(w, 201, v)
}

func (a *API) get(w http.ResponseWriter, r *http.Request) {
	r, cancel := timeout(r)
	defer cancel()
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	v, err := a.Auctions.Get(r.Context(), id)
	if err != nil {
		fail(w, r, err)
		return
	}
	respond(w, 200, v)
}

func (a *API) bid(w http.ResponseWriter, r *http.Request) {
	r, cancel := timeout(r)
	defer cancel()
	user, ok := a.user(w, r)
	if !ok {
		return
	}
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var in struct {
		Amount int64 `json:"amount"`
	}
	if !decode(w, r, &in) {
		return
	}
	v, replay, err := a.Auctions.PlaceBid(r.Context(), id, user, in.Amount, r.Header.Get("Idempotency-Key"))
	if err != nil {
		fail(w, r, err)
		return
	}
	status := 201
	if replay {
		status = 200
		w.Header().Set("Idempotency-Replayed", "true")
	}
	respond(w, status, v)
}

func limit(w http.ResponseWriter, r *http.Request) (int, bool) {
	n := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		var err error
		n, err = strconv.Atoi(v)
		if err != nil || n < 1 || n > 100 {
			writeProblem(w, r, 400, "invalid_limit", map[string]string{"limit": "must be an integer between 1 and 100"}, nil)
			return 0, false
		}
	}
	return n, true
}

type cursor struct {
	At time.Time `json:"at"`
	ID uuid.UUID `json:"id"`
}

func (a *API) list(w http.ResponseWriter, r *http.Request) {
	r, cancel := timeout(r)
	defer cancel()
	n, ok := limit(w, r)
	if !ok {
		return
	}
	c := cursor{At: time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), ID: uuid.Nil}
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		c = cursor{}
		data, err := base64.RawURLEncoding.DecodeString(raw)
		if err != nil || json.Unmarshal(data, &c) != nil || c.At.IsZero() || c.ID == uuid.Nil {
			writeProblem(w, r, 400, "invalid_cursor", map[string]string{"cursor": "must be a cursor returned by this API"}, nil)
			return
		}
	}
	items, err := a.Auctions.List(r.Context(), c.At, c.ID, n+1)
	if err != nil {
		fail(w, r, err)
		return
	}
	next := ""
	if len(items) > n {
		items = items[:n]
		last := items[n-1]
		data, _ := json.Marshal(cursor{At: last.CreatedAt, ID: last.ID})
		next = base64.RawURLEncoding.EncodeToString(data)
	}
	respond(w, 200, map[string]any{"items": items, "next_cursor": next})
}

func (a *API) bids(w http.ResponseWriter, r *http.Request) {
	r, cancel := timeout(r)
	defer cancel()
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	n, ok := limit(w, r)
	if !ok {
		return
	}
	var after int64
	var err error
	if raw := r.URL.Query().Get("after_version"); raw != "" {
		after, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || after < 0 {
			writeProblem(w, r, 400, "invalid_cursor", map[string]string{"after_version": "must be a non-negative integer"}, nil)
			return
		}
	}
	items, err := a.Auctions.Bids(r.Context(), id, after, n+1)
	if err != nil {
		fail(w, r, err)
		return
	}
	var next *int64
	if len(items) > n {
		items = items[:n]
		v := items[n-1].AuctionVersion
		next = &v
	}
	respond(w, 200, map[string]any{"items": items, "next_after_version": next})
}
