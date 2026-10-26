package httpapi

import (
	"context"
	"net/http"
	"time"
)

func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := a.Auctions.DB.Ping(ctx); err != nil {
		writeError(w, r, 503, "database_unavailable")
		return
	}
	respond(w, 200, map[string]string{"status": "ready"})
}

func (a *API) status(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	var pending, overdue, retrying int64
	var age, delay float64
	err := a.Auctions.DB.QueryRow(ctx, `SELECT count(*), COALESCE(EXTRACT(EPOCH
	FROM clock_timestamp()-min(created_at)), 0)::float8, count(DISTINCT auction_id) FILTER (WHERE last_error IS NOT NULL)
	FROM outbox_events
	WHERE published_at IS NULL`).Scan(&pending, &age, &retrying)
	if err == nil {
		err = a.Auctions.DB.QueryRow(ctx, `SELECT count(*), COALESCE(EXTRACT(EPOCH
	FROM clock_timestamp()-min(ends_at)), 0)::float8
	FROM auctions
	WHERE status IN ('active', 'scheduled') AND ends_at<=statement_timestamp()`).Scan(&overdue, &delay)
	}
	consumer := a.Consumer.Snapshot()
	status := "ok"
	if consumer.State != "running" && consumer.State != "disabled" || pending > 0 || overdue > 0 {
		status = "degraded"
	}
	dbStatus := "up"
	code := 200
	if err != nil {
		dbStatus = "unavailable"
		status = "degraded"
		code = 503
	}
	pool := a.Auctions.DB.Stat()
	data := map[string]any{
		"status":   status,
		"database": dbStatus,
		"consumer": consumer,
		"bids":     a.BidGate.snapshot(),
		"db_pool": map[string]any{
			"acquired":                pool.AcquiredConns(),
			"max":                     pool.MaxConns(),
			"total":                   pool.TotalConns(),
			"canceled_acquires_total": pool.CanceledAcquireCount(),
		},
	}
	data["websocket"] = a.Hub.Stats()
	if err == nil {
		data["outbox"] = map[string]any{"pending": pending, "oldest_age_seconds": max(0, age), "retrying_auctions": retrying}
		data["lifecycle"] = map[string]any{"overdue": overdue, "max_delay_seconds": max(0, delay)}
	}
	respond(w, code, data)
}
